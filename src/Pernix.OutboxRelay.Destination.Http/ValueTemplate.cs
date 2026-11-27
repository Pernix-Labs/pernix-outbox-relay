using System.Buffers;
using System.Buffers.Text;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Text;
using System.Text.Unicode;
using Pernix.OutboxRelay.Core.Abstractions;

namespace Pernix.OutboxRelay.Destination.Http;

internal enum TemplateTarget
{
    Url,
    HeaderValue,
}

// Fixed placeholder set only (NG1/NG5). {outbox}, {destination} and {route} fold into literals at compile.
internal sealed class ValueTemplate
{
    private const string HeaderPrefix = "header:";

    // Per segment: rendered length, header index.
    private const int ScratchStride = 2;

    private const int StackScratchBytes = 256;

    private static readonly SearchValues<byte> Unreserved =
        SearchValues.Create("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"u8);

    private static readonly SearchValues<byte> ControlBytes = SearchValues.Create("\r\n\0"u8);

    private static readonly SearchValues<char> ControlChars = SearchValues.Create("\r\n\0");

    private readonly Segment[] segments;

    private ValueTemplate(string text, TemplateTarget target, Segment[] segments)
    {
        Text = text;
        Target = target;
        this.segments = segments;
        if (segments.Length == 0)
        {
            Constant = string.Empty;
        }
        else if (segments.Length == 1 && segments[0].Kind == Kind.Literal)
        {
            Constant = segments[0].Literal;
        }
    }

    private enum Kind : byte
    {
        Literal,
        Id,
        Key,
        KeyHex,
        KeyBase64Url,
        Header,
    }

    public string Text { get; }

    public TemplateTarget Target { get; }

    [MemberNotNullWhen(true, nameof(Constant))]
    public bool IsConstant => Constant is not null;

    public string? Constant { get; }

    // route is null on a single-Url destination. The caller prefixes the error with the configuration path.
    public static bool TryCompile(
        string text,
        TemplateTarget target,
        string outbox,
        string destination,
        string? route,
        [NotNullWhen(true)] out ValueTemplate? template,
        [NotNullWhen(false)] out string? error)
    {
        ArgumentNullException.ThrowIfNull(text);
        ArgumentNullException.ThrowIfNull(outbox);
        ArgumentNullException.ThrowIfNull(destination);
        template = null;
        var list = new List<Segment>();
        var literal = new StringBuilder();
        var position = 0;
        while (position < text.Length)
        {
            var open = text.IndexOf('{', position);
            if (open < 0)
            {
                literal.Append(text, position, text.Length - position);
                break;
            }

            literal.Append(text, position, open - position);
            var close = text.IndexOf('}', open + 1);
            if (close < 0)
            {
                error = "unterminated placeholder; expected '}'";
                return false;
            }

            var token = text[open..(close + 1)];
            var name = text[(open + 1)..close];
            position = close + 1;
            switch (name)
            {
                case "outbox":
                    literal.Append(Fold(outbox, target));
                    continue;
                case "destination":
                    literal.Append(Fold(destination, target));
                    continue;
                case "route" when route is null:
                    error = "placeholder '{route}' is allowed only on a Routes entry";
                    return false;
                case "route":
                    literal.Append(Fold(route, target));
                    continue;
            }

            Segment segment;
            if (name == "id")
            {
                segment = new Segment(Kind.Id, token, missing: string.Empty);
            }
            else if (name == "key")
            {
                segment = new Segment(Kind.Key, token, "needs a non-null UTF-8 key");
            }
            else if (name == "key:hex")
            {
                segment = new Segment(Kind.KeyHex, token, "needs a non-null key");
            }
            else if (name == "key:base64url")
            {
                segment = new Segment(Kind.KeyBase64Url, token, "needs a non-null key");
            }
            else if (name.StartsWith(HeaderPrefix, StringComparison.Ordinal) && name.Length > HeaderPrefix.Length && name.IndexOf('{') < 0)
            {
                segment = new Segment(Kind.Header, token, "names a header the row does not have") { HeaderName = name[HeaderPrefix.Length..] };
            }
            else
            {
                error = $"unknown placeholder '{token}'; expected {{outbox}}, {{destination}}, {{id}}, {{route}}, {{key}}, {{key:hex}}, {{key:base64url}} or {{header:<name>}}";
                return false;
            }

            FlushLiteral(list, literal);
            list.Add(segment);
        }

        FlushLiteral(list, literal);
        if (target == TemplateTarget.HeaderValue)
        {
            foreach (var segment in list)
            {
                if (segment.Kind == Kind.Literal && segment.Literal.AsSpan().ContainsAny(ControlChars))
                {
                    error = "value contains CR, LF or NUL";
                    return false;
                }
            }
        }

        template = new ValueTemplate(text, target, [.. list]);
        error = null;
        return true;
    }

    public bool TryRenderUrl(in RenderContext row, ref char[] buffer, [NotNullWhen(true)] out string? value, [NotNullWhen(false)] out DeliveryResult? fault)
    {
        if (Target != TemplateTarget.Url)
        {
            throw new InvalidOperationException("template target is not Url");
        }

        if (Constant is not null)
        {
            value = Constant;
            fault = null;
            return true;
        }

        var scratchLength = segments.Length * ScratchStride;
        int[]? rented = null;
        var scratch = scratchLength * sizeof(int) <= StackScratchBytes
            ? stackalloc int[scratchLength]
            : (rented = ArrayPool<int>.Shared.Rent(scratchLength)).AsSpan(0, scratchLength);
        try
        {
            if (!TryMeasure(row, scratch, out var length, out fault))
            {
                value = null;
                return false;
            }

            if (buffer.Length < length)
            {
                buffer = new char[Math.Max(length, buffer.Length * 2)];
            }

            var written = buffer.AsSpan(0, length);
            Write(row, scratch, written);
            fault = FindDotSegment(written, scratch);
            if (fault is not null)
            {
                value = null;
                return false;
            }

            value = new string(written);
            return true;
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<int>.Shared.Return(rented);
            }
        }
    }

    public bool TryRenderHeaderValue(in RenderContext row, [NotNullWhen(true)] out string? value, [NotNullWhen(false)] out DeliveryResult? fault)
    {
        if (Target != TemplateTarget.HeaderValue)
        {
            throw new InvalidOperationException("template target is not HeaderValue");
        }

        if (Constant is not null)
        {
            value = Constant;
            fault = null;
            return true;
        }

        var scratchLength = segments.Length * ScratchStride;
        int[]? rented = null;
        var scratch = scratchLength * sizeof(int) <= StackScratchBytes
            ? stackalloc int[scratchLength]
            : (rented = ArrayPool<int>.Shared.Rent(scratchLength)).AsSpan(0, scratchLength);
        try
        {
            if (!TryMeasure(row, scratch, out var length, out fault))
            {
                value = null;
                return false;
            }

            value = string.Create(length, new WriteState(this, row, scratch), static (span, state) => state.Template.Write(state.Row, state.Scratch, span));
            return true;
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<int>.Shared.Return(rented);
            }
        }
    }

    private static string Fold(string value, TemplateTarget target) =>
        target == TemplateTarget.Url ? PercentEncode(value) : value;

    private static string PercentEncode(string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        return string.Create(EncodedLength(bytes), bytes, static (span, bytes) => WritePercentEncoded(bytes, span));
    }

    private static void FlushLiteral(List<Segment> list, StringBuilder literal)
    {
        if (literal.Length > 0)
        {
            list.Add(new Segment(literal.ToString()));
            literal.Clear();
        }
    }

    private static DeliveryResult.MessageFault Fault(string error) => new(error, Permanent: true);

    private static int EncodedLength(ReadOnlySpan<byte> bytes)
    {
        var length = bytes.Length;
        var index = bytes.IndexOfAnyExcept(Unreserved);
        while (index >= 0)
        {
            length += 2;
            bytes = bytes[(index + 1)..];
            index = bytes.IndexOfAnyExcept(Unreserved);
        }

        return length;
    }

    private static void WritePercentEncoded(ReadOnlySpan<byte> bytes, Span<char> destination)
    {
        var at = 0;
        foreach (var b in bytes)
        {
            if (Unreserved.Contains(b))
            {
                destination[at++] = (char)b;
            }
            else
            {
                destination[at++] = '%';
                destination[at++] = HexUpper(b >> 4);
                destination[at++] = HexUpper(b & 0xF);
            }
        }
    }

    private static char HexUpper(int nibble) => (char)(nibble < 10 ? '0' + nibble : 'A' + nibble - 10);

    private static int DigitCount(long id)
    {
        Span<char> digits = stackalloc char[20];
        id.TryFormat(digits, out var written, default, CultureInfo.InvariantCulture);
        return written;
    }

    private static int FindHeader(MessageHeaders headers, string name)
    {
        for (var i = 0; i < headers.Count; i++)
        {
            if (string.Equals(headers[i].Name, name, StringComparison.Ordinal))
            {
                return i;
            }
        }

        return -1;
    }

    private bool TryMeasure(in RenderContext row, Span<int> scratch, out int length, [NotNullWhen(false)] out DeliveryResult? fault)
    {
        var url = Target == TemplateTarget.Url;
        length = 0;
        for (var i = 0; i < segments.Length; i++)
        {
            var segment = segments[i];
            var headerIndex = -1;
            int segmentLength;
            switch (segment.Kind)
            {
                case Kind.Literal:
                    segmentLength = segment.Literal!.Length;
                    break;
                case Kind.Id:
                    segmentLength = DigitCount(row.Id);
                    break;
                case Kind.Key:
                    if (row.Key is null || !Utf8.IsValid(row.Key))
                    {
                        fault = segment.Missing;
                        return false;
                    }

                    if (!TryMeasureText(row.Key, url, segment, out segmentLength, out fault))
                    {
                        return false;
                    }

                    break;
                case Kind.KeyHex:
                    if (row.Key is null)
                    {
                        fault = segment.Missing;
                        return false;
                    }

                    segmentLength = row.Key.Length * 2;
                    break;
                case Kind.KeyBase64Url:
                    if (row.Key is null)
                    {
                        fault = segment.Missing;
                        return false;
                    }

                    segmentLength = Base64Url.GetEncodedLength(row.Key.Length);
                    break;
                default:
                    headerIndex = FindHeader(row.Headers, segment.HeaderName!);
                    if (headerIndex < 0)
                    {
                        fault = segment.Missing;
                        return false;
                    }

                    if (!TryMeasureText(row.Headers[headerIndex].Value, url, segment, out segmentLength, out fault))
                    {
                        return false;
                    }

                    break;
            }

            scratch[i * ScratchStride] = segmentLength;
            scratch[(i * ScratchStride) + 1] = headerIndex;
            length += segmentLength;
        }

        fault = null;
        return true;
    }

    private static bool TryMeasureText(byte[] bytes, bool url, Segment segment, out int length, [NotNullWhen(false)] out DeliveryResult? fault)
    {
        if (url)
        {
            length = EncodedLength(bytes);
        }
        else if (bytes.AsSpan().ContainsAny(ControlBytes))
        {
            length = 0;
            fault = segment.Control;
            return false;
        }
        else
        {
            length = Encoding.UTF8.GetCharCount(bytes);
        }

        fault = null;
        return true;
    }

    private void Write(in RenderContext row, ReadOnlySpan<int> scratch, Span<char> destination)
    {
        var url = Target == TemplateTarget.Url;
        var at = 0;
        for (var i = 0; i < segments.Length; i++)
        {
            var segment = segments[i];
            var target = destination.Slice(at, scratch[i * ScratchStride]);
            switch (segment.Kind)
            {
                case Kind.Literal:
                    segment.Literal.AsSpan().CopyTo(target);
                    break;
                case Kind.Id:
                    row.Id.TryFormat(target, out _, default, CultureInfo.InvariantCulture);
                    break;
                case Kind.Key:
                    WriteText(row.Key!, url, target);
                    break;
                case Kind.KeyHex:
                    Convert.TryToHexStringLower(row.Key, target, out _);
                    break;
                case Kind.KeyBase64Url:
                    Base64Url.EncodeToChars(row.Key, target);
                    break;
                default:
                    WriteText(row.Headers[scratch[(i * ScratchStride) + 1]].Value, url, target);
                    break;
            }

            at += target.Length;
        }
    }

    private static void WriteText(byte[] bytes, bool url, Span<char> destination)
    {
        if (url)
        {
            WritePercentEncoded(bytes, destination);
        }
        else
        {
            Encoding.UTF8.GetChars(bytes, destination);
        }
    }

    // K3. Values percent-encode '/' and '?', so segment bounds and the path end come from literals only.
    private DeliveryResult? FindDotSegment(ReadOnlySpan<char> url, ReadOnlySpan<int> scratch)
    {
        var pathEnd = url.IndexOf('?');
        if (pathEnd < 0)
        {
            pathEnd = url.Length;
        }

        var start = 0;
        for (var i = 0; i <= pathEnd; i++)
        {
            if (i < pathEnd && url[i] != '/')
            {
                continue;
            }

            var length = i - start;
            if (length is 1 or 2 && url.Slice(start, length).IndexOfAnyExcept('.') < 0)
            {
                var fault = PerRowSegmentWithin(start, i, scratch);
                if (fault is not null)
                {
                    return fault;
                }
            }

            start = i + 1;
        }

        return null;
    }

    private DeliveryResult? PerRowSegmentWithin(int start, int end, ReadOnlySpan<int> scratch)
    {
        var at = 0;
        for (var i = 0; i < segments.Length && at < end; i++)
        {
            var length = scratch[i * ScratchStride];
            if (segments[i].Kind != Kind.Literal && length > 0 && at < end && at + length > start)
            {
                return segments[i].DotSegment;
            }

            at += length;
        }

        return null;
    }

    private readonly ref struct WriteState(ValueTemplate template, RenderContext row, ReadOnlySpan<int> scratch)
    {
        public ValueTemplate Template { get; } = template;

        public RenderContext Row { get; } = row;

        public ReadOnlySpan<int> Scratch { get; } = scratch;
    }

    private sealed class Segment
    {
        private static readonly DeliveryResult None = Fault("literal");

        public Segment(string literal)
        {
            Kind = Kind.Literal;
            Literal = literal;
            Missing = Control = DotSegment = None;
        }

        public Segment(Kind kind, string token, string missing)
        {
            Kind = kind;
            Missing = Fault($"placeholder '{token}' {missing}");
            Control = Fault($"placeholder '{token}' renders CR, LF or NUL");
            DotSegment = Fault($"placeholder '{token}' renders a dot segment");
        }

        public Kind Kind { get; }

        // Url target: folded values already percent-encoded.
        public string? Literal { get; }

        public string? HeaderName { get; init; }

        public DeliveryResult Missing { get; }

        public DeliveryResult Control { get; }

        public DeliveryResult DotSegment { get; }
    }
}
