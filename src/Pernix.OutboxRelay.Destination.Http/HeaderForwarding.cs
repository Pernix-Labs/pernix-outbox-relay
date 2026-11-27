using System.Buffers;
using System.Collections.Frozen;
using System.Diagnostics.CodeAnalysis;
using System.Net.Http.Headers;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Destination.Http.Options;

namespace Pernix.OutboxRelay.Destination.Http;

internal sealed class HeaderForwarding
{
    private const string ContentType = "content-type";
    private const int PairwiseLimit = 8;

    private static readonly SearchValues<char> TokenChars =
        SearchValues.Create("!#$%&'*+-.^_`|~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz");

    private static readonly SearchValues<byte> ControlBytes = SearchValues.Create("\r\n\0"u8);

    // Listed only. Value is the prefixed name, or null to send the row's own name instance.
    private readonly FrozenDictionary<string, string?>? listed;

    public HeaderForwarding(ForwardHeadersOptions? options)
    {
        Mode = options?.Mode ?? ForwardHeadersMode.All;
        NamePrefix = string.IsNullOrEmpty(options?.NamePrefix) ? null : options.NamePrefix;
        Names = (options?.Names ?? []).ToFrozenSet(StringComparer.OrdinalIgnoreCase);
        Exclude = (options?.Exclude ?? []).ToFrozenSet(StringComparer.OrdinalIgnoreCase);
        if (Mode == ForwardHeadersMode.Listed)
        {
            listed = Names.ToFrozenDictionary(
                static name => name,
                name => NamePrefix is null ? null : string.Concat(NamePrefix, name),
                StringComparer.OrdinalIgnoreCase);
        }
    }

    public static FrozenSet<string> Reserved { get; } = new[]
    {
        "Connection", "Keep-Alive", "Proxy-Connection", "Transfer-Encoding", "TE", "Trailer", "Upgrade",
        "Host", "Content-Length", "Expect", "Authorization", "Proxy-Authorization", "Cookie",
    }.ToFrozenSet(StringComparer.OrdinalIgnoreCase);

    // Names HttpRequestHeaders rejects. Content-Type and Content-Length are handled separately.
    public static FrozenSet<string> ContentHeaders { get; } = new[]
    {
        "Allow", "Content-Disposition", "Content-Encoding", "Content-Language", "Content-Location",
        "Content-MD5", "Content-Range", "Expires", "Last-Modified",
    }.ToFrozenSet(StringComparer.OrdinalIgnoreCase);

    public ForwardHeadersMode Mode { get; }

    public FrozenSet<string> Names { get; }

    public FrozenSet<string> Exclude { get; }

    public string? NamePrefix { get; }

    public PrefixedNameCache? CreateNameCache() =>
        Mode == ForwardHeadersMode.All && NamePrefix is not null ? new PrefixedNameCache(NamePrefix) : null;

    public bool TryForward(
        MessageHeaders row,
        HttpRequestHeaders target,
        HttpContentHeaders content,
        FrozenSet<string> overridden,
        HeaderValueCache values,
        PrefixedNameCache? names,
        ref HashSet<string>? seen,
        out string? contentType,
        [NotNullWhen(false)] out DeliveryResult? fault)
    {
        contentType = null;
        fault = null;
        if (Mode == ForwardHeadersMode.None || row.Count == 0)
        {
            return true;
        }

        var pairwise = row.Count <= PairwiseLimit;
        if (!pairwise)
        {
            seen ??= new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            seen.Clear();
        }

        for (var i = 0; i < row.Count; i++)
        {
            var header = row[i];
            var name = header.Name;
            string? emitted = null;
            if (listed is not null && !listed.TryGetValue(name, out emitted))
            {
                continue;
            }

            if (Exclude.Contains(name) || Reserved.Contains(name))
            {
                continue;
            }

            var isContentType = string.Equals(name, ContentType, StringComparison.OrdinalIgnoreCase);
            if (!isContentType)
            {
                emitted ??= NamePrefix is null ? name : names?.Get(name) ?? string.Concat(NamePrefix, name);

                // A prefix can compose a reserved name, e.g. "Proxy-" + "Authorization".
                if (overridden.Contains(emitted) || Reserved.Contains(emitted))
                {
                    continue;
                }
            }

            if (name.Length == 0 || name.AsSpan().ContainsAnyExcept(TokenChars))
            {
                fault = Fault($"forwarded header '{name}' is not an RFC 9110 token");
                return false;
            }

            if (header.Value.AsSpan().ContainsAny(ControlBytes))
            {
                fault = Fault($"forwarded header '{name}' value contains CR, LF or NUL");
                return false;
            }

            if (TryFindCaseDuplicate(row, i, pairwise ? null : seen, out var other))
            {
                fault = Fault(string.Equals(other, name, StringComparison.Ordinal)
                    ? $"forwarded header '{name}' appears more than once"
                    : $"forwarded headers '{other}' and '{name}' differ only by case");
                return false;
            }

            var value = values.GetString(name, header.Value);
            if (isContentType)
            {
                contentType = value;
                continue;
            }

            var added = ContentHeaders.Contains(emitted!)
                ? content.TryAddWithoutValidation(emitted!, value)
                : target.TryAddWithoutValidation(emitted!, value);
            if (!added)
            {
                fault = Fault($"forwarded header '{emitted}' cannot be sent");
                return false;
            }
        }

        return true;
    }

    // Every filter is case-insensitive, so a case-equal earlier name was forwarded too.
    private static bool TryFindCaseDuplicate(MessageHeaders row, int index, HashSet<string>? seen, [NotNullWhen(true)] out string? other)
    {
        var name = row[index].Name;
        if (seen is not null)
        {
            if (seen.Add(name))
            {
                other = null;
                return false;
            }

            seen.TryGetValue(name, out other);
            return other is not null;
        }

        for (var j = 0; j < index; j++)
        {
            if (string.Equals(row[j].Name, name, StringComparison.OrdinalIgnoreCase))
            {
                other = row[j].Name;
                return true;
            }
        }

        other = null;
        return false;
    }

    private static DeliveryResult.MessageFault Fault(string error) => new(error, Permanent: true);
}

internal sealed class HeaderPlan
{
    private readonly (string Name, ValueTemplate Value, bool IsContent)[] entries;

    public HeaderPlan(IEnumerable<(string Name, ValueTemplate Value)> entries, IEnumerable<string> relayOwned)
    {
        var merged = new List<(string Name, ValueTemplate Value)>();
        var positions = new Dictionary<string, int>(StringComparer.OrdinalIgnoreCase);
        foreach (var entry in entries)
        {
            if (entry.Value.Target != TemplateTarget.HeaderValue)
            {
                throw new ArgumentException($"header '{entry.Name}' template target is not HeaderValue", nameof(entries));
            }

            if (positions.TryGetValue(entry.Name, out var at))
            {
                merged[at] = entry;
            }
            else
            {
                positions[entry.Name] = merged.Count;
                merged.Add(entry);
            }
        }

        this.entries = [.. merged.Select(static e => (e.Name, e.Value, HeaderForwarding.ContentHeaders.Contains(e.Name)))];
        Overridden = positions.Keys.Concat(relayOwned).ToFrozenSet(StringComparer.OrdinalIgnoreCase);
    }

    public FrozenSet<string> Overridden { get; }

    public int Count => entries.Length;

    public bool TryApply(in RenderContext row, HttpRequestHeaders target, HttpContentHeaders content, [NotNullWhen(false)] out DeliveryResult? fault)
    {
        foreach (var (name, template, isContent) in entries)
        {
            if (!template.TryRenderHeaderValue(row, out var value, out fault))
            {
                return false;
            }

            var added = isContent ? content.TryAddWithoutValidation(name, value) : target.TryAddWithoutValidation(name, value);
            if (!added)
            {
                fault = new DeliveryResult.MessageFault($"header '{name}' cannot be sent", Permanent: true);
                return false;
            }
        }

        fault = null;
        return true;
    }
}
