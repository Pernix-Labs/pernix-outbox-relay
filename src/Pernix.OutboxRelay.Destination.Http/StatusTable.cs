using Pernix.OutboxRelay.Destination.Http.Options;

namespace Pernix.OutboxRelay.Destination.Http;

internal enum StatusClass : byte
{
    Paused,
    Delivered,
    Rejected,
    Retried,
}

internal sealed class StatusTable
{
    internal const int MinCode = 100;
    internal const int MaxCode = 599;

    private static readonly int[] DefaultRejected = [400, 413, 414, 415, 422];

    private readonly StatusClass[] classes;

    private StatusTable(StatusClass[] classes) => this.classes = classes;

    public static StatusTable Default { get; } = Build(null, null, null, null);

    public StatusClass Classify(int statusCode) =>
        (uint)statusCode < (uint)classes.Length ? classes[statusCode] : StatusClass.Paused;

    public static StatusTable? TryCreate(StatusCodesOptions? options, string keyPath, List<string> failures)
    {
        if (options is null)
        {
            return Default;
        }

        var count = failures.Count;
        var delivered = ParseClass(options.Delivered, $"{keyPath}.{nameof(StatusCodesOptions.Delivered)}", failures);
        var rejected = ParseClass(options.Rejected, $"{keyPath}.{nameof(StatusCodesOptions.Rejected)}", failures);
        var retried = ParseClass(options.Retried, $"{keyPath}.{nameof(StatusCodesOptions.Retried)}", failures);
        var paused = ParseClass(options.Paused, $"{keyPath}.{nameof(StatusCodesOptions.Paused)}", failures);
        if (failures.Count > count)
        {
            return null;
        }

        var listed = new (string Name, bool[]? Codes)[]
        {
            (nameof(StatusCodesOptions.Delivered), delivered),
            (nameof(StatusCodesOptions.Rejected), rejected),
            (nameof(StatusCodesOptions.Retried), retried),
            (nameof(StatusCodesOptions.Paused), paused),
        };
        for (var i = 0; i < listed.Length; i++)
        {
            for (var j = i + 1; j < listed.Length; j++)
            {
                if (Overlaps(listed[i].Codes, listed[j].Codes))
                {
                    failures.Add($"{keyPath}.{listed[j].Name}: shares a status code with {listed[i].Name}; a code may be in one class only");
                }
            }
        }

        return failures.Count > count ? null : Build(delivered, rejected, retried, paused);
    }

    internal static bool TryParseEntry(string? entry, out int low, out int high)
    {
        low = high = 0;
        if (entry is null)
        {
            return false;
        }

        var text = entry.AsSpan().Trim();
        if (text.Length == 3 && text[0] is >= '1' and <= '5' && text[1] is 'x' or 'X' && text[2] is 'x' or 'X')
        {
            low = (text[0] - '0') * 100;
            high = low + 99;
            return true;
        }

        var dash = text.IndexOf('-');
        if (dash < 0)
        {
            if (!TryParseCode(text, out low))
            {
                return false;
            }

            high = low;
            return true;
        }

        return TryParseCode(text[..dash], out low) && TryParseCode(text[(dash + 1)..], out high) && low <= high;
    }

    internal static bool[]? ParseClass(string[]? entries, string keyPath, List<string> failures)
    {
        if (entries is null)
        {
            return null;
        }

        var codes = new bool[MaxCode + 1];
        for (var i = 0; i < entries.Length; i++)
        {
            if (!TryParseEntry(entries[i], out var low, out var high))
            {
                failures.Add($"{keyPath}[{i}]: must be a code (NNN), a class (Nxx) or a range (NNN-NNN) within {MinCode}-{MaxCode}");
                continue;
            }

            codes.AsSpan(low, high - low + 1).Fill(true);
        }

        return codes;
    }

    private static bool TryParseCode(ReadOnlySpan<char> text, out int code)
    {
        code = 0;
        if (text.Length != 3)
        {
            return false;
        }

        foreach (var c in text)
        {
            if (c is < '0' or > '9')
            {
                return false;
            }

            code = (code * 10) + (c - '0');
        }

        return code is >= MinCode and <= MaxCode;
    }

    private static bool Overlaps(bool[]? a, bool[]? b)
    {
        if (a is null || b is null)
        {
            return false;
        }

        for (var code = MinCode; code <= MaxCode; code++)
        {
            if (a[code] && b[code])
            {
                return true;
            }
        }

        return false;
    }

    private static StatusTable Build(bool[]? delivered, bool[]? rejected, bool[]? retried, bool[]? paused)
    {
        var classes = new StatusClass[MaxCode + 1];
        if (delivered is null)
        {
            classes.AsSpan(200, 100).Fill(StatusClass.Delivered);
        }

        if (rejected is null)
        {
            foreach (var code in DefaultRejected)
            {
                classes[code] = StatusClass.Rejected;
            }
        }

        Apply(classes, delivered, StatusClass.Delivered);
        Apply(classes, rejected, StatusClass.Rejected);
        Apply(classes, retried, StatusClass.Retried);
        Apply(classes, paused, StatusClass.Paused);
        return new StatusTable(classes);
    }

    private static void Apply(StatusClass[] classes, bool[]? codes, StatusClass value)
    {
        if (codes is null)
        {
            return;
        }

        for (var code = MinCode; code <= MaxCode; code++)
        {
            if (codes[code])
            {
                classes[code] = value;
            }
        }
    }
}
