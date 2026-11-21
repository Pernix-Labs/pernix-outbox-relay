using System.Globalization;
using System.Text.RegularExpressions;

namespace Pernix.OutboxRelay.Unit.Tests.Allocation;

internal readonly record struct ReportedRow(string Name, int Round, long Count, double Figure);

internal readonly record struct FlatPoint(long Count, long HeapKb, long CommittedKb);

internal readonly record struct ChurnPoint(long Sessions, long HeapKb, long Backends, long Delta);

internal static partial class HarnessOutput
{
    public const string FlatTermMarker = "flat term PerKey kafka b500 h1: ";
    public const string FlatTermHeader = "| count | heap after full GC (KB) | committed (KB) |";
    public const string FlatSessionMarker = "flat session None b500 h1: ";
    public const string FlatSessionHeader = "| rows | heap after full GC (KB) | committed (KB) |";
    public const string FlatSeparator = "|---:|---:|---:|";
    public const string ChurnMarker = "churn PerKey seeded 10: ";
    public const string ChurnHeader = "| sessions | heap after full GC (KB) | backends | delta |";
    public const string ChurnSeparator = "|---:|---:|---:|---:|";

    private static readonly string[] CleanHeaderTokens = ["runtimeEvents=off", "profile=False", "otel=False", "gcServer=False"];

    public static string[] Lines(string stdout)
    {
        var lines = stdout.Split('\n');
        for (var i = 0; i < lines.Length; i++)
        {
            lines[i] = lines[i].TrimEnd('\r');
        }

        return lines;
    }

    public static string? Header(string[] lines)
    {
        foreach (var line in lines)
        {
            if (line.StartsWith("# ", StringComparison.Ordinal))
            {
                return line;
            }
        }

        return null;
    }

    public static bool IsClean(string header)
    {
        var tokens = header.Split(' ');
        foreach (var expected in CleanHeaderTokens)
        {
            if (Array.IndexOf(tokens, expected) < 0)
            {
                return false;
            }
        }

        return true;
    }

    public static List<ReportedRow> ReportedRows(string[] lines)
    {
        var rows = new List<ReportedRow>();
        foreach (var line in lines)
        {
            var match = ReportedRowPattern().Match(line);
            if (match.Success)
            {
                rows.Add(new ReportedRow(
                    match.Groups["name"].Value,
                    int.Parse(match.Groups["round"].Value, NumberStyles.None, CultureInfo.InvariantCulture),
                    Integer(match.Groups["count"].Value),
                    double.Parse(match.Groups["value"].Value, NumberStyles.AllowThousands | NumberStyles.AllowDecimalPoint, CultureInfo.InvariantCulture)));
            }
        }

        return rows;
    }

    public static string FixedShapeLine(string cell, int warm, long calls) =>
        string.Create(CultureInfo.InvariantCulture, $"fixed {cell} warm {warm}: {calls:N0} calls per round");

    public static string IdleShapeLine(string cell, int warmUpSeconds, long seconds) =>
        string.Create(CultureInfo.InvariantCulture, $"{cell} warm-up {warmUpSeconds}s: {seconds:N0} s per round");

    public static int LineCount(string[] lines, string line)
    {
        var count = 0;
        foreach (var candidate in lines)
        {
            if (candidate == line)
            {
                count++;
            }
        }

        return count;
    }

    public static bool Matches(string rowName, string cell, bool prefix) =>
        prefix
            ? rowName == cell || rowName.StartsWith(cell + " (", StringComparison.Ordinal)
            : rowName == cell;

    public static int FlatHeaderIndex(string[] lines, bool term) =>
        term ? MarkedIndex(lines, FlatTermMarker, FlatTermHeader) : MarkedIndex(lines, FlatSessionMarker, FlatSessionHeader);

    public static List<FlatPoint> FlatPoints(string[] lines, int headerIndex)
    {
        var points = new List<FlatPoint>();
        if (headerIndex < 0 || headerIndex + 1 >= lines.Length || lines[headerIndex + 1] != FlatSeparator)
        {
            return points;
        }

        for (var i = headerIndex + 2; i < lines.Length; i++)
        {
            var match = FlatRowPattern().Match(lines[i]);
            if (!match.Success)
            {
                break;
            }

            points.Add(new FlatPoint(Integer(match.Groups[1].Value), Integer(match.Groups[2].Value), Integer(match.Groups[3].Value)));
        }

        return points;
    }

    public static List<ChurnPoint> ChurnPoints(string[] lines)
    {
        var points = new List<ChurnPoint>();
        var header = MarkedIndex(lines, ChurnMarker, ChurnHeader);
        if (header < 0 || header + 1 >= lines.Length || lines[header + 1] != ChurnSeparator)
        {
            return points;
        }

        for (var i = header + 2; i < lines.Length; i++)
        {
            var match = ChurnRowPattern().Match(lines[i]);
            if (!match.Success)
            {
                break;
            }

            points.Add(new ChurnPoint(
                Integer(match.Groups[1].Value),
                Integer(match.Groups[2].Value),
                Integer(match.Groups[3].Value),
                Integer(match.Groups[4].Value)));
        }

        return points;
    }

    private static int MarkedIndex(string[] lines, string marker, string header)
    {
        for (var i = 0; i + 1 < lines.Length; i++)
        {
            if (lines[i].StartsWith(marker, StringComparison.Ordinal) && lines[i + 1] == header)
            {
                return i + 1;
            }
        }

        return -1;
    }

    private static long Integer(string value) =>
        long.Parse(value, NumberStyles.AllowThousands | NumberStyles.AllowLeadingSign, CultureInfo.InvariantCulture);

    [GeneratedRegex(@"^\| (?<name>.+?) \| (?<round>\d+) reported \| (?<count>[\d,]+) \| (?<value>[\d,]+\.\d) \|", RegexOptions.CultureInvariant)]
    private static partial Regex ReportedRowPattern();

    [GeneratedRegex(@"^\| ([\d,]+) \| ([\d,]+) \| ([\d,]+) \|$", RegexOptions.CultureInvariant)]
    private static partial Regex FlatRowPattern();

    [GeneratedRegex(@"^\| ([\d,]+) \| ([\d,]+) \| ([\d,]+) \| (-?[\d,]+) \|$", RegexOptions.CultureInvariant)]
    private static partial Regex ChurnRowPattern();
}
