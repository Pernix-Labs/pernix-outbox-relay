using System.Globalization;

namespace Pernix.OutboxRelay.Unit.Tests.Allocation;

internal static class HeapChecks
{
    private const long MinimumAllowanceKb = 64;

    public static bool Pinned(IReadOnlyList<long> counts, int expected, long step)
    {
        if (counts.Count != expected)
        {
            return false;
        }

        for (var i = 0; i < counts.Count; i++)
        {
            if (counts[i] != (i + 1) * step)
            {
                return false;
            }
        }

        return true;
    }

    public static List<string> Rising(string check, IReadOnlyList<FlatPoint> points)
    {
        var failures = new List<string>();
        if (points.Count < 2)
        {
            return failures;
        }

        var second = points[1].HeapKb;
        var allowance = Math.Max(MinimumAllowanceKb, second / 100.0);
        for (var i = 2; i < points.Count; i++)
        {
            if (points[i].HeapKb - second > allowance)
            {
                failures.Add(string.Create(
                    CultureInfo.InvariantCulture,
                    $"{check}: heap {points[i].HeapKb:N0} KB at {points[i].Count:N0} > second checkpoint {second:N0} KB + {allowance:N0} KB (max(64 KB, 1% of the second))."));
            }
        }

        return failures;
    }

    public static List<string> NonZeroBackendDeltas(string check, IReadOnlyList<ChurnPoint> points)
    {
        var failures = new List<string>();
        foreach (var point in points)
        {
            if (point.Delta != 0)
            {
                failures.Add(string.Create(
                    CultureInfo.InvariantCulture,
                    $"{check}: relay backends delta {point.Delta:N0} at {point.Sessions:N0} sessions ({point.Backends:N0} backends), expected 0."));
            }
        }

        return failures;
    }
}
