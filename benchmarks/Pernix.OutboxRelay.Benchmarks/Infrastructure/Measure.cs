using System.Diagnostics;
using System.Globalization;

namespace Pernix.OutboxRelay.Benchmarks.Infrastructure;

internal readonly record struct Snapshot(long Bytes, int Gen0, int Gen1, int Gen2, long Ticks, long Utc)
{
    // Utc (UTC ticks) bounds the --profile window by event time.
    public static Snapshot Take() =>
        new(GC.GetTotalAllocatedBytes(precise: true), GC.CollectionCount(0), GC.CollectionCount(1), GC.CollectionCount(2), Stopwatch.GetTimestamp(), DateTime.UtcNow.Ticks);
}

internal sealed record Measurement(
    string Scenario,
    int Round,
    bool Reported,
    long Count,
    string Unit,
    double BytesPerUnit,
    double Gen0Per100k,
    double Gen1Per100k,
    double Gen2Per100k,
    double UnitsPerSecond,
    long StartUtc,
    long EndUtc,
    long Bytes)
{
    public static Measurement Of(string scenario, int round, bool reported, long count, string unit, Snapshot start, Snapshot end)
    {
        if (count <= 0)
        {
            throw new InvalidOperationException($"{scenario} round {round}: nothing measured");
        }

        var seconds = (end.Ticks - start.Ticks) / (double)Stopwatch.Frequency;
        var per100k = 100_000.0 / count;
        return new(
            scenario,
            round,
            reported,
            count,
            unit,
            (end.Bytes - start.Bytes) / (double)count,
            (end.Gen0 - start.Gen0) * per100k,
            (end.Gen1 - start.Gen1) * per100k,
            (end.Gen2 - start.Gen2) * per100k,
            count / seconds,
            start.Utc,
            end.Utc,
            end.Bytes - start.Bytes);
    }

    public static string Header(string unit) =>
        $"| Scenario | Round | Count | B/{unit} | Gen0/100k | Gen1/100k | Gen2/100k | {unit}/s |\n|---|---|---:|---:|---:|---:|---:|---:|";

    // AllocationGuardTests parses this line.
    public override string ToString() => string.Create(
        CultureInfo.InvariantCulture,
        $"| {Scenario} | {Round}{(Reported ? " reported" : string.Empty)} | {Count:N0} | {BytesPerUnit:N1} | {Gen0Per100k:N2} | {Gen1Per100k:N2} | {Gen2Per100k:N2} | {UnitsPerSecond:N0} |");
}

internal static class RoundRunner
{
    public static async Task<Measurement> RunAsync(string unit, Func<Task> warm, int rounds, Func<int, bool, Task<Measurement>> measureRound)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(rounds, 1);

        var profile = AllocProfile.Current;
        Console.WriteLine(Measurement.Header(unit));
        profile?.Reset();
        await warm();

        Measurement? last = null;
        for (var round = 1; round <= rounds; round++)
        {
            var measurement = await measureRound(round, round == rounds);
            if (profile is not null)
            {
                measurement = measurement with { Scenario = measurement.Scenario + " profiled" };
            }

            Console.WriteLine(measurement);
            last = measurement;
        }

        if (profile is not null)
        {
            await profile.DrainAsync(last!.EndUtc);
            profile.Print(last);
        }

        return last!;
    }
}
