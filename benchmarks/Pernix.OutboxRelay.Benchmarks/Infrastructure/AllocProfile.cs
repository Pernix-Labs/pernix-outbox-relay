using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.Globalization;

namespace Pernix.OutboxRelay.Benchmarks.Infrastructure;

internal sealed class AllocProfile : EventListener
{
    private const string RuntimeSource = "Microsoft-Windows-DotNETRuntime";
    private const EventKeywords GcKeyword = (EventKeywords)0x1;
    private const int TopTypes = 25;
    private static readonly TimeSpan DrainPoll = TimeSpan.FromMilliseconds(10);
    private static readonly TimeSpan DrainCap = TimeSpan.FromSeconds(1);

    private readonly Lock gate = new();
    private readonly List<Sample> samples = [];
    private long latestUtc = long.MinValue;
    private bool drainCapped;

    public static AllocProfile? Current { get; private set; }

    public static AllocProfile Enable() => Current ??= new AllocProfile();

    public void Reset()
    {
        lock (gate)
        {
            samples.Clear();
            latestUtc = long.MinValue;
            drainCapped = false;
        }
    }

    // Runtime events reach the listener tens of ms late (up to 43 ms measured): wait for one stamped after the window.
    public async Task DrainAsync(long endUtc)
    {
        var started = Stopwatch.GetTimestamp();
        while (Volatile.Read(ref latestUtc) <= endUtc)
        {
            if (Stopwatch.GetElapsedTime(started) >= DrainCap)
            {
                drainCapped = true;
                return;
            }

            await Task.Delay(DrainPoll);
        }
    }

    public void Print(Measurement reported)
    {
        var bytesByType = new Dictionary<string, long>(StringComparer.Ordinal);
        long total = 0;
        lock (gate)
        {
            foreach (var sample in samples)
            {
                if (sample.Utc < reported.StartUtc || sample.Utc > reported.EndUtc)
                {
                    continue;
                }

                bytesByType[sample.Type] = bytesByType.GetValueOrDefault(sample.Type) + sample.Amount;
                total += sample.Amount;
            }
        }

        var unit = reported.Unit;
        var units = Math.Max(1, reported.Count);
        Console.WriteLine();
        Console.WriteLine(string.Create(
            CultureInfo.InvariantCulture,
            $"sampled, diagnosis only: {total / 1024:N0} KB sampled vs {reported.Bytes / 1024:N0} KB measured in the reported round " +
            $"({100.0 * total / Math.Max(1, reported.Bytes):F1}%) over {reported.Count:N0} {unit}{(drainCapped ? " (drain capped)" : string.Empty)}"));
        Console.WriteLine($"| type | share | ~B/{unit} |");
        Console.WriteLine("|---|---:|---:|");
        foreach (var (type, bytes) in bytesByType.OrderByDescending(p => p.Value).Take(TopTypes))
        {
            Console.WriteLine(string.Create(
                CultureInfo.InvariantCulture,
                $"| `{type}` | {100.0 * bytes / Math.Max(1, total):F1}% | {bytes / (double)units:F1} |"));
        }

        Console.WriteLine();
    }

    public override void Dispose()
    {
        if (ReferenceEquals(Current, this))
        {
            Current = null;
        }

        base.Dispose();
    }

    protected override void OnEventSourceCreated(EventSource eventSource)
    {
        if (eventSource.Name == RuntimeSource)
        {
            EnableEvents(eventSource, EventLevel.Verbose, GcKeyword);
        }
    }

    protected override void OnEventWritten(EventWrittenEventArgs eventData)
    {
        if (eventData.EventName is null || !eventData.EventName.StartsWith("GCAllocationTick", StringComparison.Ordinal)
            || eventData.Payload is null || eventData.PayloadNames is null)
        {
            return;
        }

        var typeIndex = eventData.PayloadNames.IndexOf("TypeName");
        var amountIndex = eventData.PayloadNames.IndexOf("AllocationAmount64");
        var type = typeIndex >= 0 ? eventData.Payload[typeIndex] as string ?? "?" : "?";
        var amount = amountIndex >= 0 ? Convert.ToInt64(eventData.Payload[amountIndex], CultureInfo.InvariantCulture) : 100_000;
        var utc = eventData.TimeStamp.ToUniversalTime().Ticks;
        lock (gate)
        {
            samples.Add(new Sample(utc, type, amount));
            if (utc > latestUtc)
            {
                latestUtc = utc;
            }
        }
    }

    private readonly record struct Sample(long Utc, string Type, long Amount);
}
