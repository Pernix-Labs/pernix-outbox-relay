using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Stats;

namespace Pernix.OutboxRelay.Core.Diagnostics;

internal static class OutboxStatsGauges
{
    public const string PendingName = "outbox.pending";
    public const string RetryingName = "outbox.retrying";
    public const string BlockedName = "outbox.blocked";
    public const string HeadAgeName = "outbox.head_age";
    public const string GateLagName = "outbox.gate_lag";
    public const string OutboxTag = "outbox";

    private static readonly ConcurrentDictionary<OutboxStatsCache, byte> Caches = new();

    static OutboxStatsGauges()
    {
        var meter = RelayDiagnostics.Meter;
        meter.CreateObservableGauge(PendingName, static () => Counts(static s => s.Pending), unit: "{message}", description: "Rows not yet failed, retry_count = 0 (estimate)");
        meter.CreateObservableGauge(RetryingName, static () => Counts(static s => s.Retrying), unit: "{message}", description: "Failed rows not blocked: waiting to retry or dead-letter pending");
        meter.CreateObservableGauge(BlockedName, static () => Counts(static s => s.Blocked), unit: "1", description: "Blocked rows (Strict) or blocked keys (PerKey); do not sum across modes");
        meter.CreateObservableGauge(HeadAgeName, static () => Seconds(static s => s.HeadAge), unit: "s", description: "Age of the first row in read order (head)");
        meter.CreateObservableGauge(GateLagName, static () => Seconds(static s => s.GateLag.Value), unit: "s", description: "Age of the oldest transaction holding the gate");
    }

    public static void Register(OutboxStatsCache cache)
    {
        ArgumentNullException.ThrowIfNull(cache);
        Caches.TryAdd(cache, 0);
    }

    public static void Unregister(OutboxStatsCache cache)
    {
        ArgumentNullException.ThrowIfNull(cache);
        Caches.TryRemove(cache, out _);
    }

    // Enumerates the dictionary itself: Keys would lock and copy on every collection.
    private static IEnumerable<Measurement<long>> Counts(Func<OutboxStats, long> value)
    {
        foreach (var entry in Caches)
        {
            var cache = entry.Key;
            if (cache.TryGetFresh(out var stats))
            {
                yield return new Measurement<long>(value(stats), cache.Tags);
            }
        }
    }

    private static IEnumerable<Measurement<double>> Seconds(Func<OutboxStats, TimeSpan?> value)
    {
        foreach (var entry in Caches)
        {
            var cache = entry.Key;
            if (cache.TryGetFresh(out var stats) && value(stats) is { } age)
            {
                yield return new Measurement<double>(age.TotalSeconds, cache.Tags);
            }
        }
    }
}
