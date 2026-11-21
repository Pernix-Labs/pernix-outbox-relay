using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Diagnostics;
using Pernix.OutboxRelay.Core.Leadership;

namespace Pernix.OutboxRelay.Core.Stats;

internal sealed class OutboxStatsCache
{
    internal static readonly TimeSpan StaleAfter = 2 * OutboxStatsLoop.Cadence;

    private readonly OutboxRuntimeStatus status;
    private readonly TimeProvider time;
    private Reading? reading;

    public OutboxStatsCache(OutboxRuntimeStatus status, TimeProvider time)
    {
        ArgumentNullException.ThrowIfNull(status);
        ArgumentNullException.ThrowIfNull(time);
        this.status = status;
        this.time = time;
        Outbox = status.Name;
        Tags = [new KeyValuePair<string, object?>(OutboxStatsGauges.OutboxTag, status.Name)];
    }

    public string Outbox { get; }

    public KeyValuePair<string, object?>[] Tags { get; }

    public void Set(OutboxStats stats) => Volatile.Write(ref reading, new Reading(stats, time.GetTimestamp()));

    public void Clear() => Volatile.Write(ref reading, null);

    public bool TryGetFresh(out OutboxStats stats)
    {
        // Leader only: a replica that just lost the lock stops exporting at once, even mid-read.
        var current = Volatile.Read(ref reading);
        if (!status.IsLeader || current is null || time.GetElapsedTime(current.Timestamp) > StaleAfter)
        {
            stats = default;
            return false;
        }

        stats = current.Stats;
        return true;
    }

    private sealed record Reading(OutboxStats Stats, long Timestamp);
}
