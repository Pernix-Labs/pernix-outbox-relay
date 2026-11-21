using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Pernix.OutboxRelay.Core.Diagnostics;
using Pernix.OutboxRelay.Core.Leadership;

namespace Pernix.OutboxRelay.Host.Telemetry;

// One static gauge over every registered state, so several hosts in one process each report their own outboxes.
public static class LeaderGauge
{
    public const string InstrumentName = "outbox.leader";
    public const string OutboxTag = "outbox";

    private static readonly ConcurrentDictionary<OutboxRuntimeState, byte> States = new();

    static LeaderGauge()
    {
        RelayDiagnostics.Meter.CreateObservableGauge(
            InstrumentName,
            Observe,
            unit: "{leader}",
            description: "1 while this replica leads the outbox, 0 otherwise.");
    }

    public static void Register(OutboxRuntimeState state)
    {
        ArgumentNullException.ThrowIfNull(state);
        States.TryAdd(state, 0);
    }

    public static void Unregister(OutboxRuntimeState state)
    {
        ArgumentNullException.ThrowIfNull(state);
        States.TryRemove(state, out _);
    }

    private static IEnumerable<Measurement<int>> Observe()
    {
        foreach (var state in States.Keys)
        {
            foreach (var status in state.Outboxes)
            {
                yield return new Measurement<int>(status.IsLeader ? 1 : 0, new KeyValuePair<string, object?>(OutboxTag, status.Name));
            }
        }
    }
}
