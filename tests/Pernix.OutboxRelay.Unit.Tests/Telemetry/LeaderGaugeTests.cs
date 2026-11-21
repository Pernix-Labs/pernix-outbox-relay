using System.Diagnostics.Metrics;
using Pernix.OutboxRelay.Core.Diagnostics;
using Pernix.OutboxRelay.Core.Leadership;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Telemetry;

public sealed class LeaderGaugeTests
{
    [Fact]
    public void Reports_one_measurement_per_outbox_of_each_registered_state()
    {
        var prefix = Guid.NewGuid().ToString("N");
        var first = StateWith(new OutboxRuntimeStatus($"{prefix}-a") { IsLeader = true }, new OutboxRuntimeStatus($"{prefix}-b"));
        var second = StateWith(new OutboxRuntimeStatus($"{prefix}-a"));

        LeaderGauge.Register(first);
        LeaderGauge.Register(second);
        try
        {
            var measurements = Collect(prefix);

            Assert.Equal(
                [($"{prefix}-a", 0), ($"{prefix}-a", 1), ($"{prefix}-b", 0)],
                measurements.Order().ToArray());
        }
        finally
        {
            LeaderGauge.Unregister(first);
            LeaderGauge.Unregister(second);
        }
    }

    [Fact]
    public void Unregistered_state_stops_reporting()
    {
        var prefix = Guid.NewGuid().ToString("N");
        var kept = StateWith(new OutboxRuntimeStatus($"{prefix}-kept") { IsLeader = true });
        var removed = StateWith(new OutboxRuntimeStatus($"{prefix}-removed") { IsLeader = true });

        LeaderGauge.Register(kept);
        LeaderGauge.Register(removed);
        try
        {
            LeaderGauge.Unregister(removed);

            Assert.Equal([($"{prefix}-kept", 1)], Collect(prefix));
        }
        finally
        {
            LeaderGauge.Unregister(kept);
        }
    }

    [Fact]
    public async Task Registration_disposed_without_stop_stops_reporting()
    {
        var prefix = Guid.NewGuid().ToString("N");
        var registration = new LeaderGaugeRegistration(StateWith(new OutboxRuntimeStatus($"{prefix}-a") { IsLeader = true }));

        await registration.StartAsync(TestContext.Current.CancellationToken);
        Assert.Equal([($"{prefix}-a", 1)], Collect(prefix));

        registration.Dispose();
        registration.Dispose();
        await registration.StopAsync(TestContext.Current.CancellationToken);

        Assert.Empty(Collect(prefix));
    }

    private static OutboxRuntimeState StateWith(params OutboxRuntimeStatus[] statuses)
    {
        var state = new OutboxRuntimeState();
        state.SetOutboxes(statuses);
        return state;
    }

    // Other tests may register states concurrently, so only outboxes with this test's prefix are kept.
    private static List<(string Outbox, int Value)> Collect(string prefix)
    {
        var measurements = new List<(string Outbox, int Value)>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == RelayDiagnostics.Name && instrument.Name == LeaderGauge.InstrumentName)
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<int>((_, value, tags, _) =>
        {
            foreach (var tag in tags)
            {
                if (tag.Key == LeaderGauge.OutboxTag && tag.Value is string outbox && outbox.StartsWith(prefix, StringComparison.Ordinal))
                {
                    measurements.Add((outbox, value));
                }
            }
        });
        listener.Start();
        listener.RecordObservableInstruments();
        return measurements;
    }
}
