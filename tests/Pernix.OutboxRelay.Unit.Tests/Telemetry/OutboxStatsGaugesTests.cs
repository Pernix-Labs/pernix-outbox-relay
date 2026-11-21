using System.Diagnostics.Metrics;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Diagnostics;
using Pernix.OutboxRelay.Core.Leadership;
using Pernix.OutboxRelay.Core.Stats;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Telemetry;

internal sealed record GaugeMeasurement(string Instrument, string? Unit, string Outbox, double Value, int TagCount);

public sealed class OutboxStatsGaugesTests
{
    private static readonly OutboxStats Reading = new(1200, 30, 4, TimeSpan.FromMilliseconds(2500), GateLag.Of(TimeSpan.FromMilliseconds(750)));

    private readonly FakeTimeProvider time = new();
    private readonly string prefix = Guid.NewGuid().ToString("N");

    [Fact]
    public void Fresh_reading_reports_five_gauges_with_units_and_outbox_tag_only()
    {
        var cache = Registered($"{prefix}-a");
        try
        {
            cache.Set(Reading);

            var measurements = Collect(prefix);

            Assert.Equal(
                [
                    new GaugeMeasurement("outbox.blocked", "1", $"{prefix}-a", 4, 1),
                    new GaugeMeasurement("outbox.gate_lag", "s", $"{prefix}-a", 0.75, 1),
                    new GaugeMeasurement("outbox.head_age", "s", $"{prefix}-a", 2.5, 1),
                    new GaugeMeasurement("outbox.pending", "{message}", $"{prefix}-a", 1200, 1),
                    new GaugeMeasurement("outbox.retrying", "{message}", $"{prefix}-a", 30, 1),
                ],
                measurements.OrderBy(m => m.Instrument, StringComparer.Ordinal));
        }
        finally
        {
            OutboxStatsGauges.Unregister(cache);
        }
    }

    [Fact]
    public void Instruments_carry_the_design_descriptions()
    {
        var descriptions = new Dictionary<string, string?>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, _) =>
        {
            if (instrument.Meter.Name == RelayDiagnostics.Name && instrument.Name.StartsWith("outbox.", StringComparison.Ordinal))
            {
                descriptions[instrument.Name] = instrument.Description;
            }
        };
        EnsureInstruments();
        listener.Start();

        Assert.Equal("Rows not yet failed, retry_count = 0 (estimate)", descriptions[OutboxStatsGauges.PendingName]);
        Assert.Equal("Failed rows not blocked: waiting to retry or dead-letter pending", descriptions[OutboxStatsGauges.RetryingName]);
        Assert.Equal("Blocked rows (Strict) or blocked keys (PerKey); do not sum across modes", descriptions[OutboxStatsGauges.BlockedName]);
        Assert.Equal("Age of the first row in read order (head)", descriptions[OutboxStatsGauges.HeadAgeName]);
        Assert.Equal("Age of the oldest transaction holding the gate", descriptions[OutboxStatsGauges.GateLagName]);
    }

    [Fact]
    public void Stale_reading_reports_nothing()
    {
        var cache = Registered($"{prefix}-a");
        try
        {
            cache.Set(Reading);
            time.Advance(TimeSpan.FromSeconds(31));

            Assert.Empty(Collect(prefix));
        }
        finally
        {
            OutboxStatsGauges.Unregister(cache);
        }
    }

    [Fact]
    public void Missing_gate_lag_skips_only_gate_lag()
    {
        var cache = Registered($"{prefix}-a");
        try
        {
            cache.Set(Reading with { GateLag = GateLag.Unavailable("role lacks pg_monitor (pg_read_all_stats)") });

            Assert.Equal(
                ["outbox.blocked", "outbox.head_age", "outbox.pending", "outbox.retrying"],
                Collect(prefix).Select(m => m.Instrument).Order(StringComparer.Ordinal));
        }
        finally
        {
            OutboxStatsGauges.Unregister(cache);
        }
    }

    [Fact]
    public void Two_caches_report_two_series()
    {
        var first = Registered($"{prefix}-a");
        var second = Registered($"{prefix}-b");
        try
        {
            first.Set(Reading);
            second.Set(Reading with { Pending = 7 });

            var pending = Collect(prefix).Where(m => m.Instrument == OutboxStatsGauges.PendingName).Select(m => (m.Outbox, m.Value));

            Assert.Equal([($"{prefix}-a", 1200d), ($"{prefix}-b", 7d)], pending.Order());
        }
        finally
        {
            OutboxStatsGauges.Unregister(first);
            OutboxStatsGauges.Unregister(second);
        }
    }

    [Fact]
    public void Unregistered_cache_reports_nothing()
    {
        var cache = Registered($"{prefix}-a");
        cache.Set(Reading);

        OutboxStatsGauges.Unregister(cache);

        Assert.Empty(Collect(prefix));
    }

    // Other tests may register caches concurrently, so only outboxes with this prefix are kept.
    internal static List<GaugeMeasurement> Collect(string prefix)
    {
        var measurements = new List<GaugeMeasurement>();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == RelayDiagnostics.Name && instrument.Name is OutboxStatsGauges.PendingName or OutboxStatsGauges.RetryingName
                or OutboxStatsGauges.BlockedName or OutboxStatsGauges.HeadAgeName or OutboxStatsGauges.GateLagName)
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) => Add(instrument, value, tags));
        listener.SetMeasurementEventCallback<double>((instrument, value, tags, _) => Add(instrument, value, tags));
        listener.Start();
        listener.RecordObservableInstruments();
        return measurements;

        void Add(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            foreach (var tag in tags)
            {
                if (tag.Key == OutboxStatsGauges.OutboxTag && tag.Value is string outbox && outbox.StartsWith(prefix, StringComparison.Ordinal))
                {
                    lock (measurements)
                    {
                        measurements.Add(new GaugeMeasurement(instrument.Name, instrument.Unit, outbox, value, tags.Length));
                    }
                }
            }
        }
    }

    private static void EnsureInstruments() =>
        System.Runtime.CompilerServices.RuntimeHelpers.RunClassConstructor(typeof(OutboxStatsGauges).TypeHandle);

    private OutboxStatsCache Registered(string outbox)
    {
        var cache = new OutboxStatsCache(new OutboxRuntimeStatus(outbox) { IsLeader = true }, time);
        OutboxStatsGauges.Register(cache);
        return cache;
    }
}
