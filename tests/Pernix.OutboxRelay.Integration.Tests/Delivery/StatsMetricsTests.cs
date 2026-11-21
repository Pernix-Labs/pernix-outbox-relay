using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Net;
using Pernix.OutboxRelay.Core.Diagnostics;
using Pernix.OutboxRelay.Core.Leadership;
using Pernix.OutboxRelay.Host.Telemetry;
using Pernix.OutboxRelay.Integration.Tests.Kafka;
using Pernix.OutboxRelay.Integration.Tests.Leadership;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Npgsql;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.Delivery;

public sealed class StatsMetricsTests(PostgreSqlFixture postgres, KafkaFixture kafka)
{
    private const string Pending = "outbox.pending";
    private const string Retrying = "outbox.retrying";
    private const string Blocked = "outbox.blocked";
    private const string HeadAge = "outbox.head_age";
    private const string GateLag = "outbox.gate_lag";

    // Event names, not ids: framework categories reuse low ids. 130, 131 (StatsLog), 103, 107 (LeadershipLog).
    private const string StatsUnavailable = "StatsUnavailable";
    private const string StatsAvailable = "StatsAvailable";
    private const string LeadershipReleased = "LeadershipReleased";
    private const string LeadershipReleasePending = "LeadershipReleasePending";

    private static readonly string[] StatsGauges = [Pending, Retrying, Blocked, HeadAge, GateLag];

    private static readonly TimeSpan LeaderTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan DeliveryTimeout = TimeSpan.FromSeconds(30);

    private static readonly TimeSpan CadenceTimeout = TimeSpan.FromSeconds(20);

    // OutboxStatsLoop.ReadBound plus slack: a hang is past its deadline after this.
    private static readonly TimeSpan PastReadBound = TimeSpan.FromSeconds(11);

    // Hard cap.
    private static readonly TimeSpan StopBudget = TimeSpan.FromSeconds(27);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task PerKey_leader_reports_five_gauges_for_seeded_rows_within_one_cadence()
    {
        var setup = await DeliverySetup.CreateAsync(postgres, kafka, orderingMode: "PerKey");
        var seeded = Stopwatch.StartNew();
        await SeedAsync(setup);
        using var gauges = new GaugeReader(setup.Outbox);
        await using var relay = setup.Relay();
        var status = StatusOf(relay);
        Assert.True(await LeadershipTests.EventuallyAsync(() => status.IsLeader, LeaderTimeout), "no leader");

        // Pending is an estimate, so wait for the expected values rather than the first reading.
        IReadOnlyDictionary<string, IReadOnlyList<double>> values = new Dictionary<string, IReadOnlyList<double>>();
        var reported = await LeadershipTests.EventuallyAsync(
            () =>
            {
                values = gauges.Read();
                return Single(values, Retrying) == 3 && Single(values, Blocked) == 4 && Single(values, Pending) == 2
                    && values.ContainsKey(HeadAge) && values.ContainsKey(GateLag);
            },
            CadenceTimeout);

        Assert.True(reported, $"gauges not as seeded: {Describe(values)}");
        Assert.All(StatsGauges, name => Assert.Single(values[name]));
        Assert.InRange(values[HeadAge][0], 119, 121 + seeded.Elapsed.TotalSeconds);
        Assert.True(values[GateLag][0] >= 0, $"gate lag {values[GateLag][0]}");
    }

    [Fact]
    public async Task Failing_then_hanging_stats_leave_delivery_leadership_and_health_untouched_and_recover()
    {
        var setup = await DeliverySetup.CreateAsync(postgres, kafka);
        var fault = new StatsFault(StatsFaultMode.Throw);
        var logs = new EventCapture();
        using var gauges = new GaugeReader(setup.Outbox);
        await using var relay = StatsFaultingStoreProvider.Decorate(setup.Relay(), fault).WithServices(s => s.AddSingleton<ILoggerProvider>(logs));
        using var client = relay.CreateClient();
        var status = StatusOf(relay);
        Assert.True(await LeadershipTests.EventuallyAsync(() => status.IsLeader, LeaderTimeout), "no leader");

        Assert.True(await LeadershipTests.EventuallyAsync(() => logs.Count(StatsUnavailable) == 1, LeaderTimeout), "130 not logged after throw");
        await AssertIsolatedAsync(setup, client, gauges, tag: 1);

        fault.Mode = StatsFaultMode.Hang;
        Assert.True(await LeadershipTests.EventuallyAsync(() => fault.Calls >= 2, CadenceTimeout), "no hanging read");
        var hang = Stopwatch.StartNew();
        var tag = 2;
        do
        {
            await AssertIsolatedAsync(setup, client, gauges, tag++);
        }
        while (hang.Elapsed < PastReadBound);

        Assert.Equal(1, logs.Count(StatsUnavailable));
        Assert.Equal(0, logs.Count(StatsAvailable));

        fault.Mode = StatsFaultMode.PassThrough;
        Assert.True(await LeadershipTests.EventuallyAsync(() => gauges.Read().ContainsKey(Retrying), CadenceTimeout), "gauges not back after heal");
        Assert.True(await LeadershipTests.EventuallyAsync(() => logs.Count(StatsAvailable) == 1, LeaderTimeout), "131 not logged after heal");
        var healed = gauges.Read();
        Assert.All([Pending, Retrying, Blocked, HeadAge], name => Assert.Single(healed[name]));
        Assert.Equal(1, logs.Count(StatsUnavailable));
        Assert.Equal(1, logs.Count(StatsAvailable));
    }

    [Fact]
    public async Task Stop_with_hanging_stats_read_completes_within_stop_budget_and_releases_leadership()
    {
        var setup = await DeliverySetup.CreateAsync(postgres, kafka);
        var fault = new StatsFault(StatsFaultMode.Hang);
        var logs = new EventCapture();
        await using var relay = StatsFaultingStoreProvider.Decorate(setup.Relay(), fault).WithServices(s => s.AddSingleton<ILoggerProvider>(logs));
        var status = StatusOf(relay);
        Assert.True(await LeadershipTests.EventuallyAsync(() => status.IsLeader, LeaderTimeout), "no leader");
        Assert.True(await LeadershipTests.EventuallyAsync(() => fault.Calls >= 1, LeaderTimeout), "no hanging read");

        var elapsed = Stopwatch.StartNew();
        await relay.Services.GetRequiredService<IHost>().StopAsync(CancellationToken.None);

        Assert.True(elapsed.Elapsed < StopBudget, $"stop took {elapsed.Elapsed}");
        Assert.Equal(1, logs.Count(LeadershipReleased));
        Assert.Equal(0, logs.Count(LeadershipReleasePending));
        Assert.Equal(0, await postgres.CountLockHoldersAsync(setup.Table));
    }

    [Fact]
    public async Task Two_replicas_on_one_outbox_only_leader_reads_stats_and_each_gauge_has_one_series()
    {
        var setup = await DeliverySetup.CreateAsync(postgres, kafka);
        var (faultA, faultB) = (new StatsFault(), new StatsFault());
        using var gauges = new GaugeReader(setup.Outbox);
        await using var a = StatsFaultingStoreProvider.Decorate(setup.Relay(), faultA);
        await using var b = StatsFaultingStoreProvider.Decorate(setup.Relay(), faultB);
        var (statusA, statusB) = (StatusOf(a), StatusOf(b));
        Assert.True(await LeadershipTests.EventuallyAsync(() => statusA.IsLeader ^ statusB.IsLeader, LeaderTimeout), "not exactly one leader");
        var (leader, follower) = statusA.IsLeader ? (faultA, faultB) : (faultB, faultA);

        Assert.True(await LeadershipTests.EventuallyAsync(() => leader.Calls >= 1 && gauges.Read().ContainsKey(Retrying), CadenceTimeout), "leader reported no stats");

        var end = Stopwatch.StartNew();
        while (end.Elapsed < TimeSpan.FromSeconds(3))
        {
            Assert.Equal(1, Convert.ToInt32(statusA.IsLeader) + Convert.ToInt32(statusB.IsLeader));
            var values = gauges.Read();
            Assert.All(StatsGauges, name => Assert.Single(Assert.Contains(name, values)));
            Assert.Equal(0, follower.Calls);
            await Task.Delay(200, Ct);
        }

        Assert.True(leader.Calls >= 1);
        Assert.Equal(0, follower.Calls);
    }

    private async Task AssertIsolatedAsync(DeliverySetup setup, HttpClient client, GaugeReader gauges, int tag)
    {
        await setup.InsertTagsAsync(tag, 1);
        Assert.True(await LeadershipTests.EventuallyAsync(async () => await setup.CountAsync() == 0, DeliveryTimeout), $"row {tag} not delivered");

        var values = gauges.Read();
        Assert.Equal([1d], Assert.Contains(LeaderGauge.InstrumentName, values));
        Assert.All(StatsGauges, name => Assert.False(values.ContainsKey(name), $"{name} reported while stats fail"));
        Assert.Equal(1, await postgres.CountLockHoldersAsync(setup.Table));
        Assert.Equal(HttpStatusCode.OK, await StatusAsync(client, "/livez"));
        Assert.Equal(HttpStatusCode.OK, await StatusAsync(client, "/readyz"));
    }

    // PerKey, one transaction, created_at 120 s ago. Retrying 3 (r1–r3, not due). Blocked keys 4 (x, y, two NULL-key rows).
    // Pending 2: rows behind blocked key x stay held, and 10 live minus 8 failed.
    private static async Task SeedAsync(DeliverySetup setup)
    {
        await using var connection = await setup.OpenAsync();
        await using var seed = new NpgsqlCommand(
            $"""
            INSERT INTO {setup.Table} (message_key, payload, created_at, retry_count, next_attempt_at)
            SELECT CASE WHEN v.k IS NULL THEN NULL ELSE convert_to(v.k, 'UTF8') END, '\x00', now() - interval '120 s', v.retries,
                   CASE v.next WHEN 'blocked' THEN 'infinity'::timestamptz WHEN 'later' THEN now() + interval '1 hour' ELSE now() END
            FROM (VALUES (1, 'r1', 2, 'later'), (2, 'r2', 1, 'later'), (3, 'r3', 3, 'later'),
                         (4, 'x', 4, 'blocked'), (5, 'x', 4, 'blocked'), (6, 'y', 4, 'blocked'),
                         (7, NULL, 4, 'blocked'), (8, NULL, 4, 'blocked'),
                         (9, 'x', 0, 'due'), (10, 'x', 0, 'due')) AS v(n, k, retries, next)
            ORDER BY v.n;
            SELECT pg_catalog.pg_stat_force_next_flush();
            """,
            connection);
        await seed.ExecuteNonQueryAsync(Ct);
    }

    private static OutboxRuntimeStatus StatusOf(RelayFactory relay) =>
        relay.Services.GetRequiredService<OutboxRuntimeState>().Outboxes.Single();

    private static double? Single(IReadOnlyDictionary<string, IReadOnlyList<double>> values, string name) =>
        values.TryGetValue(name, out var series) && series.Count == 1 ? series[0] : null;

    private static string Describe(IReadOnlyDictionary<string, IReadOnlyList<double>> values) =>
        string.Join(", ", values.Select(v => $"{v.Key}=[{string.Join(", ", v.Value)}]"));

    private static async Task<HttpStatusCode> StatusAsync(HttpClient client, string path)
    {
        using var response = await client.GetAsync(path, Ct);
        return response.StatusCode;
    }

    private sealed class GaugeReader : IDisposable
    {
        private readonly MeterListener listener = new();
        private readonly string outbox;
        private readonly Lock gate = new();
        private Dictionary<string, List<double>> values = [];

        public GaugeReader(string outbox)
        {
            this.outbox = outbox;
            listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == RelayDiagnostics.Name
                    && (instrument.Name == LeaderGauge.InstrumentName || Array.IndexOf(StatsGauges, instrument.Name) >= 0))
                {
                    l.EnableMeasurementEvents(instrument);
                }
            };
            listener.SetMeasurementEventCallback<int>((i, v, tags, _) => Add(i, v, tags));
            listener.SetMeasurementEventCallback<long>((i, v, tags, _) => Add(i, v, tags));
            listener.SetMeasurementEventCallback<double>((i, v, tags, _) => Add(i, v, tags));
            listener.Start();
        }

        public IReadOnlyDictionary<string, IReadOnlyList<double>> Read()
        {
            lock (gate)
            {
                values = [];
                listener.RecordObservableInstruments();
                return values.ToDictionary(v => v.Key, v => (IReadOnlyList<double>)v.Value);
            }
        }

        public void Dispose() => listener.Dispose();

        private void Add(Instrument instrument, double value, ReadOnlySpan<KeyValuePair<string, object?>> tags)
        {
            foreach (var tag in tags)
            {
                if (tag.Key == "outbox" && Equals(tag.Value, outbox))
                {
                    if (!values.TryGetValue(instrument.Name, out var series))
                    {
                        values[instrument.Name] = series = [];
                    }

                    series.Add(value);
                }
            }
        }
    }

    private sealed class EventCapture : ILoggerProvider, ILogger
    {
        private readonly ConcurrentQueue<string?> events = new();

        public int Count(string eventName) => events.Count(e => e == eventName);

        public ILogger CreateLogger(string categoryName) => this;

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            events.Enqueue(eventId.Name);

        public void Dispose()
        {
        }
    }
}
