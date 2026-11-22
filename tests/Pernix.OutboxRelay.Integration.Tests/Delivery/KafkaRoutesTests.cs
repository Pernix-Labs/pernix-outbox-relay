using System.Globalization;
using System.Text;
using Pernix.OutboxRelay.Integration.Tests.Kafka;
using Pernix.OutboxRelay.Integration.Tests.Leadership;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Pernix.OutboxRelay.Destination.Kafka;
using Confluent.Kafka;
using Npgsql;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.Delivery;

// One Routes destination { alerts: <topic A>, audit: <topic B> } through the relay host.
public sealed class KafkaRoutesTests(PostgreSqlFixture postgres, KafkaFixture kafka)
{
    private const string Alerts = "alerts";
    private const string Audit = "audit";
    private const string Unmapped = "billing";
    private const string HoldPrefix = "route not mapped: ";

    private static readonly TimeSpan DeliveryTimeout = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan AbsenceWait = TimeSpan.FromSeconds(4);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Mapped_routes_reach_their_topics_and_null_or_unmapped_routes_are_held_and_produced_nowhere()
    {
        var setup = await RoutesSetup.CreateAsync(postgres, kafka, "None", partitions: 1);
        var nullRoute = await setup.InsertAsync("n", 1, route: null);
        await setup.InsertAsync("a", 2, Alerts);
        await setup.InsertAsync("b", 3, Audit);
        var unmapped = await setup.InsertAsync("u", 4, Unmapped);

        await using var relay = setup.Relay().WithRelaySettings(new Dictionary<string, string?>
        {
            ["Outboxes:0:Retry:InitialDelay"] = "00:01:00",
            ["Outboxes:0:Retry:MaxDelay"] = "00:01:00",
        });
        _ = relay.Services;

        Assert.True(
            await LeadershipTests.EventuallyAsync(
                async () => await setup.FailureStatesAsync() is [var first, var second]
                    && first.Id == nullRoute && first.LastError == HoldPrefix + "(none)"
                    && second.Id == unmapped && second.LastError == HoldPrefix + Unmapped,
                DeliveryTimeout),
            "mapped rows not delivered or null/unmapped rows not held");

        Assert.Equal([("a", 2)], await ConsumeExactlyAsync(setup.BootstrapServers, setup.AlertsTopic, 1));
        Assert.Equal([("b", 3)], await ConsumeExactlyAsync(setup.BootstrapServers, setup.AuditTopic, 1));
        Assert.Equal([1, 4], (await setup.FailureStatesAsync()).Select(s => s.Tag));
    }

    [Fact]
    public async Task PerKey_send_of_a_key_starts_only_after_its_previous_send_completed_across_topics()
    {
        const int keys = 20;
        const int perKey = 10;
        var setup = await RoutesSetup.CreateAsync(postgres, kafka, "PerKey", partitions: 3);
        await setup.InsertAlternatingAsync(keys * perKey, keys);
        var recorder = new DestinationRecorder();

        await using var relay = RecordingDestinationFactory.Decorate(setup.Relay(), recorder);
        _ = relay.Services;
        Assert.True(await LeadershipTests.EventuallyAsync(async () => await setup.CountAsync() == 0, DeliveryTimeout), "table not empty");

        var sends = SendsOf(recorder, keys * perKey);
        foreach (var key in sends.GroupBy(s => Encoding.UTF8.GetString(s.Key!)))
        {
            AssertSerial(key.ToList(), key.Key);
        }

        await AssertTopicsAsync(setup, keys * perKey, keys);
    }

    [Fact]
    public async Task Strict_send_starts_only_after_the_previous_send_of_the_outbox_completed_across_topics()
    {
        const int keys = 5;
        const int total = 100;
        var setup = await RoutesSetup.CreateAsync(postgres, kafka, "Strict", partitions: 1);
        await setup.InsertAlternatingAsync(total, keys);
        var recorder = new DestinationRecorder();

        await using var relay = RecordingDestinationFactory.Decorate(setup.Relay(), recorder);
        _ = relay.Services;
        Assert.True(await LeadershipTests.EventuallyAsync(async () => await setup.CountAsync() == 0, DeliveryTimeout), "table not empty");

        AssertSerial(SendsOf(recorder, total), "outbox");
        await AssertTopicsAsync(setup, total, keys);
    }

    [Fact]
    public async Task Routed_row_dead_letter_lands_on_the_single_dead_letter_topic_with_original_route()
    {
        var setup = await RoutesSetup.CreateAsync(
            postgres, kafka, "None", partitions: 1, alertsConfigs: new Dictionary<string, string> { ["max.message.bytes"] = "1024" }, deadLetter: true);
        await using var relay = setup.Relay();
        _ = relay.Services;

        // Oversized for the alerts topic: a permanent fault, dead-lettered on the first attempt.
        var id = await setup.InsertAsync("big", 1, Alerts, payload: new string('x', 2048));
        await setup.InsertAsync("small", 2, Audit);
        Assert.True(await LeadershipTests.EventuallyAsync(async () => await setup.CountAsync() == 0, DeliveryTimeout), "table not empty");

        var dead = Assert.Single(await ConsumeRawAsync(setup.BootstrapServers, setup.DeadLetterTopic!, 1));
        Assert.Equal("big", Encoding.UTF8.GetString(dead.Message.Key));
        Assert.Equal(Alerts, Header(dead.Message.Headers, KafkaDeadLetterHeaders.OriginalRoute));
        Assert.Equal(id.ToString(CultureInfo.InvariantCulture), Header(dead.Message.Headers, "x-original-id"));
        Assert.Empty(await KafkaFixture.ConsumeAllAsync(setup.BootstrapServers, setup.AlertsTopic, 1, AbsenceWait));
        Assert.Equal([("small", 2)], await ConsumeExactlyAsync(setup.BootstrapServers, setup.AuditTopic, 1));
    }

    private static List<RecordedSend> SendsOf(DestinationRecorder recorder, int rows)
    {
        var sends = recorder.Destinations.SelectMany(d => d.Sends).ToList();
        Assert.Equal(rows, sends.Select(s => s.Id).Distinct().Count());
        return sends;
    }

    private static void AssertSerial(List<RecordedSend> sends, string scope)
    {
        sends.Sort((x, y) => x.Start.CompareTo(y.Start));
        for (var i = 1; i < sends.Count; i++)
        {
            var previous = sends[i - 1];
            var current = sends[i];
            Assert.True(current.Id > previous.Id, $"{scope}: row {current.Id} sent before row {previous.Id}");
            Assert.True(
                current.Start > previous.End,
                $"{scope}: row {current.Id} ({current.Route}) started at {current.Start} before row {previous.Id} ({previous.Route}) ended at {previous.End}");
        }
    }

    private static async Task AssertTopicsAsync(RoutesSetup setup, int total, int keys)
    {
        var tags = Enumerable.Range(1, total).ToList();
        var expectedAlerts = tags.Where(t => RoutesSetup.RouteOf(t, keys) == Alerts).Select(t => ($"k{t % keys}", t)).ToList();
        var expectedAudit = tags.Where(t => RoutesSetup.RouteOf(t, keys) == Audit).Select(t => ($"k{t % keys}", t)).ToList();

        AssertPerKeyOrder(await ConsumeExactlyAsync(setup.BootstrapServers, setup.AlertsTopic, expectedAlerts.Count), expectedAlerts);
        AssertPerKeyOrder(await ConsumeExactlyAsync(setup.BootstrapServers, setup.AuditTopic, expectedAudit.Count), expectedAudit);
    }

    private static void AssertPerKeyOrder(IReadOnlyList<(string Key, int Tag)> records, IReadOnlyList<(string Key, int Tag)> expected)
    {
        var want = expected.GroupBy(r => r.Key).ToDictionary(g => g.Key, g => g.Select(r => r.Tag).ToList());
        var got = records.GroupBy(r => r.Key).ToDictionary(g => g.Key, g => g.Select(r => r.Tag).ToList());
        Assert.Equal(want.Keys.Order(), got.Keys.Order());
        Assert.All(want, w => Assert.Equal(w.Value, got[w.Key]));
    }

    // The absence wait proves no extra record follows.
    private static async Task<IReadOnlyList<(string Key, int Tag)>> ConsumeExactlyAsync(string bootstrapServers, string topic, int expected)
    {
        var records = await ConsumeRawAsync(bootstrapServers, topic, expected);
        return [.. records.Select(r => (Encoding.UTF8.GetString(r.Message.Key), int.Parse(Encoding.UTF8.GetString(r.Message.Value), CultureInfo.InvariantCulture)))];
    }

    private static async Task<IReadOnlyList<ConsumeResult<byte[], byte[]>>> ConsumeRawAsync(string bootstrapServers, string topic, int expected)
    {
        Assert.Equal(expected, (await KafkaFixture.ConsumeAllAsync(bootstrapServers, topic, expected, DeliveryTimeout)).Count);
        var records = await KafkaFixture.ConsumeAllAsync(bootstrapServers, topic, expected + 1, AbsenceWait);
        Assert.Equal(expected, records.Count);
        return records;
    }

    private static string? Header(Headers headers, string name) =>
        headers.TryGetLastBytes(name, out var value) ? Encoding.UTF8.GetString(value) : null;

    private sealed record HeldRow(long Id, int Tag, string? LastError);

    private sealed class RoutesSetup
    {
        private readonly string connectionString;
        private readonly Dictionary<string, string?> settings;

        private RoutesSetup(string connectionString, string bootstrapServers, string schema, string alertsTopic, string auditTopic, string? deadLetterTopic, Dictionary<string, string?> settings)
        {
            this.connectionString = connectionString;
            this.settings = settings;
            BootstrapServers = bootstrapServers;
            Table = $"{schema}.outbox";
            AlertsTopic = alertsTopic;
            AuditTopic = auditTopic;
            DeadLetterTopic = deadLetterTopic;
            Outbox = settings["Outboxes:0:Name"]!;
        }

        public string BootstrapServers { get; }

        public string Table { get; }

        public string AlertsTopic { get; }

        public string AuditTopic { get; }

        public string? DeadLetterTopic { get; }

        public string Outbox { get; }

        public static async Task<RoutesSetup> CreateAsync(
            PostgreSqlFixture postgres,
            KafkaFixture kafka,
            string orderingMode,
            int partitions,
            IReadOnlyDictionary<string, string>? alertsConfigs = null,
            bool deadLetter = false)
        {
            var schema = await postgres.CreateOutboxAsync();
            var alertsTopic = KafkaFixture.NewTopic();
            var auditTopic = KafkaFixture.NewTopic();
            await kafka.CreateTopicAsync(alertsTopic, partitions, alertsConfigs);
            await kafka.CreateTopicAsync(auditTopic, partitions);

            var settings = RelayFactory.OutboxSettings(postgres.ConnectionString, kafka.BootstrapServers, (RelayFactory.NewOutboxName(), schema));
            settings.Remove("Destinations:0:Kafka:Topic");
            settings[$"Destinations:0:Kafka:Routes:{Alerts}"] = alertsTopic;
            settings[$"Destinations:0:Kafka:Routes:{Audit}"] = auditTopic;
            settings["Outboxes:0:Retry:InitialDelay"] = "00:00:01";
            settings["Outboxes:0:Retry:MaxDelay"] = "00:00:02";
            settings["Outboxes:0:Ordering:Mode"] = orderingMode;

            string? deadLetterTopic = null;
            if (deadLetter)
            {
                deadLetterTopic = KafkaFixture.NewTopic();
                await kafka.CreateTopicAsync(deadLetterTopic, 1);
                settings["Destinations:1:Name"] = DeliverySetup.DeadLetterDestinationName;
                settings["Destinations:1:Transport"] = "Kafka";
                settings["Destinations:1:Kafka:Cluster"] = "Main";
                settings["Destinations:1:Kafka:Topic"] = deadLetterTopic;
                settings["Outboxes:0:DeadLetter:Destination"] = DeliverySetup.DeadLetterDestinationName;
                settings["Outboxes:0:Retry:MaxRetryAttempts"] = "1";
            }

            return new RoutesSetup(postgres.ConnectionString, kafka.BootstrapServers, schema, alertsTopic, auditTopic, deadLetterTopic, settings);
        }

        public static string RouteOf(int tag, int keys) => tag / keys % 2 == 0 ? Alerts : Audit;

        public RelayFactory Relay() => new RelayFactory().WithRelaySettings(settings);

        public async Task<long> InsertAsync(string key, int tag, string? route, string? payload = null)
        {
            await using var connection = await OpenAsync();
            await using var command = new NpgsqlCommand(
                $"INSERT INTO {Table} (message_key, payload, route) VALUES (convert_to(@key, 'UTF8'), convert_to(@payload, 'UTF8'), @route) RETURNING id",
                connection);
            command.Parameters.AddWithValue("key", key);
            command.Parameters.AddWithValue("payload", payload ?? tag.ToString(CultureInfo.InvariantCulture));
            command.Parameters.Add(new NpgsqlParameter("route", NpgsqlTypes.NpgsqlDbType.Text) { Value = (object?)route ?? DBNull.Value });
            return (long)(await command.ExecuteScalarAsync(Ct))!;
        }

        // One statement, ORDER BY g: id order = tag order. Same rule as RouteOf.
        public async Task InsertAlternatingAsync(int total, int keys)
        {
            await using var connection = await OpenAsync();
            await using var command = new NpgsqlCommand(
                $"INSERT INTO {Table} (message_key, payload, route) " +
                "SELECT convert_to('k' || (g % @keys), 'UTF8'), convert_to(g::text, 'UTF8'), CASE WHEN (g / @keys) % 2 = 0 THEN @alerts ELSE @audit END " +
                "FROM generate_series(1, @total) g ORDER BY g",
                connection);
            command.Parameters.AddWithValue("keys", keys);
            command.Parameters.AddWithValue("total", total);
            command.Parameters.AddWithValue("alerts", Alerts);
            command.Parameters.AddWithValue("audit", Audit);
            await command.ExecuteNonQueryAsync(Ct);
        }

        public async Task<long> CountAsync()
        {
            await using var connection = await OpenAsync();
            await using var command = new NpgsqlCommand($"SELECT count(*) FROM {Table}", connection);
            return (long)(await command.ExecuteScalarAsync(Ct))!;
        }

        public async Task<IReadOnlyList<HeldRow>> FailureStatesAsync()
        {
            await using var connection = await OpenAsync();
            await using var command = new NpgsqlCommand($"SELECT id, convert_from(payload, 'UTF8')::int, last_error FROM {Table} ORDER BY id", connection);
            await using var reader = await command.ExecuteReaderAsync(Ct);
            var rows = new List<HeldRow>();
            while (await reader.ReadAsync(Ct))
            {
                rows.Add(new HeldRow(reader.GetInt64(0), reader.GetInt32(1), reader.IsDBNull(2) ? null : reader.GetString(2)));
            }

            return rows;
        }

        private async Task<NpgsqlConnection> OpenAsync()
        {
            var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(Ct);
            return connection;
        }
    }
}
