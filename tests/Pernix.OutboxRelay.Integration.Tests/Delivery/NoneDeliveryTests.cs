using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Net;
using System.Text;
using System.Text.Json;
using Pernix.OutboxRelay.Core.Diagnostics;
using Pernix.OutboxRelay.Core.Leadership;
using Pernix.OutboxRelay.Integration.Tests.Kafka;
using Pernix.OutboxRelay.Integration.Tests.Leadership;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.Delivery;

public sealed class NoneDeliveryTests(PostgreSqlFixture postgres, KafkaFixture kafka)
{
    private static readonly TimeSpan DeliveryTimeout = TimeSpan.FromSeconds(30);

    // message.timeout.ms (30 s) plus slack: a paused broker surfaces as Local_MsgTimedOut.
    private static readonly TimeSpan BrokerStallTimeout = TimeSpan.FromSeconds(60);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Rows_arrive_with_key_value_and_headers_and_are_deleted()
    {
        var setup = await DeliverySetup.CreateAsync(postgres, kafka);
        using var delivered = new DeliveredCounter(setup.Outbox);
        await using var relay = setup.Relay();
        _ = relay.Services;

        var rows = new[]
        {
            new OutboxRow("order-1"u8.ToArray(), "{\"id\":1}"u8.ToArray(), new Dictionary<string, string> { ["idempotency-key"] = Guid.NewGuid().ToString(), ["type"] = "Ordér" }),
            new OutboxRow(null, "{\"id\":2}"u8.ToArray(), new Dictionary<string, string> { ["idempotency-key"] = Guid.NewGuid().ToString() }),
            new OutboxRow("order-3"u8.ToArray(), "{\"id\":3}"u8.ToArray(), new Dictionary<string, string>()),
        };
        await setup.InsertAsync(rows);

        var records = await KafkaFixture.ConsumeAllAsync(kafka.BootstrapServers, setup.Topic, rows.Length, DeliveryTimeout);

        Assert.Equal(rows.Length, records.Count);
        foreach (var row in rows)
        {
            var record = Assert.Single(records, r => r.Message.Value.AsSpan().SequenceEqual(row.Payload));
            Assert.Equal(row.Key, record.Message.Key);
            Assert.Equal(
                row.Headers.OrderBy(h => h.Key, StringComparer.Ordinal),
                record.Message.Headers.Select(h => KeyValuePair.Create(h.Key, Encoding.UTF8.GetString(h.GetValueBytes()))).OrderBy(h => h.Key, StringComparer.Ordinal));
        }

        Assert.True(await LeadershipTests.EventuallyAsync(async () => await setup.CountAsync() == 0, DeliveryTimeout), "table not empty");
        Assert.True(delivered.Total >= rows.Length, $"outbox.delivered = {delivered.Total}, expected >= {rows.Length}");
    }

    [Fact]
    public async Task Terminated_leader_disposes_destination_and_next_term_uses_new_one()
    {
        const int total = 2000;
        var setup = await DeliverySetup.CreateAsync(postgres, kafka, batchSize: 50);
        var recorder = new DestinationRecorder();
        await using var relay = RecordingDestinationFactory.Decorate(setup.Relay(), recorder);
        _ = relay.Services;

        // Destinations[0] is the startup bounds check: RelayStartupValidator creates the destination with the same context a term uses and disposes it in StartingAsync, before any worker runs.
        Assert.True(await LeadershipTests.EventuallyAsync(() => recorder.Destinations.Count == 2, TimeSpan.FromSeconds(10)), "no term-1 destination");
        Assert.NotNull(recorder.Destinations[0].DisposedAt);
        var term1 = recorder.Destinations[1];

        using var inserter = new StreamingInserter(setup, total, chunk: 100, pause: TimeSpan.FromMilliseconds(50));
        Assert.True(await inserter.DeliveryStartedAsync(DeliveryTimeout), "no delivery before terminate");
        var pid = await postgres.FindLockHolderPidAsync(setup.Table);
        Assert.NotNull(pid);

        var terminatedAt = DateTimeOffset.UtcNow;
        await postgres.TerminateAsync(pid.Value);

        Assert.True(await LeadershipTests.EventuallyAsync(() => term1.DisposedAt is not null, TimeSpan.FromSeconds(6)), "term-1 destination not disposed");
        Assert.True(term1.DisposedAt!.Value >= terminatedAt, $"term-1 disposed {terminatedAt - term1.DisposedAt} before terminate");
        Assert.True(term1.DisposedAt!.Value - terminatedAt <= TimeSpan.FromSeconds(6), $"term-1 disposed {term1.DisposedAt - terminatedAt} after terminate");
        Assert.True(await LeadershipTests.EventuallyAsync(() => recorder.Destinations.Count >= 3, TimeSpan.FromSeconds(10)), "no term-2 destination");
        var term2 = recorder.Destinations[2];
        Assert.NotSame(term1, term2);
        Assert.Null(term2.DisposedAt);

        await inserter.CompleteAsync();
        Assert.True(await LeadershipTests.EventuallyAsync(async () => await setup.CountAsync() == 0, DeliveryTimeout), "table not empty");
        Assert.Equal(Enumerable.Range(1, total).ToHashSet(), await setup.ConsumeDistinctTagsAsync(total, DeliveryTimeout));
    }

    [Fact]
    public async Task Oversized_row_without_dead_letter_is_dropped_and_later_rows_are_delivered()
    {
        var setup = await DeliverySetup.CreateAsync(postgres, kafka, topicConfigs: new Dictionary<string, string> { ["max.message.bytes"] = "1024" });

        await AssertRowDroppedAndDeliveryContinuesAsync(setup, new OutboxRow("k"u8.ToArray(), new byte[2048], new Dictionary<string, string>()));
    }

    [Fact]
    public async Task Null_key_on_compacted_topic_without_dead_letter_is_dropped_and_later_rows_are_delivered()
    {
        var setup = await DeliverySetup.CreateAsync(postgres, kafka, topicConfigs: new Dictionary<string, string> { ["cleanup.policy"] = "compact" });

        await AssertRowDroppedAndDeliveryContinuesAsync(setup, new OutboxRow(null, [1], new Dictionary<string, string>()));
    }

    [Fact]
    public async Task Paused_broker_keeps_rows_and_health_then_delivers_after_unpause()
    {
        await using var own = await KafkaFixture.StartOwnContainerAsync();
        var setup = await DeliverySetup.CreateAsync(postgres, own);
        var logs = new CapturingLoggerProvider();
        await using var relay = setup.Relay().WithServices(s => s.AddSingleton<ILoggerProvider>(logs));
        using var client = relay.CreateClient();
        var status = relay.Services.GetRequiredService<OutboxRuntimeState>().Outboxes.Single();
        Assert.True(await LeadershipTests.EventuallyAsync(() => status.IsLeader, TimeSpan.FromSeconds(10)), "no leader");

        await setup.InsertTagsAsync(1, 1);
        Assert.True(await LeadershipTests.EventuallyAsync(async () => await setup.CountAsync() == 0, DeliveryTimeout), "warm-up row not delivered");

        await own.Container.PauseAsync(Ct);
        try
        {
            await setup.InsertTagsAsync(2, 5);

            Assert.True(await LeadershipTests.EventuallyAsync(() => logs.Has("DeliveryPaused"), BrokerStallTimeout), "DeliveryPaused not logged");
            Assert.Equal(5, await setup.CountAsync());
            AssertNoFailureState(await setup.FailureStatesAsync());
            Assert.Equal(HttpStatusCode.OK, await StatusAsync(client, "/readyz"));
            Assert.Equal(HttpStatusCode.OK, await StatusAsync(client, "/livez"));
        }
        finally
        {
            await own.Container.UnpauseAsync(Ct);
        }

        var drained = new List<RowFailureState>();
        Assert.True(
            await LeadershipTests.EventuallyAsync(
                async () =>
                {
                    var states = await setup.FailureStatesAsync();
                    drained.AddRange(states);
                    return states.Count == 0;
                },
                BrokerStallTimeout),
            "rows not delivered after unpause");
        AssertNoFailureState(drained);
        Assert.Equal(Enumerable.Range(1, 6).ToHashSet(), await setup.ConsumeDistinctTagsAsync(6, DeliveryTimeout));
    }

    // A destination fault pauses delivery and never counts against the row.
    private static void AssertNoFailureState(IEnumerable<RowFailureState> states) =>
        Assert.All(states, s =>
        {
            Assert.Equal(0, s.RetryCount);
            Assert.Null(s.LastError);
        });

    private static async Task AssertRowDroppedAndDeliveryContinuesAsync(DeliverySetup setup, OutboxRow row)
    {
        var logs = new CapturingLoggerProvider();
        await using var relay = setup.Relay().WithServices(s => s.AddSingleton<ILoggerProvider>(logs));
        _ = relay.Services;

        await setup.InsertAsync([row]);
        Assert.True(await LeadershipTests.EventuallyAsync(() => logs.Has("MessageDropped"), DeliveryTimeout), "MessageDropped not logged");
        Assert.True(await LeadershipTests.EventuallyAsync(async () => await setup.CountAsync() == 0, DeliveryTimeout), "dropped row not deleted");

        await setup.InsertTagsAsync(1, 1);
        Assert.True(await LeadershipTests.EventuallyAsync(async () => await setup.CountAsync() == 0, DeliveryTimeout), "later row not delivered");
        Assert.Equal([1], await setup.ConsumeTagsAsync(1, DeliveryTimeout));
        Assert.Equal([1], await setup.ConsumeTagsAsync(2, TimeSpan.FromSeconds(5)));
        Assert.False(logs.Has("DeliveryPaused"));
    }

    private static async Task<HttpStatusCode> StatusAsync(HttpClient client, string path)
    {
        using var response = await client.GetAsync(path, Ct);
        return response.StatusCode;
    }
}

internal sealed record RowFailureState(long Id, int RetryCount, string? LastError);

internal sealed record OutboxRow(byte[]? Key, byte[] Payload, IReadOnlyDictionary<string, string> Headers);

internal sealed class DeliverySetup
{
    private readonly string connectionString;
    private readonly Dictionary<string, string?> settings;

    public const string DeadLetterDestinationName = "DeadLetters";

    private DeliverySetup(string connectionString, string bootstrapServers, string schema, string topic, string? deadLetterTopic, Dictionary<string, string?> settings)
    {
        this.connectionString = connectionString;
        this.settings = settings;
        BootstrapServers = bootstrapServers;
        Schema = schema;
        Topic = topic;
        DeadLetterTopic = deadLetterTopic;
        Outbox = settings["Outboxes:0:Name"]!;
    }

    public string BootstrapServers { get; }

    public string Schema { get; }

    public string Table => $"{Schema}.outbox";

    public string Topic { get; }

    public string? DeadLetterTopic { get; }

    public string Outbox { get; }

    public static async Task<DeliverySetup> CreateAsync(
        PostgreSqlFixture postgres,
        KafkaFixture kafka,
        int? batchSize = null,
        IReadOnlyDictionary<string, string>? topicConfigs = null,
        bool deadLetter = false,
        int? maxRetryAttempts = null,
        string orderingMode = "None",
        string? onPermanentFailure = null,
        int partitions = 1)
    {
        var schema = await postgres.CreateOutboxAsync();
        var topic = KafkaFixture.NewTopic();
        await kafka.CreateTopicAsync(topic, partitions, topicConfigs);

        var settings = RelayFactory.OutboxSettings(postgres.ConnectionString, kafka.BootstrapServers, (RelayFactory.NewOutboxName(), schema));
        settings["Destinations:0:Kafka:Topic"] = topic;
        settings["Outboxes:0:Retry:InitialDelay"] = "00:00:01";
        settings["Outboxes:0:Retry:MaxDelay"] = "00:00:02";
        settings["Outboxes:0:Ordering:Mode"] = orderingMode;
        if (onPermanentFailure is not null)
        {
            settings["Outboxes:0:Ordering:OnPermanentFailure"] = onPermanentFailure;
        }

        if (batchSize is { } size)
        {
            settings["Outboxes:0:Polling:BatchSize"] = size.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        if (maxRetryAttempts is { } max)
        {
            settings["Outboxes:0:Retry:MaxRetryAttempts"] = max.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        string? deadLetterTopic = null;
        if (deadLetter)
        {
            deadLetterTopic = KafkaFixture.NewTopic();
            await kafka.CreateTopicAsync(deadLetterTopic, 1);
            settings["Destinations:1:Name"] = DeadLetterDestinationName;
            settings["Destinations:1:Transport"] = "Kafka";
            settings["Destinations:1:Kafka:Cluster"] = "Main";
            settings["Destinations:1:Kafka:Topic"] = deadLetterTopic;
            settings["Outboxes:0:DeadLetter:Destination"] = DeadLetterDestinationName;
        }

        return new DeliverySetup(postgres.ConnectionString, kafka.BootstrapServers, schema, topic, deadLetterTopic, settings);
    }

    public RelayFactory Relay() => new RelayFactory().WithRelaySettings(settings);

    public async Task<IReadOnlyList<long>> InsertAsync(IEnumerable<OutboxRow> rows)
    {
        await using var connection = await OpenAsync();
        var ids = new List<long>();
        foreach (var row in rows)
        {
            await using var command = new NpgsqlCommand($"INSERT INTO {Table} (message_key, payload, headers) VALUES (@key, @payload, @headers::jsonb) RETURNING id", connection);
            command.Parameters.Add(new NpgsqlParameter("key", NpgsqlTypes.NpgsqlDbType.Bytea) { Value = (object?)row.Key ?? DBNull.Value });
            command.Parameters.AddWithValue("payload", row.Payload);
            command.Parameters.AddWithValue("headers", JsonSerializer.Serialize(row.Headers));
            ids.Add((long)(await command.ExecuteScalarAsync())!);
        }

        return ids;
    }

    // Key 'k<tag>' by default, or 'k<tag % keys>' when keys is set. ORDER BY g keeps id order = tag order.
    public async Task InsertTagsAsync(int first, int count, int? keys = null)
    {
        await using var connection = await OpenAsync();
        await using var command = new NpgsqlCommand(
            $"INSERT INTO {Table} (message_key, payload) SELECT convert_to('k' || CASE WHEN @keys::int IS NULL THEN g ELSE g % @keys::int END, 'UTF8'), convert_to(g::text, 'UTF8') FROM generate_series(@first, @last) g ORDER BY g",
            connection);
        command.Parameters.AddWithValue("first", first);
        command.Parameters.AddWithValue("last", first + count - 1);
        command.Parameters.Add(new NpgsqlParameter("keys", NpgsqlTypes.NpgsqlDbType.Integer) { Value = (object?)keys ?? DBNull.Value });
        await command.ExecuteNonQueryAsync();
    }

    public async Task<long> CountAsync()
    {
        await using var connection = await OpenAsync();
        await using var command = new NpgsqlCommand($"SELECT count(*) FROM {Table}", connection);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    public async Task<IReadOnlyList<RowFailureState>> FailureStatesAsync()
    {
        await using var connection = await OpenAsync();
        await using var command = new NpgsqlCommand($"SELECT id, retry_count, last_error FROM {Table} ORDER BY id", connection);
        await using var reader = await command.ExecuteReaderAsync();
        var states = new List<RowFailureState>();
        while (await reader.ReadAsync())
        {
            states.Add(new RowFailureState(reader.GetInt64(0), reader.GetInt32(1), reader.IsDBNull(2) ? null : reader.GetString(2)));
        }

        return states;
    }

    public async Task<IReadOnlySet<int>> TagsInTableAsync()
    {
        await using var connection = await OpenAsync();
        await using var command = new NpgsqlCommand($"SELECT convert_from(payload, 'UTF8')::int FROM {Table}", connection);
        await using var reader = await command.ExecuteReaderAsync();
        var tags = new HashSet<int>();
        while (await reader.ReadAsync())
        {
            tags.Add(reader.GetInt32(0));
        }

        return tags;
    }

    public async Task<IReadOnlyList<int>> ConsumeTagsAsync(int expected, TimeSpan timeout)
    {
        var records = await KafkaFixture.ConsumeAllAsync(BootstrapServers, Topic, expected, timeout);
        return [.. records.Select(r => int.Parse(Encoding.UTF8.GetString(r.Message.Value), System.Globalization.CultureInfo.InvariantCulture))];
    }

    // Redeliveries after a lost term are duplicates, so keep consuming until every distinct tag 1..total is seen.
    public async Task<IReadOnlySet<int>> ConsumeDistinctTagsAsync(int total, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        var expected = total;
        while (true)
        {
            var tags = await ConsumeTagsAsync(expected, deadline - DateTime.UtcNow);
            var distinct = tags.ToHashSet();
            if (distinct.Count >= total || DateTime.UtcNow >= deadline)
            {
                return distinct;
            }

            expected = tags.Count + total - distinct.Count;
        }
    }

    public async Task<NpgsqlConnection> OpenAsync()
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        return connection;
    }
}

internal sealed class StreamingInserter : IDisposable
{
    private readonly DeliverySetup setup;
    private readonly CancellationTokenSource stop = new();
    private readonly Task run;
    private int inserted;

    public StreamingInserter(DeliverySetup setup, int total, int chunk, TimeSpan pause, int? keys = null)
    {
        this.setup = setup;
        run = Task.Run(async () =>
        {
            while (inserted < total && !stop.IsCancellationRequested)
            {
                var count = Math.Min(chunk, total - inserted);
                await setup.InsertTagsAsync(inserted + 1, count, keys);
                Interlocked.Add(ref inserted, count);
                await Task.Delay(pause, CancellationToken.None);
            }
        });
    }

    public int Inserted => Volatile.Read(ref inserted);

    public Task CompleteAsync() => run;

    // Snapshot Inserted before counting, or a chunk committed after the count reads as a delivery.
    public Task<bool> DeliveryStartedAsync(TimeSpan timeout) =>
        LeadershipTests.EventuallyAsync(
            async () =>
            {
                var inserted = Inserted;
                return inserted > 0 && await setup.CountAsync() < inserted;
            },
            timeout);

    public async Task StopAsync()
    {
        await stop.CancelAsync();
        await run;
    }

    public void Dispose()
    {
        stop.Cancel();
        stop.Dispose();
    }
}

internal sealed class DeliveredCounter : IDisposable
{
    private const string InstrumentName = "outbox.delivered";

    private readonly MeterListener listener = new();
    private long total;

    public DeliveredCounter(string outbox)
    {
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == RelayDiagnostics.Name && instrument.Name == InstrumentName)
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((_, value, tags, _) =>
        {
            foreach (var tag in tags)
            {
                if (tag.Key == "outbox" && Equals(tag.Value, outbox))
                {
                    Interlocked.Add(ref total, value);
                }
            }
        });
        listener.Start();
    }

    public long Total => Interlocked.Read(ref total);

    public void Dispose() => listener.Dispose();
}

internal sealed class CapturingLoggerProvider : ILoggerProvider
{
    private readonly ConcurrentQueue<EventId> events = new();

    public bool Has(string eventName) => events.Any(e => e.Name == eventName);

    public ILogger CreateLogger(string categoryName) => new CapturingLogger(events);

    public void Dispose()
    {
    }

    private sealed class CapturingLogger(ConcurrentQueue<EventId> events) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
            events.Enqueue(eventId);
    }
}
