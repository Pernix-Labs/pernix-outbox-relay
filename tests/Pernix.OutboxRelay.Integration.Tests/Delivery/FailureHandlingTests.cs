using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using System.Globalization;
using System.Text;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Diagnostics;
using Pernix.OutboxRelay.Integration.Tests.Kafka;
using Pernix.OutboxRelay.Integration.Tests.Leadership;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Confluent.Kafka;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.Delivery;

public sealed class FailureHandlingTests(PostgreSqlFixture postgres, KafkaFixture kafka)
{
    private const string Tag = "faulty";
    private const string TransientError = "injected transient fault";
    private const string DeadLetterOutage = "injected dead-letter outage";

    private static readonly TimeSpan DeliveryTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan AbsenceWait = TimeSpan.FromSeconds(5);

    private static readonly string[] RelayHeaders =
        ["x-dead-letter-reason", "x-retry-attempts", "x-original-destination", "x-original-outbox", "x-original-id", "x-failed-at"];

    [Fact]
    public async Task Transient_fault_is_retried_with_backoff_then_delivered_and_deleted()
    {
        var setup = await DeliverySetup.CreateAsync(postgres, kafka);
        var rules = new FaultRules();
        var main = rules.Add(RelayFactory.DestinationName, Tag, 2, new DeliveryResult.MessageFault(TransientError, Permanent: false));
        using var counters = new FaultCounters(setup.Outbox);
        await using var relay = FaultInjectingDestinationFactory.Decorate(setup.Relay(), rules);
        _ = relay.Services;

        var row = TaggedRow("{\"id\":1}");
        var id = Assert.Single(await setup.InsertAsync([row]));
        var observed = await ObserveUntilDeletedAsync(setup, id);

        var retryCounts = observed.Select(s => s.RetryCount).Distinct().ToList();
        Assert.Equal([1, 2], retryCounts.Where(c => c > 0));
        Assert.All(observed.Where(s => s.RetryCount > 0), s => Assert.Equal(TransientError, s.LastError));
        Assert.Equal(3, main.Sends);

        var record = Assert.Single(await KafkaFixture.ConsumeAllAsync(setup.BootstrapServers, setup.Topic, 2, AbsenceWait));
        Assert.Equal(row.Payload, record.Message.Value);
        Assert.Equal(2, counters.Count(FaultCounters.MessageFaults, RelayFactory.DestinationName));
    }

    [Fact]
    public async Task Exhausted_retries_are_dead_lettered_with_original_record_and_relay_headers()
    {
        var setup = await DeliverySetup.CreateAsync(postgres, kafka, deadLetter: true, maxRetryAttempts: 1);
        var rules = new FaultRules();
        var main = rules.Add(RelayFactory.DestinationName, Tag, int.MaxValue, new DeliveryResult.MessageFault(TransientError, Permanent: false));
        using var counters = new FaultCounters(setup.Outbox);
        await using var relay = FaultInjectingDestinationFactory.Decorate(setup.Relay(), rules);
        _ = relay.Services;

        var row = TaggedRow("{\"id\":2}", ("idempotency-key", Guid.NewGuid().ToString()), ("type", "Ordér"));
        var startedAt = DateTimeOffset.UtcNow;
        var id = Assert.Single(await setup.InsertAsync([row]));
        await ObserveUntilDeletedAsync(setup, id);

        var record = await SingleDeadLetterAsync(setup);
        AssertDeadLetter(setup, record, row, id, TransientError, attempts: 2, startedAt);
        Assert.Equal(2, main.Sends);
        Assert.Empty(await KafkaFixture.ConsumeAllAsync(setup.BootstrapServers, setup.Topic, 1, AbsenceWait));
        Assert.Equal(2, counters.Count(FaultCounters.MessageFaults, RelayFactory.DestinationName));
        Assert.Equal(1, counters.Count(FaultCounters.DeadLettered, DeliverySetup.DeadLetterDestinationName));
    }

    [Fact]
    public async Task Oversized_for_main_topic_is_dead_lettered_on_first_attempt()
    {
        var setup = await DeliverySetup.CreateAsync(
            postgres,
            kafka,
            topicConfigs: new Dictionary<string, string> { ["max.message.bytes"] = "1024" },
            deadLetter: true,
            maxRetryAttempts: 1);
        using var counters = new FaultCounters(setup.Outbox);
        await using var relay = setup.Relay();
        _ = relay.Services;

        var row = new OutboxRow("big"u8.ToArray(), new byte[2048], new Dictionary<string, string> { ["idempotency-key"] = Guid.NewGuid().ToString() });
        var startedAt = DateTimeOffset.UtcNow;
        var id = Assert.Single(await setup.InsertAsync([row]));
        await ObserveUntilDeletedAsync(setup, id);

        var record = await SingleDeadLetterAsync(setup);
        AssertDeadLetter(setup, record, row, id, reason: null, attempts: 1, startedAt);
        Assert.Empty(await KafkaFixture.ConsumeAllAsync(setup.BootstrapServers, setup.Topic, 1, AbsenceWait));
        Assert.Equal(1, counters.Count(FaultCounters.MessageFaults, RelayFactory.DestinationName));
        Assert.Equal(1, counters.Count(FaultCounters.DeadLettered, DeliverySetup.DeadLetterDestinationName));
    }

    [Fact]
    public async Task Failed_dead_letter_publish_stays_pending_then_dead_letters_without_resending_to_main()
    {
        const int maxRetryAttempts = 1;
        var setup = await DeliverySetup.CreateAsync(postgres, kafka, deadLetter: true, maxRetryAttempts: maxRetryAttempts);
        var rules = new FaultRules();
        var main = rules.Add(RelayFactory.DestinationName, Tag, int.MaxValue, new DeliveryResult.MessageFault(TransientError, Permanent: false));
        var deadLetter = rules.Add(DeliverySetup.DeadLetterDestinationName, Tag, 2, new DeliveryResult.DestinationFault(DeadLetterOutage));
        using var counters = new FaultCounters(setup.Outbox);
        await using var relay = FaultInjectingDestinationFactory.Decorate(setup.Relay(), rules);
        _ = relay.Services;

        var row = TaggedRow("{\"id\":3}");
        var startedAt = DateTimeOffset.UtcNow;
        var id = Assert.Single(await setup.InsertAsync([row]));

        int? mainSendsWhenPending = null;
        var observed = await ObserveUntilDeletedAsync(setup, id, s =>
        {
            if (s.RetryCount > maxRetryAttempts)
            {
                mainSendsWhenPending ??= main.Sends;
            }
        });

        Assert.Contains(observed, s => s.RetryCount > maxRetryAttempts);
        Assert.All(observed.Where(s => s.RetryCount > maxRetryAttempts), s => Assert.Equal(TransientError, s.LastError));
        Assert.Equal(2, mainSendsWhenPending);
        Assert.Equal(2, main.Sends);
        Assert.Equal(3, deadLetter.Sends);

        var record = await SingleDeadLetterAsync(setup);
        AssertDeadLetter(setup, record, row, id, TransientError, attempts: 2, startedAt);
        Assert.Empty(await KafkaFixture.ConsumeAllAsync(setup.BootstrapServers, setup.Topic, 1, AbsenceWait));
        Assert.Equal(2, counters.Count(FaultCounters.DestinationFaults, DeliverySetup.DeadLetterDestinationName));
        Assert.Equal(1, counters.Count(FaultCounters.DeadLettered, DeliverySetup.DeadLetterDestinationName));
    }

    private static OutboxRow TaggedRow(string payload, params (string Name, string Value)[] headers)
    {
        var all = headers.ToDictionary(h => h.Name, h => h.Value);
        all[FaultRules.TagHeader] = Tag;
        return new OutboxRow(Encoding.UTF8.GetBytes($"key-{payload}"), Encoding.UTF8.GetBytes(payload), all);
    }

    // Samples the row every poll until the relay deletes it, so every retry_count it passes through is seen.
    private static async Task<IReadOnlyList<RowFailureState>> ObserveUntilDeletedAsync(DeliverySetup setup, long id, Action<RowFailureState>? onSample = null)
    {
        var observed = new List<RowFailureState>();
        Assert.True(
            await LeadershipTests.EventuallyAsync(
                async () =>
                {
                    var state = (await setup.FailureStatesAsync()).SingleOrDefault(s => s.Id == id);
                    if (state is null)
                    {
                        return true;
                    }

                    observed.Add(state);
                    onSample?.Invoke(state);
                    return false;
                },
                DeliveryTimeout),
            $"row {id} not deleted");
        return observed;
    }

    private static async Task<ConsumeResult<byte[], byte[]>> SingleDeadLetterAsync(DeliverySetup setup)
    {
        var first = await KafkaFixture.ConsumeAllAsync(setup.BootstrapServers, setup.DeadLetterTopic!, 1, DeliveryTimeout);
        Assert.Single(first);
        return Assert.Single(await KafkaFixture.ConsumeAllAsync(setup.BootstrapServers, setup.DeadLetterTopic!, 2, AbsenceWait));
    }

    private static void AssertDeadLetter(
        DeliverySetup setup,
        ConsumeResult<byte[], byte[]> record,
        OutboxRow row,
        long id,
        string? reason,
        int attempts,
        DateTimeOffset startedAt)
    {
        Assert.Equal(row.Key, record.Message.Key);
        Assert.Equal(row.Payload, record.Message.Value);

        var headers = record.Message.Headers.ToDictionary(h => h.Key, h => Encoding.UTF8.GetString(h.GetValueBytes()));
        Assert.Equal(row.Headers.Count + RelayHeaders.Length, headers.Count);
        Assert.All(row.Headers, h => Assert.Equal(h.Value, headers[h.Key]));
        Assert.All(RelayHeaders, name => Assert.True(headers.ContainsKey(name), $"missing {name}"));

        if (reason is not null)
        {
            Assert.Equal(reason, headers["x-dead-letter-reason"]);
        }
        else
        {
            Assert.False(string.IsNullOrWhiteSpace(headers["x-dead-letter-reason"]));
        }

        Assert.Equal(attempts.ToString(CultureInfo.InvariantCulture), headers["x-retry-attempts"]);
        Assert.Equal(RelayFactory.DestinationName, headers["x-original-destination"]);
        Assert.Equal(setup.Outbox, headers["x-original-outbox"]);
        Assert.Equal(id.ToString(CultureInfo.InvariantCulture), headers["x-original-id"]);

        var failedAt = DateTimeOffset.ParseExact(headers["x-failed-at"], "yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal);
        Assert.InRange(failedAt, startedAt.AddSeconds(-1), DateTimeOffset.UtcNow.AddSeconds(1));
    }
}

internal sealed class FaultCounters : IDisposable
{
    public const string MessageFaults = "outbox.message_faults";
    public const string DestinationFaults = "outbox.destination_faults";
    public const string DeadLettered = "outbox.dead_lettered";

    private static readonly HashSet<string> Instruments = [MessageFaults, DestinationFaults, DeadLettered];

    private readonly MeterListener listener = new();
    private readonly ConcurrentQueue<(string Instrument, string? Destination, long Value)> measurements = new();

    public FaultCounters(string outbox)
    {
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Meter.Name == RelayDiagnostics.Name && Instruments.Contains(instrument.Name))
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>((instrument, value, tags, _) =>
        {
            string? measuredOutbox = null;
            string? destination = null;
            foreach (var tag in tags)
            {
                switch (tag.Key)
                {
                    case "outbox":
                        measuredOutbox = tag.Value as string;
                        break;
                    case "destination":
                        destination = tag.Value as string;
                        break;
                }
            }

            if (measuredOutbox == outbox)
            {
                measurements.Enqueue((instrument.Name, destination, value));
            }
        });
        listener.Start();
    }

    public long Count(string instrument, string destination) =>
        measurements.Where(m => m.Instrument == instrument && m.Destination == destination).Sum(m => m.Value);

    public void Dispose() => listener.Dispose();
}
