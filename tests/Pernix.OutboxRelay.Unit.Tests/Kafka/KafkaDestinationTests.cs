using System.Reflection;
using System.Text;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Destination.Kafka;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Kafka;

public sealed class KafkaDestinationTests
{
    private readonly KafkaHarness harness = new();

    [Fact]
    public async Task Send_maps_key_value_and_headers_and_returns_delivered()
    {
        await using var destination = harness.Create();
        var message = KafkaHarness.Message([1, 2], [3, 4, 5], new Dictionary<string, string> { ["traceparent"] = "00-abc", ["type"] = "Order" });

        var send = destination.SendAsync(message, CancellationToken.None);
        var produced = Assert.Single(Assert.Single(harness.Producers).Sends);
        produced.Persist();

        Assert.IsType<DeliveryResult.Delivered>(await send);
        Assert.Equal(KafkaHarness.Topic, produced.Topic);
        Assert.Equal(new byte[] { 1, 2 }, produced.Message.Key);
        Assert.Equal(new byte[] { 3, 4, 5 }, produced.Message.Value);
        Assert.Equal(["traceparent", "type"], produced.Message.Headers.Select(h => h.Key));
        Assert.Equal("00-abc", Encoding.UTF8.GetString(produced.Message.Headers[0].GetValueBytes()));
    }

    [Theory]
    [InlineData(OrderingMode.None, 30_000)]
    [InlineData(OrderingMode.PerKey, null)]
    [InlineData(OrderingMode.Strict, null)]
    public async Task Send_bound_is_the_forced_message_timeout_of_the_mode(OrderingMode mode, int? expectedMs)
    {
        var (options, relay) = harness.Bind();
        Assert.Empty(harness.Factory.Validate(options, $"Destinations[{options.Name}]", relay));
        await using var destination = harness.Factory.Create(options, new DestinationContext(mode));

        Assert.Equal(expectedMs is { } ms ? TimeSpan.FromMilliseconds(ms) : null, destination.SendBound);
    }

    [Fact]
    public async Task Null_key_is_sent_as_null()
    {
        await using var destination = harness.Create();

        var send = destination.SendAsync(KafkaHarness.Message(null, [1]), CancellationToken.None);
        var produced = Assert.Single(Assert.Single(harness.Producers).Sends);
        produced.Persist();

        Assert.IsType<DeliveryResult.Delivered>(await send);
        Assert.Null(produced.Message.Key);
    }

    [Fact]
    public async Task Send_before_topic_is_described_is_destination_fault_without_produce()
    {
        var (options, relay) = harness.Bind();
        Assert.Empty(harness.Factory.Validate(options, $"Destinations[{options.Name}]", relay));
        await using var destination = harness.Factory.Create(options, new DestinationContext(OrderingMode.None));

        var result = await destination.SendAsync(KafkaHarness.Message([1], [1]), CancellationToken.None);

        var fault = Assert.IsType<DeliveryResult.DestinationFault>(result);
        Assert.Equal("Local_State: topic not described, ValidateAsync has not run", fault.Error);
        Assert.Empty(harness.Producers);
    }

    [Fact]
    public async Task Oversized_message_is_permanent_fault_without_produce()
    {
        await using var destination = harness.Create(new TopicConfig(1024, Compacted: false));

        var result = await destination.SendAsync(KafkaHarness.Message([1], new byte[2048]), CancellationToken.None);

        var fault = Assert.IsType<DeliveryResult.MessageFault>(result);
        Assert.True(fault.Permanent);
        Assert.Equal($"MsgSizeTooLarge: record up to {KafkaRecordSize.UpperBound([1], new byte[2048], MessageHeaders.Empty)} bytes exceeds limit 1024", fault.Error);
        Assert.Empty(harness.Producers);
    }

    [Fact]
    public async Task Producer_message_max_bytes_limits_size_when_smaller_than_topic()
    {
        harness.Set("KafkaClusters:Main:Producer:message.max.bytes", "500");
        await using var destination = harness.Create(new TopicConfig(1_000_000, Compacted: false));

        var result = await destination.SendAsync(KafkaHarness.Message([1], new byte[600]), CancellationToken.None);

        Assert.EndsWith("exceeds limit 500", Assert.IsType<DeliveryResult.MessageFault>(result).Error, StringComparison.Ordinal);
        Assert.Empty(harness.Producers);
    }

    [Fact]
    public async Task Null_key_on_compacted_topic_is_permanent_fault_without_produce()
    {
        await using var destination = harness.Create(new TopicConfig(1_000_000, Compacted: true));

        var result = await destination.SendAsync(KafkaHarness.Message(null, [1]), CancellationToken.None);

        Assert.Equal(new DeliveryResult.MessageFault("InvalidRecord: null key on compacted topic", Permanent: true), result);
        Assert.Empty(harness.Producers);
    }

    [Fact]
    public async Task Compacted_topic_named_as_dead_letter_destination_fails_validation()
    {
        harness.Set("Outboxes:0:Name", "Orders");
        harness.Set("Outboxes:0:DeadLetter:Destination", KafkaHarness.Destination.ToUpperInvariant());
        await using var destination = harness.Create(new TopicConfig(1_000_000, Compacted: true), DestinationContext.Probe);

        var ex = await Assert.ThrowsAsync<DestinationValidationException>(() => destination.ValidateAsync(CancellationToken.None));

        Assert.Equal($"Destinations[{KafkaHarness.Destination}]: topic '{KafkaHarness.Topic}' is compacted and cannot hold dead letters of outbox 'Orders'", ex.Message);
        Assert.Empty(harness.Producers);
    }

    [Fact]
    public async Task Compacted_topic_not_named_as_dead_letter_destination_passes_validation()
    {
        harness.Set("Outboxes:0:Name", "Orders");
        harness.Set("Outboxes:0:DeadLetter:Destination", "");
        await using var destination = harness.Create(new TopicConfig(1_000_000, Compacted: true), DestinationContext.Probe);

        await destination.ValidateAsync(CancellationToken.None);
    }

    [Fact]
    public async Task Keyed_message_on_compacted_topic_is_sent()
    {
        await using var destination = harness.Create(new TopicConfig(1_000_000, Compacted: true));

        var send = destination.SendAsync(KafkaHarness.Message([1], [1]), CancellationToken.None);
        Assert.Single(Assert.Single(harness.Producers).Sends).Persist();

        Assert.IsType<DeliveryResult.Delivered>(await send);
    }

    [Fact]
    public async Task Dead_letter_send_carries_original_and_x_headers()
    {
        await using var destination = harness.Create();
        var message = KafkaHarness.Message([1], [2], new Dictionary<string, string> { ["type"] = "Order", [KafkaDeadLetterHeaders.OriginalId] = "999" }) with
        {
            DeadLetter = new DeadLetterInfo("InvalidRecord: bad", 3, new DateTimeOffset(2026, 11, 3, 4, 5, 6, 789, TimeSpan.FromHours(2))),
        };

        var send = destination.SendAsync(message, CancellationToken.None);
        var produced = Assert.Single(Assert.Single(harness.Producers).Sends);
        produced.Persist();

        Assert.IsType<DeliveryResult.Delivered>(await send);
        var headers = produced.Message.Headers.ToDictionary(h => h.Key, h => Encoding.UTF8.GetString(h.GetValueBytes()));
        Assert.Equal(7, produced.Message.Headers.Count);
        Assert.Equal("Order", headers["type"]);
        Assert.Equal("1", headers[KafkaDeadLetterHeaders.OriginalId]);
        Assert.Equal("3", headers[KafkaDeadLetterHeaders.RetryAttempts]);
        Assert.Equal("2026-11-03T02:05:06.789Z", headers[KafkaDeadLetterHeaders.FailedAt]);
    }

    [Fact]
    public async Task Dead_letter_sends_share_cached_name_bytes_left_unchanged()
    {
        const int Sends = 100;
        await using var destination = harness.Create();
        var message = KafkaHarness.Message([1], [2], new Dictionary<string, string> { ["type"] = "Order" }) with
        {
            DeadLetter = new DeadLetterInfo("InvalidRecord: bad", 3, DateTimeOffset.UnixEpoch),
        };

        for (var i = 0; i < Sends; i++)
        {
            var send = destination.SendAsync(message, CancellationToken.None);
            Assert.Single(harness.Producers).Sends[^1].Persist();
            Assert.IsType<DeliveryResult.Delivered>(await send);
        }

        var fresh = KafkaDeadLetterHeaders.Merge(message);
        var outbox = fresh.Single(h => h.Name == KafkaDeadLetterHeaders.OriginalOutbox).Value;
        var destinationName = fresh.Single(h => h.Name == KafkaDeadLetterHeaders.OriginalDestination).Value;
        var produced = Assert.Single(harness.Producers).Sends;
        Assert.Equal(Sends, produced.Count);
        foreach (var sent in produced)
        {
            Assert.Same(outbox, sent.Message.Headers.Single(h => h.Key == KafkaDeadLetterHeaders.OriginalOutbox).GetValueBytes());
            Assert.Same(destinationName, sent.Message.Headers.Single(h => h.Key == KafkaDeadLetterHeaders.OriginalDestination).GetValueBytes());
        }

        Assert.Equal(Encoding.UTF8.GetBytes(message.Source.Outbox), outbox);
        Assert.Equal(Encoding.UTF8.GetBytes(message.Source.Destination), destinationName);
    }

    [Fact]
    public async Task Send_without_dead_letter_adds_no_headers()
    {
        await using var destination = harness.Create();

        var send = destination.SendAsync(KafkaHarness.Message([1], [2], new Dictionary<string, string> { ["type"] = "Order" }), CancellationToken.None);
        var produced = Assert.Single(Assert.Single(harness.Producers).Sends);
        produced.Persist();

        Assert.IsType<DeliveryResult.Delivered>(await send);
        Assert.Equal(["type"], produced.Message.Headers.Select(h => h.Key));
    }

    [Fact]
    public async Task Dead_letter_oversized_only_by_x_headers_is_permanent_fault_without_produce()
    {
        var message = KafkaHarness.Message([1], new byte[100]) with
        {
            DeadLetter = new DeadLetterInfo("reason", 1, DateTimeOffset.UnixEpoch),
        };
        var plain = KafkaRecordSize.UpperBound(message.Key, message.Payload, message.Headers);
        var merged = KafkaRecordSize.UpperBound(message.Key, message.Payload, KafkaDeadLetterHeaders.Merge(message));
        await using var destination = harness.Create(new TopicConfig((int)plain, Compacted: false));

        var result = await destination.SendAsync(message, CancellationToken.None);

        Assert.Equal(new DeliveryResult.MessageFault($"MsgSizeTooLarge: record up to {merged} bytes exceeds limit {plain}", Permanent: true), result);
        Assert.Empty(harness.Producers);
    }

    [Theory]
    [InlineData(PersistenceStatus.NotPersisted)]
    [InlineData(PersistenceStatus.PossiblyPersisted)]
    public async Task Report_not_persisted_is_destination_fault(PersistenceStatus status)
    {
        await using var destination = harness.Create();

        var send = destination.SendAsync(KafkaHarness.Message([1], [1]), CancellationToken.None);
        Assert.Single(Assert.Single(harness.Producers).Sends).Complete(status);

        Assert.Equal(new DeliveryResult.DestinationFault($"{status}: not confirmed"), await send);
    }

    [Fact]
    public async Task Produce_failure_is_classified_by_error_code()
    {
        await using var destination = harness.Create();

        var send = destination.SendAsync(KafkaHarness.Message([1], [1]), CancellationToken.None);
        Assert.Single(Assert.Single(harness.Producers).Sends).Fail(new Error(ErrorCode.Local_MsgTimedOut, "Message timed out"));

        Assert.Equal(new DeliveryResult.DestinationFault("Local_MsgTimedOut: Message timed out"), await send);
    }

    [Fact]
    public async Task Out_of_order_completions_each_return_their_own_result()
    {
        await using var destination = harness.Create();

        var first = destination.SendAsync(KafkaHarness.Message([1], [1]), CancellationToken.None);
        var second = destination.SendAsync(KafkaHarness.Message([2], [2]), CancellationToken.None);
        var third = destination.SendAsync(KafkaHarness.Message([3], [3]), CancellationToken.None);
        var sends = Assert.Single(harness.Producers).Sends;

        sends[2].Fail(new Error(ErrorCode.MsgSizeTooLarge, "too large"));
        sends[0].Fail(new Error(ErrorCode.Local_AllBrokersDown, "down"));
        sends[1].Persist();

        Assert.Equal(new DeliveryResult.DestinationFault("Local_AllBrokersDown: down"), await first);
        Assert.IsType<DeliveryResult.Delivered>(await second);
        Assert.Equal(new DeliveryResult.MessageFault("MsgSizeTooLarge: too large", Permanent: true), await third);
    }

    [Fact]
    public async Task Cancelled_send_ends_the_await_and_ignores_the_later_report()
    {
        await using var destination = harness.Create();
        using var cts = new CancellationTokenSource();

        var send = destination.SendAsync(KafkaHarness.Message([1], [1]), cts.Token).AsTask();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => send);
        Assert.Single(Assert.Single(harness.Producers).Sends).Persist();
        Assert.True(send.IsCanceled);
    }

    [Fact]
    public async Task Producer_is_built_lazily_once()
    {
        await using var destination = harness.Create();
        Assert.Empty(harness.Producers);

        var first = destination.SendAsync(KafkaHarness.Message([1], [1]), CancellationToken.None).AsTask();
        var second = destination.SendAsync(KafkaHarness.Message([2], [2]), CancellationToken.None).AsTask();
        foreach (var send in Assert.Single(harness.Producers).Sends)
        {
            send.Persist();
        }

        await Task.WhenAll(first, second);
        Assert.Single(harness.Producers);
    }

    [Fact]
    public async Task Fatal_error_returns_destination_fault_and_next_send_builds_new_producer()
    {
        await using var destination = harness.Create();

        var failing = destination.SendAsync(KafkaHarness.Message([1], [1]), CancellationToken.None);
        Assert.Single(Assert.Single(harness.Producers).Sends).Fail(new Error(ErrorCode.Local_Fatal, "fatal idempotence error", isFatal: true));
        Assert.Equal(new DeliveryResult.DestinationFault("Local_Fatal: fatal idempotence error"), await failing);

        var next = destination.SendAsync(KafkaHarness.Message([2], [2]), CancellationToken.None);
        await KafkaHarness.WaitUntilAsync(() => harness.Producers.Count == 2 && harness.Producers[1].Sends.Count == 1);
        harness.Producers[1].Sends[0].Persist();

        Assert.IsType<DeliveryResult.Delivered>(await next);
        Assert.Equal(1, harness.Producers[0].DisposeCalls);
        Assert.Equal(0, harness.Producers[0].FlushCalls);
        Assert.Empty(harness.Producers[0].Sends.Skip(1));
    }

    [Fact]
    public async Task Rebuild_ends_sends_outstanding_on_the_old_producer_with_destination_fault()
    {
        await using var destination = harness.Create();

        var failing = destination.SendAsync(KafkaHarness.Message([1], [1]), CancellationToken.None);
        var outstanding = destination.SendAsync(KafkaHarness.Message([2], [2]), CancellationToken.None).AsTask();
        Assert.Single(harness.Producers).Sends[0].Fail(new Error(ErrorCode.Local_Fatal, "fatal idempotence error", isFatal: true));
        await failing;

        var next = destination.SendAsync(KafkaHarness.Message([3], [3]), CancellationToken.None);
        await KafkaHarness.WaitUntilAsync(() => harness.Producers.Count == 2 && harness.Producers[1].Sends.Count == 1);

        Assert.Equal(
            new DeliveryResult.DestinationFault("Local_Fatal: producer failed fatally before delivery report"),
            await outstanding.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
        harness.Producers[1].Sends[0].Persist();
        Assert.IsType<DeliveryResult.Delivered>(await next);
    }

    [Fact]
    public async Task Dispose_after_fatal_drops_broken_producer_reference()
    {
        var destination = harness.Create();
        var failing = destination.SendAsync(KafkaHarness.Message([1], [1]), CancellationToken.None);
        Assert.Single(Assert.Single(harness.Producers).Sends).Fail(new Error(ErrorCode.Local_Fatal, "fatal idempotence error", isFatal: true));
        await failing;
        Assert.NotNull(BrokenProducer(destination));

        await destination.DisposeAsync();

        Assert.Null(BrokenProducer(destination));
    }

    [Fact]
    public async Task Fatal_from_error_handler_ends_outstanding_send_without_further_send()
    {
        await using var destination = harness.Create();

        var outstanding = destination.SendAsync(KafkaHarness.Message([1], [1]), CancellationToken.None).AsTask();
        Assert.Single(Assert.Single(harness.Producers).Sends);

        Assert.Single(harness.FatalCallbacks)(new Error(ErrorCode.Local_Fatal, "fatal idempotence error", isFatal: true));

        Assert.Equal(
            new DeliveryResult.DestinationFault("Local_Fatal: producer failed fatally before delivery report"),
            await outstanding.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
        Assert.Single(harness.Producers);
    }

    private static object? BrokenProducer(IDestination destination) =>
        destination.GetType().GetField("brokenProducer", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(destination);

    [Fact]
    public async Task Fatal_flag_on_any_code_marks_producer_broken()
    {
        await using var destination = harness.Create();

        var failing = destination.SendAsync(KafkaHarness.Message([1], [1]), CancellationToken.None);
        Assert.Single(Assert.Single(harness.Producers).Sends).Fail(new Error(ErrorCode.OutOfOrderSequenceNumber, "sequence", isFatal: true));
        await failing;

        _ = destination.SendAsync(KafkaHarness.Message([2], [2]), CancellationToken.None);

        await KafkaHarness.WaitUntilAsync(() => harness.Producers.Count == 2);
    }

    [Fact]
    public async Task Fatal_from_error_handler_while_idle_next_send_builds_new_producer()
    {
        await using var destination = harness.Create();

        var first = destination.SendAsync(KafkaHarness.Message([1], [1]), CancellationToken.None);
        Assert.Single(Assert.Single(harness.Producers).Sends).Persist();
        Assert.IsType<DeliveryResult.Delivered>(await first);

        Assert.Single(harness.FatalCallbacks)(new Error(ErrorCode.Local_Fatal, "fatal idempotence error", isFatal: true));

        var next = destination.SendAsync(KafkaHarness.Message([2], [2]), CancellationToken.None);
        await KafkaHarness.WaitUntilAsync(() => harness.Producers.Count == 2 && harness.Producers[1].Sends.Count == 1);
        harness.Producers[1].Sends[0].Persist();

        Assert.IsType<DeliveryResult.Delivered>(await next);
        Assert.Equal(1, harness.Producers[0].DisposeCalls);
        Assert.Equal(0, harness.Producers[0].FlushCalls);
        Assert.Single(harness.Producers[0].Sends);
    }

    [Fact]
    public async Task Fatal_from_error_handler_during_build_marks_that_producer_broken()
    {
        await using var destination = harness.Create();
        harness.FatalDuringBuild = new Error(ErrorCode.Local_Fatal, "fatal during build", isFatal: true);

        _ = destination.SendAsync(KafkaHarness.Message([1], [1]), CancellationToken.None);
        await KafkaHarness.WaitUntilAsync(() => harness.Producers.Count == 1 && harness.Producers[0].Sends.Count == 1);

        var next = destination.SendAsync(KafkaHarness.Message([2], [2]), CancellationToken.None);
        await KafkaHarness.WaitUntilAsync(() => harness.Producers.Count == 2 && harness.Producers[1].Sends.Count == 1);
        harness.Producers[1].Sends[0].Persist();

        Assert.IsType<DeliveryResult.Delivered>(await next);
        Assert.Equal(1, harness.Producers[0].DisposeCalls);
        Assert.Equal(0, harness.Producers[0].FlushCalls);
    }

    [Fact]
    public async Task Non_fatal_error_keeps_producer()
    {
        await using var destination = harness.Create();

        var failing = destination.SendAsync(KafkaHarness.Message([1], [1]), CancellationToken.None);
        Assert.Single(Assert.Single(harness.Producers).Sends).Fail(new Error(ErrorCode.Local_AllBrokersDown, "down"));
        await failing;

        _ = destination.SendAsync(KafkaHarness.Message([2], [2]), CancellationToken.None);

        Assert.Single(harness.Producers);
    }

    [Fact]
    public async Task Dispose_disposes_producer_without_flush_and_is_idempotent()
    {
        var destination = harness.Create();
        var pending = destination.SendAsync(KafkaHarness.Message([1], [1]), CancellationToken.None);
        var producer = Assert.Single(harness.Producers);

        await destination.DisposeAsync();
        await destination.DisposeAsync();

        Assert.Equal(1, producer.DisposeCalls);
        Assert.Equal(0, producer.FlushCalls);
        Assert.False(pending.IsCompleted);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => destination.SendAsync(KafkaHarness.Message([1], [1]), CancellationToken.None).AsTask());
    }

    [Fact]
    public async Task Dispose_without_send_builds_no_producer()
    {
        var destination = harness.Create();

        await destination.DisposeAsync();

        Assert.Empty(harness.Producers);
    }

    [Fact]
    public async Task Cancelling_send_token_cancels_the_await_only()
    {
        await using var destination = harness.Create();
        using var cts = new CancellationTokenSource();

        var send = destination.SendAsync(KafkaHarness.Message([1], [1]), cts.Token).AsTask();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => send);
        Assert.Equal(0, Assert.Single(harness.Producers).DisposeCalls);
    }

    [Fact]
    public async Task Validate_on_cache_hit_for_outbox_destination_builds_no_producer()
    {
        await using var destination = harness.Create();

        await destination.ValidateAsync(CancellationToken.None);

        Assert.Empty(harness.Producers);
    }

    [Fact]
    public async Task Probe_validate_builds_and_disposes_one_producer()
    {
        await using var destination = harness.Create(context: DestinationContext.Probe);

        await destination.ValidateAsync(CancellationToken.None);

        var producer = Assert.Single(harness.Producers);
        Assert.Equal(1, producer.DisposeCalls);
        Assert.Equal(0, producer.FlushCalls);
    }

    [Fact]
    public async Task Probe_validate_with_invalid_producer_property_fails_without_value()
    {
        var harness = new KafkaHarness(_ => throw new ArgumentException("Invalid value \"secret-value-91\" for configuration property \"linger.ms\""));
        await using var destination = harness.Create(context: DestinationContext.Probe);

        var ex = await Assert.ThrowsAsync<DestinationValidationException>(() => destination.ValidateAsync(CancellationToken.None));

        Assert.Equal("Destinations[OrdersTopic]: invalid producer configuration for cluster 'Main'", ex.Message);
        Assert.DoesNotContain("secret-value-91", ex.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Send_with_unbuildable_producer_is_destination_fault()
    {
        var harness = new KafkaHarness(_ => throw new ArgumentException("Invalid value \"secret-value-91\""));
        await using var destination = harness.Create();

        var result = await destination.SendAsync(KafkaHarness.Message([1], [1]), CancellationToken.None);

        var fault = Assert.IsType<DeliveryResult.DestinationFault>(result);
        Assert.DoesNotContain("secret-value-91", fault.Error, StringComparison.Ordinal);
    }

    [Fact]
    public void Producer_receives_forced_settings()
    {
        ProducerConfig? seen = null;
        var harness = new KafkaHarness(config =>
        {
            seen = config;
            return new FakeProducer();
        });
        harness.Set("KafkaClusters:Main:Producer:acks", "1");
        var destination = harness.Create();

        _ = destination.SendAsync(KafkaHarness.Message([1], [1]), CancellationToken.None);

        Assert.Equal("all", seen!.Get("acks"));
        Assert.Equal("kafka-1:9092", seen.Get("bootstrap.servers"));
    }

    [Fact]
    public async Task Routes_destination_produces_each_route_to_its_mapped_topic()
    {
        harness.UseRoutes(("alerts", "risk.alerts"), ("audit", "risk.audit"));
        await using var destination = harness.Create();

        var alerts = destination.SendAsync(Routed("alerts"), CancellationToken.None);
        var audit = destination.SendAsync(Routed("audit"), CancellationToken.None);
        var sends = Assert.Single(harness.Producers).Sends;
        foreach (var send in sends)
        {
            send.Persist();
        }

        Assert.IsType<DeliveryResult.Delivered>(await alerts);
        Assert.IsType<DeliveryResult.Delivered>(await audit);
        Assert.Equal(["risk.alerts", "risk.audit"], sends.Select(s => s.Topic));
        var byRoute = harness.Factory.AddressesOf(KafkaHarness.Destination).ByRoute!;
        Assert.Same(byRoute["alerts"].TopicPartition, sends[0].TopicPartition);
        Assert.Same(byRoute["audit"].TopicPartition, sends[1].TopicPartition);
    }

    [Fact]
    public async Task Two_routes_on_one_topic_share_its_address()
    {
        harness.UseRoutes(("alerts", "risk.events"), ("audit", "risk.events"));
        await using var destination = harness.Create();

        _ = destination.SendAsync(Routed("alerts"), CancellationToken.None);
        _ = destination.SendAsync(Routed("audit"), CancellationToken.None);
        var sends = Assert.Single(harness.Producers).Sends;

        Assert.Equal(["risk.events", "risk.events"], sends.Select(s => s.Topic));
        Assert.Same(sends[0].TopicPartition, sends[1].TopicPartition);
        Assert.Single(harness.Factory.AddressesOf(KafkaHarness.Destination).Distinct);
    }

    [Fact]
    public async Task Topic_destination_produces_with_its_cached_topic_partition()
    {
        await using var destination = harness.Create();

        _ = destination.SendAsync(KafkaHarness.Message([1], [1]), CancellationToken.None);
        _ = destination.SendAsync(KafkaHarness.Message([2], [2]), CancellationToken.None);
        var sends = Assert.Single(harness.Producers).Sends;

        var cached = harness.Factory.AddressesOf(KafkaHarness.Destination).Single!.TopicPartition;
        Assert.All(sends, s => Assert.Same(cached, s.TopicPartition));
        Assert.Equal(new TopicPartition(KafkaHarness.Topic, Partition.Any), cached);
    }

    [Theory]
    [InlineData("alerts")]
    [InlineData(null)]
    public async Task Dead_letter_on_topic_destination_goes_to_its_topic(string? route)
    {
        await using var destination = harness.Create();
        var message = KafkaHarness.Message([1], [1]) with
        {
            Route = route,
            DeadLetter = new DeadLetterInfo("reason", 1, DateTimeOffset.UnixEpoch),
        };

        var send = destination.SendAsync(message, CancellationToken.None);
        var produced = Assert.Single(Assert.Single(harness.Producers).Sends);
        produced.Persist();

        Assert.IsType<DeliveryResult.Delivered>(await send);
        Assert.Equal(KafkaHarness.Topic, produced.Topic);
    }

    [Fact]
    public async Task Routed_main_send_on_topic_destination_faults_without_produce()
    {
        await using var destination = harness.Create();

        var ex = await Assert.ThrowsAsync<ArgumentException>(() => destination.SendAsync(Routed("alerts"), CancellationToken.None).AsTask());

        Assert.StartsWith("Destinations[OrdersTopic]: route 'alerts' is not accepted by this destination", ex.Message, StringComparison.Ordinal);
        Assert.Empty(harness.Producers);
    }

    [Theory]
    [InlineData(null, "route (none) is not accepted by this destination")]
    [InlineData("billing", "route 'billing' is not accepted by this destination")]
    [InlineData("Alerts", "route 'Alerts' is not accepted by this destination")]
    public async Task Unaccepted_route_on_routes_destination_faults_without_produce(string? route, string expected)
    {
        harness.UseRoutes(("alerts", "risk.alerts"), ("audit", "risk.audit"));
        await using var destination = harness.Create();

        var ex = await Assert.ThrowsAsync<ArgumentException>(() => destination.SendAsync(Routed(route), CancellationToken.None).AsTask());

        Assert.StartsWith($"Destinations[OrdersTopic]: {expected}", ex.Message, StringComparison.Ordinal);
        Assert.Empty(harness.Producers);
    }

    [Fact]
    public async Task Dead_letter_on_routes_destination_faults_without_produce()
    {
        harness.UseRoutes(("alerts", "risk.alerts"));
        await using var destination = harness.Create();
        var message = Routed("alerts") with { DeadLetter = new DeadLetterInfo("reason", 1, DateTimeOffset.UnixEpoch) };

        var ex = await Assert.ThrowsAsync<ArgumentException>(() => destination.SendAsync(message, CancellationToken.None).AsTask());

        Assert.StartsWith("Destinations[OrdersTopic]: dead letter on a Routes destination", ex.Message, StringComparison.Ordinal);
        Assert.Empty(harness.Producers);
    }

    [Fact]
    public async Task Size_limit_is_the_record_topics_limit()
    {
        harness.UseRoutes(("alerts", "risk.alerts"), ("audit", "risk.audit"));
        await using var destination = harness.Create(topicConfigs: new Dictionary<string, TopicConfig>
        {
            ["risk.alerts"] = new(1_000_000, Compacted: false),
            ["risk.audit"] = new(1024, Compacted: false),
        });

        var fits = destination.SendAsync(Routed("alerts", new byte[2048]), CancellationToken.None);
        var refused = await destination.SendAsync(Routed("audit", new byte[2048]), CancellationToken.None);

        Assert.EndsWith("exceeds limit 1024", Assert.IsType<DeliveryResult.MessageFault>(refused).Error, StringComparison.Ordinal);
        var produced = Assert.Single(Assert.Single(harness.Producers).Sends);
        Assert.Equal("risk.alerts", produced.Topic);
        produced.Persist();
        Assert.IsType<DeliveryResult.Delivered>(await fits);
    }

    [Fact]
    public async Task Null_key_is_refused_only_on_a_compacted_mapped_topic()
    {
        harness.UseRoutes(("alerts", "risk.alerts"), ("audit", "risk.audit"));
        await using var destination = harness.Create(topicConfigs: new Dictionary<string, TopicConfig>
        {
            ["risk.alerts"] = new(1_000_000, Compacted: true),
            ["risk.audit"] = new(1_000_000, Compacted: false),
        });

        var refused = await destination.SendAsync(KafkaHarness.Message(null, [1]) with { Route = "alerts" }, CancellationToken.None);
        var sent = destination.SendAsync(KafkaHarness.Message(null, [1]) with { Route = "audit" }, CancellationToken.None);

        Assert.Equal(new DeliveryResult.MessageFault("InvalidRecord: null key on compacted topic", Permanent: true), refused);
        var produced = Assert.Single(Assert.Single(harness.Producers).Sends);
        Assert.Equal("risk.audit", produced.Topic);
        produced.Persist();
        Assert.IsType<DeliveryResult.Delivered>(await sent);
    }

    [Fact]
    public async Task Dead_letter_oversized_only_by_original_route_is_permanent_fault_without_produce()
    {
        var withoutRoute = KafkaHarness.Message([1], new byte[100]) with
        {
            DeadLetter = new DeadLetterInfo("reason", 1, DateTimeOffset.UnixEpoch),
        };
        var message = withoutRoute with { Route = "alerts" };
        var limit = KafkaRecordSize.UpperBound(message.Key, message.Payload, KafkaDeadLetterHeaders.Merge(withoutRoute));
        var merged = KafkaRecordSize.UpperBound(message.Key, message.Payload, KafkaDeadLetterHeaders.Merge(message));
        await using var destination = harness.Create(new TopicConfig((int)limit, Compacted: false));

        var result = await destination.SendAsync(message, CancellationToken.None);

        Assert.Equal(new DeliveryResult.MessageFault($"MsgSizeTooLarge: record up to {merged} bytes exceeds limit {limit}", Permanent: true), result);
        Assert.Empty(harness.Producers);
    }

    [Fact]
    public async Task Zero_header_sends_share_one_empty_headers_instance()
    {
        await using var destination = harness.Create();

        var first = destination.SendAsync(KafkaHarness.Message([1], [1]), CancellationToken.None).AsTask();
        var second = destination.SendAsync(KafkaHarness.Message([2], [2]), CancellationToken.None).AsTask();
        var sends = Assert.Single(harness.Producers).Sends;
        foreach (var send in sends)
        {
            send.Persist();
        }

        await Task.WhenAll(first, second);
        Assert.Same(MessageHeaders.Empty, sends[0].MessageHeaders);
        Assert.Same(MessageHeaders.Empty, sends[1].MessageHeaders);
    }

    [Fact]
    public async Task Send_with_headers_gets_its_own_headers_instance()
    {
        await using var destination = harness.Create();

        var plain = KafkaHarness.Message([1], [1]);
        var first = KafkaHarness.Message([2], [2], new Dictionary<string, string> { ["type"] = "Order" });
        var second = KafkaHarness.Message([3], [3], new Dictionary<string, string> { ["type"] = "Order" });
        _ = destination.SendAsync(plain, CancellationToken.None);
        _ = destination.SendAsync(first, CancellationToken.None);
        _ = destination.SendAsync(second, CancellationToken.None);
        var sends = Assert.Single(harness.Producers).Sends;

        Assert.Same(plain.Headers, sends[0].MessageHeaders);
        Assert.Same(first.Headers, sends[1].MessageHeaders);
        Assert.Same(second.Headers, sends[2].MessageHeaders);
        Assert.NotSame(sends[1].MessageHeaders, sends[2].MessageHeaders);
        Assert.Single(sends[1].MessageHeaders);
    }

    [Fact]
    public async Task Send_before_mapped_topic_is_described_is_destination_fault_without_produce()
    {
        harness.UseRoutes(("alerts", "risk.alerts"));
        var (options, relay) = harness.Bind();
        Assert.Empty(harness.Factory.Validate(options, $"Destinations[{options.Name}]", relay));
        await using var destination = harness.Factory.Create(options, new DestinationContext(OrderingMode.None));

        var result = await destination.SendAsync(Routed("alerts"), CancellationToken.None);

        Assert.Equal(new DeliveryResult.DestinationFault("Local_State: topic not described, ValidateAsync has not run"), result);
        Assert.Empty(harness.Producers);
    }

    [Fact]
    public async Task Validate_on_routes_destination_with_every_topic_described_builds_no_client()
    {
        harness.UseRoutes(("alerts", "risk.alerts"), ("audit", "risk.audit"));
        await using var destination = harness.Create();

        await destination.ValidateAsync(CancellationToken.None);

        Assert.Empty(harness.Producers);
    }

    [Theory]
    [InlineData(PersistenceStatus.NotPersisted)]
    [InlineData(PersistenceStatus.PossiblyPersisted)]
    public async Task Routed_report_not_persisted_fault_names_route_and_topic(PersistenceStatus status)
    {
        harness.UseRoutes(("alerts", "risk.alerts"), ("audit", "risk.audit"));
        await using var destination = harness.Create();

        var send = destination.SendAsync(Routed("audit"), CancellationToken.None);
        Assert.Single(Assert.Single(harness.Producers).Sends).Complete(status);

        Assert.Equal(new DeliveryResult.DestinationFault($"route 'audit' topic 'risk.audit': {status}: not confirmed"), await send);
    }

    [Fact]
    public async Task Routed_report_error_fault_names_route_and_topic()
    {
        harness.UseRoutes(("alerts", "risk.alerts"), ("audit", "risk.audit"));
        await using var destination = harness.Create();

        var send = destination.SendAsync(Routed("alerts"), CancellationToken.None);
        Assert.Single(Assert.Single(harness.Producers).Sends).Fail(new Error(ErrorCode.Local_MsgTimedOut, "Message timed out"));

        Assert.Equal(new DeliveryResult.DestinationFault("route 'alerts' topic 'risk.alerts': Local_MsgTimedOut: Message timed out"), await send);
    }

    [Theory]
    [InlineData(true, "route 'alerts' topic 'risk.alerts': Local_AllBrokersDown: down")]
    [InlineData(false, "Local_AllBrokersDown: down")]
    public async Task Sync_produce_throw_fault_names_route_and_topic_only_on_routes_destination(bool routed, string expected)
    {
        var harness = new KafkaHarness(_ => ThrowingProducer.Create(new Error(ErrorCode.Local_AllBrokersDown, "down")));
        if (routed)
        {
            harness.UseRoutes(("alerts", "risk.alerts"), ("audit", "risk.audit"));
        }

        await using var destination = harness.Create();

        var result = await destination.SendAsync(routed ? Routed("alerts") : KafkaHarness.Message([1], [1]), CancellationToken.None);

        Assert.Equal(new DeliveryResult.DestinationFault(expected), result);
    }

    [Fact]
    public async Task Two_routes_on_one_topic_each_fault_names_its_own_route()
    {
        harness.UseRoutes(("alerts", "risk.events"), ("audit", "risk.events"));
        await using var destination = harness.Create();

        var alerts = destination.SendAsync(Routed("alerts"), CancellationToken.None);
        var audit = destination.SendAsync(Routed("audit"), CancellationToken.None);
        var sends = Assert.Single(harness.Producers).Sends;
        sends[1].Fail(new Error(ErrorCode.Local_AllBrokersDown, "down"));
        sends[0].Fail(new Error(ErrorCode.Local_AllBrokersDown, "down"));

        Assert.Equal(new DeliveryResult.DestinationFault("route 'alerts' topic 'risk.events': Local_AllBrokersDown: down"), await alerts);
        Assert.Equal(new DeliveryResult.DestinationFault("route 'audit' topic 'risk.events': Local_AllBrokersDown: down"), await audit);
    }

    [Fact]
    public async Task Routed_message_fault_keeps_its_text()
    {
        harness.UseRoutes(("alerts", "risk.alerts"));
        await using var destination = harness.Create();

        var send = destination.SendAsync(Routed("alerts"), CancellationToken.None);
        Assert.Single(Assert.Single(harness.Producers).Sends).Fail(new Error(ErrorCode.MsgSizeTooLarge, "too large"));

        Assert.Equal(new DeliveryResult.MessageFault("MsgSizeTooLarge: too large", Permanent: true), await send);
    }

    [Fact]
    public async Task Routed_send_retired_after_fatal_keeps_its_text()
    {
        harness.UseRoutes(("alerts", "risk.alerts"));
        await using var destination = harness.Create();

        var outstanding = destination.SendAsync(Routed("alerts"), CancellationToken.None).AsTask();
        Assert.Single(Assert.Single(harness.Producers).Sends);
        Assert.Single(harness.FatalCallbacks)(new Error(ErrorCode.Local_Fatal, "fatal idempotence error", isFatal: true));

        Assert.Equal(
            new DeliveryResult.DestinationFault("Local_Fatal: producer failed fatally before delivery report"),
            await outstanding.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Routed_send_with_unbuildable_producer_keeps_its_text()
    {
        var harness = new KafkaHarness(_ => throw new ArgumentException("Invalid value"));
        harness.UseRoutes(("alerts", "risk.alerts"));
        await using var destination = harness.Create();

        var result = await destination.SendAsync(Routed("alerts"), CancellationToken.None);

        Assert.Equal(new DeliveryResult.DestinationFault("Local_InvalidArg: producer cannot be built"), result);
    }

    [Fact]
    public async Task Slot_is_reused_across_sends()
    {
        await using var destination = harness.Create();

        for (var i = 0; i < 5; i++)
        {
            var send = destination.SendAsync(KafkaHarness.Message([1], [1]), CancellationToken.None);
            Assert.Single(harness.Producers).Sends[^1].Persist();
            Assert.IsType<DeliveryResult.Delivered>(await send);
        }

        Assert.Equal(1, destination.SlotsCreated);
        Assert.Equal(1, destination.FreeSlots);
    }

    [Fact]
    public async Task Cancel_then_reuse()
    {
        await using var destination = harness.Create();
        using var cts = new CancellationTokenSource();

        var cancelled = destination.SendAsync(KafkaHarness.Message([1], [1]), cts.Token);
        await cts.CancelAsync();
        await Assert.ThrowsAsync<TaskCanceledException>(async () => await cancelled);
        Assert.Equal(1, destination.FreeSlots);

        var next = destination.SendAsync(KafkaHarness.Message([2], [2]), CancellationToken.None);
        Assert.Equal(0, destination.FreeSlots);
        Assert.Single(harness.Producers).Sends[1].Persist();

        Assert.IsType<DeliveryResult.Delivered>(await next);
        Assert.Equal(1, destination.SlotsCreated);
        Assert.Equal(1, destination.FreeSlots);
    }

    [Fact]
    public async Task Cancel_throws_TaskCanceledException_with_caller_token()
    {
        await using var destination = harness.Create();
        using var cts = new CancellationTokenSource();
        await BuildProducerAsync(destination, harness, KafkaHarness.Message([1], [1]));

        var send = destination.SendAsync(KafkaHarness.Message([2], [2]), cts.Token);
        await cts.CancelAsync();

        var ex = await Assert.ThrowsAsync<TaskCanceledException>(async () => await send);
        Assert.Equal(cts.Token, ex.CancellationToken);
    }

    [Fact]
    public async Task Retire_with_outstanding_slots_completes_each_once()
    {
        await using var destination = harness.Create();
        var retired = new DeliveryResult.DestinationFault("Local_Fatal: producer failed fatally before delivery report");

        var first = destination.SendAsync(KafkaHarness.Message([1], [1]), CancellationToken.None);
        var second = destination.SendAsync(KafkaHarness.Message([2], [2]), CancellationToken.None);
        var third = destination.SendAsync(KafkaHarness.Message([3], [3]), CancellationToken.None);
        var old = Assert.Single(harness.Producers).Sends;
        Assert.Single(harness.FatalCallbacks)(new Error(ErrorCode.Local_Fatal, "fatal idempotence error", isFatal: true));

        Assert.Equal(retired, await first);
        Assert.Equal(retired, await second);
        Assert.Equal(retired, await third);
        Assert.Equal(3, destination.FreeSlots);

        var next = destination.SendAsync(KafkaHarness.Message([4], [4]), CancellationToken.None);
        await KafkaHarness.WaitUntilAsync(() => harness.Producers.Count == 2 && harness.Producers[1].Sends.Count == 1);

        foreach (var pending in old)
        {
            pending.Persist();
        }

        Assert.False(next.IsCompleted);
        harness.Producers[1].Sends[0].Fail(new Error(ErrorCode.Local_AllBrokersDown, "down"));

        Assert.Equal(new DeliveryResult.DestinationFault("Local_AllBrokersDown: down"), await next);
        Assert.Equal(3, destination.SlotsCreated);
        Assert.Equal(3, destination.FreeSlots);
    }

    [Fact]
    public async Task Report_racing_cancel_completes_once()
    {
        await using var destination = harness.Create();
        const int iterations = 500;

        for (var i = 0; i < iterations; i++)
        {
            using var cts = new CancellationTokenSource();
            using var start = new Barrier(2);
            var send = destination.SendAsync(KafkaHarness.Message([1], [1]), cts.Token);
            var pending = Assert.Single(harness.Producers).Sends[^1];

            var reporter = Task.Run(() =>
            {
                start.SignalAndWait(TestContext.Current.CancellationToken);
                pending.Persist();
            }, TestContext.Current.CancellationToken);
            var canceller = Task.Run(() =>
            {
                start.SignalAndWait(TestContext.Current.CancellationToken);
                cts.Cancel();
            }, TestContext.Current.CancellationToken);
            await Task.WhenAll(reporter, canceller);

            try
            {
                Assert.IsType<DeliveryResult.Delivered>(await send);
            }
            catch (TaskCanceledException ex)
            {
                Assert.Equal(cts.Token, ex.CancellationToken);
            }

            Assert.Equal(1, destination.FreeSlots);
        }

        var next = destination.SendAsync(KafkaHarness.Message([2], [2]), CancellationToken.None);
        Assert.False(next.IsCompleted);
        Assert.Single(harness.Producers).Sends[^1].Fail(new Error(ErrorCode.Local_AllBrokersDown, "down"));

        Assert.Equal(new DeliveryResult.DestinationFault("Local_AllBrokersDown: down"), await next);
        Assert.Equal(1, destination.SlotsCreated);
    }

    [Fact]
    public async Task Second_GetResult_on_consumed_token_throws()
    {
        await using var destination = harness.Create();

        // The first send builds the producer through an async method, so the slot's own ValueTask is the second send's.
        await BuildProducerAsync(destination, harness, KafkaHarness.Message([1], [1]));
        var send = destination.SendAsync(KafkaHarness.Message([2], [2]), CancellationToken.None);
        Assert.Single(harness.Producers).Sends[^1].Persist();
        Assert.IsType<DeliveryResult.Delivered>(await send);
        Assert.Equal(1, destination.FreeSlots);

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await send);
        Assert.Equal(1, destination.FreeSlots);
        Assert.Equal(1, destination.SlotsCreated);
    }

    [Fact]
    public async Task Late_report_after_cancel_is_dropped()
    {
        await using var destination = harness.Create();
        using var cts = new CancellationTokenSource();

        var cancelled = destination.SendAsync(KafkaHarness.Message([1], [1]), cts.Token);
        await cts.CancelAsync();
        await Assert.ThrowsAsync<TaskCanceledException>(async () => await cancelled);

        var next = destination.SendAsync(KafkaHarness.Message([2], [2]), CancellationToken.None);
        var sends = Assert.Single(harness.Producers).Sends;
        sends[0].Persist();
        Assert.False(next.IsCompleted);
        sends[1].Fail(new Error(ErrorCode.Local_AllBrokersDown, "down"));

        Assert.Equal(new DeliveryResult.DestinationFault("Local_AllBrokersDown: down"), await next);
        Assert.Equal(1, destination.SlotsCreated);
        Assert.Equal(1, destination.FreeSlots);
    }

    [Fact]
    public async Task Late_report_after_dispose_is_dropped()
    {
        var destination = harness.Create();
        using var cts = new CancellationTokenSource();

        var cancelled = destination.SendAsync(KafkaHarness.Message([1], [1]), cts.Token);
        await cts.CancelAsync();
        await Assert.ThrowsAsync<TaskCanceledException>(async () => await cancelled);
        await destination.DisposeAsync();

        var pending = Assert.Single(Assert.Single(harness.Producers).Sends);
        pending.Persist();
        pending.Persist();

        Assert.Equal(1, destination.SlotsCreated);
        Assert.Equal(1, destination.FreeSlots);
    }

    [Fact]
    public async Task Duplicate_report_after_cancel_is_dropped()
    {
        await using var destination = harness.Create();
        using var cts = new CancellationTokenSource();

        var cancelled = destination.SendAsync(KafkaHarness.Message([1], [1]), cts.Token);
        await cts.CancelAsync();
        var first = Assert.Single(Assert.Single(harness.Producers).Sends);
        first.Persist();
        await Assert.ThrowsAsync<TaskCanceledException>(async () => await cancelled);

        var next = destination.SendAsync(KafkaHarness.Message([2], [2]), CancellationToken.None);
        first.Persist();
        Assert.False(next.IsCompleted);
        Assert.Single(harness.Producers).Sends[1].Persist();

        Assert.IsType<DeliveryResult.Delivered>(await next);
        Assert.Equal(1, destination.SlotsCreated);
        Assert.Equal(1, destination.FreeSlots);
    }

    [Fact]
    public async Task Duplicate_report_before_reuse_is_dropped()
    {
        await using var destination = harness.Create();

        var send = destination.SendAsync(KafkaHarness.Message([1], [1]), CancellationToken.None);
        var pending = Assert.Single(Assert.Single(harness.Producers).Sends);
        pending.Persist();
        pending.Fail(new Error(ErrorCode.Local_AllBrokersDown, "down"));

        Assert.IsType<DeliveryResult.Delivered>(await send);
        Assert.Equal(1, destination.FreeSlots);

        var next = destination.SendAsync(KafkaHarness.Message([2], [2]), CancellationToken.None);
        Assert.False(next.IsCompleted);
        Assert.Single(harness.Producers).Sends[1].Fail(new Error(ErrorCode.Local_AllBrokersDown, "down"));

        Assert.Equal(new DeliveryResult.DestinationFault("Local_AllBrokersDown: down"), await next);
        Assert.Equal(1, destination.SlotsCreated);
        Assert.Equal(1, destination.FreeSlots);
    }

    // The frozen route lookup and the route as a slot field add no bytes over a Topic send.
    [Fact]
    public async Task Routed_send_adds_no_per_send_allocation_over_topic_send()
    {
        var topicSide = new KafkaHarness();
        topicSide.Set("Destinations:0:Kafka:Topic", "risk.alerts");
        var routedSide = new KafkaHarness();
        routedSide.UseRoutes(("alerts", "risk.alerts"));
        var message = KafkaHarness.Message([1], [1, 2, 3, 4, 5, 6], new Dictionary<string, string> { ["type"] = "Alert" });
        await using var topicDestination = topicSide.Create();
        await using var routedDestination = routedSide.Create();

        var routedMessage = message with { Route = "alerts" };
        await BuildProducerAsync(topicDestination, topicSide, message);
        await BuildProducerAsync(routedDestination, routedSide, routedMessage);

        Warm(topicDestination, topicSide, message);
        Warm(routedDestination, routedSide, routedMessage);

        // Topic and Routes sends alternate inside each round, so runtime drift (tiering, one-off allocations) hits both sides alike.
        // The smallest per-send cost a routed send could add is one field or object, 8 B, which adds at least 80,000 B to every
        // round. The bound is therefore routed excess under 8 B per send, which tolerates drift and still fails on any such cost.
        const long bound = 8L * MeasuredSends;
        long topicBytes = 0, routedBytes = 0;
        for (var round = 0; round < MeasureRounds && (round == 0 || routedBytes - topicBytes >= bound); round++)
        {
            (topicBytes, routedBytes) = AllocatedBytes(topicDestination, topicSide, message, routedDestination, routedSide, routedMessage);
            TestContext.Current.TestOutputHelper?.WriteLine(
                $"round {round}: bytes per send Topic {topicBytes / (double)MeasuredSends}, Routes {routedBytes / (double)MeasuredSends}");
        }

        Assert.True(routedBytes - topicBytes < bound, $"routed excess {routedBytes - topicBytes} B over {MeasuredSends} sends, bound {bound} B");
    }

    private const int WarmSends = 1_000;
    private const int MeasuredSends = 10_000;
    private const int MeasureRounds = 3;

    private static async Task BuildProducerAsync(KafkaDestination destination, KafkaHarness harness, OutgoingMessage message)
    {
        var send = destination.SendAsync(message, CancellationToken.None);
        Assert.Single(Assert.Single(harness.Producers).Sends).Persist();
        Assert.IsType<DeliveryResult.Delivered>(await send);
    }

    private static void Warm(KafkaDestination destination, KafkaHarness harness, OutgoingMessage message)
    {
        for (var i = 0; i < WarmSends; i++)
        {
            _ = SendAndPersist(destination, harness, message);
        }
    }

    // Counts the destination's send and the inline delivery report. The harness's copy of the pending list is outside the window.
    private static (long Topic, long Routed) AllocatedBytes(
        KafkaDestination topicDestination, KafkaHarness topicSide, OutgoingMessage message,
        KafkaDestination routedDestination, KafkaHarness routedSide, OutgoingMessage routedMessage)
    {
        long topic = 0, routed = 0;
        for (var i = 0; i < MeasuredSends; i++)
        {
            topic += SendAndPersist(topicDestination, topicSide, message);
            routed += SendAndPersist(routedDestination, routedSide, routedMessage);
        }

        return (topic, routed);
    }

    private static long SendAndPersist(KafkaDestination destination, KafkaHarness harness, OutgoingMessage message)
    {
        var start = GC.GetAllocatedBytesForCurrentThread();
        var send = destination.SendAsync(message, CancellationToken.None);
        var sent = GC.GetAllocatedBytesForCurrentThread();
        var pending = Assert.Single(harness.Producers).Sends[^1];
        var reporting = GC.GetAllocatedBytesForCurrentThread();
        pending.Persist();
        var reported = GC.GetAllocatedBytesForCurrentThread();

        Assert.True(send.IsCompletedSuccessfully);
        Assert.IsType<DeliveryResult.Delivered>(send.Result);
        return sent - start + (reported - reporting);
    }

    private static OutgoingMessage Routed(string? route, byte[]? payload = null) =>
        KafkaHarness.Message([1], payload ?? [1]) with { Route = route };
}

internal sealed class KafkaHarness
{
    public const string Destination = "OrdersTopic";
    public const string Topic = "orders.events";

    private readonly Dictionary<string, string?> settings = new()
    {
        ["KafkaClusters:Main:BootstrapServers"] = "kafka-1:9092",
        ["Destinations:0:Name"] = Destination,
        ["Destinations:0:Transport"] = "Kafka",
        ["Destinations:0:Kafka:Cluster"] = "Main",
        ["Destinations:0:Kafka:Topic"] = Topic,
    };

    private readonly List<FakeProducer> producers = [];
    private readonly List<Action<Error>> fatalCallbacks = [];

    public KafkaHarness(Func<ProducerConfig, IKafkaRawProducer>? producerFactory = null)
    {
        Factory = new KafkaDestinationFactory(NullLoggerFactory.Instance, (config, _, onFatal, onReport) =>
        {
            var built = producerFactory?.Invoke(config) ?? new FakeProducer();
            if (built is FakeProducer fake)
            {
                fake.OnReport = onReport;
                lock (producers)
                {
                    producers.Add(fake);
                    fatalCallbacks.Add(onFatal);
                }
            }

            if (Interlocked.Exchange(ref FatalDuringBuild, null) is { } error)
            {
                onFatal(error);
            }

            return built;
        });
    }

    // Raised through the error handler while the next producer is being built, once.
    public Error? FatalDuringBuild;

    public IReadOnlyList<Action<Error>> FatalCallbacks
    {
        get
        {
            lock (producers)
            {
                return [.. fatalCallbacks];
            }
        }
    }

    public KafkaDestinationFactory Factory { get; }

    public IReadOnlyList<FakeProducer> Producers
    {
        get
        {
            lock (producers)
            {
                return [.. producers];
            }
        }
    }

    public static OutgoingMessage Message(byte[]? key, byte[] payload, IReadOnlyDictionary<string, string>? headers = null) => new()
    {
        Source = new MessageSource("Orders", 1, Destination),
        Key = key,
        Payload = payload,
        Headers = headers is null ? MessageHeaders.Empty : TestHeaders.Of(headers),
    };

    public static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "condition not met within 5 s");
            await Task.Delay(10);
        }
    }

    public void Set(string key, string? value) => settings[key] = value;

    public void UseRoutes(params (string Route, string Topic)[] routes)
    {
        Remove("Destinations:0:Kafka:Topic");
        foreach (var (route, topic) in routes)
        {
            settings[$"Destinations:0:Kafka:Routes:{route}"] = topic;
        }
    }

    public void Remove(string prefix)
    {
        foreach (var key in settings.Keys.Where(k => k == prefix || k.StartsWith(prefix + ":", StringComparison.Ordinal)).ToList())
        {
            settings.Remove(key);
        }
    }

    public (DestinationOptions Destination, RelayOptions Relay) Bind()
    {
        var relay = RelayOptionsBinder.Bind(new ConfigurationBuilder().AddInMemoryCollection(settings).Build());
        return (relay.Destinations[0], relay);
    }

    public IReadOnlyList<string> Validate()
    {
        var (destination, relay) = Bind();
        return Factory.Validate(destination, $"Destinations[{destination.Name}]", relay);
    }

    // Marks every topic of the destination described: topicConfig, or the per-topic entry in topicConfigs when present.
    public KafkaDestination Create(
        TopicConfig? topicConfig = null,
        DestinationContext? context = null,
        IReadOnlyDictionary<string, TopicConfig>? topicConfigs = null)
    {
        var (destination, relay) = Bind();
        Assert.Empty(Factory.Validate(destination, $"Destinations[{destination.Name}]", relay));
        foreach (var address in Factory.AddressesOf(Destination).Distinct)
        {
            address.Config = topicConfigs?.GetValueOrDefault(address.Topic) ?? topicConfig ?? new TopicConfig(1_000_000, Compacted: false);
        }

        return Assert.IsType<KafkaDestination>(Factory.Create(destination, context ?? new DestinationContext(OrderingMode.None)));
    }
}

internal sealed class ThrowingProducer(Error error) : IKafkaRawProducer
{
    internal static IKafkaRawProducer Create(Error error) => new ThrowingProducer(error);

    public Error? Produce(KafkaAddress address, byte[]? key, byte[] value, MessageHeaders headers, nint opaque) => error;

    public void Dispose()
    {
    }
}
