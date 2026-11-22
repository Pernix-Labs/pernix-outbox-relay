using System.Text;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Destination.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.Kafka;

public sealed class KafkaDestinationIntegrationTests(KafkaFixture kafka)
{
    private const string Destination = "Events";
    private static readonly TimeSpan ConsumeTimeout = TimeSpan.FromSeconds(20);

    [Fact]
    public async Task Sent_records_carry_key_payload_and_headers()
    {
        var topic = KafkaFixture.NewTopic();
        await kafka.CreateTopicAsync(topic, 1);
        await using var destination = await CreateValidatedAsync(topic);

        var keyed = Message("order-1"u8.ToArray(), "{\"id\":1}"u8.ToArray(), new Dictionary<string, string> { ["traceparent"] = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01", ["type"] = "Ordér" });
        var unkeyed = Message(null, "{\"id\":2}"u8.ToArray(), new Dictionary<string, string>());
        Assert.IsType<DeliveryResult.Delivered>(await destination.SendAsync(keyed, TestContext.Current.CancellationToken));
        Assert.IsType<DeliveryResult.Delivered>(await destination.SendAsync(unkeyed, TestContext.Current.CancellationToken));

        var records = await kafka.ConsumeAllAsync(topic, 2, ConsumeTimeout);

        Assert.Equal(2, records.Count);
        Assert.Equal(keyed.Key, records[0].Message.Key);
        Assert.Equal(keyed.Payload, records[0].Message.Value);
        Assert.Equal(
            keyed.Headers.Select(h => (h.Name, Encoding.UTF8.GetString(h.Value))),
            records[0].Message.Headers.Select(h => (h.Key, Encoding.UTF8.GetString(h.GetValueBytes()))));
        Assert.Null(records[1].Message.Key);
        Assert.Equal(unkeyed.Payload, records[1].Message.Value);
        Assert.Empty(records[1].Message.Headers);
    }

    [Fact]
    public async Task Dead_letter_record_carries_original_record_and_x_headers()
    {
        var topic = KafkaFixture.NewTopic();
        await kafka.CreateTopicAsync(topic, 1);
        await using var destination = await CreateValidatedAsync(topic);
        var message = Message("order-1"u8.ToArray(), "{\"id\":1}"u8.ToArray(), new Dictionary<string, string> { ["type"] = "Order", ["x-original-id"] = "spoofed" }) with
        {
            Source = new MessageSource("Orders", 42, "Orders"),
            DeadLetter = new DeadLetterInfo("InvalidRecord: rejected", 4, new DateTimeOffset(2026, 11, 3, 10, 20, 30, 456, TimeSpan.Zero)),
        };

        Assert.IsType<DeliveryResult.Delivered>(await destination.SendAsync(message, TestContext.Current.CancellationToken));

        var record = Assert.Single(await kafka.ConsumeAllAsync(topic, 1, ConsumeTimeout));
        Assert.Equal(message.Key, record.Message.Key);
        Assert.Equal(message.Payload, record.Message.Value);
        Assert.Equal(
            new Dictionary<string, string>
            {
                ["type"] = "Order",
                ["x-dead-letter-reason"] = "InvalidRecord: rejected",
                ["x-retry-attempts"] = "4",
                ["x-original-destination"] = "Orders",
                ["x-original-outbox"] = "Orders",
                ["x-original-id"] = "42",
                ["x-failed-at"] = "2026-11-03T10:20:30.456Z",
            },
            record.Message.Headers.ToDictionary(h => h.Key, h => Encoding.UTF8.GetString(h.GetValueBytes())));
        Assert.Equal(7, record.Message.Headers.Count);
    }

    [Fact]
    public async Task Keyed_records_land_on_java_murmur2_partition()
    {
        const int partitions = 12;
        var topic = KafkaFixture.NewTopic();
        await kafka.CreateTopicAsync(topic, partitions);
        await using var destination = await CreateValidatedAsync(topic);
        var keys = Enumerable.Range(0, 20).Select(i => Encoding.UTF8.GetBytes($"customer-{i}")).ToList();

        var results = await Task.WhenAll(keys.Select(key => destination.SendAsync(Message(key, [1], new Dictionary<string, string>()), TestContext.Current.CancellationToken).AsTask()));
        Assert.All(results, result => Assert.IsType<DeliveryResult.Delivered>(result));

        var records = await kafka.ConsumeAllAsync(topic, keys.Count, ConsumeTimeout);

        Assert.Equal(keys.Count, records.Count);
        Assert.All(records, record => Assert.Equal(JavaMurmur2.Partition(record.Message.Key, partitions), record.Partition.Value));
        Assert.True(records.Select(r => r.Partition.Value).Distinct().Count() > 1);
    }

    [Theory]
    [InlineData("21", -973932308)]
    [InlineData("foobar", -790332482)]
    [InlineData("a-little-bit-long-string", -985981536)]
    [InlineData("a-little-bit-longer-string", -1486304829)]
    [InlineData("lkjh234lh9fiuh90y23oiuhsafujhadof229phr9h19h89h8", -58897971)]
    [InlineData("abc", 479470107)]
    public void Murmur2_port_matches_kafka_utils_test_vectors(string input, int expected)
    {
        Assert.Equal(expected, JavaMurmur2.Hash(Encoding.UTF8.GetBytes(input)));
    }

    [Fact]
    public async Task Validate_missing_topic_names_destination_and_topic()
    {
        var topic = KafkaFixture.NewTopic();
        await using var destination = Create(topic, DestinationContext.Probe);

        var ex = await Assert.ThrowsAsync<DestinationValidationException>(() => destination.ValidateAsync(TestContext.Current.CancellationToken));

        Assert.Equal($"Destinations[{Destination}]: topic '{topic}' does not exist", ex.Message);
    }

    [Fact]
    public async Task Validate_invalid_producer_property_fails_without_value()
    {
        const string secret = "not-a-number-5be1";
        var topic = KafkaFixture.NewTopic();
        await kafka.CreateTopicAsync(topic, 1);
        await using var destination = Create(topic, DestinationContext.Probe, new() { ["KafkaClusters:Main:Producer:linger.ms"] = secret });

        var ex = await Assert.ThrowsAsync<DestinationValidationException>(() => destination.ValidateAsync(TestContext.Current.CancellationToken));

        Assert.Equal($"Destinations[{Destination}]: invalid producer configuration for cluster 'Main'", ex.Message);
        Assert.DoesNotContain(secret, ex.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Validate_unknown_producer_property_fails_without_value()
    {
        const string secret = "value-7c2d";
        var topic = KafkaFixture.NewTopic();
        await kafka.CreateTopicAsync(topic, 1);
        await using var destination = Create(topic, DestinationContext.Probe, new() { ["KafkaClusters:Main:Producer:no.such.property"] = secret });

        var ex = await Assert.ThrowsAsync<DestinationValidationException>(() => destination.ValidateAsync(TestContext.Current.CancellationToken));

        Assert.Equal($"Destinations[{Destination}]: invalid producer configuration for cluster 'Main'", ex.Message);
        Assert.DoesNotContain(secret, ex.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task Probe_validate_passes_for_existing_topic()
    {
        var topic = KafkaFixture.NewTopic();
        await kafka.CreateTopicAsync(topic, 1);
        await using var destination = Create(topic, DestinationContext.Probe);

        await destination.ValidateAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Oversized_record_is_permanent_fault_and_nothing_reaches_topic()
    {
        var topic = KafkaFixture.NewTopic();
        await kafka.CreateTopicAsync(topic, 1, new Dictionary<string, string> { ["max.message.bytes"] = "1024" });
        await using var destination = await CreateValidatedAsync(topic);

        var result = await destination.SendAsync(Message("k"u8.ToArray(), new byte[2048], new Dictionary<string, string>()), TestContext.Current.CancellationToken);

        Assert.True(Assert.IsType<DeliveryResult.MessageFault>(result).Permanent);
        Assert.Empty(await kafka.ConsumeAllAsync(topic, 1, TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task Null_key_on_compacted_topic_is_permanent_fault_and_nothing_reaches_topic()
    {
        var topic = KafkaFixture.NewTopic();
        await kafka.CreateTopicAsync(topic, 1, new Dictionary<string, string> { ["cleanup.policy"] = "compact" });
        await using var destination = await CreateValidatedAsync(topic);

        var result = await destination.SendAsync(Message(null, [1], new Dictionary<string, string>()), TestContext.Current.CancellationToken);

        Assert.Equal(new DeliveryResult.MessageFault("InvalidRecord: null key on compacted topic", Permanent: true), result);
        Assert.Empty(await kafka.ConsumeAllAsync(topic, 1, TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task Probe_validate_refuses_compacted_dead_letter_topic()
    {
        var topic = KafkaFixture.NewTopic();
        await kafka.CreateTopicAsync(topic, 1, new Dictionary<string, string> { ["cleanup.policy"] = "compact" });
        await using var destination = Create(topic, DestinationContext.Probe, new Dictionary<string, string?>
        {
            ["Outboxes:0:Name"] = "Orders",
            ["Outboxes:0:DeadLetter:Destination"] = Destination,
        });

        var ex = await Assert.ThrowsAsync<DestinationValidationException>(() => destination.ValidateAsync(TestContext.Current.CancellationToken));

        Assert.Equal($"Destinations[{Destination}]: topic '{topic}' is compacted and cannot hold dead letters of outbox 'Orders'", ex.Message);
    }

    [Fact]
    public async Task Validate_routes_destination_names_only_the_missing_mapped_topic()
    {
        var existing = KafkaFixture.NewTopic();
        var missing = KafkaFixture.NewTopic();
        await kafka.CreateTopicAsync(existing, 1);
        await using var destination = Create(null, DestinationContext.Probe, Routes(("alerts", existing), ("audit", missing)));

        var ex = await Assert.ThrowsAsync<DestinationValidationException>(() => destination.ValidateAsync(TestContext.Current.CancellationToken));

        Assert.Equal($"Destinations[{Destination}]: topic '{missing}' does not exist", ex.Message);
        Assert.DoesNotContain(existing, ex.Message, StringComparison.Ordinal);
    }

    // Describe failures naming every topic are covered here only.
    [Fact]
    public async Task Validate_routes_destination_names_every_missing_mapped_topic_in_one_failure()
    {
        var first = KafkaFixture.NewTopic();
        var second = KafkaFixture.NewTopic();
        await using var destination = Create(null, DestinationContext.Probe, Routes(("alerts", first), ("audit", second)));

        var ex = await Assert.ThrowsAsync<DestinationValidationException>(() => destination.ValidateAsync(TestContext.Current.CancellationToken));

        Assert.Equal(
            new[] { first, second }.Select(t => $"Destinations[{Destination}]: topic '{t}' does not exist").Order(StringComparer.Ordinal),
            ex.Message.Split("; ").Order(StringComparer.Ordinal));
    }

    [Fact]
    public void Topic_and_routes_both_set_is_refused()
    {
        var topic = KafkaFixture.NewTopic();

        var (relay, factory) = Bind(topic, Routes(("alerts", topic)));

        Assert.Equal(
            [$"Destinations[{Destination}].Kafka: Topic and Routes are both set; set exactly one"],
            factory.Validate(relay.Destinations[0], $"Destinations[{Destination}]", relay));
    }

    private static Dictionary<string, string?> Routes(params (string Route, string Topic)[] routes)
    {
        var settings = new Dictionary<string, string?>();
        foreach (var (route, topic) in routes)
        {
            settings[$"Destinations:0:Kafka:Routes:{route}"] = topic;
        }

        return settings;
    }

    private async Task<IDestination> CreateValidatedAsync(string topic)
    {
        var destination = Create(topic, new DestinationContext(OrderingMode.None));
        await destination.ValidateAsync(TestContext.Current.CancellationToken);
        return destination;
    }

    private IDestination Create(string? topic, DestinationContext context, Dictionary<string, string?>? extra = null)
    {
        var (relay, factory) = Bind(topic, extra);
        Assert.Empty(factory.Validate(relay.Destinations[0], $"Destinations[{Destination}]", relay));
        return factory.Create(relay.Destinations[0], context);
    }

    // A null topic leaves Kafka.Topic unset, for Routes destinations.
    private (RelayOptions Relay, KafkaDestinationFactory Factory) Bind(string? topic, Dictionary<string, string?>? extra)
    {
        var settings = new Dictionary<string, string?>
        {
            ["KafkaClusters:Main:BootstrapServers"] = kafka.BootstrapServers,
            ["Destinations:0:Name"] = Destination,
            ["Destinations:0:Transport"] = "Kafka",
            ["Destinations:0:Kafka:Cluster"] = "Main",
        };
        if (topic is not null)
        {
            settings["Destinations:0:Kafka:Topic"] = topic;
        }

        foreach (var (key, value) in extra ?? [])
        {
            settings[key] = value;
        }

        var config = new ConfigurationBuilder().AddInMemoryCollection(settings).Build();
        return (RelayOptionsBinder.Bind(config), new KafkaDestinationFactory(NullLoggerFactory.Instance, new KafkaClusterSection(new RelayConfiguration(config))));
    }

    private static OutgoingMessage Message(byte[]? key, byte[] payload, IReadOnlyDictionary<string, string> headers) => new()
    {
        Source = new MessageSource("Orders", 1, Destination),
        Key = key,
        Payload = payload,
        Headers = TestHeaders.Of(headers),
    };
}
