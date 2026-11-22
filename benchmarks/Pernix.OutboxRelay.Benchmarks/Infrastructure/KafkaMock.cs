using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Destination.Kafka;
using Microsoft.Extensions.Logging.Abstractions;

namespace Pernix.OutboxRelay.Benchmarks.Infrastructure;

internal static class KafkaMock
{
    public const string Topic = "bench";
    public const string DeadLetterTopic = "bench-dlq";
    public const string DeadLetterName = "bench-dlq";
    private const string ClusterName = "mock";
    private const int MaxMessageBytes = 1_048_588;

    public static IDestination Create(OrderingMode mode) => Create(Workload.Destination, Topic, mode, []);

    public static IDestination CreateDeadLetter(OrderingMode mode) => Create(DeadLetterName, DeadLetterTopic, mode, [Workload.Outbox]);

    private static IDestination Create(string name, string topic, OrderingMode mode, IReadOnlyList<string> deadLetterOf)
    {
        var cluster = new KafkaClusterOptions { BootstrapServers = "localhost:1" };
        cluster.Producer["test.mock.num.brokers"] = "1";
        var settings = KafkaClientSettings.Build(cluster, mode);

        // Pre-filled so ValidateAsync skips the admin describe.
        var addresses = KafkaAddressMap.ForTopic(topic);
        addresses.Single!.Config = new TopicConfig(MaxMessageBytes, false);

        return new KafkaDestination(
            name, addresses, ClusterName, settings, probe: false, deadLetterOf, KafkaDestinationFactory.BuildProducer, NullLogger.Instance);
    }
}
