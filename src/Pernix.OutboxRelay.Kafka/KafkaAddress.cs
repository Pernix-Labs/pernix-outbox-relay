using Confluent.Kafka;

namespace Pernix.OutboxRelay.Kafka;

internal sealed class KafkaAddress(string topic)
{
    public string Topic { get; } = topic;

    public TopicPartition TopicPartition { get; } = new(topic, Partition.Any);

    // Set once by ValidateAsync, shared by every instance of the destination. Volatile read/write.
    public TopicConfig? Config;
}
