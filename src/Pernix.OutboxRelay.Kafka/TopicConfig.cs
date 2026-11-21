namespace Pernix.OutboxRelay.Kafka;

internal sealed record TopicConfig(int MaxMessageBytes, bool Compacted);
