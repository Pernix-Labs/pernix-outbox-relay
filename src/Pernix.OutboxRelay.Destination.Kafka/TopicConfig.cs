namespace Pernix.OutboxRelay.Destination.Kafka;

internal sealed record TopicConfig(int MaxMessageBytes, bool Compacted);
