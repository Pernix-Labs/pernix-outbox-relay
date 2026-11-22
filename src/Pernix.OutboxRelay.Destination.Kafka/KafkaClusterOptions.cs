namespace Pernix.OutboxRelay.Destination.Kafka;

internal sealed class KafkaClusterOptions
{
    public string? BootstrapServers { get; set; }
    public Dictionary<string, string> Producer { get; set; } = new(StringComparer.Ordinal);
}
