namespace Pernix.OutboxRelay.Destination.Kafka;

public sealed class KafkaDestinationOptions
{
    public string? Cluster { get; set; }
    public string? Topic { get; set; }

    // Route -> topic. Set exactly one of Topic and Routes.
    public Dictionary<string, string>? Routes { get; set; }
}
