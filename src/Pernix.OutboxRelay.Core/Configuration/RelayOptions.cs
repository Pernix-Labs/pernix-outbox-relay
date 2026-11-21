namespace Pernix.OutboxRelay.Core.Configuration;

public sealed class RelayOptions
{
    public Dictionary<string, KafkaClusterOptions> KafkaClusters { get; set; } = new(StringComparer.OrdinalIgnoreCase);
    public List<DestinationOptions> Destinations { get; set; } = [];
    public List<OutboxOptions> Outboxes { get; set; } = [];

    internal List<string> BindingFailures { get; } = [];
}

public sealed class KafkaClusterOptions
{
    public string? BootstrapServers { get; set; }
    public Dictionary<string, string> Producer { get; set; } = new(StringComparer.Ordinal);
}
