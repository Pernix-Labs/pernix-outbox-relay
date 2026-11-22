namespace Pernix.OutboxRelay.Core.Configuration;

public sealed class RelayOptions
{
    public List<DestinationOptions> Destinations { get; set; } = [];
    public List<OutboxOptions> Outboxes { get; set; } = [];

    internal List<string> BindingFailures { get; } = [];
}
