namespace Pernix.OutboxRelay.Destination.Http.Options;

internal sealed class HealthProbeOptions
{
    public string? Url { get; set; }
    public string? Method { get; set; }
    public string[]? Expected { get; set; }
}
