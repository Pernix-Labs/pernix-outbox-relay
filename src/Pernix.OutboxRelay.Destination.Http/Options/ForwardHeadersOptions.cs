namespace Pernix.OutboxRelay.Destination.Http.Options;

internal enum ForwardHeadersMode
{
    All,
    None,
    Listed,
}

internal sealed class ForwardHeadersOptions
{
    public ForwardHeadersMode? Mode { get; set; }
    public string[]? Names { get; set; }
    public string[]? Exclude { get; set; }
    public string? NamePrefix { get; set; }
}
