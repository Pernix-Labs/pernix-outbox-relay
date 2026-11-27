namespace Pernix.OutboxRelay.Destination.Http.Options;

internal sealed class HttpRouteOptions
{
    public string? Url { get; set; }
    public string? Method { get; set; }
    public string? ContentType { get; set; }
    public Dictionary<string, string>? Headers { get; set; }
}
