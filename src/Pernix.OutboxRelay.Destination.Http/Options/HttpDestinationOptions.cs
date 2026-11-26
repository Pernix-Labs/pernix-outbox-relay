namespace Pernix.OutboxRelay.Destination.Http.Options;

internal sealed class HttpDestinationOptions
{
    public string? Url { get; set; }
    public Dictionary<string, HttpRouteOptions>? Routes { get; set; }
    public string? Method { get; set; }
    public string? ContentType { get; set; }
    public Dictionary<string, string>? Headers { get; set; }
    public ForwardHeadersOptions? ForwardHeaders { get; set; }
    public StatusCodesOptions? StatusCodes { get; set; }
    public TimeSpan? SendTimeout { get; set; }
    public TimeSpan? ConnectTimeout { get; set; }
    public int? MaxRequestsInFlight { get; set; }
    public string? HttpVersion { get; set; }
    public AuthenticationOptions? Authentication { get; set; }
    public SigningOptions? Signing { get; set; }
    public TlsOptions? Tls { get; set; }
    public HealthProbeOptions? HealthProbe { get; set; }
    public string? RequestMapper { get; set; }
    public string? ResponseClassifier { get; set; }
}
