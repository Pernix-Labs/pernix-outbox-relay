namespace Pernix.OutboxRelay.Destination.Http;

internal sealed class HttpDestinationPlan
{
    internal static readonly TimeSpan DefaultSendTimeout = TimeSpan.FromSeconds(30);
    internal static readonly TimeSpan DefaultConnectTimeout = TimeSpan.FromSeconds(10);
    internal const int DefaultMaxRequestsInFlight = 64;

    public required string Name { get; init; }

    public required HttpEndpointMap Endpoints { get; init; }

    public required HeaderForwarding Forwarding { get; init; }

    public required AuthPlan Auth { get; init; }

    public SigningPlan? Signing { get; init; }

    public required HandlerSettings Handler { get; init; }

    public required TimeSpan SendTimeout { get; init; }

    public int MaxRequestsInFlight => Handler.MaxRequestsInFlight;

    public string? RequestMapper { get; init; }

    public string? ResponseClassifier { get; init; }

    public HealthProbePlan? Probe { get; init; }

    public required string Host { get; init; }
}

internal sealed class HealthProbePlan(Uri url, HttpMethod method, bool[] expected)
{
    public Uri Url { get; } = url;

    public HttpMethod Method { get; } = method;

    public bool IsExpected(int statusCode) => (uint)statusCode < (uint)expected.Length && expected[statusCode];
}
