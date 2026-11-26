using System.Globalization;
using Pernix.OutboxRelay.Core.Abstractions;

namespace Pernix.OutboxRelay.Destination.Http;

internal sealed class HttpEndpoint
{
    private const int CachedCodes = 1000;

    private readonly DeliveryResult?[] faultsByStatus = new DeliveryResult?[CachedCodes];

    public HttpEndpoint(
        HttpMethod method,
        Uri? staticUri,
        ValueTemplate? urlTemplate,
        HeaderPlan headers,
        string contentType,
        string? routeLabel,
        StatusTable status,
        ValueTemplate? webhookId = null)
    {
        ArgumentNullException.ThrowIfNull(method);
        ArgumentNullException.ThrowIfNull(headers);
        ArgumentNullException.ThrowIfNull(contentType);
        ArgumentNullException.ThrowIfNull(status);
        if ((staticUri is null) == (urlTemplate is null))
        {
            throw new ArgumentException("endpoint needs exactly one of a static Uri or a URL template", nameof(staticUri));
        }

        Method = method;
        StaticUri = staticUri;
        UrlTemplate = urlTemplate;
        Headers = headers;
        ContentType = contentType;
        RouteLabel = routeLabel;
        Status = status;
        WebhookId = webhookId;
    }

    public HttpMethod Method { get; }

    public Uri? StaticUri { get; }

    public ValueTemplate? UrlTemplate { get; }

    public HeaderPlan Headers { get; }

    public string ContentType { get; }

    public string? RouteLabel { get; }

    public StatusTable Status { get; }

    public ValueTemplate? WebhookId { get; }

    public DeliveryResult Classify(int statusCode)
    {
        var status = Status.Classify(statusCode);
        if (status == StatusClass.Delivered)
        {
            return DeliveryResult.Delivered.Instance;
        }

        if ((uint)statusCode >= CachedCodes)
        {
            return Create(status, statusCode);
        }

        // Racing writers store equal immutable records, so the first read wins harmlessly.
        return Volatile.Read(ref faultsByStatus[statusCode]) ?? (faultsByStatus[statusCode] = Create(status, statusCode));
    }

    private DeliveryResult Create(StatusClass status, int statusCode)
    {
        var code = statusCode.ToString(CultureInfo.InvariantCulture);
        var text = RouteLabel is null ? $"HTTP {code}" : $"route '{RouteLabel}': HTTP {code}";
        return status switch
        {
            StatusClass.Rejected => new DeliveryResult.MessageFault(text, Permanent: true),
            StatusClass.Retried => new DeliveryResult.MessageFault(text, Permanent: false),
            _ => new DeliveryResult.DestinationFault(text),
        };
    }
}
