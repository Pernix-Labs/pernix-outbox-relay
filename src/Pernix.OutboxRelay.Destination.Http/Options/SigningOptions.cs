namespace Pernix.OutboxRelay.Destination.Http.Options;

internal enum SigningScheme
{
    StandardWebhooks,
    HmacSha256,
}

internal enum SignatureEncoding
{
    Hex,
    Base64,
}

internal sealed class SigningOptions
{
    public SigningScheme? Scheme { get; set; }
    public string? Credential { get; set; }
    public string? HeaderName { get; set; }
    public SignatureEncoding? Encoding { get; set; }
    public string? ValuePrefix { get; set; }
    public string? WebhookId { get; set; }
}
