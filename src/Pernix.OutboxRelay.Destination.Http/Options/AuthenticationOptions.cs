namespace Pernix.OutboxRelay.Destination.Http.Options;

internal enum AuthenticationScheme
{
    None,
    Basic,
    Bearer,
    ApiKey,
    OAuth2ClientCredentials,
}

internal enum ClientCredentialsLocation
{
    Header,
    Body,
}

internal sealed class AuthenticationOptions
{
    public AuthenticationScheme? Scheme { get; set; }
    public string? Credential { get; set; }
    public string? HeaderName { get; set; }
    public string? ValuePrefix { get; set; }
    public string? TokenUrl { get; set; }
    public string? Scope { get; set; }
    public string? Audience { get; set; }
    public ClientCredentialsLocation? ClientCredentialsIn { get; set; }
}
