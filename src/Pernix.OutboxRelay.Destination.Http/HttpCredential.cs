namespace Pernix.OutboxRelay.Destination.Http;

internal sealed class HttpCredential
{
    public string? Username { get; set; }
    public string? Password { get; set; }
    public string? Token { get; set; }
    public string? ClientId { get; set; }
    public string? ClientSecret { get; set; }
    public string? ApiKey { get; set; }
    public string? SigningSecret { get; set; }
    public string? CertificatePassword { get; set; }
}
