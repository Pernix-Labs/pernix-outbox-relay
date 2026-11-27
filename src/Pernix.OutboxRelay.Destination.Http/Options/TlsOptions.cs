namespace Pernix.OutboxRelay.Destination.Http.Options;

internal sealed class TlsOptions
{
    public string? CaCertificatePath { get; set; }
    public ClientCertificateOptions? ClientCertificate { get; set; }
}

internal sealed class ClientCertificateOptions
{
    public string? CertificatePath { get; set; }
    public string? KeyPath { get; set; }
    public string? PfxPath { get; set; }
    public string? Credential { get; set; }
}
