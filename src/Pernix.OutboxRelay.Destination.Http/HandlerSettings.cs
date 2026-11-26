using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Security.Cryptography.X509Certificates;
using Pernix.OutboxRelay.Destination.Http.Options;

namespace Pernix.OutboxRelay.Destination.Http;

internal sealed class HandlerSettings
{
    internal const string Http11 = "1.1";
    internal const string Http2 = "2";
    internal const string Http2Cleartext = "2-cleartext";

    public HandlerSettings(
        TimeSpan connectTimeout,
        int maxRequestsInFlight,
        string? httpVersion,
        X509Certificate2Collection? trustedRoots = null,
        X509Certificate2? clientCertificate = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(connectTimeout, TimeSpan.Zero);
        ArgumentOutOfRangeException.ThrowIfLessThan(maxRequestsInFlight, 1);
        if (!TryParseVersion(httpVersion, out var version, out var policy))
        {
            throw new ArgumentException($"unknown HttpVersion; expected \"{Http11}\", \"{Http2}\" or \"{Http2Cleartext}\"", nameof(httpVersion));
        }

        ConnectTimeout = connectTimeout;
        MaxRequestsInFlight = maxRequestsInFlight;
        RequestVersion = version;
        VersionPolicy = policy;
        TrustedRoots = trustedRoots;
        ClientCertificate = clientCertificate;
    }

    public TimeSpan ConnectTimeout { get; }
    public int MaxRequestsInFlight { get; }

    public Version RequestVersion { get; }
    public HttpVersionPolicy VersionPolicy { get; }

    public X509Certificate2Collection? TrustedRoots { get; }
    public X509Certificate2? ClientCertificate { get; }

    internal static bool TryParseVersion(string? value, [NotNullWhen(true)] out Version? version, out HttpVersionPolicy policy)
    {
        switch (value)
        {
            case null or Http11:
                version = HttpVersion.Version11;
                policy = HttpVersionPolicy.RequestVersionOrLower;
                return true;
            case Http2 or Http2Cleartext:
                version = HttpVersion.Version20;
                policy = HttpVersionPolicy.RequestVersionExact;
                return true;
            default:
                version = null;
                policy = default;
                return false;
        }
    }

    internal static X509Certificate2Collection LoadCaBundle(string path)
    {
        ArgumentNullException.ThrowIfNull(path);

        var roots = new X509Certificate2Collection();
        roots.ImportFromPemFile(path);
        if (roots.Count == 0)
        {
            throw new InvalidDataException($"'{path}' holds no PEM certificate");
        }

        return roots;
    }

    internal static X509Certificate2 LoadClientCertificate(ClientCertificateOptions options, string? password)
    {
        ArgumentNullException.ThrowIfNull(options);

        if (options.PfxPath is { } pfx)
        {
            return X509CertificateLoader.LoadPkcs12FromFile(pfx, password);
        }

        if (options.CertificatePath is not { } certificate || options.KeyPath is not { } key)
        {
            throw new ArgumentException("client certificate needs CertificatePath and KeyPath, or PfxPath", nameof(options));
        }

        var loaded = X509Certificate2.CreateFromPemFile(certificate, key);
        if (!OperatingSystem.IsWindows())
        {
            return loaded;
        }

        // SChannel cannot present a certificate whose key is ephemeral, so the PEM pair is round-tripped through PKCS#12.
        using (loaded)
        {
            return X509CertificateLoader.LoadPkcs12(loaded.Export(X509ContentType.Pkcs12), null);
        }
    }
}
