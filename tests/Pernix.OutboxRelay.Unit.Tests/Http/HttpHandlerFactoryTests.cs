using System.Collections;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text;
using Pernix.OutboxRelay.Destination.Http;
using Pernix.OutboxRelay.Destination.Http.Options;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Http;

public sealed class HttpHandlerFactoryTests : IDisposable
{
    private const string TestPassword = "test-pfx-password";

    private readonly string directory = Directory.CreateTempSubdirectory("relay-http-tls-").FullName;

    public void Dispose() => Directory.Delete(directory, recursive: true);

    [Fact]
    public void Every_handler_setting_is_applied()
    {
        using var handler = HttpHandlerFactory.Create(new HandlerSettings(TimeSpan.FromSeconds(3), 17, null), new Hashtable());

        Assert.False(handler.AllowAutoRedirect);
        Assert.False(handler.UseCookies);
        Assert.Equal(DecompressionMethods.None, handler.AutomaticDecompression);
        Assert.False(handler.UseProxy);
        Assert.Null(handler.Proxy);
        Assert.Null(handler.ActivityHeadersPropagator);
        Assert.False(handler.PreAuthenticate);
        Assert.Null(handler.Credentials);
        Assert.Equal(17, handler.MaxConnectionsPerServer);
        Assert.Equal(TimeSpan.FromMinutes(5), handler.PooledConnectionLifetime);
        Assert.Equal(TimeSpan.FromSeconds(90), handler.PooledConnectionIdleTimeout);
        Assert.Equal(TimeSpan.FromSeconds(3), handler.ConnectTimeout);
        Assert.Equal(64 * 1024, handler.MaxResponseDrainSize);
        Assert.Equal(TimeSpan.FromSeconds(2), handler.ResponseDrainTimeout);
        Assert.True(handler.EnableMultipleHttp2Connections);
        Assert.Null(handler.SslOptions.RemoteCertificateValidationCallback);
        Assert.Null(handler.SslOptions.ClientCertificates);
    }

    [Theory]
    [InlineData("HTTPS_PROXY")]
    [InlineData("HTTP_PROXY")]
    [InlineData("ALL_PROXY")]
    [InlineData("https_proxy")]
    [InlineData("http_proxy")]
    [InlineData("all_proxy")]
    [InlineData("Http_Proxy")]
    public void Proxy_env_variable_enables_proxy(string name)
    {
        using var handler = HttpHandlerFactory.Create(
            new HandlerSettings(TimeSpan.FromSeconds(1), 1, null), new Hashtable { [name] = "http://proxy.test:3128" });

        Assert.True(handler.UseProxy);
        Assert.Null(handler.Proxy);
    }

    [Fact]
    public void Proxy_is_off_without_proxy_env_variable()
    {
        var environment = new Hashtable { ["NO_PROXY"] = "internal.test", ["HTTP_PROXY"] = "", ["PATH"] = "/usr/bin" };

        using var handler = HttpHandlerFactory.Create(new HandlerSettings(TimeSpan.FromSeconds(1), 1, null), environment);

        Assert.False(handler.UseProxy);
    }

    [Theory]
    [InlineData(null, "1.1", HttpVersionPolicy.RequestVersionOrLower)]
    [InlineData("1.1", "1.1", HttpVersionPolicy.RequestVersionOrLower)]
    [InlineData("2", "2.0", HttpVersionPolicy.RequestVersionExact)]
    [InlineData("2-cleartext", "2.0", HttpVersionPolicy.RequestVersionExact)]
    public void Http_version_maps_to_request_version_and_policy(string? setting, string version, HttpVersionPolicy policy)
    {
        var settings = new HandlerSettings(TimeSpan.FromSeconds(1), 1, setting);

        Assert.Equal(Version.Parse(version), settings.RequestVersion);
        Assert.Equal(policy, settings.VersionPolicy);
        Assert.Same(version == "1.1" ? HttpVersion.Version11 : HttpVersion.Version20, settings.RequestVersion);
    }

    [Theory]
    [InlineData("2.0")]
    [InlineData("h2c")]
    [InlineData("1.0")]
    [InlineData("")]
    public void Unknown_http_version_is_refused(string setting)
    {
        Assert.False(HandlerSettings.TryParseVersion(setting, out _, out _));
        Assert.Throws<ArgumentException>(() => new HandlerSettings(TimeSpan.FromSeconds(1), 1, setting));
    }

    [Fact]
    public void Private_ca_accepts_its_certificate_and_rejects_others()
    {
        using var ca = CreateCa("Relay Test CA");
        using var otherCa = CreateCa("Other Test CA");
        using var trusted = CreateLeaf(ca, "127.0.0.1", serverAuth: true);
        using var untrusted = CreateLeaf(otherCa, "127.0.0.1", serverAuth: true);
        using var selfSigned = CreateCa("Self Signed");
        var roots = HandlerSettings.LoadCaBundle(WritePem("ca.pem", ca));

        using var handler = HttpHandlerFactory.Create(new HandlerSettings(TimeSpan.FromSeconds(1), 1, null, roots));
        var callback = handler.SslOptions.RemoteCertificateValidationCallback!;

        Assert.True(callback(this, trusted, null, SslPolicyErrors.RemoteCertificateChainErrors));
        Assert.False(callback(this, untrusted, null, SslPolicyErrors.RemoteCertificateChainErrors));
        Assert.False(callback(this, selfSigned, null, SslPolicyErrors.RemoteCertificateChainErrors));
        Assert.False(callback(this, trusted, null, SslPolicyErrors.RemoteCertificateNameMismatch));
        Assert.False(callback(this, null, null, SslPolicyErrors.RemoteCertificateNotAvailable));
    }

    [Fact]
    public void Ca_bundle_loads_every_certificate()
    {
        using var first = CreateCa("First CA");
        using var second = CreateCa("Second CA");
        var path = Path.Combine(directory, "bundle.pem");
        File.WriteAllText(path, first.ExportCertificatePem() + "\n" + second.ExportCertificatePem());

        var roots = HandlerSettings.LoadCaBundle(path);

        Assert.Equal(2, roots.Count);
    }

    [Fact]
    public void Ca_bundle_without_certificate_fails()
    {
        var path = Path.Combine(directory, "empty.pem");
        File.WriteAllText(path, "not a certificate");

        Assert.Throws<InvalidDataException>(() => HandlerSettings.LoadCaBundle(path));
        Assert.ThrowsAny<IOException>(() => HandlerSettings.LoadCaBundle(Path.Combine(directory, "missing.pem")));
    }

    [Fact]
    public void Client_certificate_loads_from_pem_pair()
    {
        using var ca = CreateCa("Relay Test CA");
        using var client = CreateLeaf(ca, "relay-client", serverAuth: false);
        var certificatePath = WritePem("client.crt", client);
        var keyPath = Path.Combine(directory, "client.key");
        File.WriteAllText(keyPath, client.GetRSAPrivateKey()!.ExportPkcs8PrivateKeyPem());

        using var loaded = HandlerSettings.LoadClientCertificate(
            new ClientCertificateOptions { CertificatePath = certificatePath, KeyPath = keyPath }, null);

        Assert.Equal(client.Thumbprint, loaded.Thumbprint);
        Assert.True(loaded.HasPrivateKey);
    }

    [Fact]
    public void Client_certificate_loads_from_pfx_with_password()
    {
        using var ca = CreateCa("Relay Test CA");
        using var client = CreateLeaf(ca, "relay-client", serverAuth: false);
        var pfxPath = Path.Combine(directory, "client.pfx");
        File.WriteAllBytes(pfxPath, client.Export(X509ContentType.Pkcs12, TestPassword));
        var options = new ClientCertificateOptions { PfxPath = pfxPath, Credential = "Ledger" };

        using var loaded = HandlerSettings.LoadClientCertificate(options, TestPassword);

        Assert.Equal(client.Thumbprint, loaded.Thumbprint);
        Assert.True(loaded.HasPrivateKey);
        Assert.ThrowsAny<CryptographicException>(() => HandlerSettings.LoadClientCertificate(options, "wrong"));
    }

    [Fact]
    public void Client_certificate_without_paths_fails()
    {
        Assert.Throws<ArgumentException>(() =>
            HandlerSettings.LoadClientCertificate(new ClientCertificateOptions { CertificatePath = "client.crt" }, null));
    }

    [Fact]
    public async Task Handshake_trusts_private_ca_and_presents_client_certificate()
    {
        using var ca = CreateCa("Relay Test CA");
        using var server = Reimport(CreateLeaf(ca, "127.0.0.1", serverAuth: true));
        using var client = CreateLeaf(ca, "relay-client", serverAuth: false);
        var roots = HandlerSettings.LoadCaBundle(WritePem("ca.pem", ca));
        var pfxPath = Path.Combine(directory, "client.pfx");
        File.WriteAllBytes(pfxPath, client.Export(X509ContentType.Pkcs12, TestPassword));
        using var loadedClient = HandlerSettings.LoadClientCertificate(new ClientCertificateOptions { PfxPath = pfxPath }, TestPassword);

        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        var presented = ServeOnceAsync(listener, server);

        var settings = new HandlerSettings(TimeSpan.FromSeconds(5), 1, null, roots, loadedClient);
        using var invoker = new HttpMessageInvoker(HttpHandlerFactory.Create(settings));
        using var request = new HttpRequestMessage(HttpMethod.Get, $"https://127.0.0.1:{port}/")
        {
            Version = settings.RequestVersion,
            VersionPolicy = settings.VersionPolicy,
        };
        using var response = await invoker.SendAsync(request, TestContext.Current.CancellationToken);

        Assert.Equal(HttpStatusCode.NoContent, response.StatusCode);
        Assert.Equal(client.Thumbprint, (await presented)?.Thumbprint);
    }

    [Fact]
    public async Task Handshake_rejects_server_from_other_ca()
    {
        using var ca = CreateCa("Relay Test CA");
        using var otherCa = CreateCa("Other Test CA");
        using var server = Reimport(CreateLeaf(otherCa, "127.0.0.1", serverAuth: true));
        var roots = HandlerSettings.LoadCaBundle(WritePem("ca.pem", ca));

        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        var serving = ServeOnceAsync(listener, server);

        using var invoker = new HttpMessageInvoker(HttpHandlerFactory.Create(new HandlerSettings(TimeSpan.FromSeconds(5), 1, null, roots)));
        using var request = new HttpRequestMessage(HttpMethod.Get, $"https://127.0.0.1:{port}/");

        await Assert.ThrowsAsync<HttpRequestException>(() => invoker.SendAsync(request, TestContext.Current.CancellationToken));
        _ = await Record.ExceptionAsync(() => serving);
    }

    private static async Task<X509Certificate2?> ServeOnceAsync(TcpListener listener, X509Certificate2 certificate)
    {
        using var socket = await listener.AcceptTcpClientAsync();
        X509Certificate2? presented = null;
        await using var tls = new SslStream(socket.GetStream(), leaveInnerStreamOpen: false, (_, remote, _, _) =>
        {
            presented = remote is null ? null : X509CertificateLoader.LoadCertificate(remote.GetRawCertData());
            return true;
        });
        await tls.AuthenticateAsServerAsync(new SslServerAuthenticationOptions
        {
            ServerCertificate = certificate,
            ClientCertificateRequired = true,
            EnabledSslProtocols = System.Security.Authentication.SslProtocols.Tls12 | System.Security.Authentication.SslProtocols.Tls13,
        });

        var buffer = new byte[4096];
        var received = new StringBuilder();
        while (!received.ToString().Contains("\r\n\r\n", StringComparison.Ordinal))
        {
            var read = await tls.ReadAsync(buffer);
            if (read == 0)
            {
                break;
            }

            received.Append(Encoding.ASCII.GetString(buffer, 0, read));
        }

        await tls.WriteAsync("HTTP/1.1 204 No Content\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"u8.ToArray());
        await tls.FlushAsync();
        return presented;
    }

    private string WritePem(string name, X509Certificate2 certificate)
    {
        var path = Path.Combine(directory, name);
        File.WriteAllText(path, certificate.ExportCertificatePem());
        return path;
    }

    private static X509Certificate2 Reimport(X509Certificate2 certificate)
    {
        using (certificate)
        {
            return X509CertificateLoader.LoadPkcs12(certificate.Export(X509ContentType.Pkcs12), null);
        }
    }

    private static X509Certificate2 CreateCa(string name)
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest($"CN={name}", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(true, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.KeyCertSign | X509KeyUsageFlags.CrlSign, true));
        request.CertificateExtensions.Add(new X509SubjectKeyIdentifierExtension(request.PublicKey, false));
        return request.CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(30));
    }

    private static X509Certificate2 CreateLeaf(X509Certificate2 issuer, string name, bool serverAuth)
    {
        using var key = RSA.Create(2048);
        var request = new CertificateRequest($"CN={name}", key, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1);
        request.CertificateExtensions.Add(new X509BasicConstraintsExtension(false, false, 0, true));
        request.CertificateExtensions.Add(new X509KeyUsageExtension(X509KeyUsageFlags.DigitalSignature | X509KeyUsageFlags.KeyEncipherment, true));
        request.CertificateExtensions.Add(new X509EnhancedKeyUsageExtension(
            [new Oid(serverAuth ? "1.3.6.1.5.5.7.3.1" : "1.3.6.1.5.5.7.3.2")], false));
        if (serverAuth)
        {
            var names = new SubjectAlternativeNameBuilder();
            names.AddIpAddress(IPAddress.Parse(name));
            request.CertificateExtensions.Add(names.Build());
        }

        var serial = RandomNumberGenerator.GetBytes(16);
        using var issued = request.Create(issuer, DateTimeOffset.UtcNow.AddHours(-1), DateTimeOffset.UtcNow.AddDays(7), serial);
        return issued.CopyWithPrivateKey(key);
    }
}
