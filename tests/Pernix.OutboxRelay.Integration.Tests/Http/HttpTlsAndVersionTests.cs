using Pernix.OutboxRelay.Integration.Tests.Leadership;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.Http;

public sealed class HttpTlsAndVersionTests(PostgreSqlFixture postgres) : IDisposable
{
    private static readonly TimeSpan DeliveryTimeout = TimeSpan.FromSeconds(60);

    private static readonly HttpRow Row = new("k-1", "{\"id\":1}", new Dictionary<string, string> { ["tenant-id"] = "t-1" });

    private readonly TestCertificates certificates = TestCertificates.Create();

    [Fact]
    public async Task Private_ca_and_client_certificate_deliver_over_mutual_tls()
    {
        await using var endpoint = await HttpTestEndpoint.StartAsync(new EndpointOptions
        {
            ServerCertificate = certificates.Server,
            ClientCertificateRoots = certificates.Roots,
        });
        var setup = await TlsOutboxAsync(endpoint, httpVersion: null);
        setup.Http("Tls:ClientCertificate:CertificatePath", certificates.ClientCertificatePath);
        setup.Http("Tls:ClientCertificate:KeyPath", certificates.ClientKeyPath);

        var request = await DeliverOneAsync(setup, endpoint);

        Assert.Equal("HTTP/1.1", request.Protocol);
        Assert.Equal(certificates.Client.Thumbprint, request.ClientCertificateThumbprint);
    }

    [Fact]
    public async Task Version_2_over_tls_with_alpn_h2_delivers_over_http2()
    {
        await using var endpoint = await HttpTestEndpoint.StartAsync(new EndpointOptions
        {
            Protocols = HttpProtocols.Http2,
            ServerCertificate = certificates.Server,
        });
        var setup = await TlsOutboxAsync(endpoint, "2");

        var request = await DeliverOneAsync(setup, endpoint);

        Assert.Equal("HTTP/2", request.Protocol);
    }

    [Fact]
    public async Task Version_2_cleartext_delivers_to_an_h2c_endpoint_over_http2()
    {
        await using var endpoint = await HttpTestEndpoint.StartAsync(new EndpointOptions { Protocols = HttpProtocols.Http2 });
        var setup = await HttpOutbox.CreateAsync(postgres, url: endpoint.Url("/orders"));
        setup.Http("HttpVersion", "2-cleartext");

        var request = await DeliverOneAsync(setup, endpoint);

        Assert.Equal("HTTP/2", request.Protocol);
    }

    [Fact]
    public async Task Version_2_against_an_http11_only_tls_endpoint_is_a_destination_fault_without_downgrade()
    {
        await using var endpoint = await HttpTestEndpoint.StartAsync(new EndpointOptions
        {
            Protocols = HttpProtocols.Http1,
            ServerCertificate = certificates.Server,
        });
        var setup = await TlsOutboxAsync(endpoint, "2");
        var id = Assert.Single(await setup.InsertAsync(Row));

        await using var relay = setup.Relay();
        _ = relay.Services;

        Assert.True(
            await LeadershipTests.EventuallyAsync(() => setup.Logs.PauseErrors.Count >= 2, DeliveryTimeout),
            "no repeated destination-fault pause");
        Assert.All(setup.Logs.PauseErrors, e => Assert.StartsWith("HttpRequestException: ", e, StringComparison.Ordinal));
        Assert.Empty(endpoint.Requests);

        // A destination fault spends no retry and leaves the row as written.
        var state = Assert.Single(await setup.FailureStatesAsync());
        Assert.Equal(new(id, 0, null), state);
    }

    public void Dispose() => certificates.Dispose();

    private async Task<HttpOutbox> TlsOutboxAsync(HttpTestEndpoint endpoint, string? httpVersion)
    {
        Assert.Equal("https", endpoint.BaseUri.Scheme);
        var setup = await HttpOutbox.CreateAsync(postgres, url: endpoint.Url("/orders"));
        setup.Http("Tls:CaCertificatePath", certificates.CaPath);
        if (httpVersion is not null)
        {
            setup.Http("HttpVersion", httpVersion);
        }

        return setup;
    }

    private static async Task<RecordedRequest> DeliverOneAsync(HttpOutbox setup, HttpTestEndpoint endpoint)
    {
        Assert.Single(await setup.InsertAsync(Row));
        await using var relay = setup.Relay();
        _ = relay.Services;
        Assert.True(await setup.DrainedAsync(DeliveryTimeout), $"row not delivered: {string.Join(" | ", setup.Logs.PauseErrors)}");

        var request = Assert.Single(endpoint.Requests);
        Assert.Equal("POST", request.Method);
        Assert.Equal("/orders", request.Target);
        Assert.Equal(Row.PayloadBytes, request.Body);
        HttpDeliveryTests.AssertHeaders(request, new Dictionary<string, string>
        {
            ["Content-Type"] = "application/octet-stream",
            ["tenant-id"] = "t-1",
        });
        return request;
    }
}
