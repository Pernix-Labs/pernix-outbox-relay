using Pernix.OutboxRelay.Destination.Http;
using Pernix.OutboxRelay.Hosting;
using Pernix.OutboxRelay.Integration.Tests.Embedded;
using Pernix.OutboxRelay.Integration.Tests.Leadership;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Pernix.OutboxRelay.Outbox.PostgreSql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.Http;

public sealed class HttpEmbeddedTests(PostgreSqlFixture postgres)
{
    private static readonly TimeSpan DeliveryTimeout = TimeSpan.FromSeconds(30);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Embedded_relay_with_postgresql_outbox_and_http_destination_delivers_a_row()
    {
        await using var endpoint = await HttpTestEndpoint.StartAsync();
        var setup = await HttpOutbox.CreateAsync(postgres, url: endpoint.Url("/embedded/{id}"));

        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(EmbeddedRelayHost.Prefixed(setup.Settings));
        builder.AddOutboxRelay().AddPostgreSqlOutbox().AddHttpDestination();
        using var host = builder.Build();
        await host.StartAsync(Ct);

        try
        {
            var row = new HttpRow("embedded-1", "{\"id\":1}", new Dictionary<string, string> { ["type"] = "Embedded" });
            var id = Assert.Single(await setup.InsertAsync(row));

            Assert.True(await LeadershipTests.EventuallyAsync(() => endpoint.Requests.Count > 0, DeliveryTimeout), "no request");
            Assert.True(await setup.DrainedAsync(DeliveryTimeout), "row not deleted");

            var request = Assert.Single(endpoint.Requests);
            Assert.Equal($"/embedded/{id}", request.Target);
            Assert.Equal(row.PayloadBytes, request.Body);
            Assert.Equal("Embedded", request.Header("type"));
        }
        finally
        {
            await host.StopAsync(Ct);
        }
    }
}
