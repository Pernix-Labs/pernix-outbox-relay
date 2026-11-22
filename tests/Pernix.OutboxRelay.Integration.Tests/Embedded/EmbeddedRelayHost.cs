using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Destination.Kafka;
using Pernix.OutboxRelay.Hosting;
using Pernix.OutboxRelay.Outbox.PostgreSql;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.Configuration;

namespace Pernix.OutboxRelay.Integration.Tests.Embedded;

// Sample web host embedding the relay through the package entry points. Never uses the image's Program.
internal sealed class EmbeddedRelayHost : IAsyncDisposable
{
    private readonly WebApplication app;

    private EmbeddedRelayHost(WebApplication app)
    {
        this.app = app;
        HttpClient = app.GetTestClient();
    }

    public HttpClient HttpClient { get; }

    public IServiceProvider Services => app.Services;

    public static async Task<EmbeddedRelayHost> StartAsync(IDictionary<string, string?> settings, CancellationToken cancellationToken)
    {
        var builder = WebApplication.CreateBuilder();
        builder.WebHost.UseTestServer();
        builder.Configuration.AddInMemoryCollection(Prefixed(settings));
        builder.AddOutboxRelay().AddPostgreSqlOutbox().AddKafkaDestination();

        var app = builder.Build();
        app.MapOutboxRelayHealth();

        try
        {
            await app.StartAsync(cancellationToken);
        }
        catch
        {
            await app.DisposeAsync();
            throw;
        }

        return new EmbeddedRelayHost(app);
    }

    public static IEnumerable<KeyValuePair<string, string?>> Prefixed(IDictionary<string, string?> settings) =>
        settings.Select(kv => KeyValuePair.Create(RelayConfiguration.SectionName + ":" + kv.Key, kv.Value));

    public async ValueTask DisposeAsync()
    {
        HttpClient.Dispose();
        await app.StopAsync();
        await app.DisposeAsync();
    }
}
