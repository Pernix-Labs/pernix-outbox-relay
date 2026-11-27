using System.Net;
using Pernix.OutboxRelay.Core.Leadership;
using Pernix.OutboxRelay.Integration.Tests.Kafka;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Microsoft.Extensions.DependencyInjection;
using Testcontainers.PostgreSql;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.Leadership;

// Own container: pausing the shared fixture would break every other test.
public sealed class ReadinessTests(KafkaFixture kafka)
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(15);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Unreachable_database_fails_readiness_not_liveness_and_recovers()
    {
        await using var container = TestContainer.PostgreSql("postgres:17.11").Build();
        await container.StartBoundedAsync(Ct);
        var schema = PostgreSqlFixture.NewSchema();
        await PostgreSqlFixture.CreateOutboxSchemaAsync(container.GetConnectionString(), schema);
        var outbox = RelayFactory.NewOutboxName();

        await using var factory = new RelayFactory()
            .WithRelaySettings(RelayFactory.OutboxSettings(container.GetConnectionString(), kafka.BootstrapServers, (outbox, schema)));
        using var client = factory.CreateClient();
        var state = factory.Services.GetRequiredService<OutboxRuntimeState>();
        Assert.True(await LeadershipTests.EventuallyAsync(() => state.Outboxes.Single().IsLeader, TimeSpan.FromSeconds(10)));
        Assert.Equal(HttpStatusCode.OK, await StatusAsync(client, "/readyz"));

        await container.PauseAsync(Ct);

        Assert.True(await EventuallyStatusAsync(client, "/readyz", HttpStatusCode.ServiceUnavailable), "/readyz not 503 within 15 s");
        Assert.Equal(HttpStatusCode.OK, await StatusAsync(client, "/livez"));

        await container.UnpauseAsync(Ct);

        Assert.True(await EventuallyStatusAsync(client, "/readyz", HttpStatusCode.OK), "/readyz not 200 within 15 s");
    }

    [Fact]
    public async Task Shutdown_while_leader_check_hangs_ends_loop_without_fault()
    {
        await using var container = TestContainer.PostgreSql("postgres:17.11").Build();
        await container.StartBoundedAsync(Ct);
        var schema = PostgreSqlFixture.NewSchema();
        await PostgreSqlFixture.CreateOutboxSchemaAsync(container.GetConnectionString(), schema);

        var factory = new RelayFactory()
            .WithRelaySettings(RelayFactory.OutboxSettings(container.GetConnectionString(), kafka.BootstrapServers, (RelayFactory.NewOutboxName(), schema)));
        var status = factory.Services.GetRequiredService<OutboxRuntimeState>().Outboxes.Single();
        Assert.True(await LeadershipTests.EventuallyAsync(() => status.IsLeader, TimeSpan.FromSeconds(10)));

        await container.PauseAsync(Ct);
        await Task.Delay(TimeSpan.FromSeconds(1.5), Ct);
        await factory.DisposeAsync();
        await container.UnpauseAsync(Ct);

        Assert.Equal(TaskStatus.RanToCompletion, status.Loop!.Status);
        Assert.False(status.IsLeader);
    }

    private static async Task<HttpStatusCode> StatusAsync(HttpClient client, string path)
    {
        using var response = await client.GetAsync(path, Ct);
        return response.StatusCode;
    }

    private static async Task<bool> EventuallyStatusAsync(HttpClient client, string path, HttpStatusCode expected)
    {
        var end = DateTime.UtcNow + Bound;
        while (DateTime.UtcNow < end)
        {
            if (await StatusAsync(client, path) == expected)
            {
                return true;
            }

            await Task.Delay(250, Ct);
        }

        return false;
    }
}
