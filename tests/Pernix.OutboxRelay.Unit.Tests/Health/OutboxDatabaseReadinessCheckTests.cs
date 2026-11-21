using Pernix.OutboxRelay.Core.Leadership;
using Pernix.OutboxRelay.Hosting.Health;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Health;

public sealed class OutboxDatabaseReadinessCheckTests
{
    [Fact]
    public async Task No_outboxes_is_healthy()
    {
        Assert.Equal(HealthStatus.Healthy, await CheckAsync(new OutboxRuntimeState()));
    }

    [Fact]
    public async Task All_reachable_is_healthy()
    {
        var state = new OutboxRuntimeState();
        state.SetOutboxes([new OutboxRuntimeStatus("a"), new OutboxRuntimeStatus("b")]);

        Assert.Equal(HealthStatus.Healthy, await CheckAsync(state));
    }

    [Fact]
    public async Task One_unreachable_is_unhealthy()
    {
        var state = new OutboxRuntimeState();
        state.SetOutboxes([new OutboxRuntimeStatus("a"), new OutboxRuntimeStatus("b") { DatabaseReachable = false }]);

        Assert.Equal(HealthStatus.Unhealthy, await CheckAsync(state));
    }

    [Fact]
    public async Task Leadership_difference_does_not_affect_readiness()
    {
        var state = new OutboxRuntimeState();
        state.SetOutboxes([new OutboxRuntimeStatus("a") { IsLeader = true }, new OutboxRuntimeStatus("b") { IsLeader = false }]);

        Assert.Equal(HealthStatus.Healthy, await CheckAsync(state));
    }

    private static async Task<HealthStatus> CheckAsync(OutboxRuntimeState state)
    {
        var check = new OutboxDatabaseReadinessCheck(state);
        var context = new HealthCheckContext
        {
            Registration = new HealthCheckRegistration("outbox-database", check, HealthStatus.Unhealthy, [OutboxRelayHealthChecks.ReadyTag]),
        };

        var result = await check.CheckHealthAsync(context, TestContext.Current.CancellationToken);
        return result.Status;
    }
}
