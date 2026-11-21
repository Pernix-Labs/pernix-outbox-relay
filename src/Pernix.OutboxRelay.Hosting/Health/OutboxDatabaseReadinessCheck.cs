using Pernix.OutboxRelay.Core.Leadership;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Pernix.OutboxRelay.Hosting.Health;

// Unhealthy while any worker's latest connect attempt failed. Leadership does not affect readiness.
public sealed class OutboxDatabaseReadinessCheck(OutboxRuntimeState state) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var unreachable = state.Outboxes.Where(o => !o.DatabaseReachable).Select(o => o.Name).ToArray();

        return Task.FromResult(unreachable.Length == 0
            ? HealthCheckResult.Healthy()
            : HealthCheckResult.Unhealthy($"Outbox database unreachable: {string.Join(", ", unreachable)}"));
    }
}
