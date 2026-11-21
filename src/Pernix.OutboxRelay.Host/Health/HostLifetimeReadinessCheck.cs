using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Pernix.OutboxRelay.Host.Health;

public sealed class HostLifetimeReadinessCheck(IHostApplicationLifetime lifetime) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        var ready = lifetime.ApplicationStarted.IsCancellationRequested
            && !lifetime.ApplicationStopping.IsCancellationRequested;

        return Task.FromResult(ready ? HealthCheckResult.Healthy() : HealthCheckResult.Unhealthy());
    }
}
