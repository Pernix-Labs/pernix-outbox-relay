using Pernix.OutboxRelay.Core.Leadership;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace Pernix.OutboxRelay.Host.Health;

// Unhealthy if any worker loop ended or faulted while the host is not stopping. DB errors never end a loop.
// Also unhealthy if any worker is past its progress due time.
public sealed class WorkerLoopLivenessCheck(OutboxRuntimeState state, IHostApplicationLifetime lifetime, TimeProvider timeProvider) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        if (lifetime.ApplicationStopping.IsCancellationRequested)
        {
            return Task.FromResult(HealthCheckResult.Healthy());
        }

        var now = timeProvider.GetUtcNow();
        var outboxes = state.Outboxes;
        var ended = outboxes.Where(o => o.Loop is { IsCompleted: true }).Select(o => o.Name).ToArray();
        var overdue = outboxes.Where(o => o.Loop is not { IsCompleted: true } && now > o.ProgressDueAt).Select(o => o.Name).ToArray();

        var problems = new List<string>(2);
        if (ended.Length > 0)
        {
            problems.Add($"Outbox worker loop ended: {string.Join(", ", ended)}");
        }

        if (overdue.Length > 0)
        {
            problems.Add($"Outbox worker progress overdue: {string.Join(", ", overdue)}");
        }

        return Task.FromResult(problems.Count == 0
            ? HealthCheckResult.Healthy()
            : HealthCheckResult.Unhealthy(string.Join("; ", problems)));
    }
}
