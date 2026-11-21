using Pernix.OutboxRelay.Host.Health;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Health;

public sealed class HostLifetimeReadinessCheckTests
{
    [Fact]
    public async Task Before_started_is_unhealthy()
    {
        using var lifetime = new FakeLifetime();

        Assert.Equal(HealthStatus.Unhealthy, await CheckAsync(lifetime));
    }

    [Fact]
    public async Task After_started_is_healthy()
    {
        using var lifetime = new FakeLifetime();
        await lifetime.Started.CancelAsync();

        Assert.Equal(HealthStatus.Healthy, await CheckAsync(lifetime));
    }

    [Fact]
    public async Task After_stopping_is_unhealthy()
    {
        using var lifetime = new FakeLifetime();
        await lifetime.Started.CancelAsync();
        await lifetime.Stopping.CancelAsync();

        Assert.Equal(HealthStatus.Unhealthy, await CheckAsync(lifetime));
    }

    private static async Task<HealthStatus> CheckAsync(IHostApplicationLifetime lifetime)
    {
        var check = new HostLifetimeReadinessCheck(lifetime);
        var context = new HealthCheckContext
        {
            Registration = new HealthCheckRegistration("host-lifetime", check, HealthStatus.Unhealthy, [HealthTags.Ready]),
        };

        var result = await check.CheckHealthAsync(context, TestContext.Current.CancellationToken);
        return result.Status;
    }

    private sealed class FakeLifetime : IHostApplicationLifetime, IDisposable
    {
        public CancellationTokenSource Started { get; } = new();
        public CancellationTokenSource Stopping { get; } = new();
        public CancellationTokenSource Stopped { get; } = new();

        public CancellationToken ApplicationStarted => Started.Token;
        public CancellationToken ApplicationStopping => Stopping.Token;
        public CancellationToken ApplicationStopped => Stopped.Token;

        public void StopApplication() => Stopping.Cancel();

        public void Dispose()
        {
            Started.Dispose();
            Stopping.Dispose();
            Stopped.Dispose();
        }
    }
}
