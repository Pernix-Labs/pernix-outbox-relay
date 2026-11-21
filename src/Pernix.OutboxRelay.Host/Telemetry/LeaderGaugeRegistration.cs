using Pernix.OutboxRelay.Core.Leadership;

namespace Pernix.OutboxRelay.Host.Telemetry;

public sealed class LeaderGaugeRegistration(OutboxRuntimeState state) : IHostedService, IDisposable
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        LeaderGauge.Register(state);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken)
    {
        LeaderGauge.Unregister(state);
        return Task.CompletedTask;
    }

    public void Dispose() => LeaderGauge.Unregister(state);
}
