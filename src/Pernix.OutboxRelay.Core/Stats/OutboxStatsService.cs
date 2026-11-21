using Pernix.OutboxRelay.Core.Diagnostics;
using Pernix.OutboxRelay.Core.Leadership;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Pernix.OutboxRelay.Core.Stats;

public sealed class OutboxStatsService(
    ValidatedOutboxes validatedOutboxes,
    OutboxRuntimeState state,
    IHostApplicationLifetime lifetime,
    TimeProvider timeProvider,
    ILogger<OutboxStatsService> logger) : IHostedService, IDisposable
{
    private readonly CancellationTokenSource stopping = new();
    private CancellationTokenRegistration lifetimeRegistration;
    private OutboxStatsCache[] caches = [];
    private Task[] loops = [];
    private int disposed;

    internal IReadOnlyList<OutboxStatsCache> Caches => caches;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        var entries = validatedOutboxes.Entries;
        var statuses = state.Outboxes;
        if (entries.Count != statuses.Count)
        {
            throw new InvalidOperationException($"{nameof(OutboxStatsService)} needs one runtime status per validated outbox. Register it after {nameof(OutboxWorkerService)}.");
        }

        // Resolved before anything starts, so a missing name leaves nothing registered.
        var paired = new OutboxRuntimeStatus[entries.Count];
        for (var i = 0; i < entries.Count; i++)
        {
            paired[i] = StatusOf(statuses, entries[i].Options.Name!);
        }

        lifetimeRegistration = lifetime.ApplicationStopping.Register(static s => ((CancellationTokenSource)s!).Cancel(), stopping);

        var stoppingToken = stopping.Token;
        caches = new OutboxStatsCache[entries.Count];
        loops = new Task[entries.Count];
        for (var i = 0; i < entries.Count; i++)
        {
            var status = paired[i];
            var cache = new OutboxStatsCache(status, timeProvider);
            OutboxStatsGauges.Register(cache);
            caches[i] = cache;

            var loop = new OutboxStatsLoop(status.Name, entries[i].Store, status, cache, timeProvider, logger);
            loops[i] = Task.Run(() => loop.RunAsync(stoppingToken), CancellationToken.None);
        }

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        if (Volatile.Read(ref disposed) != 0)
        {
            return;
        }

        await stopping.CancelAsync().ConfigureAwait(false);

        await Task.WhenAll(loops).WaitAsync(cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        UnregisterAll();
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
        {
            return;
        }

        lifetimeRegistration.Dispose();
        stopping.Cancel();
        stopping.Dispose();
        UnregisterAll();
    }

    private static OutboxRuntimeStatus StatusOf(IReadOnlyList<OutboxRuntimeStatus> statuses, string name)
    {
        for (var i = 0; i < statuses.Count; i++)
        {
            if (string.Equals(statuses[i].Name, name, StringComparison.Ordinal))
            {
                return statuses[i];
            }
        }

        throw new InvalidOperationException($"{nameof(OutboxStatsService)} found no runtime status for outbox '{name}'. Register it after {nameof(OutboxWorkerService)}.");
    }

    private void UnregisterAll()
    {
        foreach (var cache in caches)
        {
            OutboxStatsGauges.Unregister(cache);
        }
    }
}
