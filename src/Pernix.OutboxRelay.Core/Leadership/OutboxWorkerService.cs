using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Pernix.OutboxRelay.Core.Leadership;

public sealed class OutboxWorkerService(
    ValidatedOutboxes validatedOutboxes,
    OutboxRuntimeState state,
    ILeadershipJitter jitter,
    TimeProvider timeProvider,
    ILogger<OutboxWorkerService> logger) : IHostedService, IDisposable
{
    private readonly CancellationTokenSource stopping = new();
    private Task[] loops = [];
    private int disposed;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        var statuses = validatedOutboxes.Entries.Select(e => new OutboxRuntimeStatus(e.Options.Name!)).ToArray();
        state.SetOutboxes(statuses);

        var stoppingToken = stopping.Token;
        loops = new Task[statuses.Length];
        for (var i = 0; i < statuses.Length; i++)
        {
            var status = statuses[i];

            // Initial due covers the start jitter (≤ 2 s) plus grace.
            status.ExpectProgress(OutboxWorker.StartDelayBound, timeProvider);
            var worker = new OutboxWorker(validatedOutboxes.Entries[i], status, state, jitter, timeProvider, logger);
            var loop = Task.Run(() => worker.RunAsync(stoppingToken), CancellationToken.None);
            _ = loop.ContinueWith(
                t => logger.WorkerFaulted(status.Name, t.Exception!.GetBaseException()),
                CancellationToken.None,
                TaskContinuationOptions.OnlyOnFaulted,
                TaskScheduler.Default);
            status.Loop = loop;
            loops[i] = loop;
        }

        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        // A failed start disposes the host's services, and the host may still call stop afterwards.
        if (Volatile.Read(ref disposed) != 0)
        {
            return;
        }

        await stopping.CancelAsync().ConfigureAwait(false);

        // Faults are already logged by the continuation. Stop returns when loops end or the host's shutdown token fires.
        await Task.WhenAll(loops).WaitAsync(cancellationToken).ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
        {
            return;
        }

        stopping.Cancel();
        stopping.Dispose();
    }
}
