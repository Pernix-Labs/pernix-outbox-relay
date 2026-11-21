using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Leadership;
using Microsoft.Extensions.Logging;

namespace Pernix.OutboxRelay.Core.Stats;

internal sealed class OutboxStatsLoop(
    string outbox,
    IValidatedOutboxStore store,
    OutboxRuntimeStatus status,
    OutboxStatsCache cache,
    TimeProvider time,
    ILogger logger)
{
    internal static readonly TimeSpan Tick = TimeSpan.FromSeconds(1);
    internal static readonly TimeSpan Cadence = TimeSpan.FromSeconds(15);
    internal static readonly TimeSpan ReadBound = TimeSpan.FromSeconds(10);

    internal const string ReadBoundReason = "stats read exceeded 10 s";
    internal const string FaultedReason = "stats loop faulted";

    // Monotonic timestamp, so a wall-clock step never stalls the cadence. Null: read on the next leader tick.
    private long? nextReadAt;
    private bool failing;
    private bool gateLagWarned;

    public async Task RunAsync(CancellationToken stopping)
    {
        try
        {
            while (true)
            {
                await Task.Delay(Tick, time, stopping).ConfigureAwait(false);
                await TickAsync(stopping).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stopping.IsCancellationRequested)
        {
        }
        catch (Exception ex)
        {
            logger.StatsUnavailable(outbox, $"{FaultedReason}: {ex.GetType().Name}");
        }
        finally
        {
            cache.Clear();
        }
    }

    private async Task TickAsync(CancellationToken stopping)
    {
        if (!status.IsLeader)
        {
            cache.Clear();
            nextReadAt = null;
            return;
        }

        if (nextReadAt is { } due && time.GetElapsedTime(due) < Cadence)
        {
            return;
        }

        nextReadAt = time.GetTimestamp();

        Task<OutboxStats>? read = null;
        OutboxStats stats;
        try
        {
            read = store.GetStatsAsync(stopping);
            stats = await read.WaitAsync(ReadBound, time, stopping).ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (stopping.IsCancellationRequested)
        {
            Abandon(read);
            throw;
        }
        catch (Exception ex)
        {
            Abandon(read);
            cache.Clear();
            if (!failing)
            {
                failing = true;
                logger.StatsUnavailable(outbox, Reason(ex, read));
            }

            return;
        }

        // Leadership lost while reading: drop the result.
        if (!status.IsLeader)
        {
            cache.Clear();
            nextReadAt = null;
            return;
        }

        cache.Set(stats);
        if (failing)
        {
            failing = false;
            logger.StatsAvailable(outbox);
        }

        if (stats.GateLag.UnavailableReason is { } reason && !gateLagWarned)
        {
            gateLagWarned = true;
            logger.GateLagUnavailable(outbox, reason);
        }
    }

    private static string Reason(Exception ex, Task<OutboxStats>? read) => ex switch
    {
        OutboxStatsUnavailableException u => u.Reason,
        TimeoutException when read is { IsCompleted: false } => ReadBoundReason,
        _ => ex.GetType().Name,
    };

    // An abandoned read may fault later, so observe it.
    private static void Abandon(Task<OutboxStats>? read)
    {
        if (read is { IsCompleted: false })
        {
            _ = read.ContinueWith(static t => _ = t.Exception, CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted, TaskScheduler.Default);
        }
    }
}
