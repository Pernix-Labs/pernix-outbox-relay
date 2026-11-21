using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Core.Delivery;
using Microsoft.Extensions.Logging;

namespace Pernix.OutboxRelay.Core.Leadership;

public sealed class OutboxWorker(
    ValidatedOutbox entry,
    OutboxRuntimeStatus status,
    OutboxRuntimeState state,
    ILeadershipJitter jitter,
    TimeProvider timeProvider,
    ILogger logger)
{
    internal static readonly TimeSpan StartDelayBound = TimeSpan.FromSeconds(2);
    internal static readonly TimeSpan SessionCommandBound = TimeSpan.FromSeconds(5);

    // A session dispose unlocks under the command deadline, then closes.
    internal static readonly TimeSpan SessionDisposeBound = SessionCommandBound + SessionCommandBound;
    internal static readonly TimeSpan StopCloseDeadline = TimeSpan.FromSeconds(27);
    private static readonly TimeSpan RetryBase = TimeSpan.FromMilliseconds(500);
    private static readonly TimeSpan RetryPerHeldLock = TimeSpan.FromMilliseconds(500);
    private static readonly TimeSpan RetryMax = TimeSpan.FromSeconds(5);

    private const string ReleasePendingReason = "lock may stay held until the server ends the backend (abandoned command, failed or timed-out unlock, broken connection or failed close)";
    private const string CloseAbandonedReason = "session close abandoned at stop deadline";

    private readonly string outboxName = entry.Options.Name!;
    private long stopSignalTicks;

    // Test-only seam, null in production: runs first in the stop-cancelled dispose wait's catch filter, to drive the stop/completion race.
    internal Action? BeforeDisposeOutcome { get; set; }

    public async Task RunAsync(CancellationToken stoppingToken)
    {
        IOutboxSession? session = null;
        DateTimeOffset? stopSignalAt = null;
        using var stopRegistration = stoppingToken.Register(StampStopSignal);
        try
        {
            ExpectProgress(StartDelayBound);
            var startDelay = await jitter.NextStartDelayAsync(outboxName, stoppingToken).ConfigureAwait(false);
            await DelayAsync(startDelay, stoppingToken).ConfigureAwait(false);

            while (true)
            {
                if (session is null)
                {
                    try
                    {
                        ExpectProgress(SessionCommandBound);
                        session = await entry.Store.OpenSessionAsync(stoppingToken).ConfigureAwait(false);
                        SetReachable(true, reason: null);
                    }
                    catch (OutboxDatabaseUnreachableException ex)
                    {
                        SetReachable(false, ex.Message);
                        await DelayAsync(RetryInterval(), stoppingToken).ConfigureAwait(false);
                        continue;
                    }
                }

                // A non-leader keeps its session between attempts and reconnects only after loss.
                string lossReason;
                try
                {
                    ExpectProgress(SessionCommandBound);
                    if (!await session.TryLockAsync(stoppingToken).ConfigureAwait(false))
                    {
                        await DelayAsync(RetryInterval(), stoppingToken).ConfigureAwait(false);
                        continue;
                    }

                    var term = NewTerm();
                    BecomeLeader();
                    var result = await term.RunAsync(session, stoppingToken).ConfigureAwait(false);
                    if (result is DeliveryTermResult.Stopped stopped)
                    {
                        stopSignalAt = stopped.StopSignalAt;
                        return;
                    }

                    lossReason = ((DeliveryTermResult.Lost)result).Reason;
                }
                catch (OutboxSessionLostException ex)
                {
                    lossReason = ex.Message;
                }

                await LoseAsync(session, lossReason, stoppingToken).ConfigureAwait(false);
                session = null;
                await DelayAsync(RetryInterval(), stoppingToken).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
        {
        }
        finally
        {
            await CloseAsync(session, stopSignalAt, stoppingToken).ConfigureAwait(false);
        }
    }

    // The only place a term is chosen by mode. A mode without a term throws rather than running as another.
    private DeliveryTerm NewTerm()
    {
        var mode = entry.Options.Ordering?.Mode;
        var context = new DestinationContext(mode.GetValueOrDefault());
        var main = new DestinationBinding(entry.Destination, entry.DestinationFactory);
        var deadLetter = entry.DeadLetterDestination;
        var batchSize = entry.Options.Polling.BatchSize;
        IDeliveryOrdering ordering = mode switch
        {
            OrderingMode.None => new UnorderedDelivery(batchSize),
            OrderingMode.PerKey or OrderingMode.Strict => new ScopedDelivery(mode.Value, batchSize),
            _ => throw new InvalidOperationException($"Outboxes[{outboxName}]: Ordering.Mode '{mode}' has no delivery term in this build."),
        };

        return new DeliveryTerm(
            outboxName,
            entry.Options,
            main.Options.Name!,
            () => main.Create(context),
            ordering,
            status,
            timeProvider,
            logger,
            deadLetter is null ? null : new DeadLetterTarget(deadLetter.Options.Name!, () => deadLetter.Create(context)));
    }

    // Dispose first, then stop leading. On stop the close is capped at T0 + 27 s and abandoned on expiry.
    private async Task CloseAsync(IOutboxSession? session, DateTimeOffset? stopSignalAt, CancellationToken stoppingToken)
    {
        string? disposeFailure = null;
        if (session is not null)
        {
            if (stopSignalAt is null && stoppingToken.IsCancellationRequested)
            {
                // A cancelled wait can resume before the stop registration has stamped T0.
                StampStopSignal();
            }

            var stopAt = stopSignalAt ?? StopSignalAt;
            if (stopAt is null)
            {
                ExpectProgress(SessionDisposeBound);
            }

            Task? dispose = null;
            try
            {
                dispose = session.DisposeAsync().AsTask();
                await (stopAt is { } t0 ? WaitStopCloseAsync(dispose, t0) : AwaitSessionDisposeAsync(dispose, stoppingToken)).ConfigureAwait(false);
            }
            catch (TimeoutException) when (dispose is { IsCompleted: false })
            {
                disposeFailure = CloseAbandonedReason;
            }
            catch (TimeoutException) when (dispose is { IsCompleted: true })
            {
                // The cap can win the race against a dispose that completes at the same moment. ReleasePending then decides.
            }
            catch (Exception ex)
            {
                disposeFailure = $"session dispose failed ({ex.GetType().Name})";
            }

            if (dispose is not null)
            {
                Observe(dispose);
            }
        }

        if (status.IsLeader)
        {
            StopLeading();
            if (disposeFailure is not null)
            {
                logger.LeadershipReleasePending(outboxName, disposeFailure);
            }
            else if (session?.ReleasePending == true)
            {
                logger.LeadershipReleasePending(outboxName, ReleasePendingReason);
            }
            else
            {
                logger.LeadershipReleased(outboxName);
            }
        }
    }

    private DateTimeOffset? StopSignalAt
    {
        get
        {
            var ticks = Interlocked.Read(ref stopSignalTicks);
            return ticks == 0 ? null : new DateTimeOffset(ticks, TimeSpan.Zero);
        }
    }

    private void StampStopSignal() => Interlocked.CompareExchange(ref stopSignalTicks, timeProvider.GetUtcNow().UtcTicks, 0);

    private void BecomeLeader()
    {
        status.IsLeader = true;
        logger.LeadershipAcquired(outboxName);
    }

    private void StopLeading() => status.IsLeader = false;

    private Task WaitStopCloseAsync(Task dispose, DateTimeOffset t0)
    {
        var remaining = Positive(t0 + StopCloseDeadline - timeProvider.GetUtcNow());
        ExpectProgress(remaining);
        return dispose.WaitAsync(remaining, timeProvider);
    }

    private async Task LoseAsync(IOutboxSession session, string reason, CancellationToken stoppingToken)
    {
        if (status.IsLeader)
        {
            StopLeading();
            logger.LeadershipLost(outboxName, reason);
        }

        ExpectProgress(SessionDisposeBound);
        var dispose = session.DisposeAsync().AsTask();
        try
        {
            await AwaitSessionDisposeAsync(dispose, stoppingToken).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            // The timeout can win the race against a dispose that completes at the same moment.
            Observe(dispose);
        }
    }

    // LoseAsync and CloseAsync: wait for a session dispose; a stop that arrives mid-dispose caps the wait at T0 + 27 s.
    private async Task AwaitSessionDisposeAsync(Task dispose, CancellationToken stoppingToken)
    {
        try
        {
            await dispose.WaitAsync(stoppingToken).ConfigureAwait(false);
            return;
        }
        catch (OperationCanceledException) when (RunBeforeDisposeOutcome() && stoppingToken.IsCancellationRequested)
        {
        }

        if (dispose.IsCompleted)
        {
            // Stop and completion raced: the dispose's own outcome wins (success, or its own exception).
            await dispose.ConfigureAwait(false);
            return;
        }

        // The wait can resume before the stop registration has stamped T0.
        StampStopSignal();
        await WaitStopCloseAsync(dispose, StopSignalAt!.Value).ConfigureAwait(false);
    }

    private bool RunBeforeDisposeOutcome()
    {
        BeforeDisposeOutcome?.Invoke();
        return true;
    }

    private void SetReachable(bool reachable, string? reason)
    {
        if (status.DatabaseReachable == reachable)
        {
            return;
        }

        status.DatabaseReachable = reachable;
        if (reachable)
        {
            logger.OutboxDatabaseReachable(outboxName);
        }
        else
        {
            logger.OutboxDatabaseUnreachable(outboxName, reason ?? string.Empty);
        }
    }

    private TimeSpan RetryInterval()
    {
        var interval = RetryBase + (RetryPerHeldLock * state.HeldCount);
        return interval < RetryMax ? interval : RetryMax;
    }

    private Task DelayAsync(TimeSpan delay, CancellationToken stoppingToken)
    {
        ExpectProgress(delay);
        return Task.Delay(delay, timeProvider, stoppingToken);
    }

    private void ExpectProgress(TimeSpan bound) => status.ExpectProgress(bound, timeProvider);

    private static void Observe(Task task) =>
        _ = task.ContinueWith(t => _ = t.Exception, CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted, TaskScheduler.Default);

    private static TimeSpan Positive(TimeSpan value) => value > TimeSpan.Zero ? value : TimeSpan.Zero;
}
