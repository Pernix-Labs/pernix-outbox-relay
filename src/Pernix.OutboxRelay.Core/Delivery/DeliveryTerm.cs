using System.Threading.Channels;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Core.Diagnostics;
using Pernix.OutboxRelay.Core.Leadership;
using Microsoft.Extensions.Logging;

namespace Pernix.OutboxRelay.Core.Delivery;

// The dead-letter destination's Name and how to create it, so the two cannot disagree.
internal sealed record DeadLetterTarget(string DestinationName, Func<IDestination> Create);

internal sealed class DeliveryTerm(
    string outbox,
    OutboxOptions options,
    string destinationName,
    Func<IDestination> createDestination,
    IDeliveryOrdering ordering,
    OutboxRuntimeStatus status,
    TimeProvider time,
    ILogger logger,
    DeadLetterTarget? deadLetter = null)
{
    internal const int CommitGroupMax = 100;
    internal const string LostSignalReason = "session lost signal";
    internal const string MalformedHeadersError = "headers is not a JSON object of string values";
    internal const string UnknownResultError = "destination returned no delivery result";

    internal static readonly TimeSpan CheckInterval = TimeSpan.FromSeconds(1);
    internal static readonly TimeSpan SessionCommandBound = TimeSpan.FromSeconds(5);
    internal static readonly TimeSpan LossDisposeBound = TimeSpan.FromSeconds(5);
    internal static readonly TimeSpan StopDrainDeadline = TimeSpan.FromSeconds(20);
    internal static readonly TimeSpan StopFinalDeadline = TimeSpan.FromSeconds(25);

    private readonly Dictionary<long, HeldRow> held = [];
    private readonly Queue<Outcome> pendingOutcomes = new();
    // Deadline ticks of bounded in-flight sends. Keys[0] reads the earliest without an enumerator.
    private readonly SortedList<long, int> inFlightByDeadline = [];
    private readonly Channel<SendResult> results = Channel.CreateUnbounded<SendResult>(new UnboundedChannelOptions { SingleReader = true });
    private readonly CancellationTokenSource termCts = new();
    private readonly PauseSchedule pauses = new(options.Retry);
    private readonly FailurePolicy policy = new(options.Retry, deadLetter is not null, options.Ordering?.OnPermanentFailure ?? OnPermanentFailure.Continue);
    private readonly List<OutboxMessage> sendable = [];
    private readonly List<OutboxMessage> queued = [];
    private readonly List<long> heldKeys = [];
    private readonly List<long> evicted = [];
    private readonly List<string> rewindReasons = [];
    private readonly Stack<PendingSend> pendingPool = new();

    private static readonly SendResult WakeSignal = new(0, 0, SendKind.Wake, DeliveryResult.Delivered.Instance);

    private IDestination? destination;
    private IReadOnlySet<string>? routes;
    private IDestination? deadLetterDestination;
    private Task? deadLetterValidation;
    private bool validated;
    private bool destinationDisposed;
    private Func<long, bool>? isHeld;
    private TimeSpan? mainBound;
    private TimeSpan? deadLetterBound;
    private int inFlight;
    private int epoch;
    private DateTimeOffset? pauseEnd;
    private DateTimeOffset nextCheck;
    private DateTimeOffset nextRead;
    private DateTimeOffset nextSummary;
    private long stopSignalTicks;
    private int deferredByStop;
    private bool lastReadFull;
    private ITimer? wakeTimer;
    private CancellationTokenRegistration lostWake;

    // Pool proof only: never logged, metered or exported.
    internal int PendingCreated { get; private set; }

    private enum RowState { Queued, Sending, DeadLettering, Settled, Parked }

    private enum SendKind { Main, DeadLetter, Wake }

    public async Task<DeliveryTermResult> RunAsync(IOutboxSession session, CancellationToken stopping)
    {
        ArgumentNullException.ThrowIfNull(session);
        using var stopRegistration = stopping.UnsafeRegister(static s => ((DeliveryTerm)s!).OnStopping(), this);
        try
        {
            destination = createDestination();
            routes = destination.Routes;
            deadLetterDestination = deadLetter?.Create();
            mainBound = destination.SendBound;
            deadLetterBound = deadLetterDestination?.SendBound;
            var start = time.GetUtcNow();
            nextCheck = start + CheckInterval;
            nextRead = start;
            nextSummary = start;
            wakeTimer = time.CreateTimer(static s => ((DeliveryTerm)s!).Wake(), this, Timeout.InfiniteTimeSpan, Timeout.InfiniteTimeSpan);

            // An already cancelled token runs the callback inline, so the first wait returns at once.
            lostWake = session.Lost.UnsafeRegister(static s => ((DeliveryTerm)s!).Wake(), this);
            return await LoopAsync(session, stopping).ConfigureAwait(false);
        }
        catch (OutboxSessionLostException ex)
        {
            return await LoseAsync(ex.Message).ConfigureAwait(false);
        }
        finally
        {
            // Complete before the wake sources go: a late timer callback only meets the completed channel.
            results.Writer.TryComplete();
            termCts.Cancel();
            wakeTimer?.Dispose();
            lostWake.Dispose();

            // A fault out of the loop skips the Lost and Finish paths, so both destinations are disposed here.
            if (!destinationDisposed)
            {
                await DisposeDestinationWithinAsync(LossDisposeBound).ConfigureAwait(false);
            }

            termCts.Dispose();
            status.TrackSendDeadline(null);
        }
    }

    private async Task<DeliveryTermResult> LoopAsync(IOutboxSession session, CancellationToken stopping)
    {
        var term = termCts.Token;
        while (true)
        {
            if (session.Lost.IsCancellationRequested)
            {
                return await LoseAsync(LostSignalReason).ConfigureAwait(false);
            }

            HandleReadyResults();
            var now = time.GetUtcNow();
            if (stopping.IsCancellationRequested)
            {
                Interlocked.CompareExchange(ref stopSignalTicks, now.UtcTicks, 0);
            }

            var stopAt = StopSignalAt;

            // Remaining commits run concurrently with dispose.
            if (stopAt is { } t0 && (inFlight == 0 || now >= t0 + StopDrainDeadline))
            {
                return await FinishAsync(session, t0).ConfigureAwait(false);
            }

            if (now >= nextCheck)
            {
                ExpectProgress(SessionCommandBound);
                await session.CheckAsync(term).ConfigureAwait(false);
                nextCheck = time.GetUtcNow() + CheckInterval;
                continue;
            }

            if (pendingOutcomes.Count > 0)
            {
                await CommitGroupAsync(session, term).ConfigureAwait(false);
                continue;
            }

            if (stopAt is null && pauseEnd is { } end && now >= end)
            {
                Unpause(now);
                continue;
            }

            if (stopAt is null && pauseEnd is null && ReadStepDue(now))
            {
                await ReadAsync(session, term).ConfigureAwait(false);
                continue;
            }

            await WaitAsync(now, stopAt).ConfigureAwait(false);
        }
    }

    private int Room => ordering.Capacity - held.Count;

    // Ordered steps also run without room, for the summary and probe, and re-read once room frees after a full read.
    private bool ReadStepDue(DateTimeOffset now)
    {
        if (!ordering.UsesSummary)
        {
            return now >= nextRead && Room > 0;
        }

        return now >= nextRead || (lastReadFull && !ordering.AwaitingMoveBack && ordering.NextRead(Room, []) is not null);
    }

    private DateTimeOffset? StopSignalAt
    {
        get
        {
            var ticks = Interlocked.Read(ref stopSignalTicks);
            return ticks == 0 ? null : new DateTimeOffset(ticks, TimeSpan.Zero);
        }
    }

    private void HandleReadyResults()
    {
        while (results.Reader.TryRead(out var result))
        {
            Handle(result);
        }
    }

    private void Handle(SendResult result)
    {
        if (result.Kind == SendKind.Wake)
        {
            return;
        }

        if (result.Pending is { } pending)
        {
            pendingPool.Push(pending);
        }

        var expected = result.Kind == SendKind.Main ? RowState.Sending : RowState.DeadLettering;
        if (!held.TryGetValue(result.Id, out var row) || row.State != expected)
        {
            return;
        }

        RemoveDeadline(row);
        if (result.Kind == SendKind.DeadLetter)
        {
            inFlight--;
            HandleDeadLetter(row, result.Result);
            return;
        }

        inFlight--;

        switch (result.Result)
        {
            case DeliveryResult.Delivered:
                Settle(row, new Outcome.Remove(row.Id));
                Dispatch();

                // Only a send started after the last pause ended resets the backoff.
                if (pauseEnd is null && result.Epoch == epoch)
                {
                    pauses.Reset();
                }

                break;

            case DeliveryResult.MessageFault fault:
                var message = row.Message!;
                logger.MessageFaulted(outbox, destinationName, row.Id, message.RetryCount, fault.Permanent, fault.Error);
                ApplyDecision(row, policy.OnMessageFault(message, fault, time.GetUtcNow()));
                Dispatch();
                break;

            case DeliveryResult.DestinationFault fault:
                PauseOnDestinationFault(row, result.Epoch, fault.Error);
                break;
        }
    }

    // A fault from a send started before the last pause ended frees the row without a new pause.
    private void PauseOnDestinationFault(HeldRow row, int sendEpoch, string error)
    {
        if (sendEpoch != epoch)
        {
            Unpark([row.Id]);
            Dispatch();
            return;
        }

        row.State = RowState.Parked;
        StartPause(nameof(DeliveryResult.DestinationFault), error);
    }

    // Dead-letter faults never pause. The row keeps its original failure reason.
    private void HandleDeadLetter(HeldRow row, DeliveryResult result)
    {
        switch (result)
        {
            case DeliveryResult.Delivered:
                logger.MessageDeadLettered(outbox, row.Id, deadLetter!.DestinationName, row.DeadLetter!.Attempts);
                if (policy.AfterDeadLetter(row.Message!, row.DeadLetter.Reason) is FailureDecision.Block block)
                {
                    SettleBlocked(row, block);
                }
                else
                {
                    Settle(row, new Outcome.Remove(row.Id));
                }

                break;

            case DeliveryResult.MessageFault fault:
                RetryDeadLetter(row, fault.Error);
                break;

            case DeliveryResult.DestinationFault fault:
                RetryDeadLetter(row, fault.Error);
                break;
        }

        Dispatch();
    }

    private void DeferByStop(HeldRow row)
    {
        row.State = RowState.Parked;
        row.Message = null;
        deferredByStop++;
    }

    private void RetryDeadLetter(HeldRow row, string error)
    {
        var message = row.Message!;
        logger.DeadLetterFailed(outbox, row.Id, deadLetter!.DestinationName, message.RetryCount, error);
        var retry = policy.OnDeadLetterFailed(message, row.DeadLetter!.Reason).Outcome;
        logger.DeadLetterRetryScheduled(outbox, row.Id, deadLetter.DestinationName, retry.Delay);
        Settle(row, retry);
    }

    private void ApplyDecision(HeldRow row, FailureDecision decision)
    {
        switch (decision)
        {
            case FailureDecision.Retry retry:
                logger.MessageRetryScheduled(outbox, row.Id, retry.Outcome.RetryCount, retry.Outcome.Delay);
                Settle(row, retry.Outcome);
                break;

            case FailureDecision.Drop drop:
                logger.MessageDropped(outbox, row.Id, destinationName, row.Message!.RetryCount, drop.Reason);
                Settle(row, drop.Outcome);
                break;

            case FailureDecision.Block block:
                SettleBlocked(row, block);
                break;

            // While stopping no send starts, so the row stays untouched for the next leader.
            case FailureDecision.DeadLetter when StopSignalAt is not null:
                DeferByStop(row);
                break;

            case FailureDecision.DeadLetter { Info: var info }:
                SendDeadLetter(row, info);
                break;
        }
    }

    private void SettleBlocked(HeldRow row, FailureDecision.Block block)
    {
        logger.MessageBlocked(outbox, row.Id, destinationName, block.Outcome.RetryCount, block.Reason);
        Settle(row, block.Outcome);
    }

    // Remove frees the scope before its commit. Fail and Block hold it until the commit and evict its queue.
    private void Settle(HeldRow row, Outcome outcome)
    {
        row.State = RowState.Settled;
        row.Message = null;
        pendingOutcomes.Enqueue(outcome);
        if (outcome is Outcome.Remove)
        {
            ordering.Release(row.Id);
            return;
        }

        evicted.Clear();
        ordering.Fail(row.Id, evicted);
        foreach (var id in evicted)
        {
            Leave(id);
        }

        LogRewinds();
    }

    // Unpark untracks the parked rows, so they leave held without Left.
    private void Unpark(IReadOnlyCollection<long> ids)
    {
        evicted.Clear();
        ordering.Unpark(ids, evicted);
        foreach (var id in ids)
        {
            held.Remove(id);
        }

        foreach (var id in evicted)
        {
            Leave(id);
        }

        LogRewinds();
    }

    private void Leave(long id)
    {
        if (held.Remove(id))
        {
            ordering.Left(id);
        }
    }

    // Queued rows stay untouched while paused or stopping.
    private void Dispatch()
    {
        if (pauseEnd is not null || StopSignalAt is not null)
        {
            return;
        }

        var term = termCts.Token;
        while (true)
        {
            sendable.Clear();
            ordering.Sendable(sendable);
            if (sendable.Count == 0)
            {
                return;
            }

            var sentAt = time.GetUtcNow();
            foreach (var message in sendable)
            {
                if (!held.TryGetValue(message.Id, out var row) || row.State != RowState.Queued)
                {
                    continue;
                }

                row.State = RowState.Sending;

                if (policy.IsDeadLetterPending(message))
                {
                    ApplyDecision(row, policy.OnDeadLetterPendingRead(message, sentAt));
                    continue;
                }

                if (!RouteAcceptance.Accepts(routes, message.Route))
                {
                    Hold(row, message);
                    continue;
                }

                inFlight++;
                AddDeadline(row, sentAt, mainBound);
                Send(message, term);
            }
        }
    }

    private void Hold(HeldRow row, OutboxMessage message)
    {
        var heldRoute = FailurePolicy.HoldRoute(message.Route);
        logger.MessageHeld(outbox, destinationName, row.Id, heldRoute);
        Settle(row, policy.Hold(message, heldRoute));
    }

    private void LogRewinds()
    {
        rewindReasons.Clear();
        ordering.DrainRewinds(rewindReasons);
        foreach (var reason in rewindReasons)
        {
            logger.DeliveryRewound(outbox, reason);
        }
    }

    private void StartPause(string kind, string error)
    {
        if (pauseEnd is not null || StopSignalAt is not null)
        {
            return;
        }

        var pause = pauses.Next();
        pauseEnd = time.GetUtcNow() + pause;
        logger.DeliveryPaused(outbox, destinationName, kind, error, pause);
    }

    private void Unpause(DateTimeOffset now)
    {
        Unpark([.. held.Where(r => r.Value.State == RowState.Parked).Select(r => r.Key)]);
        epoch++;
        pauseEnd = null;
        nextRead = now;
        logger.DeliveryResumed(outbox);
        Dispatch();
    }

    private List<Outcome> TakeCommitGroup()
    {
        var group = new List<Outcome>(Math.Min(pendingOutcomes.Count, CommitGroupMax));
        while (group.Count < CommitGroupMax && pendingOutcomes.TryDequeue(out var outcome))
        {
            group.Add(outcome);
        }

        return group;
    }

    private async Task CommitGroupAsync(IOutboxSession session, CancellationToken term)
    {
        var group = TakeCommitGroup();
        ExpectProgress(SessionCommandBound);
        await session.CommitAsync(group, term).ConfigureAwait(false);
        foreach (var outcome in group)
        {
            Leave(outcome.Id);
        }
    }

    private async Task ReadAsync(IOutboxSession session, CancellationToken term)
    {
        if (!validated)
        {
            ExpectProgress(SessionCommandBound);
            try
            {
                await destination!.ValidateAsync(term).ConfigureAwait(false);
                validated = true;
            }
            catch (Exception ex) when (ex is not OperationCanceledException)
            {
                StartPause(nameof(DeliveryResult.DestinationFault), ex.GetType().Name);
                return;
            }
        }

        if (ordering.UsesSummary)
        {
            var now = time.GetUtcNow();
            ordering.Tick(now);
            if (now >= nextSummary)
            {
                ExpectProgress(SessionCommandBound);
                var summary = await session.ReadFailedSummaryAsync(term).ConfigureAwait(false);
                ordering.Summary(summary);
                nextSummary = now + options.Polling.Interval;
            }

            LogRewinds();
        }

        // Only None excludes held rows in SQL. ScopedDelivery.NextRead ignores held.
        IReadOnlyCollection<long> exclude = Array.Empty<long>();
        if (ordering.ExcludesHeld)
        {
            heldKeys.Clear();
            heldKeys.AddRange(held.Keys);
            exclude = heldKeys;
        }

        var request = ordering.NextRead(Room, exclude);
        if (request is null)
        {
            nextRead = time.GetUtcNow() + options.Polling.Interval;
            return;
        }

        ExpectProgress(SessionCommandBound);
        var rows = await session.ReadAsync(request, term).ConfigureAwait(false);

        var readAt = time.GetUtcNow();
        ordering.Accept(rows, isHeld ??= held.ContainsKey, queued);
        foreach (var row in queued)
        {
            held.Add(row.Id, new HeldRow(row));
        }

        queued.Clear();

        Dispatch();
        if (ordering.UsesSummary)
        {
            lastReadFull = rows.Count >= request.Max && !ordering.Halted;
            nextRead = readAt + options.Polling.Interval;
            return;
        }

        nextRead = rows.Count >= request.Max && Room > 0 ? readAt : readAt + options.Polling.Interval;
    }

    private void Send(OutboxMessage row, CancellationToken term)
    {
        if (row.HeadersMalformed)
        {
            CompleteMain(row.Id, epoch, new DeliveryResult.MessageFault(MalformedHeadersError, Permanent: true), null, term, null);
            return;
        }

        var message = new OutgoingMessage
        {
            Source = new MessageSource(outbox, row.Id, destinationName),
            Key = row.Key,
            Payload = row.Payload,
            Headers = row.Headers,
            Route = row.Route,
        };
        SendMain(message, row.Id, term);
    }

    private void SendMain(OutgoingMessage message, long id, CancellationToken term)
    {
        ValueTask<DeliveryResult> send;
        try
        {
            send = destination!.SendAsync(message, term);
        }
        catch (Exception ex)
        {
            CompleteMain(id, epoch, null, ex, term, null);
            return;
        }

        if (send.IsCompleted)
        {
            var result = Consume(send, out var error);
            CompleteMain(id, epoch, result, error, term, null);
            return;
        }

        var pending = pendingPool.TryPop(out var free) ? free : NewPending(term);
        pending.Send = send;
        pending.Id = id;
        pending.Epoch = epoch;

        // Last touch on the loop flow: the continuation may run before this returns.
        send.ConfigureAwait(false).GetAwaiter().UnsafeOnCompleted(pending.Continuation);
    }

    // Single-await rule: consumes a completed send exactly once, never through AsTask.
    private static DeliveryResult? Consume(ValueTask<DeliveryResult> send, out Exception? error)
    {
        try
        {
            error = null;
            return send.GetAwaiter().GetResult();
        }
        catch (Exception ex)
        {
            error = ex;
            return null;
        }
    }

    private PendingSend NewPending(CancellationToken term)
    {
        PendingCreated++;
        return new PendingSend(this, term);
    }

    // Continuation path: metrics, a fault log on a non-cancelled fault and TryWrite. Never blocks.
    private void CompleteMain(long id, int sendEpoch, DeliveryResult? result, Exception? error, CancellationToken term, PendingSend? pending) =>
        CompleteSend(SendKind.Main, destinationName, id, sendEpoch, result, error, term, pending);

    private void CompleteSend(
        SendKind kind,
        string targetName,
        long id,
        int sendEpoch,
        DeliveryResult? result,
        Exception? error,
        CancellationToken term,
        PendingSend? pending)
    {
        if (error is not null)
        {
            if (term.IsCancellationRequested)
            {
                return;
            }

            logger.DeliverySendFailed(outbox, error.GetType().Name);
            result = new DeliveryResult.DestinationFault(error.GetType().Name);
        }

        // A null result is a transport bug. As a destination fault it is counted and handled like one, and its row does not hold a slot for the rest of the term.
        if (result is null)
        {
            logger.DeliverySendFailed(outbox, UnknownResultError);
            result = new DeliveryResult.DestinationFault(UnknownResultError);
        }

        // Counted when the destination confirms, even if the loop never handles the result.
        switch (result)
        {
            case DeliveryResult.Delivered when kind == SendKind.Main:
                DeliveryMetrics.Record(outbox);
                break;
            case DeliveryResult.Delivered:
                DeliveryMetrics.RecordDeadLettered(outbox, targetName);
                break;
            case DeliveryResult.MessageFault:
                DeliveryMetrics.RecordMessageFault(outbox, targetName);
                break;
            case DeliveryResult.DestinationFault:
                DeliveryMetrics.RecordDestinationFault(outbox, targetName);
                break;
        }

        results.Writer.TryWrite(new SendResult(id, sendEpoch, kind, result, pending));
    }

    private void SendDeadLetter(HeldRow row, DeadLetterInfo info)
    {
        var message = row.Message!;
        var sentAt = time.GetUtcNow();
        row.State = RowState.DeadLettering;
        row.DeadLetter = info;
        inFlight++;
        AddDeadline(row, sentAt, deadLetterBound);

        if (deadLetterValidation is { IsCompleted: true, IsCompletedSuccessfully: false })
        {
            deadLetterValidation = null;
        }

        var term = termCts.Token;
        deadLetterValidation ??= ValidateDeadLetterAsync(deadLetterDestination!, term);
        var outgoing = new OutgoingMessage
        {
            Source = new MessageSource(outbox, row.Id, destinationName),
            Key = message.Key,
            Payload = message.Payload,
            Headers = message.Headers,
            Route = message.Route,
            DeadLetter = info,
        };
        _ = SendDetachedAsync(deadLetterDestination!, deadLetter!.DestinationName, outgoing, epoch, deadLetterValidation, term);
    }

    private static async Task ValidateDeadLetterAsync(IDestination target, CancellationToken term) =>
        await target.ValidateAsync(term).ConfigureAwait(false);

    private async Task SendDetachedAsync(
        IDestination target,
        string targetName,
        OutgoingMessage message,
        int sendEpoch,
        Task? validation,
        CancellationToken term)
    {
        try
        {
            if (validation is not null)
            {
                await validation.ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            if (term.IsCancellationRequested)
            {
                return;
            }

            // A result, not an error: a failed validation is counted without the send-failed log, as before.
            CompleteSend(SendKind.DeadLetter, targetName, message.Source.Id, sendEpoch, new DeliveryResult.DestinationFault(ex.GetType().Name), null, term, null);
            return;
        }

        DeliveryResult? result = null;
        Exception? error = null;
        try
        {
            result = await target.SendAsync(message, term).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            error = ex;
        }

        CompleteSend(SendKind.DeadLetter, targetName, message.Source.Id, sendEpoch, result, error, term, null);
    }

    private void OnStopping()
    {
        Interlocked.CompareExchange(ref stopSignalTicks, time.GetUtcNow().UtcTicks, 0);
        Wake();
    }

    private void Wake() => results.Writer.TryWrite(WakeSignal);

    // No disarm after a wake: the next wait re-arms, a late fire is one stale sentinel.
    private ValueTask<bool> WaitAsync(DateTimeOffset now, DateTimeOffset? stopAt)
    {
        var until = nextCheck;
        if (stopAt is { } t0)
        {
            until = Min(until, t0 + StopDrainDeadline);
        }
        else if (pauseEnd is { } end)
        {
            until = Min(until, end);
        }
        else if (Room > 0 || ordering.UsesSummary)
        {
            until = Min(until, nextRead);
        }

        var delay = until - now;
        if (delay <= TimeSpan.Zero)
        {
            return new ValueTask<bool>(true);
        }

        ExpectProgress(delay);
        wakeTimer!.Change(delay, Timeout.InfiniteTimeSpan);
        return results.Reader.WaitToReadAsync(CancellationToken.None);
    }

    private async Task<DeliveryTermResult> FinishAsync(IOutboxSession session, DateTimeOffset t0)
    {
        if (inFlight > 0)
        {
            logger.DeliveryStopAbandoned(outbox, inFlight);
        }

        if (deferredByStop > 0)
        {
            logger.DeadLetterDeferredByStop(outbox, deferredByStop);
        }

        var groups = new List<IReadOnlyList<Outcome>>();
        while (pendingOutcomes.Count > 0)
        {
            groups.Add(TakeCommitGroup());
        }

        var uncommitted = groups.Sum(g => g.Count);
        var deadline = t0 + StopFinalDeadline;
        var remaining = deadline - time.GetUtcNow();
        var lost = session.Lost;
        ExpectProgress(remaining);

        // Commits outlive this method when the deadline expires, so no group starts after it or after loss.
        async Task CommitFinalAsync()
        {
            foreach (var group in groups)
            {
                if (lost.IsCancellationRequested || time.GetUtcNow() >= deadline)
                {
                    return;
                }

                await session.CommitAsync(group, CancellationToken.None).ConfigureAwait(false);
                Interlocked.Add(ref uncommitted, -group.Count);
            }
        }

        // Abandoned sends fail once the destinations are disposed; cancelling first keeps them out of the log.
        await termCts.CancelAsync().ConfigureAwait(false);
        var commits = CommitFinalAsync();
        var dispose = DisposeDestinationAsync();
        try
        {
            await Task.WhenAll(commits, dispose).WaitAsync(Positive(remaining), time).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.DeliveryWaitAbandoned(outbox, "stop commit and dispose", ex.GetType().Name);
        }

        Observe(commits);
        Observe(dispose);
        var left = Volatile.Read(ref uncommitted);
        if (left > 0)
        {
            logger.DeliveryStopCommitIncomplete(outbox, left);
        }

        return new DeliveryTermResult.Stopped(t0);
    }

    private async Task<DeliveryTermResult> LoseAsync(string reason)
    {
        await termCts.CancelAsync().ConfigureAwait(false);
        results.Writer.TryComplete();
        held.Clear();
        pendingOutcomes.Clear();
        inFlightByDeadline.Clear();
        inFlight = 0;

        if (destination is not null)
        {
            await DisposeDestinationWithinAsync(LossDisposeBound).ConfigureAwait(false);
        }

        return new DeliveryTermResult.Lost(reason);
    }

    private async Task DisposeDestinationWithinAsync(TimeSpan bound)
    {
        ExpectProgress(bound);
        var dispose = DisposeDestinationAsync();
        try
        {
            await dispose.WaitAsync(bound, time).ConfigureAwait(false);
        }
        catch (Exception ex)
        {
            logger.DeliveryWaitAbandoned(outbox, "destination dispose", ex.GetType().Name);
        }

        Observe(dispose);
    }

    // Off the loop so a blocking dispose cannot hold it.
    private Task DisposeDestinationAsync()
    {
        var main = destination;
        var secondary = deadLetterDestination;
        if (main is null && secondary is null)
        {
            return Task.CompletedTask;
        }

        destinationDisposed = true;
        return Task.WhenAll(DisposeOffLoop(main), DisposeOffLoop(secondary));
    }

    private static Task DisposeOffLoop(IDestination? target) =>
        target is null ? Task.CompletedTask : Task.Run(async () => await target.DisposeAsync().ConfigureAwait(false));

    private void ExpectProgress(TimeSpan bound)
    {
        status.ExpectProgress(bound, time);
        // An unbounded send waiting on its target is not a hung loop.
        status.TrackSendDeadline(inFlightByDeadline.Count == 0 ? null : new DateTimeOffset(inFlightByDeadline.Keys[0], TimeSpan.Zero));
    }

    private void AddDeadline(HeldRow row, DateTimeOffset sentAt, TimeSpan? bound)
    {
        if (bound is not { } b)
        {
            row.DeadlineKey = 0;
            return;
        }

        var max = DateTimeOffset.MaxValue.UtcTicks;
        var ticks = sentAt.UtcTicks;
        var key = b.Ticks >= max - ticks ? max : ticks + Math.Max(b.Ticks, 0);
        inFlightByDeadline.TryGetValue(key, out var count);
        inFlightByDeadline[key] = count + 1;
        row.DeadlineKey = key;
    }

    private void RemoveDeadline(HeldRow row)
    {
        var key = row.DeadlineKey;
        row.DeadlineKey = 0;
        if (key == 0 || !inFlightByDeadline.TryGetValue(key, out var count))
        {
            return;
        }

        if (count <= 1)
        {
            inFlightByDeadline.Remove(key);
        }
        else
        {
            inFlightByDeadline[key] = count - 1;
        }
    }

    private static void Observe(Task task) =>
        _ = task.ContinueWith(t => _ = t.Exception, CancellationToken.None, TaskContinuationOptions.OnlyOnFaulted, TaskScheduler.Default);

    private static TimeSpan Positive(TimeSpan value) => value > TimeSpan.Zero ? value : TimeSpan.Zero;

    private static DateTimeOffset Min(DateTimeOffset a, DateTimeOffset b) => a < b ? a : b;

    private sealed class HeldRow(OutboxMessage message)
    {
        public long Id { get; } = message.Id;

        public RowState State { get; set; } = RowState.Queued;

        public OutboxMessage? Message { get; set; } = message;

        public DeadLetterInfo? DeadLetter { get; set; }

        public long DeadlineKey { get; set; }
    }

    private readonly record struct SendResult(long Id, int Epoch, SendKind Kind, DeliveryResult Result, PendingSend? Pending = null);

    private sealed class PendingSend
    {
        private readonly DeliveryTerm owner;
        // termCts.Token throws once termCts is disposed.
        private readonly CancellationToken term;

        public PendingSend(DeliveryTerm owner, CancellationToken term)
        {
            this.owner = owner;
            this.term = term;
            Continuation = OnCompleted;
        }

        public Action Continuation { get; }

        public ValueTask<DeliveryResult> Send { get; set; }

        public long Id { get; set; }

        public int Epoch { get; set; }

        private void OnCompleted()
        {
            var send = Send;
            Send = default;
            var result = Consume(send, out var error);
            owner.CompleteMain(Id, Epoch, result, error, term, this);
        }
    }
}
