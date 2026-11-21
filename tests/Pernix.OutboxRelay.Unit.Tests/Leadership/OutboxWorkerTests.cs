using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Core.Leadership;
using Pernix.OutboxRelay.Unit.Tests.Configuration;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Leadership;

public sealed class OutboxWorkerTests : IDisposable
{
    private const string Outbox = "Orders";
    private const string SentinelConnectionString = "Host=sentinel-db;Password=sentinel-secret-7f3a";
    private static readonly TimeSpan Step = TimeSpan.FromMilliseconds(100);
    private static readonly TimeSpan RetryWait = TimeSpan.FromSeconds(0.5);

    private readonly WorkerClock time = new();
    private readonly FakeOutboxStore store;
    private readonly OutboxRuntimeState state = new();
    private readonly OutboxRuntimeStatus status = new(Outbox);
    private readonly RecordingLogger<OutboxWorker> logger = new();
    private readonly CancellationTokenSource stopping = new();
    private readonly FakeDestinationFactory factory = new();
    private TimeSpan jitter = TimeSpan.Zero;
    private DestinationBinding? deadLetter;
    private Task? loop;

    public OutboxWorkerTests()
    {
        store = new FakeOutboxStore(time);
        state.SetOutboxes([status]);
    }

    public void Dispose() => stopping.Dispose();

    private static ValidatedOutbox Validated(string name, IValidatedOutboxStore store, FakeDestinationFactory? factory = null, OrderingMode mode = OrderingMode.None, DestinationBinding? deadLetter = null) =>
        new(new OutboxOptions { Name = name, Ordering = new OrderingOptions { Mode = mode } }, store, new DestinationOptions { Name = "Events" }, factory ?? new FakeDestinationFactory())
        {
            DeadLetterDestination = deadLetter,
        };

    [Fact]
    public async Task First_lock_attempt_only_after_jitter_delay()
    {
        jitter = TimeSpan.FromSeconds(1.5);
        await StartAsync();

        await AdvanceAsync(TimeSpan.FromSeconds(1.4));
        Assert.Empty(store.OpenAttempts);

        await AdvanceAsync(Step);
        Assert.Equal([TimeSpan.FromSeconds(1.5)], store.CurrentSession.LockAttempts);
    }

    [Theory]
    [InlineData(0, 0.5)]
    [InlineData(3, 2.0)]
    [InlineData(20, 5.0)]
    public async Task Refused_lock_retries_after_interval_for_held_count(int heldCount, double expectedSeconds)
    {
        state.SetOutboxes([status, .. Enumerable.Range(0, heldCount).Select(i => new OutboxRuntimeStatus($"held-{i}") { IsLeader = true })]);

        store.LockGranted = false;
        await StartAsync();

        await AdvanceAsync(TimeSpan.FromSeconds(expectedSeconds) - Step);
        Assert.Single(store.CurrentSession.LockAttempts);

        await AdvanceAsync(Step);
        Assert.Equal([TimeSpan.Zero, TimeSpan.FromSeconds(expectedSeconds)], store.CurrentSession.LockAttempts);
    }

    [Fact]
    public async Task Leader_checks_once_per_second_and_keeps_session_for_60_seconds()
    {
        await StartAsync();

        await AdvanceAsync(TimeSpan.FromSeconds(60));

        var session = Assert.Single(store.Sessions);
        Assert.Equal(Enumerable.Range(1, 60).Select(s => TimeSpan.FromSeconds(s)), session.Checks);
        Assert.Single(session.LockAttempts);
        Assert.False(session.Disposed);
        Assert.True(status.IsLeader);
        Assert.Equal(1, state.HeldCount);
        Assert.Single(logger.WithEvent(101));
    }

    [Fact]
    public async Task Leader_runs_delivery_term_with_one_destination()
    {
        await StartAsync();

        await AdvanceAsync(TimeSpan.FromSeconds(3));

        var (destination, context) = Assert.Single(factory.Created);
        Assert.Equal("Events", destination);
        Assert.Equal(new DestinationContext(OrderingMode.None), context);
        Assert.Single(factory.ProbeStarted);
        Assert.Empty(factory.Disposed);
    }

    [Fact]
    public async Task Leader_with_dead_letter_creates_main_and_dead_letter_destination_per_term()
    {
        deadLetter = new DestinationBinding(new DestinationOptions { Name = "DeadLetters" }, factory);
        await StartAsync();

        await AdvanceAsync(TimeSpan.FromSeconds(3));

        Assert.Equal(
            [("Events", new DestinationContext(OrderingMode.None)), ("DeadLetters", new DestinationContext(OrderingMode.None))],
            factory.Created);
        Assert.Equal(["Events"], factory.ProbeStarted);
        Assert.Empty(factory.Disposed);
    }

    [Fact]
    public async Task Loss_disposes_both_destinations_and_next_term_creates_new_ones()
    {
        deadLetter = new DestinationBinding(new DestinationOptions { Name = "DeadLetters" }, factory);
        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(1));

        var mark = time.Created;
        store.CurrentSession.SignalLost();
        await time.WaitForTimerAsync(RetryWait, mark);
        await WorkerClock.WaitUntilAsync(() => factory.Disposed.Count == 2);

        Assert.Equal(["DeadLetters", "Events"], factory.Disposed.Order());
        Assert.False(status.IsLeader);

        await AdvanceAsync(RetryWait);

        Assert.True(status.IsLeader);
        Assert.Equal(["Events", "DeadLetters", "Events", "DeadLetters"], factory.Created.Select(c => c.Destination));
        Assert.Equal(2, factory.Disposed.Count);
    }

    [Theory]
    [InlineData(OrderingMode.PerKey)]
    [InlineData(OrderingMode.Strict)]
    public async Task Ordered_mode_builds_a_delivery_term_and_leads(OrderingMode mode)
    {
        var worker = new OutboxWorker(Validated(Outbox, store, factory, mode), status, state, new FixedJitter(jitter), time, logger);
        loop = worker.RunAsync(stopping.Token);
        await WorkerClock.WaitUntilAsync(() => time.Active > 0 || Settled());

        await AdvanceAsync(TimeSpan.FromSeconds(3));

        Assert.False(Loop.IsCompleted);
        Assert.True(status.IsLeader);
        Assert.Equal([("Events", new DestinationContext(mode))], factory.Created);
        Assert.NotEmpty(store.CurrentSession.SummaryReads);
    }

    [Fact]
    public async Task Loss_returns_to_lock_attempts_and_next_term_creates_new_destination()
    {
        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(1));

        var mark = time.Created;
        store.CurrentSession.SignalLost();
        await time.WaitForTimerAsync(RetryWait, mark);

        Assert.True(store.Sessions[0].Disposed);
        Assert.Equal(["Events"], factory.Disposed);
        Assert.False(status.IsLeader);

        await AdvanceAsync(RetryWait);

        Assert.Equal(2, store.Sessions.Count);
        Assert.Equal([TimeSpan.FromSeconds(1.5)], store.CurrentSession.LockAttempts);
        Assert.True(status.IsLeader);
        Assert.Equal(2, factory.Created.Count);
    }

    [Fact]
    public async Task Check_failure_disposes_session_and_reconnects_after_retry_interval()
    {
        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(3));
        store.NextCheckFailure = new OutboxSessionLostException("Outboxes[Orders]: session lost (timeout)");

        var mark = time.Created;
        await AdvanceAsync(TimeSpan.FromSeconds(1));
        await time.WaitForTimerAsync(RetryWait, mark);

        var first = store.Sessions[0];
        Assert.True(first.Disposed);
        Assert.False(status.IsLeader);
        Assert.Equal(0, state.HeldCount);
        Assert.Single(logger.WithEvent(102));

        await AdvanceAsync(RetryWait);
        Assert.Equal([TimeSpan.Zero, TimeSpan.FromSeconds(4.5)], store.OpenAttempts);
        Assert.True(status.IsLeader);
    }

    [Fact]
    public async Task Lost_signal_mid_wait_disposes_session_without_waiting_for_check_tick()
    {
        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(0.3));

        var mark = time.Created;
        store.CurrentSession.SignalLost();
        await time.WaitForTimerAsync(RetryWait, mark);

        var first = store.Sessions[0];
        Assert.True(first.Disposed);
        Assert.Empty(first.Checks);
        Assert.False(status.IsLeader);
        Assert.Equal(0, state.HeldCount);

        await AdvanceAsync(RetryWait);
        Assert.Equal([TimeSpan.Zero, TimeSpan.FromSeconds(0.8)], store.OpenAttempts);
    }

    [Fact]
    public async Task Lock_failure_as_non_leader_disposes_session_and_reconnects()
    {
        store.NextLockFailure = new OutboxSessionLostException("Outboxes[Orders]: session lost (timeout)");
        await StartAsync();

        Assert.True(store.Sessions[0].Disposed);
        Assert.Empty(logger.WithEvent(102));

        await AdvanceAsync(RetryWait);
        Assert.Equal([TimeSpan.Zero, TimeSpan.FromSeconds(0.5)], store.OpenAttempts);
    }

    [Fact]
    public async Task Non_leader_keeps_one_session_across_refused_attempts()
    {
        store.LockGranted = false;
        await StartAsync();

        await AdvanceAsync(TimeSpan.FromSeconds(5));

        var session = Assert.Single(store.Sessions);
        Assert.Equal(11, session.LockAttempts.Count);
        Assert.False(session.Disposed);
        Assert.Single(store.OpenAttempts);
    }

    [Fact]
    public async Task Database_reachability_transitions_log_once_each()
    {
        store.OpenFailure = new OutboxDatabaseUnreachableException("Outboxes[Orders]: database unreachable (SocketException ConnectionRefused)");
        await StartAsync();

        Assert.False(status.DatabaseReachable);
        await AdvanceAsync(TimeSpan.FromSeconds(2));
        Assert.Equal(5, store.OpenAttempts.Count);
        Assert.False(status.DatabaseReachable);
        Assert.Single(logger.WithEvent(104));

        store.OpenFailure = null;
        await AdvanceAsync(RetryWait);

        Assert.True(status.DatabaseReachable);
        Assert.Single(logger.WithEvent(105));
        Assert.Single(logger.WithEvent(104));
    }

    [Fact]
    public async Task First_successful_connect_logs_no_reachability_transition()
    {
        await StartAsync();

        Assert.True(status.DatabaseReachable);
        Assert.Empty(logger.WithEvent(104));
        Assert.Empty(logger.WithEvent(105));
    }

    public enum StopPoint
    {
        StartDelay,
        LockWait,
        Leading,
        RetryWait,
    }

    [Theory]
    [InlineData(StopPoint.StartDelay)]
    [InlineData(StopPoint.LockWait)]
    [InlineData(StopPoint.Leading)]
    [InlineData(StopPoint.RetryWait)]
    public async Task Stopping_ends_loop_and_disposes_session(StopPoint point)
    {
        switch (point)
        {
            case StopPoint.StartDelay:
                jitter = TimeSpan.FromSeconds(1);
                break;
            case StopPoint.LockWait:
                store.LockGranted = false;
                break;
            case StopPoint.RetryWait:
                store.OpenFailure = new OutboxDatabaseUnreachableException("Outboxes[Orders]: database unreachable (timeout)");
                break;
        }

        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(0.3));

        await StopAsync();

        Assert.Equal(TaskStatus.RanToCompletion, Loop.Status);
        Assert.All(store.Sessions, s => Assert.True(s.Disposed));
        Assert.False(status.IsLeader);
        Assert.Equal(0, state.HeldCount);
        Assert.Equal(point == StopPoint.Leading ? 1 : 0, logger.WithEvent(103).Count());
        Assert.Equal(point is StopPoint.LockWait or StopPoint.Leading ? 1 : 0, store.Sessions.Count);
    }

    [Fact]
    public async Task Stopping_leader_disposes_session_before_it_stops_leading()
    {
        await StartAsync();
        await AdvanceAsync(Step);
        bool? leadingAtDispose = null;
        store.CurrentSession.OnDispose = () => leadingAtDispose = status.IsLeader;

        await StopAsync();

        Assert.True(leadingAtDispose);
        Assert.False(status.IsLeader);
        Assert.Single(logger.WithEvent(103));
        Assert.Empty(logger.WithEvent(107));
        Assert.Equal(["Events"], factory.Disposed);
    }

    [Fact]
    public async Task Stopping_leader_whose_dispose_throws_still_stops_leading()
    {
        await StartAsync();
        await AdvanceAsync(Step);
        Assert.True(status.IsLeader);
        store.CurrentSession.OnDispose = () => throw new InvalidOperationException("dispose failed");

        await StopAsync();

        Assert.True(Loop.IsCompletedSuccessfully);
        Assert.False(status.IsLeader);
        Assert.Empty(logger.WithEvent(103));
        var entry = Assert.Single(logger.WithEvent(107));
        Assert.Equal("Leadership release pending for outbox Orders: session dispose failed (InvalidOperationException)", entry.Message);
    }

    [Fact]
    public async Task Stopping_leader_with_deferred_close_logs_release_pending()
    {
        await StartAsync();
        await AdvanceAsync(Step);
        store.CurrentSession.ReleasePending = true;

        await StopAsync();

        Assert.False(status.IsLeader);
        Assert.Empty(logger.WithEvent(103));
        var entry = Assert.Single(logger.WithEvent(107));
        Assert.Equal("Leadership release pending for outbox Orders: lock may stay held until the server ends the backend (abandoned command, failed or timed-out unlock, broken connection or failed close)", entry.Message);
    }

    [Fact]
    public async Task Stopping_leader_with_hung_session_close_returns_at_T0_plus_27_seconds()
    {
        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(1));
        var session = store.CurrentSession;
        session.DisposeGate = new TaskCompletionSource();
        var t0 = store.Now;

        var mark = time.Created;
        await stopping.CancelAsync();
        await time.WaitForTimerAsync(TimeSpan.FromSeconds(27), mark);
        Assert.True(session.DisposeStarted);

        await AdvanceAsync(TimeSpan.FromSeconds(26.9));
        Assert.False(Loop.IsCompleted);
        Assert.True(status.IsLeader);

        await AdvanceAsync(Step);
        await Loop.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Equal(t0 + TimeSpan.FromSeconds(27), store.Now);
        Assert.True(Loop.IsCompletedSuccessfully);
        Assert.False(status.IsLeader);
        Assert.Empty(logger.WithEvent(103));
        var entry = Assert.Single(logger.WithEvent(107));
        Assert.Equal("Leadership release pending for outbox Orders: session close abandoned at stop deadline", entry.Message);
        session.DisposeGate.SetResult();
    }

    [Fact]
    public async Task Stopping_non_leader_with_hung_session_close_returns_at_T0_plus_27_seconds()
    {
        store.LockGranted = false;
        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(0.3));
        var session = store.CurrentSession;
        session.DisposeGate = new TaskCompletionSource();
        var t0 = store.Now;

        var mark = time.Created;
        await stopping.CancelAsync();
        await time.WaitForTimerAsync(TimeSpan.FromSeconds(27), mark);

        await AdvanceAsync(TimeSpan.FromSeconds(26.9));
        Assert.False(Loop.IsCompleted);

        await AdvanceAsync(Step);
        await Loop.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Equal(t0 + TimeSpan.FromSeconds(27), store.Now);
        Assert.True(Loop.IsCompletedSuccessfully);
        Assert.Empty(logger.WithEvent(107));
        session.DisposeGate.SetResult();
    }

    [Fact]
    public async Task Stop_resuming_before_stop_signal_is_stamped_still_caps_hung_close_at_T0_plus_27_seconds()
    {
        store.LockHangsUntilCancelled = true;
        var worker = new OutboxWorker(Validated(Outbox, store, factory), status, state, new FixedJitter(jitter), time, logger);
        loop = worker.RunAsync(stopping.Token);
        var session = store.CurrentSession;
        Assert.Single(session.LockAttempts);
        session.DisposeGate = new TaskCompletionSource();
        var t0 = store.Now;

        // The lock's callback runs before the worker's stop registration, so the worker reaches its close unstamped.
        var mark = time.Created;
        await Task.Run(stopping.Cancel, TestContext.Current.CancellationToken);
        await time.WaitForTimerAsync(TimeSpan.FromSeconds(27), mark);

        await AdvanceAsync(TimeSpan.FromSeconds(26.9));
        Assert.False(Loop.IsCompleted);

        await AdvanceAsync(Step);
        await Loop.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Equal(t0 + TimeSpan.FromSeconds(27), store.Now);
        Assert.True(Loop.IsCompletedSuccessfully);
        session.DisposeGate.SetResult();
    }

    [Fact]
    public async Task Loss_while_stopping_with_hung_session_close_returns_at_T0_plus_27_seconds()
    {
        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(1));
        var session = store.CurrentSession;
        session.DisposeGate = new TaskCompletionSource();

        session.SignalLost();
        await WorkerClock.WaitUntilAsync(() => session.DisposeStarted);
        Assert.False(status.IsLeader);
        var t0 = store.Now;

        var mark = time.Created;
        await stopping.CancelAsync();
        await time.WaitForTimerAsync(TimeSpan.FromSeconds(27), mark);

        await AdvanceAsync(TimeSpan.FromSeconds(26.9));
        Assert.False(Loop.IsCompleted);

        await AdvanceAsync(Step);
        await Loop.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Equal(t0 + TimeSpan.FromSeconds(27), store.Now);
        Assert.True(Loop.IsCompletedSuccessfully);
        Assert.Single(store.Sessions);
        session.DisposeGate.SetResult();
    }

    [Fact]
    public async Task Held_count_follows_leader_flags()
    {
        var other = new OutboxRuntimeStatus("Other") { IsLeader = true };
        state.SetOutboxes([status, other]);
        Assert.Equal(1, state.HeldCount);

        await StartAsync();
        Assert.Equal(2, state.HeldCount);

        other.IsLeader = false;
        Assert.Equal(1, state.HeldCount);
    }

    [Fact]
    public async Task Unexpected_exception_faults_loop_and_logs_worker_faulted()
    {
        store.OpenFailure = new InvalidOperationException("bug");
        var validatedOutboxes = new ValidatedOutboxes();
        validatedOutboxes.Add(Validated(Outbox, store));
        var serviceLogger = new RecordingLogger<OutboxWorkerService>();
        using var service = new OutboxWorkerService(validatedOutboxes, state, new FixedJitter(TimeSpan.Zero), time, serviceLogger);

        await service.StartAsync(TestContext.Current.CancellationToken);
        var faulted = state.Outboxes[0].Loop!;
        await Assert.ThrowsAsync<InvalidOperationException>(() => faulted);

        Assert.True(faulted.IsFaulted);
        await WorkerClock.WaitUntilAsync(() => serviceLogger.WithEvent(106).Any());
        await service.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Service_sets_initial_progress_due_to_start_jitter_plus_grace()
    {
        var validatedOutboxes = new ValidatedOutboxes();
        validatedOutboxes.Add(Validated(Outbox, store));
        using var service = new OutboxWorkerService(validatedOutboxes, state, new FixedJitter(TimeSpan.FromSeconds(2)), time, new RecordingLogger<OutboxWorkerService>());
        var started = time.GetUtcNow();

        await service.StartAsync(TestContext.Current.CancellationToken);

        Assert.Equal(started + TimeSpan.FromSeconds(12), state.Outboxes[0].ProgressDueAt);
        await service.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Service_starts_one_loop_per_validated_outbox_and_stop_ends_them()
    {
        var second = new FakeOutboxStore(time);
        var validatedOutboxes = new ValidatedOutboxes();
        validatedOutboxes.Add(Validated("A", store));
        validatedOutboxes.Add(Validated("B", second));
        using var service = new OutboxWorkerService(validatedOutboxes, state, new FixedJitter(TimeSpan.Zero), time, new RecordingLogger<OutboxWorkerService>());

        await service.StartAsync(TestContext.Current.CancellationToken);
        Assert.Equal(["A", "B"], state.Outboxes.Select(o => o.Name));
        await WorkerClock.WaitUntilAsync(() => state.HeldCount == 2);

        await service.StopAsync(TestContext.Current.CancellationToken);

        Assert.All(state.Outboxes, o => Assert.Equal(TaskStatus.RanToCompletion, o.Loop!.Status));
        Assert.All(state.Outboxes, o => Assert.False(o.IsLeader));
        Assert.True(store.CurrentSession.Disposed);
        Assert.True(second.CurrentSession.Disposed);
    }

    [Fact]
    public async Task Service_disposed_right_after_start_ends_loops_without_fault()
    {
        for (var i = 0; i < 50; i++)
        {
            var iterationState = new OutboxRuntimeState();
            var validatedOutboxes = new ValidatedOutboxes();
            validatedOutboxes.Add(Validated("A", new FakeOutboxStore(time)));
            validatedOutboxes.Add(Validated("B", new FakeOutboxStore(time)));
            var serviceLogger = new RecordingLogger<OutboxWorkerService>();
            var service = new OutboxWorkerService(validatedOutboxes, iterationState, new FixedJitter(TimeSpan.Zero), time, serviceLogger);

            await service.StartAsync(TestContext.Current.CancellationToken);
            service.Dispose();

            foreach (var outbox in iterationState.Outboxes)
            {
                await outbox.Loop!.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken)
                    .ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing | ConfigureAwaitOptions.ContinueOnCapturedContext);
                Assert.Equal(TaskStatus.RanToCompletion, outbox.Loop.Status);
            }

            Assert.Empty(serviceLogger.WithEvent(106));
        }
    }

    [Fact]
    public async Task Service_stop_after_dispose_does_not_throw()
    {
        var service = new OutboxWorkerService(new ValidatedOutboxes(), state, new FixedJitter(TimeSpan.Zero), time, new RecordingLogger<OutboxWorkerService>());

        service.Dispose();
        service.Dispose();
        await service.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Log_text_never_contains_connection_string()
    {
        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(1.5));
        store.NextCheckFailure = new OutboxSessionLostException("Outboxes[Orders]: session lost (timeout)");
        store.OpenFailure = new OutboxDatabaseUnreachableException("Outboxes[Orders]: database unreachable (timeout)");

        var mark = time.Created;
        await AdvanceAsync(TimeSpan.FromSeconds(0.5));
        await time.WaitForTimerAsync(RetryWait, mark);
        await AdvanceAsync(RetryWait);
        Assert.Single(logger.WithEvent(104));

        store.OpenFailure = null;
        await AdvanceAsync(RetryWait);
        await StopAsync();

        Assert.Equal([101, 102, 104, 105, 101, 103], logger.Entries.Select(e => e.EventId.Id));
        Assert.All(logger.Entries, e =>
        {
            Assert.Contains(Outbox, e.Message);
            Assert.DoesNotContain("sentinel", e.Message, StringComparison.OrdinalIgnoreCase);
            Assert.DoesNotContain(SentinelConnectionString, e.Message);
        });
    }

    [Fact]
    public async Task Loss_dispose_expects_progress_within_session_dispose_bound()
    {
        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(1));
        var session = store.CurrentSession;
        session.DisposeGate = new TaskCompletionSource();
        var lossAt = time.GetUtcNow();

        session.SignalLost();
        await WorkerClock.WaitUntilAsync(() => session.DisposeStarted);

        Assert.Equal(lossAt + OutboxWorker.SessionDisposeBound + OutboxRuntimeStatus.ProgressGrace, status.ProgressDueAt);

        time.Advance(TimeSpan.FromSeconds(19.9));
        Assert.False(time.GetUtcNow() > status.ProgressDueAt);

        time.Advance(TimeSpan.FromSeconds(0.2));
        Assert.True(time.GetUtcNow() > status.ProgressDueAt);
        session.DisposeGate.SetResult();
    }

    [Fact]
    public async Task Loss_dispose_completes_before_bound()
    {
        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(1));
        var session = store.CurrentSession;
        session.DisposeGate = new TaskCompletionSource();

        session.SignalLost();
        await WorkerClock.WaitUntilAsync(() => session.DisposeStarted);
        time.Advance(TimeSpan.FromSeconds(9));

        var mark = time.Created;
        session.DisposeGate.SetResult();
        await time.WaitForTimerAsync(RetryWait, mark);

        Assert.True(session.Disposed);
        Assert.Equal(time.GetUtcNow() + RetryWait + OutboxRuntimeStatus.ProgressGrace, status.ProgressDueAt);
    }

    [Fact]
    public async Task Fault_close_with_stop_mid_dispose_is_capped_at_T0_plus_27_seconds()
    {
        var session = await StartFaultCloseAsync();
        var t0 = store.Now;

        var mark = time.Created;
        await stopping.CancelAsync();
        await time.WaitForTimerAsync(TimeSpan.FromSeconds(27), mark);

        await AdvanceAsync(TimeSpan.FromSeconds(26.9));
        Assert.False(Loop.IsCompleted);
        Assert.True(status.IsLeader);

        await AdvanceAsync(Step);
        var fault = await Assert.ThrowsAsync<InvalidOperationException>(() => Loop.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));

        Assert.Equal("bug", fault.Message);
        Assert.Equal(t0 + TimeSpan.FromSeconds(27), store.Now);
        Assert.False(status.IsLeader);
        Assert.Empty(logger.WithEvent(103));
        var entry = Assert.Single(logger.WithEvent(107));
        Assert.Equal("Leadership release pending for outbox Orders: session close abandoned at stop deadline", entry.Message);
        session.DisposeGate!.SetResult();
    }

    [Fact]
    public async Task Fault_close_with_stop_mid_dispose_completing_before_cap_logs_released()
    {
        var session = await StartFaultCloseAsync();
        var t0 = store.Now;

        var mark = time.Created;
        await stopping.CancelAsync();
        await time.WaitForTimerAsync(TimeSpan.FromSeconds(27), mark);

        await AdvanceAsync(TimeSpan.FromSeconds(5));
        session.DisposeGate!.SetResult();
        var fault = await Assert.ThrowsAsync<InvalidOperationException>(() => Loop.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));

        Assert.Equal("bug", fault.Message);
        Assert.Equal(t0 + TimeSpan.FromSeconds(5), store.Now);
        Assert.False(status.IsLeader);
        Assert.Single(logger.WithEvent(103));
        Assert.Empty(logger.WithEvent(107));
        Assert.Equal(1, session.DisposeCalls);
    }

    [Fact]
    public async Task Fault_close_with_stop_cap_tying_dispose_completion_logs_released()
    {
        var session = await StartFaultCloseAsync();
        var t0 = store.Now;

        // Registered before the worker's await, so it completes the dispose after the cap times out and before the worker handles it.
        time.TimerCreating = (dueTime, state) =>
        {
            if (dueTime == OutboxWorker.StopCloseDeadline && state is Task cappedWait)
            {
                _ = cappedWait.ContinueWith(_ => session.DisposeGate!.TrySetResult(), CancellationToken.None, TaskContinuationOptions.ExecuteSynchronously, TaskScheduler.Default);
            }
        };
        var mark = time.Created;
        await stopping.CancelAsync();
        await time.WaitForTimerAsync(OutboxWorker.StopCloseDeadline, mark);

        await Task.Run(() => AdvanceAsync(OutboxWorker.StopCloseDeadline), TestContext.Current.CancellationToken);
        var fault = await Assert.ThrowsAsync<InvalidOperationException>(() => Loop.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));

        Assert.Equal("bug", fault.Message);
        Assert.Equal(t0 + OutboxWorker.StopCloseDeadline, store.Now);
        Assert.True(session.Disposed);
        Assert.False(status.IsLeader);
        Assert.Single(logger.WithEvent(103));
        Assert.Empty(logger.WithEvent(107));
        Assert.Equal(1, session.DisposeCalls);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Fault_close_stop_and_dispose_completion_race_awaits_dispose_outcome(bool completeAtOutcome)
    {
        var run = await RunStopDisposeRaceAsync(fault: true, completeAtOutcome);

        var fault = await Assert.ThrowsAsync<InvalidOperationException>(() => run.Loop);
        Assert.Equal("bug", fault.Message);
        Assert.Single(run.Logger.WithEvent(103));
        Assert.Empty(run.Logger.WithEvent(107));
        Assert.Equal(1, run.Session.DisposeCalls);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Loss_stop_and_dispose_completion_race_disposes_once(bool completeAtOutcome)
    {
        var run = await RunStopDisposeRaceAsync(fault: false, completeAtOutcome);

        Assert.True(run.Loop.IsCompletedSuccessfully);
        Assert.Empty(run.Logger.WithEvent(107));
        Assert.Single(run.Store.Sessions);
        Assert.Equal(1, run.Session.DisposeCalls);
    }

    private sealed record RaceRun(Task Loop, FakeOutboxStore Store, FakeOutboxSession Session, RecordingLogger<OutboxWorker> Logger);

    // With completeAtOutcome the seam completes the dispose inside the catch filter, so the dispose outcome wins.
    // Without it the test completes the dispose only after the worker has started its 27 s stop-deadline wait.
    // The test decides both branches, so no callback order or thread-pool timing does.
    private static async Task<RaceRun> RunStopDisposeRaceAsync(bool fault, bool completeAtOutcome)
    {
        var clock = new WorkerClock();
        var raceStore = new FakeOutboxStore(clock);
        var raceStatus = new OutboxRuntimeStatus(Outbox);
        var raceState = new OutboxRuntimeState();
        raceState.SetOutboxes([raceStatus]);
        var raceLogger = new RecordingLogger<OutboxWorker>();
        using var stop = new CancellationTokenSource();
        var gate = new TaskCompletionSource();
        var seamRan = false;
        var worker = new OutboxWorker(Validated(Outbox, raceStore), raceStatus, raceState, new FixedJitter(TimeSpan.Zero), clock, raceLogger)
        {
            BeforeDisposeOutcome = () =>
            {
                if (completeAtOutcome)
                {
                    gate.TrySetResult();
                }

                seamRan = true;
            },
        };
        var run = worker.RunAsync(stop.Token);
        bool Settled() => run.IsCompleted || raceStore.Sessions.Any(s => s.DisposeStarted && !s.Disposed);
        await WorkerClock.WaitUntilAsync(() => clock.Active > 0 || Settled());
        var session = raceStore.CurrentSession;
        session.DisposeGate = gate;

        if (fault)
        {
            raceStore.NextCheckFailure = new InvalidOperationException("bug");
            await clock.AdvanceAsync(TimeSpan.FromSeconds(1), Step, Settled);
        }
        else
        {
            session.SignalLost();
        }

        await WorkerClock.WaitUntilAsync(() => session.DisposeStarted);
        var mark = clock.Created;
        await Task.Run(stop.Cancel, TestContext.Current.CancellationToken);
        if (!completeAtOutcome)
        {
            await clock.WaitForTimerAsync(OutboxWorker.StopCloseDeadline, mark);
            gate.TrySetResult();
        }

        await run.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken)
            .ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing | ConfigureAwaitOptions.ContinueOnCapturedContext);
        Assert.True(run.IsCompleted);
        if (completeAtOutcome)
        {
            // The dispose outcome won, so the worker never started the stop-deadline wait.
            Assert.DoesNotContain(OutboxWorker.StopCloseDeadline, clock.DueTimes.Skip(mark));
        }

        Assert.True(seamRan);
        return new RaceRun(run, raceStore, session, raceLogger);
    }

    private async Task<FakeOutboxSession> StartFaultCloseAsync()
    {
        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(0.5));
        var session = store.CurrentSession;
        session.DisposeGate = new TaskCompletionSource();
        store.NextCheckFailure = new InvalidOperationException("bug");

        await AdvanceAsync(TimeSpan.FromSeconds(0.5));
        await WorkerClock.WaitUntilAsync(() => session.DisposeStarted);
        Assert.False(Loop.IsCompleted);
        Assert.True(status.IsLeader);
        Assert.Null(store.NextCheckFailure);
        return session;
    }

    private Task Loop => loop!;

    private async Task StartAsync()
    {
        var worker = new OutboxWorker(Validated(Outbox, store, factory, deadLetter: deadLetter), status, state, new FixedJitter(jitter), time, logger);
        loop = worker.RunAsync(stopping.Token);
        await WorkerClock.WaitUntilAsync(() => time.Active > 0 || Settled());
    }

    private async Task StopAsync()
    {
        await stopping.CancelAsync();
        await Loop.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
    }

    private Task AdvanceAsync(TimeSpan total) => time.AdvanceAsync(total, Step, Settled);

    private bool Settled() => loop is { IsCompleted: true } || store.Sessions.Any(s => s.DisposeStarted && !s.Disposed);
}
