using System.Collections.Concurrent;
using System.Diagnostics.Metrics;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Core.Delivery;
using Pernix.OutboxRelay.Core.Diagnostics;
using Pernix.OutboxRelay.Core.Leadership;
using Pernix.OutboxRelay.Unit.Tests.Leadership;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Delivery;

public sealed partial class DeliveryTermTests : IDisposable
{
    private const string Destination = "events";
    private const string DeadLetterName = "dead-letters";
    private static readonly TimeSpan Step = TimeSpan.FromMilliseconds(100);

    private readonly string outbox = $"orders-{Guid.NewGuid():N}";
    private readonly TermClock time = new();
    private readonly DateTimeOffset origin;
    private readonly FakeOutboxSession session;
    private readonly FakeDestination destination = new();
    private readonly FakeDestination deadLetter = new();
    private readonly OutboxRuntimeStatus status;
    private readonly RecordingLogger<DeliveryTerm> logger = new();
    private readonly CancellationTokenSource stopping = new();
    private readonly OutboxOptions options = new()
    {
        Polling = new PollingOptions { Interval = TimeSpan.FromSeconds(1), BatchSize = 10 },
        Retry = new RetryOptions { InitialDelay = TimeSpan.FromSeconds(10), BackoffMultiplier = 2, MaxDelay = TimeSpan.FromSeconds(30) },
    };

    private Task<DeliveryTermResult>? term;
    private int destinationsCreated;
    private bool withDeadLetter;
    private OrderingMode mode = OrderingMode.None;
    private ScopedDelivery? scoped;
    private HeldRecorder? recorder;
    private DateTimeOffset? disposeBound;

    public DeliveryTermTests()
    {
        origin = time.GetUtcNow();
        session = new FakeOutboxSession(time);
        status = new OutboxRuntimeStatus(outbox);
        options.Name = outbox;
    }

    public void Dispose() => stopping.Dispose();

    private Task<DeliveryTermResult> Term => term ?? throw new InvalidOperationException("term not started");

    private TimeSpan Now => time.GetUtcNow() - origin;

    [Fact]
    public async Task Read_max_is_batch_size_bounded_by_room_and_held_rows_are_capped()
    {
        options.Polling.Interval = TimeSpan.FromSeconds(3);
        session.Insert([.. Enumerable.Range(1, 200).Select(i => (long)i)]);
        destination.Respond = null;

        await StartAsync();

        Assert.Equal(Enumerable.Repeat(10, 10), session.Reads.Select(r => r.Max));
        Assert.Equal(Enumerable.Range(0, 10).Select(i => i * 10), session.Reads.Select(r => r.Exclude.Count));
        Assert.Equal(100, destination.Pending.Count);

        // Room frees at the 1 s check, before the capping read's Interval elapses.
        await WhileCheckBlockedAsync(() =>
        {
            foreach (var id in Enumerable.Range(1, 5))
            {
                destination.Complete(id, new DeliveryResult.Delivered());
            }
        });
        Assert.Equal(TimeSpan.FromSeconds(1), Now);

        await AdvanceAsync(TimeSpan.FromSeconds(1.9));
        Assert.Equal(10, session.Reads.Count);

        await AdvanceAsync(Step);

        var last = session.Reads[^1];
        Assert.Equal(11, session.Reads.Count);
        Assert.Equal(session.Reads[9].At + options.Polling.Interval, last.At);
        Assert.Equal(5, last.Max);
        Assert.Equal(Enumerable.Range(6, 95).Select(i => (long)i), last.Exclude.Order());
        Assert.Equal(100, destination.Pending.Count);
    }

    [Fact]
    public async Task Full_batch_reads_again_at_once_and_short_batch_waits_polling_interval()
    {
        options.Polling.Interval = TimeSpan.FromSeconds(3);
        session.Insert([.. Enumerable.Range(1, 25).Select(i => (long)i)]);

        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(6));

        Assert.Equal(
            [TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(6)],
            session.Reads.Select(r => r.At));
        Assert.Equal([10, 10, 5, 0, 0], session.Reads.Select(r => r.Returned.Count));
        Assert.Equal([10, 10, 5], session.Commits.Select(c => c.Ids.Count));
        Assert.Empty(session.TableIds);
        Assert.Equal(1, destinationsCreated);
        Assert.Equal(1, destination.ValidateCalls);
    }

    [Fact]
    public async Task Remove_outcomes_commit_in_groups_of_at_most_100_in_arrival_order()
    {
        options.Polling.BatchSize = 500;
        session.Insert([.. Enumerable.Range(1, 250).Select(i => (long)i)]);
        destination.Respond = null;

        await StartAsync();
        long[] arrival = [.. Enumerable.Range(1, 250).Select(i => (long)i).Reverse()];
        await WhileCheckBlockedAsync(() =>
        {
            foreach (var id in arrival)
            {
                destination.Complete(id, new DeliveryResult.Delivered());
            }
        });

        Assert.Equal([100, 100, 50], session.Commits.Select(c => c.Ids.Count));
        Assert.Equal(arrival, session.Commits.SelectMany(c => c.Ids));
    }

    [Fact]
    public async Task Checks_every_second_while_sends_are_pending()
    {
        session.Insert(1, 2, 3);
        destination.Respond = null;

        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(5));

        Assert.Equal(Enumerable.Range(1, 5).Select(s => TimeSpan.FromSeconds(s)), session.Checks);
        Assert.Equal(3, destination.Pending.Count);
        Assert.Empty(session.Commits);
    }

    [Fact]
    public async Task Pauses_follow_backoff_curve_and_saturate_at_max_delay()
    {
        session.Insert(1);
        destination.Respond = _ => new DeliveryResult.DestinationFault("broker down");

        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(100));

        double[] starts = [0, 10, 30, 60, 90];
        Assert.Equal(starts.Select(TimeSpan.FromSeconds), session.Reads.Select(r => r.At));
        var paused = logger.WithEvent(110).Select(e => e.Message).ToList();
        Assert.Equal(5, paused.Count);
        Assert.All(paused, m => Assert.Contains("DestinationFault: broker down", m, StringComparison.Ordinal));
        Assert.Equal(["00:00:10", "00:00:20", "00:00:30", "00:00:30", "00:00:30"], paused.Select(m => m[^8..]));
        Assert.Equal(4, logger.WithEvent(112).Count());
        Assert.Empty(logger.WithEvent(111));
        Assert.Empty(session.Commits);
        Assert.Equal([1L], session.TableIds);
    }

    [Fact]
    public async Task Pause_count_resets_only_on_delivered_send_started_after_the_last_pause()
    {
        options.Retry.MaxDelay = TimeSpan.FromMinutes(5);
        session.Insert(1, 2);
        destination.Respond = null;
        await StartAsync();

        await ActAsync(() => destination.Complete(1, new DeliveryResult.DestinationFault("timed out")));
        await ActAsync(() => destination.Complete(2, new DeliveryResult.Delivered()));
        Assert.Equal([2L], session.Commits.SelectMany(c => c.Ids));

        await AdvanceAsync(TimeSpan.FromSeconds(10));
        Assert.Equal([1L], session.Reads[^1].Returned);
        await ActAsync(() => destination.Complete(1, new DeliveryResult.DestinationFault("timed out")));

        await AdvanceAsync(TimeSpan.FromSeconds(20));
        Assert.Equal(TimeSpan.FromSeconds(30), session.Reads[^1].At);
        await ActAsync(() => destination.Complete(1, new DeliveryResult.Delivered()));

        session.Insert(3);
        await AdvanceAsync(TimeSpan.FromSeconds(1));
        Assert.Equal([3L], session.Reads[^1].Returned);
        await ActAsync(() => destination.Complete(3, new DeliveryResult.DestinationFault("timed out")));

        Assert.Equal(["00:00:10", "00:00:20", "00:00:10"], logger.WithEvent(110).Select(e => e.Message[^8..]));
    }

    [Fact]
    public async Task Delivered_after_pause_end_from_send_started_before_pause_does_not_reset_backoff()
    {
        options.Retry.MaxDelay = TimeSpan.FromMinutes(5);
        session.Insert(1, 2);
        destination.Respond = null;
        await StartAsync();

        await ActAsync(() => destination.Complete(1, new DeliveryResult.DestinationFault("timed out")));
        await AdvanceAsync(TimeSpan.FromSeconds(10));
        Assert.Single(logger.WithEvent(112));
        Assert.Equal([1L], session.Reads[^1].Returned);

        await ActAsync(() => destination.Complete(2, new DeliveryResult.Delivered()));
        Assert.Equal([2L], session.Commits.SelectMany(c => c.Ids));
        await ActAsync(() => destination.Complete(1, new DeliveryResult.DestinationFault("timed out")));

        Assert.Equal(["00:00:10", "00:00:20"], logger.WithEvent(110).Select(e => e.Message[^8..]));
    }

    [Fact]
    public async Task Destination_faulted_rows_leave_held_set_at_pause_end_and_are_read_again()
    {
        session.Insert(1);
        var calls = 0;
        destination.Respond = _ => ++calls == 1
            ? new DeliveryResult.DestinationFault("Local_MsgTimedOut: timed out")
            : new DeliveryResult.Delivered();

        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(9.9));
        Assert.Single(session.Reads);

        await AdvanceAsync(Step);

        var reread = session.Reads[^1];
        Assert.Equal(TimeSpan.FromSeconds(10), reread.At);
        Assert.Empty(reread.Exclude);
        Assert.Equal([1L, 1L], destination.Sent);
        Assert.Empty(session.TableIds);
        Assert.Empty(logger.WithEvent(111));
        Assert.Single(logger.WithEvent(112));
    }

    [Fact]
    public async Task Malformed_headers_without_dead_letter_drop_the_row_without_send_or_pause()
    {
        session.Insert(FakeOutboxSession.Row(1, malformedHeaders: true));
        session.Insert(2);

        await StartAsync();

        Assert.Equal([2L], destination.Sent);
        Assert.Empty(session.TableIds);
        Assert.All(session.Commits.SelectMany(c => c.Outcomes), o => Assert.IsType<Outcome.Remove>(o));
        var faulted = Assert.Single(logger.WithEvent(111));
        Assert.Contains("Message 1 ", faulted.Message, StringComparison.Ordinal);
        Assert.Contains("retry count 0, permanent True: headers is not a JSON object of string values", faulted.Message, StringComparison.Ordinal);
        var dropped = Assert.Single(logger.WithEvent(120));
        Assert.Equal(Microsoft.Extensions.Logging.LogLevel.Warning, dropped.Level);
        Assert.Contains("Message 1 ", dropped.Message, StringComparison.Ordinal);
        Assert.Contains($"destination {Destination} dropped at retry count 0: {DeliveryTerm.MalformedHeadersError}", dropped.Message, StringComparison.Ordinal);
        Assert.Empty(logger.WithEvent(110));
    }

    [Fact]
    public async Task Transient_message_fault_spends_a_retry_without_pausing_and_other_rows_keep_sending()
    {
        session.Insert(1, 2, 3);
        destination.Respond = m => m.Source.Id == 1
            ? new DeliveryResult.MessageFault("boom", Permanent: false)
            : new DeliveryResult.Delivered();

        await StartAsync();

        var first = Assert.Single(session.Commits);
        Assert.Equal([1L, 2L, 3L], first.Ids);
        Assert.Equal(
            new Outcome.Fail(1) { ReadRetryCount = 0, RetryCount = 1, Delay = TimeSpan.FromSeconds(10), Error = "boom" },
            first.Outcomes[0]);
        Assert.Equal(1, session.Get(1)!.RetryCount);
        Assert.Contains("retry count 0, permanent False: boom", Assert.Single(logger.WithEvent(111)).Message, StringComparison.Ordinal);
        var scheduled = Assert.Single(logger.WithEvent(117));
        Assert.Equal(Microsoft.Extensions.Logging.LogLevel.Debug, scheduled.Level);

        session.Insert(4);
        await AdvanceAsync(TimeSpan.FromSeconds(1));
        Assert.Equal([4L], session.Reads[^1].Returned);
        Assert.Empty(session.Reads[^1].Exclude);

        await AdvanceAsync(TimeSpan.FromSeconds(9));
        Assert.Equal([1L], session.Reads[^1].Returned);
        Assert.Equal(TimeSpan.FromSeconds(10), session.Reads[^1].At);
        Assert.Equal(2, session.Get(1)!.RetryCount);
        Assert.Equal(TimeSpan.FromSeconds(20), Assert.IsType<Outcome.Fail>(session.Commits[^1].Outcomes[0]).Delay);
        Assert.Empty(logger.WithEvent(110));
        Assert.Equal([1L, 2L, 3L, 4L, 1L], destination.Sent);
    }

    [Fact]
    public async Task Mixed_outcomes_commit_in_groups_of_at_most_100_in_arrival_order()
    {
        options.Polling.BatchSize = 500;
        session.Insert([.. Enumerable.Range(1, 250).Select(i => (long)i)]);
        destination.Respond = null;

        await StartAsync();
        long[] arrival = [.. Enumerable.Range(1, 250).Select(i => (long)i).Reverse()];
        await WhileCheckBlockedAsync(() =>
        {
            foreach (var id in arrival)
            {
                destination.Complete(id, id % 2 == 0 ? new DeliveryResult.Delivered() : new DeliveryResult.MessageFault("boom", Permanent: false));
            }
        });

        Assert.Equal([100, 100, 50], session.Commits.Select(c => c.Ids.Count));
        var outcomes = session.Commits.SelectMany(c => c.Outcomes).ToList();
        Assert.Equal(arrival, outcomes.Select(o => o.Id));
        Assert.All(outcomes, o => Assert.Equal(o.Id % 2 == 0 ? typeof(Outcome.Remove) : typeof(Outcome.Fail), o.GetType()));
        Assert.Empty(logger.WithEvent(110));
    }

    [Fact]
    public async Task Permanent_fault_sends_dead_letter_with_info_then_removes_the_row()
    {
        withDeadLetter = true;
        session.Insert(FakeOutboxSession.Row(1, retryCount: 2));
        destination.Respond = _ => new DeliveryResult.MessageFault("MsgSizeTooLarge: too big", Permanent: true);

        await StartAsync();

        var sent = Assert.Single(deadLetter.Messages);
        Assert.Equal(new MessageSource(outbox, 1, Destination), sent.Source);
        Assert.Equal(new DeadLetterInfo("MsgSizeTooLarge: too big", 3, origin), sent.DeadLetter);
        Assert.Equal([(byte)1], sent.Key!);
        Assert.Equal([1, 2, 3], sent.Payload);
        Assert.Equal(1, deadLetter.ValidateCalls);
        Assert.Equal([1L], destination.Sent);
        Assert.IsType<Outcome.Remove>(Assert.Single(Assert.Single(session.Commits).Outcomes));
        Assert.Empty(session.TableIds);
        var dead = Assert.Single(logger.WithEvent(118));
        Assert.Contains($"dead-lettered to destination {DeadLetterName} after 3 attempts", dead.Message, StringComparison.Ordinal);
        Assert.Empty(logger.WithEvent(110));
    }

    [Fact]
    public async Task Exhausted_retries_dead_letter_with_attempts_capped_at_max_plus_one()
    {
        withDeadLetter = true;
        session.Insert(FakeOutboxSession.Row(1, retryCount: 3));
        destination.Respond = _ => new DeliveryResult.MessageFault("boom", Permanent: false);

        await StartAsync();

        Assert.Equal(new DeadLetterInfo("boom", 4, origin), Assert.Single(deadLetter.Messages).DeadLetter);
        Assert.Empty(session.TableIds);
    }

    [Fact]
    public async Task Dead_letter_fault_makes_row_dead_letter_pending_without_pausing_and_reread_sends_only_the_dead_letter()
    {
        withDeadLetter = true;
        session.Insert(1);
        destination.Respond = _ => new DeliveryResult.MessageFault("MsgSizeTooLarge: too big", Permanent: true);
        var calls = 0;
        deadLetter.Respond = _ => ++calls == 1 ? new DeliveryResult.DestinationFault("dl down") : new DeliveryResult.Delivered();

        await StartAsync();

        Assert.Equal(
            new Outcome.Fail(1) { ReadRetryCount = 0, RetryCount = 4, Delay = TimeSpan.FromSeconds(10), Error = "MsgSizeTooLarge: too big" },
            Assert.Single(Assert.Single(session.Commits).Outcomes));
        var failed = Assert.Single(logger.WithEvent(119));
        Assert.Contains($"to destination {DeadLetterName} failed at retry count 0: dl down", failed.Message, StringComparison.Ordinal);
        var rescheduled = Assert.Single(logger.WithEvent(122));
        Assert.EndsWith($"to destination {DeadLetterName} scheduled for retry in 00:00:10", rescheduled.Message, StringComparison.Ordinal);
        Assert.Empty(logger.WithEvent(117));
        Assert.Empty(logger.WithEvent(110));

        await AdvanceAsync(TimeSpan.FromSeconds(10));

        Assert.Equal([1L], destination.Sent);
        Assert.Equal([1L, 1L], deadLetter.Sent);
        Assert.Equal(new DeadLetterInfo("MsgSizeTooLarge: too big", 4, origin + TimeSpan.FromSeconds(10)), deadLetter.Messages[^1].DeadLetter);
        Assert.Empty(session.TableIds);
        Assert.Empty(logger.WithEvent(110));
    }

    [Fact]
    public async Task Dead_letter_fault_on_pending_row_backs_off_from_retry_count_past_max()
    {
        withDeadLetter = true;
        session.Insert(FakeOutboxSession.Row(1, retryCount: 5, lastError: "original"));
        deadLetter.Respond = _ => new DeliveryResult.MessageFault("too big for dead letters", Permanent: true);

        await StartAsync();

        Assert.Empty(destination.Sent);
        Assert.Equal(new DeadLetterInfo("original", 4, origin), Assert.Single(deadLetter.Messages).DeadLetter);
        Assert.Equal(
            new Outcome.Fail(1) { ReadRetryCount = 5, RetryCount = 6, Delay = TimeSpan.FromSeconds(30), Error = "original" },
            Assert.Single(Assert.Single(session.Commits).Outcomes));
        Assert.Empty(logger.WithEvent(110));
    }

    [Fact]
    public async Task Dead_letter_pending_row_without_dead_letter_is_dropped_unsent()
    {
        session.Insert(FakeOutboxSession.Row(1, retryCount: 4, lastError: "original"));
        session.Insert(2);

        await StartAsync();

        Assert.Equal([2L], destination.Sent);
        Assert.Empty(session.TableIds);
        Assert.Contains("dropped at retry count 4: original", Assert.Single(logger.WithEvent(120)).Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Dead_letter_validation_failure_is_a_dead_letter_destination_fault_and_is_retried()
    {
        withDeadLetter = true;
        session.Insert(1);
        destination.Respond = _ => new DeliveryResult.MessageFault("MsgSizeTooLarge: too big", Permanent: true);
        deadLetter.ValidateFailure = new InvalidOperationException("secret");

        await StartAsync();

        Assert.Empty(deadLetter.Sent);
        Assert.Equal(4, Assert.IsType<Outcome.Fail>(Assert.Single(Assert.Single(session.Commits).Outcomes)).RetryCount);
        Assert.EndsWith(": InvalidOperationException", Assert.Single(logger.WithEvent(119)).Message, StringComparison.Ordinal);
        Assert.Empty(logger.WithEvent(110));

        await AdvanceAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(2, deadLetter.ValidateCalls);
        Assert.Equal([1L], deadLetter.Sent);
        Assert.Empty(session.TableIds);
    }

    [Fact]
    public async Task Stale_epoch_destination_fault_frees_the_row_without_pause_or_backoff_change()
    {
        options.Retry.MaxDelay = TimeSpan.FromMinutes(5);
        session.Insert(1, 2);
        destination.Respond = null;
        await StartAsync();

        await ActAsync(() => destination.Complete(1, new DeliveryResult.DestinationFault("Local_MsgTimedOut")));
        await AdvanceAsync(TimeSpan.FromSeconds(10));
        Assert.Equal([1L], session.Reads[^1].Returned);

        // Row 2 was sent before the pause: its late fault opens no pause and frees the row at once.
        await ActAsync(() => destination.Complete(2, new DeliveryResult.DestinationFault("Local_MsgTimedOut")));
        Assert.Single(logger.WithEvent(110));
        await AdvanceAsync(TimeSpan.FromSeconds(1));
        Assert.Equal([2L], session.Reads[^1].Returned);
        Assert.Equal([1L], session.Reads[^1].Exclude);

        await ActAsync(() => destination.Complete(1, new DeliveryResult.DestinationFault("Local_MsgTimedOut")));

        Assert.Equal(["00:00:10", "00:00:20"], logger.WithEvent(110).Select(e => e.Message[^8..]));
        Assert.Equal([1L, 2L], session.TableIds);
        Assert.Empty(session.Commits);
    }

    [Fact]
    public async Task Stop_defers_dead_letters_and_commits_retries()
    {
        withDeadLetter = true;
        session.Insert(1, 2);
        destination.Respond = null;
        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(1));

        await ActAsync(stopping.Cancel);
        await ActAsync(() => destination.Complete(1, new DeliveryResult.MessageFault("MsgSizeTooLarge: too big", Permanent: true)));
        await ActAsync(() => destination.Complete(2, new DeliveryResult.MessageFault("boom", Permanent: false)));

        Assert.IsType<DeliveryTermResult.Stopped>(await WaitForTermAsync());
        Assert.Empty(deadLetter.Sent);
        Assert.Equal(0, deadLetter.ValidateCalls);
        Assert.Equal(2L, Assert.IsType<Outcome.Fail>(Assert.Single(session.Commits.SelectMany(c => c.Outcomes))).Id);
        Assert.Equal(0, session.Get(1)!.RetryCount);
        Assert.Equal(1, session.Get(2)!.RetryCount);
        Assert.Contains("deferred 1 dead letters", Assert.Single(logger.WithEvent(121)).Message, StringComparison.Ordinal);
        Assert.Equal(1, destination.DisposeCalls);
        Assert.Equal(1, deadLetter.DisposeCalls);
    }

    [Fact]
    public async Task Stop_drains_in_flight_dead_letter_sends_and_commits_them()
    {
        withDeadLetter = true;
        session.Insert(1);
        destination.Respond = _ => new DeliveryResult.MessageFault("MsgSizeTooLarge: too big", Permanent: true);
        deadLetter.Respond = null;
        await StartAsync();
        Assert.Equal([1L], deadLetter.Pending);

        await ActAsync(stopping.Cancel);
        await AdvanceAsync(TimeSpan.FromSeconds(3));
        Assert.False(Term.IsCompleted);
        await ActAsync(() => deadLetter.Complete(1, new DeliveryResult.Delivered()));

        Assert.IsType<DeliveryTermResult.Stopped>(await WaitForTermAsync());
        Assert.Empty(session.TableIds);
        Assert.Empty(logger.WithEvent(114));
    }

    [Fact]
    public async Task Hung_dispose_of_both_destinations_on_loss_is_abandoned_after_5_seconds()
    {
        withDeadLetter = true;
        session.Insert(1);
        destination.Respond = null;
        destination.DisposeGate = new TaskCompletionSource();
        deadLetter.DisposeGate = new TaskCompletionSource();
        await StartAsync();

        ExpectDisposeBound(time.GetUtcNow() + DeliveryTerm.LossDisposeBound);
        await ActAsync(session.SignalLost);
        await WaitUntilAsync(() => destination.DisposeBlocked && deadLetter.DisposeBlocked);
        await AdvanceAsync(TimeSpan.FromSeconds(4.9));
        Assert.False(Term.IsCompleted);

        await AdvanceAsync(Step);
        Assert.IsType<DeliveryTermResult.Lost>(await WaitForTermAsync());
        Assert.Equal(TimeSpan.FromSeconds(5), Now);
        Assert.Equal(1, destination.DisposeCalls);
        Assert.Equal(1, deadLetter.DisposeCalls);
    }

    [Fact]
    public async Task Hung_dispose_of_both_destinations_on_stop_ends_at_T0_plus_25_seconds()
    {
        withDeadLetter = true;
        session.Insert(1);
        destination.Respond = null;
        destination.DisposeGate = new TaskCompletionSource();
        deadLetter.DisposeGate = new TaskCompletionSource();
        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(1));

        ExpectDisposeBound(time.GetUtcNow() + DeliveryTerm.StopFinalDeadline);
        await ActAsync(stopping.Cancel);
        await AdvanceAsync(TimeSpan.FromSeconds(24.9));
        await WaitUntilAsync(() => destination.DisposeBlocked && deadLetter.DisposeBlocked);
        Assert.False(Term.IsCompleted);

        await AdvanceAsync(Step);
        var stopped = Assert.IsType<DeliveryTermResult.Stopped>(await WaitForTermAsync());
        Assert.Equal(origin + TimeSpan.FromSeconds(1), stopped.StopSignalAt);
        Assert.Equal(TimeSpan.FromSeconds(26), Now);
    }

    [Fact]
    public async Task Fault_and_dead_letter_counters_carry_outbox_and_destination_tags()
    {
        using var recorder = new MeasurementRecorder(outbox);
        withDeadLetter = true;
        session.Insert(FakeOutboxSession.Row(1, malformedHeaders: true));
        session.Insert(2, 3, 4, 5, 6);
        destination.Respond = m => m.Source.Id switch
        {
            2 => new DeliveryResult.MessageFault("boom", Permanent: false),
            3 => new DeliveryResult.DestinationFault("broker down"),
            4 => new DeliveryResult.Delivered(),
            _ => new DeliveryResult.MessageFault("MsgSizeTooLarge: too big", Permanent: true),
        };
        deadLetter.Respond = m => m.Source.Id == 6
            ? new DeliveryResult.MessageFault("too big for dead letters", Permanent: true)
            : new DeliveryResult.Delivered();

        await StartAsync();

        Assert.Equal(
            [
                (DeliveryMetrics.DeadLetteredName, DeadLetterName),
                (DeliveryMetrics.DeadLetteredName, DeadLetterName),
                (DeliveryMetrics.DestinationFaultsName, Destination),
                (DeliveryMetrics.MessageFaultsName, DeadLetterName),
                (DeliveryMetrics.MessageFaultsName, Destination),
                (DeliveryMetrics.MessageFaultsName, Destination),
                (DeliveryMetrics.MessageFaultsName, Destination),
                (DeliveryMetrics.MessageFaultsName, Destination),
            ],
            recorder.Tagged.Order());
        Assert.Equal(1, recorder.Delivered);
        Assert.All(recorder.Units, u => Assert.Equal("{message}", u));
    }

    [Fact]
    public async Task Unexpected_send_exception_becomes_destination_fault_and_pauses()
    {
        session.Insert(1);
        destination.SendFailure = new InvalidOperationException("secret-payload-text");

        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(2));

        var failed = Assert.Single(logger.WithEvent(113));
        Assert.Contains("InvalidOperationException", failed.Message, StringComparison.Ordinal);
        Assert.Contains("DestinationFault: InvalidOperationException", Assert.Single(logger.WithEvent(110)).Message, StringComparison.Ordinal);
        Assert.DoesNotContain(logger.Entries, e => e.Message.Contains("secret-payload-text", StringComparison.Ordinal));
        Assert.Equal([1L], session.TableIds);
        Assert.False(Term.IsCompleted);
    }

    [Fact]
    public async Task Null_send_result_fails_the_row_as_destination_fault_so_it_is_read_again()
    {
        using var recorder = new MeasurementRecorder(outbox);
        session.Insert(1);
        var calls = 0;
        destination.Respond = _ => ++calls == 1 ? null! : new DeliveryResult.Delivered();

        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(10));

        Assert.Contains(DeliveryTerm.UnknownResultError, Assert.Single(logger.WithEvent(113)).Message, StringComparison.Ordinal);
        Assert.Contains($"DestinationFault: {DeliveryTerm.UnknownResultError}", Assert.Single(logger.WithEvent(110)).Message, StringComparison.Ordinal);
        var reread = session.Reads[^1];
        Assert.Equal(TimeSpan.FromSeconds(10), reread.At);
        Assert.Empty(reread.Exclude);
        Assert.Equal([1L, 1L], destination.Sent);
        Assert.Empty(session.TableIds);
        Assert.Equal([(DeliveryMetrics.DestinationFaultsName, Destination)], recorder.Tagged);
    }

    [Fact]
    public async Task Successful_delivery_logs_nothing()
    {
        session.Insert([.. Enumerable.Range(1, 25).Select(i => (long)i)]);

        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(3));

        Assert.Empty(session.TableIds);
        Assert.Empty(logger.Entries);
    }

    [Fact]
    public async Task Delivered_counter_counts_each_delivered_result_with_outbox_tag()
    {
        using var counter = new DeliveredCounter(outbox);
        session.Insert([.. Enumerable.Range(1, 25).Select(i => (long)i)]);
        session.Insert(FakeOutboxSession.Row(26, malformedHeaders: true));

        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(2));

        Assert.Equal(25, counter.Total);
        Assert.Equal("{message}", counter.Unit);
    }

    [Fact]
    public async Task Stop_drains_in_flight_sends_commits_them_and_releases()
    {
        session.Insert(1, 2, 3);
        destination.Respond = null;
        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(2));

        await ActAsync(stopping.Cancel);
        await AdvanceAsync(TimeSpan.FromSeconds(3));
        Assert.False(Term.IsCompleted);
        await ActAsync(() => destination.CompleteAll(new DeliveryResult.Delivered()));

        var stopped = Assert.IsType<DeliveryTermResult.Stopped>(await WaitForTermAsync());
        Assert.Equal(origin + TimeSpan.FromSeconds(2), stopped.StopSignalAt);
        Assert.Equal([1L, 2L, 3L], session.Commits.SelectMany(c => c.Ids).Order());
        Assert.Empty(session.TableIds);
        Assert.DoesNotContain(session.Reads, r => r.At > TimeSpan.FromSeconds(2));
        Assert.Equal(1, destination.DisposeCalls);
        Assert.Empty(logger.WithEvent(114));
        Assert.Empty(logger.WithEvent(115));
    }

    [Fact]
    public async Task Stop_abandons_in_flight_sends_at_T0_plus_20_seconds()
    {
        session.Insert(1, 2);
        destination.Respond = null;
        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(1));

        await ActAsync(stopping.Cancel);
        await AdvanceAsync(TimeSpan.FromSeconds(19.9));
        Assert.False(Term.IsCompleted);

        await AdvanceAsync(Step);
        var stopped = Assert.IsType<DeliveryTermResult.Stopped>(await WaitForTermAsync());
        Assert.Equal(origin + TimeSpan.FromSeconds(1), stopped.StopSignalAt);
        Assert.Contains("abandoned 2 in-flight", Assert.Single(logger.WithEvent(114)).Message, StringComparison.Ordinal);
        Assert.Equal(1, destination.DisposeCalls);
        Assert.Equal([1L, 2L], session.TableIds);
    }

    [Fact]
    public async Task Hung_dispose_after_abandoned_drain_ends_at_T0_plus_25_seconds()
    {
        session.Insert(1);
        destination.Respond = null;
        destination.DisposeGate = new TaskCompletionSource();
        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(1));

        ExpectDisposeBound(time.GetUtcNow() + DeliveryTerm.StopFinalDeadline);
        await ActAsync(stopping.Cancel);
        await AdvanceAsync(TimeSpan.FromSeconds(24.9));
        await WaitUntilAsync(() => destination.DisposeBlocked);
        Assert.False(Term.IsCompleted);

        await AdvanceAsync(Step);
        var stopped = Assert.IsType<DeliveryTermResult.Stopped>(await WaitForTermAsync());
        Assert.Equal(origin + TimeSpan.FromSeconds(1), stopped.StopSignalAt);
        Assert.Equal(TimeSpan.FromSeconds(26), Now);
    }

    [Fact]
    public async Task Hung_final_commit_and_hung_dispose_end_at_absolute_T0_plus_25_when_drain_ends_early()
    {
        session.Insert(1, 2, 3);
        destination.Respond = null;
        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(1));
        var t0 = time.GetUtcNow();
        ExpectDisposeBound(t0 + DeliveryTerm.StopFinalDeadline);
        await ActAsync(stopping.Cancel);
        await AdvanceAsync(TimeSpan.FromSeconds(2));

        await WhileCheckBlockedAsync(() =>
        {
            destination.CompleteAll(new DeliveryResult.Delivered());
            session.CommitGate = new TaskCompletionSource();
            destination.DisposeGate = new TaskCompletionSource();
        });

        // Dispose starts on the thread pool, so the commit block alone does not prove it has begun.
        await WaitUntilAsync(() => destination.DisposeBlocked && time.HasActiveTimerDueAt(t0 + DeliveryTerm.StopFinalDeadline));
        Assert.Equal(TimeSpan.FromSeconds(4), Now);
        Assert.True(session.Blocked);
        await AdvanceAsync(TimeSpan.FromSeconds(21.9));
        Assert.True(destination.DisposeBlocked);
        Assert.False(Term.IsCompleted);

        await AdvanceAsync(Step);
        var stopped = Assert.IsType<DeliveryTermResult.Stopped>(await WaitForTermAsync());
        Assert.Equal(origin + TimeSpan.FromSeconds(1), stopped.StopSignalAt);
        Assert.Equal(TimeSpan.FromSeconds(26), Now);
        Assert.Contains("left 3 delivered", Assert.Single(logger.WithEvent(115)).Message, StringComparison.Ordinal);
        Assert.Empty(logger.WithEvent(114));
    }

    [Fact]
    public async Task Stop_while_paused_does_not_wait_for_the_pause()
    {
        session.Insert(1);
        destination.Respond = _ => new DeliveryResult.DestinationFault("broker down");
        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(1));

        await ActAsync(stopping.Cancel);

        var stopped = Assert.IsType<DeliveryTermResult.Stopped>(await WaitForTermAsync());
        Assert.Equal(origin + TimeSpan.FromSeconds(1), stopped.StopSignalAt);
        Assert.Equal(TimeSpan.FromSeconds(1), Now);
        Assert.Single(session.Reads);
        Assert.Equal(1, destination.DisposeCalls);
    }

    [Fact]
    public async Task Lost_signal_drops_state_disposes_destination_and_ignores_late_results()
    {
        session.Insert(1, 2, 3);
        destination.Respond = null;
        destination.HoldThroughCancellation = true;
        await StartAsync();

        await ActAsync(session.SignalLost);
        var lost = Assert.IsType<DeliveryTermResult.Lost>(await WaitForTermAsync());
        Assert.Equal("session lost signal", lost.Reason);
        Assert.Equal(1, destination.DisposeCalls);

        destination.CompleteAll(new DeliveryResult.Delivered());
        time.Advance(TimeSpan.FromSeconds(5));

        Assert.Empty(session.Commits);
        Assert.Single(session.Reads);
        Assert.Equal([1L, 2L, 3L], session.TableIds);
    }

    [Fact]
    public async Task Delivered_counter_counts_confirmed_sends_whose_results_the_loop_never_handles()
    {
        using var counter = new DeliveredCounter(outbox);
        session.Insert(1, 2, 3);
        destination.Respond = null;
        destination.HoldThroughCancellation = true;
        await StartAsync();

        await ActAsync(session.SignalLost);
        Assert.IsType<DeliveryTermResult.Lost>(await WaitForTermAsync());
        destination.CompleteAll(new DeliveryResult.Delivered());

        Assert.Equal(3, counter.Total);
        Assert.Empty(session.Commits);
    }

    [Fact]
    public async Task Unexpected_read_exception_faults_term_and_disposes_destination()
    {
        session.Insert(1);
        session.NextReadFailure = new InvalidOperationException("bug");

        await StartAsync();

        await Assert.ThrowsAsync<InvalidOperationException>(WaitForTermAsync);
        Assert.Equal(1, destination.DisposeCalls);
    }

    [Fact]
    public async Task Unexpected_check_exception_faults_term_and_disposes_destination()
    {
        session.Insert(1);
        destination.Respond = null;
        session.NextCheckFailure = new InvalidOperationException("bug");

        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(1));

        await Assert.ThrowsAsync<InvalidOperationException>(WaitForTermAsync);
        Assert.Equal(1, destination.DisposeCalls);
    }

    [Fact]
    public async Task Final_commit_starts_no_group_after_T0_plus_25_seconds()
    {
        options.Polling.BatchSize = 500;
        session.Insert([.. Enumerable.Range(1, 250).Select(i => (long)i)]);
        destination.Respond = null;
        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(1));
        var t0 = time.GetUtcNow();
        await ActAsync(stopping.Cancel);
        await AdvanceAsync(TimeSpan.FromSeconds(2));

        var gate = new TaskCompletionSource();
        await WhileCheckBlockedAsync(() =>
        {
            destination.CompleteAll(new DeliveryResult.Delivered());
            session.CommitGate = gate;
        });
        Assert.True(session.Blocked);
        await WaitUntilAsync(() => time.HasActiveTimerDueAt(t0 + DeliveryTerm.StopFinalDeadline));

        await AdvanceAsync(TimeSpan.FromSeconds(22));
        Assert.IsType<DeliveryTermResult.Stopped>(await WaitForTermAsync());
        Assert.Contains("left 250 delivered", Assert.Single(logger.WithEvent(115)).Message, StringComparison.Ordinal);
        var ended = Assert.Single(logger.WithEvent(116));
        Assert.Equal(Microsoft.Extensions.Logging.LogLevel.Debug, ended.Level);
        Assert.Contains("stop commit and dispose", ended.Message, StringComparison.Ordinal);
        Assert.EndsWith("TimeoutException", ended.Message, StringComparison.Ordinal);

        session.CommitGate = null;
        gate.SetResult();
        await WaitUntilAsync(() => !session.Blocked);
        await Task.Delay(200, TestContext.Current.CancellationToken);

        Assert.Single(session.Commits);
    }

    [Fact]
    public async Task Final_commit_starts_no_group_after_session_loss()
    {
        options.Polling.BatchSize = 500;
        session.Insert([.. Enumerable.Range(1, 250).Select(i => (long)i)]);
        destination.Respond = null;
        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(1));
        await ActAsync(stopping.Cancel);
        await AdvanceAsync(TimeSpan.FromSeconds(2));

        var gate = new TaskCompletionSource();
        await WhileCheckBlockedAsync(() =>
        {
            destination.CompleteAll(new DeliveryResult.Delivered());
            session.CommitGate = gate;
        });
        Assert.True(session.Blocked);

        session.SignalLost();
        session.CommitGate = null;
        gate.SetResult();

        Assert.IsType<DeliveryTermResult.Stopped>(await WaitForTermAsync());
        Assert.Single(session.Commits);
        Assert.Contains("left 150 delivered", Assert.Single(logger.WithEvent(115)).Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Stop_abandoned_sends_failing_on_dispose_log_no_send_failure()
    {
        session.Insert(1, 2);
        destination.Respond = null;
        destination.HoldThroughCancellation = true;
        destination.FailPendingOnDispose = new ObjectDisposedException("producer");
        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(1));

        await ActAsync(stopping.Cancel);
        await AdvanceAsync(TimeSpan.FromSeconds(20));

        Assert.IsType<DeliveryTermResult.Stopped>(await WaitForTermAsync());
        Assert.Empty(destination.Pending);
        Assert.Empty(logger.WithEvent(113));
    }

    [Fact]
    public async Task Session_lost_exception_from_read_ends_term_with_its_reason()
    {
        session.Insert(1);
        session.NextReadFailure = new OutboxSessionLostException("session lost: read failed");

        await StartAsync();

        var lost = Assert.IsType<DeliveryTermResult.Lost>(await WaitForTermAsync());
        Assert.Equal("session lost: read failed", lost.Reason);
        Assert.Equal(1, destination.DisposeCalls);
        Assert.Empty(destination.Sent);
    }

    [Fact]
    public async Task Session_lost_exception_from_check_ends_term_with_its_reason()
    {
        session.Insert(1);
        destination.Respond = null;
        session.NextCheckFailure = new OutboxSessionLostException("session lost: check failed");

        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(1));

        var lost = Assert.IsType<DeliveryTermResult.Lost>(await WaitForTermAsync());
        Assert.Equal("session lost: check failed", lost.Reason);
        Assert.Empty(session.Commits);
    }

    [Fact]
    public async Task Hung_dispose_on_loss_is_abandoned_after_5_seconds()
    {
        session.Insert(1);
        destination.Respond = null;
        destination.DisposeGate = new TaskCompletionSource();
        await StartAsync();

        ExpectDisposeBound(time.GetUtcNow() + DeliveryTerm.LossDisposeBound);
        await ActAsync(session.SignalLost);
        await AdvanceAsync(TimeSpan.FromSeconds(4.9));
        Assert.False(Term.IsCompleted);

        await AdvanceAsync(Step);
        Assert.IsType<DeliveryTermResult.Lost>(await WaitForTermAsync());
        Assert.Equal(TimeSpan.FromSeconds(5), Now);
        var ended = Assert.Single(logger.WithEvent(116));
        Assert.Contains("destination dispose", ended.Message, StringComparison.Ordinal);
        Assert.EndsWith("TimeoutException", ended.Message, StringComparison.Ordinal);
    }

    // Kafka parity: None forces a 30 s message.timeout.ms, so its send is tracked at 30 s + 10 s.
    [Fact]
    public async Task Progress_due_tracks_loop_waits_and_earliest_send_deadline()
    {
        session.Insert(1);
        destination.SendBound = TimeSpan.FromSeconds(30);
        destination.Respond = null;
        await StartAsync();

        await AdvanceAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(origin + TimeSpan.FromSeconds(16), status.ProgressDueAt);

        await AdvanceAsync(TimeSpan.FromSeconds(30));
        Assert.Equal(origin + TimeSpan.FromSeconds(40), status.ProgressDueAt);

        await ActAsync(() => destination.Complete(1, new DeliveryResult.Delivered()));
        Assert.Equal(origin + TimeSpan.FromSeconds(46), status.ProgressDueAt);
    }

    [Fact]
    public async Task Bounded_main_send_drives_progress_due_and_unbounded_dead_letter_send_does_not()
    {
        withDeadLetter = true;
        session.Insert(1, 2);
        destination.SendBound = TimeSpan.FromSeconds(3);
        destination.Respond = null;
        deadLetter.Respond = null;
        await StartAsync();
        await ActAsync(() => destination.Complete(1, new DeliveryResult.MessageFault("MsgSizeTooLarge: too big", Permanent: true)));
        Assert.Equal([1L], deadLetter.Pending);

        await AdvanceAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(origin + TimeSpan.FromSeconds(13), status.ProgressDueAt);

        await ActAsync(() => destination.Complete(2, new DeliveryResult.Delivered()));
        Assert.Equal([1L], deadLetter.Pending);
        Assert.True(status.ProgressDueAt > origin + TimeSpan.FromSeconds(13), $"progress due {status.ProgressDueAt - origin}");
    }

    [Fact]
    public async Task Bounded_dead_letter_send_drives_progress_due_and_unbounded_main_send_does_not()
    {
        withDeadLetter = true;
        session.Insert(1, 2);
        destination.Respond = null;
        deadLetter.SendBound = TimeSpan.FromSeconds(3);
        deadLetter.Respond = null;
        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(1));
        await ActAsync(() => destination.Complete(1, new DeliveryResult.MessageFault("MsgSizeTooLarge: too big", Permanent: true)));
        Assert.Equal([1L], deadLetter.Pending);
        Assert.Equal([2L], destination.Pending);

        await AdvanceAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(origin + TimeSpan.FromSeconds(14), status.ProgressDueAt);

        await ActAsync(() => deadLetter.Complete(1, new DeliveryResult.Delivered()));
        Assert.Equal([2L], destination.Pending);
        Assert.True(status.ProgressDueAt > origin + TimeSpan.FromSeconds(14), $"progress due {status.ProgressDueAt - origin}");
    }

    [Fact]
    public async Task PerKey_sends_the_next_row_of_a_key_only_after_the_previous_is_delivered()
    {
        Ordered(OrderingMode.PerKey);
        session.Insert(Keyed(1, 0xA), Keyed(2, 0xA), Keyed(3, 0xB));
        destination.Respond = null;

        await StartAsync();
        Assert.Equal([1L, 3L], destination.Sent);

        await ActAsync(() => destination.Complete(1, new DeliveryResult.Delivered()));

        Assert.Equal([1L, 3L, 2L], destination.Sent);
        Assert.Equal([1L], session.Commits.SelectMany(c => c.Ids));
        Assert.Empty(session.Reads[0].Exclude);
        Assert.Null(session.Reads[0].After);
    }

    [Fact]
    public async Task Ordered_dispatch_waits_out_a_pause_and_unpause_resends_the_parked_head_first()
    {
        Ordered(OrderingMode.PerKey);
        session.Insert(Keyed(1, 0xA), Keyed(2, 0xA), Keyed(3, 0xB), Keyed(4, 0xB));
        destination.Respond = null;
        await StartAsync();
        Assert.Equal([1L, 3L], destination.Sent);

        await ActAsync(() => destination.Complete(1, new DeliveryResult.DestinationFault("broker down")));
        await ActAsync(() => destination.Complete(3, new DeliveryResult.Delivered()));
        await AdvanceAsync(TimeSpan.FromSeconds(9.9));

        Assert.Equal([1L, 3L], destination.Sent);
        Assert.Equal([3L], session.Commits.SelectMany(c => c.Ids));

        await AdvanceAsync(Step);

        Assert.Equal([1L, 3L, 4L, 1L], destination.Sent);
        Assert.Null(session.Reads[^1].After);
        Assert.Equal([1L, 2L, 4L], session.Reads[^1].Returned);
        Assert.Contains(ScopedDelivery.UnparkReason, Assert.Single(logger.WithEvent(124)).Message, StringComparison.Ordinal);

        await ActAsync(() => destination.Complete(1, new DeliveryResult.Delivered()));
        Assert.Equal([1L, 3L, 4L, 1L, 2L], destination.Sent);
    }

    [Fact]
    public async Task Ordered_stale_epoch_fault_unparks_its_scope_and_the_row_is_reread_first()
    {
        options.Retry.MaxDelay = TimeSpan.FromMinutes(5);
        Ordered(OrderingMode.PerKey);
        session.Insert(Keyed(1, 0xA), Keyed(2, 0xB), Keyed(3, 0xB));
        destination.Respond = null;
        await StartAsync();
        Assert.Equal([1L, 2L], destination.Sent);

        await ActAsync(() => destination.Complete(1, new DeliveryResult.DestinationFault("Local_MsgTimedOut")));
        await AdvanceAsync(TimeSpan.FromSeconds(10));
        Assert.Equal([1L, 2L, 1L], destination.Sent);

        await ActAsync(() => destination.Complete(2, new DeliveryResult.DestinationFault("Local_MsgTimedOut")));
        Assert.Single(logger.WithEvent(110));
        Assert.Equal(2, logger.WithEvent(124).Count());

        await AdvanceAsync(TimeSpan.FromSeconds(1));

        Assert.Null(session.Reads[^1].After);
        Assert.Equal([1L, 2L, 1L, 2L], destination.Sent);
        Assert.Single(logger.WithEvent(110));
        Assert.Empty(session.Commits);
    }

    [Fact]
    public async Task Block_after_dead_letter_is_committed_holds_the_key_and_is_logged_once()
    {
        withDeadLetter = true;
        Ordered(OrderingMode.PerKey, OnPermanentFailure.Block);
        session.Insert(Keyed(1, 0xA), Keyed(2, 0xA), Keyed(3, 0xB));
        destination.Respond = m => m.Source.Id == 1
            ? new DeliveryResult.MessageFault("MsgSizeTooLarge: too big", Permanent: true)
            : new DeliveryResult.Delivered();

        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(5));

        Assert.Equal([1L], deadLetter.Sent);
        Assert.Equal([1L, 3L], destination.Sent.Order());
        Assert.Contains(
            new Outcome.Block(1) { ReadRetryCount = 0, RetryCount = 1, Error = "MsgSizeTooLarge: too big" },
            session.Commits.SelectMany(c => c.Outcomes));
        var row = session.Get(1)!;
        Assert.Equal(1, row.RetryCount);
        Assert.Null(row.NextAttemptAt);
        Assert.Equal([1L, 2L], session.TableIds);
        var blocked = Assert.Single(logger.WithEvent(123));
        Assert.Equal(Microsoft.Extensions.Logging.LogLevel.Warning, blocked.Level);
        Assert.Contains($"Message 1 of outbox {outbox} to destination {Destination} blocked at retry count 1: MsgSizeTooLarge: too big", blocked.Message, StringComparison.Ordinal);
        Assert.Single(logger.WithEvent(118));
        Assert.Contains(ScopedDelivery.FailureReason, logger.WithEvent(124).First().Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Summary_change_rewinds_and_rereads_from_head()
    {
        Ordered(OrderingMode.PerKey);
        session.Insert(Keyed(1, 0xA), Keyed(2, 0xB), Keyed(3, 0xC));

        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(1));
        Assert.Empty(session.TableIds);

        session.Insert(Keyed(2, 0xB));
        await AdvanceAsync(TimeSpan.FromSeconds(1));
        Assert.Equal([1L, 2L, 3L], destination.Sent);

        session.Summary = new FailedRowsSummary(1, 42, null);
        await AdvanceAsync(TimeSpan.FromSeconds(1));

        var third = new OutboxPosition(3, 3);
        Assert.Equal([null, third, third, null], session.Reads.Select(r => r.After));
        Assert.Equal([1L, 2L, 3L, 2L], destination.Sent);
        Assert.Equal(4, session.SummaryReads.Count);
        var rewound = Assert.Single(logger.WithEvent(124));
        Assert.Equal(Microsoft.Extensions.Logging.LogLevel.Debug, rewound.Level);
        Assert.Contains(ScopedDelivery.SummaryReason, rewound.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Same_step_safety_and_summary_rewinds_log_each_reason_once()
    {
        Ordered(OrderingMode.PerKey);
        session.Insert(Keyed(1, 0xA), Keyed(2, 0xB));

        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(59.5));
        Assert.Empty(logger.WithEvent(124));

        session.Summary = new FailedRowsSummary(1, 42, null);
        await AdvanceAsync(TimeSpan.FromSeconds(1));

        var rewound = logger.WithEvent(124).ToList();
        Assert.Equal(2, rewound.Count);
        Assert.Contains(ScopedDelivery.SafetyReason, rewound[0].Message, StringComparison.Ordinal);
        Assert.Contains(ScopedDelivery.SummaryReason, rewound[1].Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Queued_rows_are_untouched_on_stop()
    {
        Ordered(OrderingMode.PerKey);
        session.Insert(Keyed(1, 0xA), Keyed(2, 0xA));
        destination.Respond = null;
        await StartAsync();

        await ActAsync(stopping.Cancel);
        await ActAsync(() => destination.Complete(1, new DeliveryResult.Delivered()));

        Assert.IsType<DeliveryTermResult.Stopped>(await WaitForTermAsync());
        Assert.Equal([1L], destination.Sent);
        Assert.Equal([1L], session.Commits.SelectMany(c => c.Ids));
        Assert.Equal([2L], session.TableIds);
        Assert.Equal(0, session.Get(2)!.RetryCount);
    }

    [Fact]
    public async Task Strict_sends_one_at_a_time_and_reads_again_once_its_queue_drains()
    {
        options.Polling.BatchSize = 2;
        Ordered(OrderingMode.Strict);
        session.Insert(Keyed(1, 0xA), Keyed(2, 0xB), Keyed(3, 0xC));
        destination.Respond = null;
        await StartAsync();
        Assert.Equal([1L], destination.Sent);

        await ActAsync(() => destination.Complete(1, new DeliveryResult.Delivered()));
        Assert.Equal([1L, 2L], destination.Sent);
        Assert.Single(session.Reads);

        await ActAsync(() => destination.Complete(2, new DeliveryResult.Delivered()));

        Assert.Equal([1L, 2L, 3L], destination.Sent);
        Assert.Equal([TimeSpan.Zero, TimeSpan.Zero], session.Reads.Select(r => r.At));
        Assert.Equal([null, new OutboxPosition(2, 2)], session.Reads.Select(r => r.After));
    }

    [Fact]
    public async Task Strict_retried_head_holds_later_rows_and_probes_until_it_is_due()
    {
        Ordered(OrderingMode.Strict);
        session.Insert(Keyed(1, 0xA), Keyed(2, 0xB));
        var calls = 0;
        destination.Respond = m => m.Source.Id == 1 && ++calls == 1
            ? new DeliveryResult.MessageFault("boom", Permanent: false)
            : new DeliveryResult.Delivered();

        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(9.9));

        Assert.Equal([1L], destination.Sent);
        Assert.Equal([1L, 2L], session.TableIds);
        Assert.All(session.Reads.Skip(2), r => Assert.Equal(1, r.Max));

        await AdvanceAsync(TimeSpan.FromSeconds(1));

        Assert.Equal([1L, 1L, 2L], destination.Sent);
        Assert.Empty(session.TableIds);
    }

    [Fact]
    public async Task Ordered_progress_due_is_not_bounded_by_an_old_send()
    {
        Ordered(OrderingMode.PerKey);
        session.Insert(Keyed(1, 0xA));
        destination.Respond = null;
        await StartAsync();

        await AdvanceAsync(TimeSpan.FromSeconds(35));

        Assert.Equal([1L], destination.Pending);
        Assert.True(status.ProgressDueAt > origin + TimeSpan.FromSeconds(40), $"progress due {status.ProgressDueAt - origin}");
    }

    [Fact]
    public async Task Ordered_summary_runs_at_most_once_per_interval_and_not_before_immediate_rereads()
    {
        options.Polling.Interval = TimeSpan.FromSeconds(3);
        Ordered(OrderingMode.PerKey);
        session.Insert([.. Enumerable.Range(1, 25).Select(i => Keyed(i, (byte)i))]);

        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(6));

        Assert.Equal(
            [TimeSpan.Zero, TimeSpan.Zero, TimeSpan.Zero, TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(6)],
            session.Reads.Select(r => r.At));
        Assert.Equal([10, 10, 5, 0, 0], session.Reads.Select(r => r.Returned.Count));
        Assert.Equal([TimeSpan.Zero, TimeSpan.FromSeconds(3), TimeSpan.FromSeconds(6)], session.SummaryReads);
    }

    [Fact]
    public async Task PerKey_single_key_backlog_of_ten_batches_reads_at_most_two_steps_per_half_batch()
    {
        Ordered(OrderingMode.PerKey);
        var batch = options.Polling.BatchSize;
        var total = 10 * batch;
        session.Insert([.. Enumerable.Range(1, total).Select(i => Keyed(i, 0xA))]);
        destination.Respond = null;

        await StartAsync();
        for (var steps = 0; session.TableIds.Count > 0; steps++)
        {
            Assert.True(steps < 10 * total, "backlog not drained");
            if (destination.Pending.Count == 0)
            {
                await AdvanceAsync(Step);
            }
            else
            {
                await ActAsync(() => destination.Complete(Assert.Single(destination.Pending), new DeliveryResult.Delivered()));
            }
        }

        Assert.Equal(Enumerable.Range(1, total).Select(i => (long)i), destination.Sent);

        Assert.InRange(session.Reads.Count, 1, (2 * total / (batch / 2)) + 2);
    }

    [Fact]
    public async Task PerKey_blocked_key_tail_is_walked_once_and_later_polls_read_only_new_rows()
    {
        Ordered(OrderingMode.PerKey);
        session.Insert(FakeOutboxSession.Row(1, retryCount: 1) with { Key = [0xA], NextAttemptAt = null });
        session.Insert([.. Enumerable.Range(2, 34).Select(i => Keyed(i, 0xA))]);

        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(3));

        var tailEnd = new OutboxPosition(35, 35);
        Assert.Empty(destination.Sent);
        Assert.Equal([10, 10, 10, 5], session.Reads.Take(4).Select(r => r.Returned.Count));
        Assert.Equal(35, session.Reads.Sum(r => r.Returned.Count));
        Assert.True(session.Reads.Count > 4, $"{session.Reads.Count} reads");
        Assert.All(session.Reads.Skip(4), r => Assert.Equal(tailEnd, r.After));

        session.Insert(Keyed(36, 0xB));
        await AdvanceAsync(TimeSpan.FromSeconds(1));

        Assert.Equal([36L], destination.Sent);
        Assert.Equal(36, session.Reads.Sum(r => r.Returned.Count));
    }

    [Fact]
    public async Task PerKey_retried_head_that_becomes_due_between_summaries_is_delivered_before_a_later_row_of_its_key()
    {
        options.Polling.Interval = TimeSpan.FromSeconds(10);
        Ordered(OrderingMode.PerKey);
        session.Insert(FakeOutboxSession.Row(1, retryCount: 1) with { Key = [0xA], NextAttemptAt = origin + TimeSpan.FromSeconds(5) });

        await StartAsync();
        Assert.Equal([1L], session.Reads[0].Returned);

        await AdvanceAsync(TimeSpan.FromSeconds(6));
        session.Insert(Keyed(2, 0xA));
        await AdvanceAsync(TimeSpan.FromSeconds(5));

        Assert.Equal([2L], session.Reads[^1].Returned);
        Assert.Empty(destination.Sent);

        session.Summary = new FailedRowsSummary(1, 1, null);
        await AdvanceAsync(TimeSpan.FromSeconds(10));

        Assert.Equal([1L, 2L], destination.Sent);
        Assert.Empty(session.TableIds);
    }

    [Theory]
    [InlineData(OrderingMode.PerKey)]
    [InlineData(OrderingMode.Strict)]
    public async Task Ordered_read_passes_no_held_ids_while_rows_are_held(OrderingMode ordered)
    {
        Ordered(ordered);
        session.Insert(Keyed(1, 0xA), Keyed(2, 0xB), Keyed(3, 0xC));
        destination.Respond = null;

        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(3));

        Assert.NotEmpty(destination.Pending);
        Assert.True(recorder!.Held.Count >= 2);
        Assert.All(recorder.Held, Assert.Empty);
    }

    [Fact]
    public async Task None_read_passes_exactly_the_held_ids()
    {
        session.Insert(1, 2, 3);
        destination.Respond = null;

        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(1));

        Assert.Equal(3, destination.Pending.Count);
        Assert.Empty(recorder!.Held[0]);
        Assert.Equal([1L, 2L, 3L], recorder.Held[^1].Order());
    }

    private void Ordered(OrderingMode ordered, OnPermanentFailure onPermanent = OnPermanentFailure.Continue)
    {
        mode = ordered;
        options.Ordering = new OrderingOptions { Mode = ordered, OnPermanentFailure = onPermanent };
    }

    private static OutboxMessage Keyed(long id, byte key) => FakeOutboxSession.Row(id) with { Key = [key] };

    [Fact]
    public async Task Lost_before_first_wait_wakes_at_once()
    {
        session.SignalLost();

        await StartAsync();

        var lost = Assert.IsType<DeliveryTermResult.Lost>(await WaitForTermAsync());
        Assert.Equal(DeliveryTerm.LostSignalReason, lost.Reason);
        Assert.Equal(TimeSpan.Zero, Now);
        Assert.Equal(0, time.Fired);
    }

    [Fact]
    public async Task Stopping_before_first_wait_wakes_at_once()
    {
        await stopping.CancelAsync();

        await StartAsync();

        var stopped = Assert.IsType<DeliveryTermResult.Stopped>(await WaitForTermAsync());
        Assert.Equal(origin, stopped.StopSignalAt);
        Assert.Equal(TimeSpan.Zero, Now);
        Assert.Equal(0, time.Fired);
        Assert.Empty(session.Reads);
    }

    [Fact]
    public async Task Lost_during_wait_wakes_loop()
    {
        await StartAsync();
        await WaitUntilAsync(() => time.Created >= 2);

        session.SignalLost();

        var lost = Assert.IsType<DeliveryTermResult.Lost>(await WaitForTermAsync());
        Assert.Equal(DeliveryTerm.LostSignalReason, lost.Reason);
        Assert.Equal(TimeSpan.Zero, Now);
        Assert.Equal(0, time.Fired);
    }

    [Fact]
    public async Task Stopping_during_wait_wakes_loop()
    {
        await StartAsync();
        await WaitUntilAsync(() => time.Created >= 2);

        await stopping.CancelAsync();

        var stopped = Assert.IsType<DeliveryTermResult.Stopped>(await WaitForTermAsync());
        Assert.Equal(origin, stopped.StopSignalAt);
        Assert.Equal(TimeSpan.Zero, Now);
        Assert.Equal(0, time.Fired);
        Assert.Single(session.Reads);
    }

    [Fact]
    public async Task Stop_drain_does_not_spin()
    {
        session.Insert(1);
        destination.Respond = null;
        await StartAsync();
        await WaitUntilAsync(() => time.Created >= 2);

        await ActAsync(stopping.Cancel);
        var armed = time.Created;
        await Task.Delay(TimeSpan.FromMilliseconds(200), TestContext.Current.CancellationToken);
        Assert.Equal(armed, time.Created);

        await AdvanceAsync(TimeSpan.FromSeconds(5));
        Assert.InRange(time.Created - armed, 1, 5);
        Assert.Equal(5, session.Checks.Count);
        Assert.False(Term.IsCompleted);
    }

    [Fact]
    public async Task Stale_wake_changes_no_state()
    {
        session.Insert(1);
        destination.Respond = null;
        await StartAsync();
        await WaitUntilAsync(() => time.Created >= 2);

        // The result wakes the loop before the armed timer, which stays armed (no disarm after a wake).
        var gate = new TaskCompletionSource();
        session.CommitGate = gate;
        destination.Complete(1, new DeliveryResult.Delivered());
        await WaitUntilAsync(() => session.Blocked);

        var fired = time.Fired;
        time.Advance(TimeSpan.FromSeconds(1));
        Assert.Equal(fired + 1, time.Fired);

        session.CommitGate = null;
        await ActAsync(gate.SetResult);

        Assert.Equal([1L], session.Commits.SelectMany(c => c.Ids));
        Assert.Equal([TimeSpan.FromSeconds(1)], session.Checks);
        Assert.Equal([TimeSpan.Zero, TimeSpan.FromSeconds(1)], session.Reads.Select(r => r.At));
        Assert.Equal([1L], destination.Sent);
        Assert.Empty(session.TableIds);
        Assert.False(Term.IsCompleted);
    }

    [Fact]
    public async Task Timer_fire_after_term_end_is_harmless()
    {
        var clock = new CallbackClock();
        var lateSession = new FakeOutboxSession(clock);
        var sut = new DeliveryTerm(outbox, options, Destination, () => destination, new UnorderedDelivery(options.Polling.BatchSize), status, clock, logger);
        await stopping.CancelAsync();

        Assert.IsType<DeliveryTermResult.Stopped>(await sut.RunAsync(lateSession, stopping.Token));

        var (callback, state) = Assert.Single(clock.Idle);
        callback(state);
        callback(state);
        Assert.Equal(1, destination.DisposeCalls);
        Assert.Empty(lateSession.Reads);
    }

    private async Task StartAsync()
    {
        session.Mode = mode;
        scoped = mode == OrderingMode.None ? null : new ScopedDelivery(mode, options.Polling.BatchSize);
        var sut = new DeliveryTerm(
            outbox,
            options,
            Destination,
            () =>
            {
                destinationsCreated++;
                return destination;
            },
            recorder = new HeldRecorder(scoped ?? (IDeliveryOrdering)new UnorderedDelivery(options.Polling.BatchSize)),
            status,
            time,
            logger,
            withDeadLetter ? new DeadLetterTarget(DeadLetterName, () => deadLetter) : null);
        term = sut.RunAsync(session, stopping.Token);
        await WaitUntilAsync(() => time.Active > 0 || Settled());
    }

    private async Task AdvanceAsync(TimeSpan total)
    {
        var end = Now + total;
        while (Now < end)
        {
            var created = time.Created;
            var fired = time.Fired;
            time.Advance(Step);
            if (time.Fired != fired)
            {
                await WaitUntilAsync(() => (time.Created > created && time.Active > 0) || Settled());
            }
        }
    }

    private async Task ActAsync(Action action)
    {
        var created = time.Created;
        action();
        await WaitUntilAsync(() => (time.Created > created && time.Active > 0) || Settled());
    }

    private async Task WhileCheckBlockedAsync(Action action)
    {
        var gate = new TaskCompletionSource();
        session.CheckGate = gate;
        for (var steps = 0; !session.Blocked; steps++)
        {
            Assert.True(steps < 20, "check not reached within 2 s");
            await AdvanceAsync(Step);
        }

        session.CheckGate = null;
        action();
        await ActAsync(gate.SetResult);
    }

    // Declares the bound timer a stop or loss path registers after its dispose starts.
    private void ExpectDisposeBound(DateTimeOffset dueAt) => disposeBound = dueAt;

    private bool Settled() =>
        Term.IsCompleted
        || session.Blocked
        || ((destination.DisposeBlocked || deadLetter.DisposeBlocked) && disposeBound is { } due && time.HasActiveTimerDueAt(due));

    private async Task<DeliveryTermResult> WaitForTermAsync() =>
        await Term.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

    private async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, $"condition not met within 5 s at {Now}");
            await Task.Delay(5, TestContext.Current.CancellationToken);
        }
    }

    // Delegates to the real ordering and snapshots the held ids each NextRead receives (the term reuses its list).
    private sealed class HeldRecorder(IDeliveryOrdering inner) : IDeliveryOrdering
    {
        public List<long[]> Held { get; } = [];

        public int Capacity => inner.Capacity;

        public bool UsesSummary => inner.UsesSummary;

        public bool ExcludesHeld => inner.ExcludesHeld;

        public bool Halted => inner.Halted;

        public bool AwaitingMoveBack => inner.AwaitingMoveBack;

        public void Tick(DateTimeOffset now) => inner.Tick(now);

        public ReadRequest? NextRead(int room, IReadOnlyCollection<long> held)
        {
            Held.Add([.. held]);
            return inner.NextRead(room, held);
        }

        public void Accept(IReadOnlyList<OutboxMessage> rows, Func<long, bool> isHeld, ICollection<OutboxMessage> queued) =>
            inner.Accept(rows, isHeld, queued);

        public void Sendable(ICollection<OutboxMessage> output) => inner.Sendable(output);

        public void Release(long id) => inner.Release(id);

        public void Fail(long id, ICollection<long> evicted) => inner.Fail(id, evicted);

        public void Unpark(IReadOnlyCollection<long> ids, ICollection<long> evicted) => inner.Unpark(ids, evicted);

        public void Left(long id) => inner.Left(id);

        public void Summary(FailedRowsSummary summary) => inner.Summary(summary);

        public void DrainRewinds(ICollection<string> reasons) => inner.DrainRewinds(reasons);
    }

    private sealed class DeliveredCounter : IDisposable
    {
        private readonly MeterListener listener = new();
        private readonly string outbox;
        private long total;

        public DeliveredCounter(string outbox)
        {
            this.outbox = outbox;
            listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter == RelayDiagnostics.Meter && instrument.Name == DeliveryMetrics.DeliveredName)
                {
                    Unit = instrument.Unit;
                    l.EnableMeasurementEvents(instrument);
                }
            };
            listener.SetMeasurementEventCallback<long>(OnMeasurement);
            listener.Start();
        }

        public long Total => Interlocked.Read(ref total);

        public string? Unit { get; private set; }

        public void Dispose() => listener.Dispose();

        private void OnMeasurement(Instrument instrument, long value, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
        {
            foreach (var tag in tags)
            {
                if (tag.Key == "outbox" && Equals(tag.Value, outbox))
                {
                    Interlocked.Add(ref total, value);
                }
            }
        }
    }

    private sealed class MeasurementRecorder : IDisposable
    {
        private readonly MeterListener listener = new();
        private readonly string outbox;
        private readonly System.Collections.Concurrent.ConcurrentQueue<(string Name, string Destination)> tagged = new();
        private readonly System.Collections.Concurrent.ConcurrentQueue<string?> units = new();
        private long delivered;

        public MeasurementRecorder(string outbox)
        {
            this.outbox = outbox;
            string[] names = [DeliveryMetrics.DeliveredName, DeliveryMetrics.MessageFaultsName, DeliveryMetrics.DestinationFaultsName, DeliveryMetrics.DeadLetteredName];
            listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter == RelayDiagnostics.Meter && names.Contains(instrument.Name))
                {
                    units.Enqueue(instrument.Unit);
                    l.EnableMeasurementEvents(instrument);
                }
            };
            listener.SetMeasurementEventCallback<long>(OnMeasurement);
            listener.Start();
        }

        public IReadOnlyList<(string Name, string Destination)> Tagged => [.. tagged];

        public IReadOnlyList<string?> Units => [.. units];

        public long Delivered => Interlocked.Read(ref delivered);

        public void Dispose() => listener.Dispose();

        private void OnMeasurement(Instrument instrument, long value, ReadOnlySpan<KeyValuePair<string, object?>> tags, object? state)
        {
            string? tagOutbox = null;
            string? tagDestination = null;
            foreach (var tag in tags)
            {
                if (tag.Key == "outbox")
                {
                    tagOutbox = tag.Value as string;
                }
                else if (tag.Key == "destination")
                {
                    tagDestination = tag.Value as string;
                }
            }

            if (tagOutbox != outbox)
            {
                return;
            }

            if (instrument.Name == DeliveryMetrics.DeliveredName)
            {
                Interlocked.Add(ref delivered, value);
                return;
            }

            for (var i = 0; i < value; i++)
            {
                tagged.Enqueue((instrument.Name, tagDestination!));
            }
        }
    }

    // Counts timers so a test advances only once the term is idle on its next wait.
    // Due instants let a test wait for one specific timer.
    private sealed class TermClock : FakeTimeProvider
    {
        private readonly ConcurrentDictionary<TrackedTimer, DateTimeOffset> due = new();
        private int created;
        private int active;
        private int fired;

        public int Created => Volatile.Read(ref created);

        public int Active => Volatile.Read(ref active);

        public int Fired => Volatile.Read(ref fired);

        public bool HasActiveTimerDueAt(DateTimeOffset at)
        {
            foreach (var entry in due)
            {
                if (entry.Value == at)
                {
                    return true;
                }
            }

            return false;
        }

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            var tracked = new TrackedTimer(this);
            tracked.Inner = base.CreateTimer(
                s =>
                {
                    Interlocked.Increment(ref fired);
                    tracked.End();
                    callback(s);
                },
                state,
                dueTime,
                period);
            tracked.Track(dueTime);

            // Count after registration so a test never advances time before this timer is scheduled.
            Interlocked.Increment(ref active);
            Interlocked.Increment(ref created);
            return tracked;
        }

        private sealed class TrackedTimer(TermClock clock) : ITimer
        {
            private int ended;

            public ITimer? Inner { get; set; }

            public void Track(TimeSpan dueTime)
            {
                if (dueTime == Timeout.InfiniteTimeSpan)
                {
                    clock.due.TryRemove(this, out _);
                }
                else
                {
                    clock.due[this] = clock.GetUtcNow() + dueTime;
                }

                // A timer that ended meanwhile keeps no due instant.
                if (Volatile.Read(ref ended) == 1)
                {
                    clock.due.TryRemove(this, out _);
                }
            }

            public void End()
            {
                if (Interlocked.Exchange(ref ended, 1) == 0)
                {
                    clock.due.TryRemove(this, out _);
                    Interlocked.Decrement(ref clock.active);
                }
            }

            // A finite re-arm counts as a new timer, so a test sees the term idle on its next wait.
            public bool Change(TimeSpan dueTime, TimeSpan period)
            {
                var finite = dueTime != Timeout.InfiniteTimeSpan;
                if (finite && Interlocked.CompareExchange(ref ended, 0, 1) == 1)
                {
                    Interlocked.Increment(ref clock.active);
                }

                var changed = Inner!.Change(dueTime, period);
                Track(dueTime);
                if (finite)
                {
                    Interlocked.Increment(ref clock.created);
                }

                return changed;
            }

            public void Dispose()
            {
                End();
                Inner?.Dispose();
            }

            public ValueTask DisposeAsync()
            {
                Dispose();
                return ValueTask.CompletedTask;
            }
        }
    }

    // Keeps the callback of each timer created disarmed, so a test can fire it after the term ended.
    private sealed class CallbackClock : FakeTimeProvider
    {
        private readonly ConcurrentQueue<(TimerCallback Callback, object? State)> idle = new();

        public IReadOnlyList<(TimerCallback Callback, object? State)> Idle => [.. idle];

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
        {
            if (dueTime == Timeout.InfiniteTimeSpan)
            {
                idle.Enqueue((callback, state));
            }

            return base.CreateTimer(callback, state, dueTime, period);
        }
    }
}
