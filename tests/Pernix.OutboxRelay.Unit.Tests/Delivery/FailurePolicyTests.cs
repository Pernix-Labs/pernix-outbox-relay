using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Core.Delivery;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Delivery;

public sealed class FailurePolicyTests
{
    private static readonly DateTimeOffset Now = new(2026, 11, 3, 12, 0, 0, TimeSpan.Zero);

    private static RetryOptions Retry(int max = 3) => new()
    {
        MaxRetryAttempts = max,
        InitialDelay = TimeSpan.FromSeconds(10),
        BackoffMultiplier = 2.0,
        MaxDelay = TimeSpan.FromMinutes(5),
    };

    private static FailurePolicy Policy(int max = 3, bool deadLetter = true, OnPermanentFailure onPermanent = OnPermanentFailure.Continue) =>
        new(Retry(max), deadLetter, onPermanent);

    private static FailurePolicy Blocking(int max = 3, bool deadLetter = false) => Policy(max, deadLetter, OnPermanentFailure.Block);

    private static OutboxMessage Row(int retryCount, string? lastError = null) => new()
    {
        Id = 42,
        Position = new OutboxPosition(42, 42),
        Payload = [1],
        Headers = MessageHeaders.Empty,
        CreatedAt = Now,
        RetryCount = retryCount,
        LastError = lastError,
        IsDue = true,
    };

    private static DeliveryResult.MessageFault Transient(string error = "boom") => new(error, Permanent: false);

    private static DeliveryResult.MessageFault Permanent(string error = "too large") => new(error, Permanent: true);

    private static Outcome.Fail AssertRetry(FailureDecision decision) =>
        Assert.IsType<FailureDecision.Retry>(decision).Outcome;

    private static DeadLetterInfo AssertDeadLetter(FailureDecision decision) =>
        Assert.IsType<FailureDecision.DeadLetter>(decision).Info;

    private static FailureDecision.Block AssertBlock(FailureDecision decision) =>
        Assert.IsType<FailureDecision.Block>(decision);

    [Theory]
    [InlineData(0, false)]
    [InlineData(3, false)]
    [InlineData(4, true)]
    public void Dead_letter_pending_when_retry_count_exceeds_max(int retryCount, bool pending)
    {
        Assert.Equal(pending, Policy().IsDeadLetterPending(Row(retryCount)));
    }

    [Fact]
    public void Transient_with_retries_left_schedules_retry()
    {
        var fail = AssertRetry(Policy().OnMessageFault(Row(1), Transient(), Now));

        Assert.Equal(42, fail.Id);
        Assert.Equal(1, fail.ReadRetryCount);
        Assert.Equal(2, fail.RetryCount);
        Assert.Equal(TimeSpan.FromSeconds(20), fail.Delay);
        Assert.Equal("boom", fail.Error);
    }

    [Fact]
    public void Transient_at_max_is_exhausted_and_dead_lettered()
    {
        var info = AssertDeadLetter(Policy().OnMessageFault(Row(3), Transient(), Now));

        Assert.Equal(new DeadLetterInfo("boom", 4, Now), info);
    }

    [Fact]
    public void Transient_at_max_without_dead_letter_is_dropped()
    {
        var drop = Assert.IsType<FailureDecision.Drop>(Policy(deadLetter: false).OnMessageFault(Row(3), Transient(), Now));

        Assert.Equal(new Outcome.Remove(42), drop.Outcome);
    }

    [Fact]
    public void Max_zero_makes_first_failure_final()
    {
        var info = AssertDeadLetter(Policy(max: 0).OnMessageFault(Row(0), Transient(), Now));

        Assert.Equal(1, info.Attempts);
    }

    [Fact]
    public void Max_zero_without_dead_letter_drops_on_first_failure()
    {
        Assert.IsType<FailureDecision.Drop>(Policy(max: 0, deadLetter: false).OnMessageFault(Row(0), Transient(), Now));
    }

    [Fact]
    public void Permanent_at_read_zero_skips_retries()
    {
        var info = AssertDeadLetter(Policy().OnMessageFault(Row(0), Permanent(), Now));

        Assert.Equal(new DeadLetterInfo("too large", 1, Now), info);
    }

    [Fact]
    public void Permanent_at_read_above_zero_skips_remaining_retries()
    {
        var info = AssertDeadLetter(Policy().OnMessageFault(Row(2), Permanent(), Now));

        Assert.Equal(new DeadLetterInfo("too large", 3, Now), info);
    }

    [Fact]
    public void Permanent_without_dead_letter_is_dropped()
    {
        var drop = Assert.IsType<FailureDecision.Drop>(Policy(deadLetter: false).OnMessageFault(Row(0), Permanent(), Now));

        Assert.Equal(42, drop.Outcome.Id);
        Assert.Equal("too large", drop.Reason);
    }

    [Fact]
    public void Pending_read_with_dead_letter_uses_last_error_and_capped_attempts()
    {
        var info = AssertDeadLetter(Policy().OnDeadLetterPendingRead(Row(6, lastError: "original"), Now));

        Assert.Equal(new DeadLetterInfo("original", 4, Now), info);
    }

    [Fact]
    public void Pending_read_without_last_error_uses_default_reason()
    {
        var info = AssertDeadLetter(Policy().OnDeadLetterPendingRead(Row(4), Now));

        Assert.Equal("dead letter pending", info.Reason);
    }

    [Fact]
    public void Pending_read_without_dead_letter_is_dropped()
    {
        var drop = Assert.IsType<FailureDecision.Drop>(Policy(deadLetter: false).OnDeadLetterPendingRead(Row(4, "original"), Now));

        Assert.Equal(new FailureDecision.Drop(new Outcome.Remove(42), "original"), drop);
    }

    [Fact]
    public void Dead_letter_failed_from_read_below_max_jumps_past_max()
    {
        var fail = Policy().OnDeadLetterFailed(Row(1), "too large").Outcome;

        Assert.Equal(1, fail.ReadRetryCount);
        Assert.Equal(4, fail.RetryCount);
        Assert.Equal(TimeSpan.FromSeconds(10), fail.Delay);
        Assert.Equal("too large", fail.Error);
    }

    [Fact]
    public void Dead_letter_failed_from_read_at_max_moves_past_max()
    {
        var fail = Policy().OnDeadLetterFailed(Row(3), "boom").Outcome;

        Assert.Equal(4, fail.RetryCount);
        Assert.Equal(TimeSpan.FromSeconds(10), fail.Delay);
    }

    [Fact]
    public void Dead_letter_failed_from_read_above_max_grows_backoff()
    {
        var fail = Policy().OnDeadLetterFailed(Row(5), "boom").Outcome;

        Assert.Equal(5, fail.ReadRetryCount);
        Assert.Equal(6, fail.RetryCount);
        Assert.Equal(TimeSpan.FromSeconds(40), fail.Delay);
    }

    [Fact]
    public void Dead_letter_failed_backoff_saturates_at_max_delay()
    {
        var fail = Policy().OnDeadLetterFailed(Row(100), "boom").Outcome;

        Assert.Equal(TimeSpan.FromMinutes(5), fail.Delay);
    }

    [Fact]
    public void Attempts_cap_at_max_plus_one()
    {
        var info = AssertDeadLetter(Policy().OnMessageFault(Row(10), Permanent(), Now));

        Assert.Equal(4, info.Attempts);
    }

    [Fact]
    public void Max_int_never_overflows()
    {
        var policy = Policy(max: int.MaxValue);

        var retry = AssertRetry(policy.OnMessageFault(Row(int.MaxValue - 1), Transient(), Now));
        Assert.Equal(int.MaxValue, retry.RetryCount);
        Assert.Equal(TimeSpan.FromMinutes(5), retry.Delay);

        var info = AssertDeadLetter(policy.OnMessageFault(Row(int.MaxValue), Permanent(), Now));
        Assert.Equal(int.MaxValue, info.Attempts);

        var fail = policy.OnDeadLetterFailed(Row(int.MaxValue), "boom").Outcome;
        Assert.Equal(int.MaxValue, fail.RetryCount);
        Assert.True(fail.Delay >= TimeSpan.Zero);
        Assert.False(policy.IsDeadLetterPending(Row(int.MaxValue)));
    }

    [Fact]
    public void Read_count_at_int_max_saturates_with_small_max()
    {
        var policy = Policy();

        Assert.True(policy.IsDeadLetterPending(Row(int.MaxValue)));
        Assert.Equal(4, AssertDeadLetter(policy.OnDeadLetterPendingRead(Row(int.MaxValue), Now)).Attempts);
        Assert.Equal(int.MaxValue, policy.OnDeadLetterFailed(Row(int.MaxValue), "boom").Outcome.RetryCount);
    }

    // Worked example, row by row (MaxRetryAttempts 3, 10/20/40 s, MaxDelay 5 min).
    [Fact]
    public void Worked_example()
    {
        var policy = Policy();

        var a1 = AssertRetry(policy.OnMessageFault(Row(0), Transient("e1"), Now));
        Assert.Equal((1, TimeSpan.FromSeconds(10)), (a1.RetryCount, a1.Delay));

        var a2 = AssertRetry(policy.OnMessageFault(Row(a1.RetryCount, a1.Error), Transient("e2"), Now));
        Assert.Equal((2, TimeSpan.FromSeconds(20)), (a2.RetryCount, a2.Delay));

        var a3 = AssertRetry(policy.OnMessageFault(Row(a2.RetryCount, a2.Error), Transient("e3"), Now));
        Assert.Equal((3, TimeSpan.FromSeconds(40)), (a3.RetryCount, a3.Delay));

        var row3 = Row(a3.RetryCount, a3.Error);
        var a4 = AssertDeadLetter(policy.OnMessageFault(row3, Transient("e4"), Now));
        Assert.Equal(4, a4.Attempts);

        var dl1 = policy.OnDeadLetterFailed(row3, a4.Reason).Outcome;
        Assert.Equal((4, TimeSpan.FromSeconds(10), "e4"), (dl1.RetryCount, dl1.Delay, dl1.Error));

        var row4 = Row(dl1.RetryCount, dl1.Error);
        Assert.True(policy.IsDeadLetterPending(row4));
        var reread = AssertDeadLetter(policy.OnDeadLetterPendingRead(row4, Now));
        Assert.Equal(new DeadLetterInfo("e4", 4, Now), reread);

        var dl2 = policy.OnDeadLetterFailed(row4, reread.Reason).Outcome;
        Assert.Equal((5, TimeSpan.FromSeconds(20), "e4"), (dl2.RetryCount, dl2.Delay, dl2.Error));

        // Last row (dead letter Delivered → row deleted) is the term's job: re-read still decides DeadLetter.
        Assert.IsType<FailureDecision.DeadLetter>(policy.OnDeadLetterPendingRead(Row(dl2.RetryCount, dl2.Error), Now));
    }

    [Fact]
    public void Block_permanent_without_dead_letter_blocks()
    {
        var block = AssertBlock(Blocking().OnMessageFault(Row(0), Permanent(), Now));

        Assert.Equal(new FailureDecision.Block(new Outcome.Block(42) { ReadRetryCount = 0, RetryCount = 1, Error = "too large" }, "too large"), block);
    }

    [Fact]
    public void Block_exhausted_without_dead_letter_blocks()
    {
        var block = AssertBlock(Blocking().OnMessageFault(Row(3), Transient(), Now));

        Assert.Equal(new Outcome.Block(42) { ReadRetryCount = 3, RetryCount = 4, Error = "boom" }, block.Outcome);
        Assert.Equal("boom", block.Reason);
    }

    [Fact]
    public void Block_transient_with_retries_left_still_retries()
    {
        Assert.Equal(2, AssertRetry(Blocking().OnMessageFault(Row(1), Transient(), Now)).RetryCount);
    }

    [Fact]
    public void Block_pending_read_without_dead_letter_blocks()
    {
        var block = AssertBlock(Blocking().OnDeadLetterPendingRead(Row(5, "original"), Now));

        Assert.Equal(new Outcome.Block(42) { ReadRetryCount = 5, RetryCount = 6, Error = "original" }, block.Outcome);
    }

    [Fact]
    public void Block_pending_read_without_last_error_uses_default_reason()
    {
        Assert.Equal("dead letter pending", AssertBlock(Blocking().OnDeadLetterPendingRead(Row(4), Now)).Outcome.Error);
    }

    [Fact]
    public void Block_with_dead_letter_dead_letters_then_blocks()
    {
        var policy = Blocking(deadLetter: true);
        var row = Row(0);

        var info = AssertDeadLetter(policy.OnMessageFault(row, Permanent(), Now));
        Assert.Equal(new DeadLetterInfo("too large", 1, Now), info);

        var block = AssertBlock(policy.AfterDeadLetter(row, info.Reason));
        Assert.Equal(new Outcome.Block(42) { ReadRetryCount = 0, RetryCount = 1, Error = "too large" }, block.Outcome);
    }

    [Fact]
    public void Block_with_dead_letter_pending_read_dead_letters_then_blocks()
    {
        var policy = Blocking(deadLetter: true);
        var row = Row(4, "original");

        var info = AssertDeadLetter(policy.OnDeadLetterPendingRead(row, Now));
        Assert.Equal(new DeadLetterInfo("original", 4, Now), info);

        var block = AssertBlock(policy.AfterDeadLetter(row, info.Reason));
        Assert.Equal(new Outcome.Block(42) { ReadRetryCount = 4, RetryCount = 5, Error = "original" }, block.Outcome);
    }

    [Fact]
    public void Block_dead_letter_failed_is_unchanged()
    {
        var fail = Blocking(deadLetter: true).OnDeadLetterFailed(Row(3), "boom").Outcome;

        Assert.Equal((4, TimeSpan.FromSeconds(10), "boom"), (fail.RetryCount, fail.Delay, fail.Error));
    }

    [Fact]
    public void Continue_after_dead_letter_drops()
    {
        var drop = Assert.IsType<FailureDecision.Drop>(Policy().AfterDeadLetter(Row(4, "original"), "original"));

        Assert.Equal(new FailureDecision.Drop(new Outcome.Remove(42), "original"), drop);
    }

    [Fact]
    public void Block_retry_count_saturates_at_int_max()
    {
        Assert.Equal(int.MaxValue, AssertBlock(Blocking().OnDeadLetterPendingRead(Row(int.MaxValue), Now)).Outcome.RetryCount);
        Assert.Equal(int.MaxValue, AssertBlock(Blocking(max: int.MaxValue).OnMessageFault(Row(int.MaxValue), Permanent(), Now)).Outcome.RetryCount);
        Assert.Equal(int.MaxValue, AssertBlock(Blocking(deadLetter: true).AfterDeadLetter(Row(int.MaxValue), "boom")).Outcome.RetryCount);
    }

    private static OutboxMessage Routed(int retryCount, string? route, string? lastError = null) =>
        Row(retryCount, lastError) with { Route = route };

    private static string HeldError(string? route) => FailurePolicy.HoldPrefix + FailurePolicy.HoldRoute(route);

    [Theory]
    [InlineData(0, null, 1, "route not mapped: (none)")]
    [InlineData(2, null, 2, "route not mapped: (none)")]
    [InlineData(0, "orders", 1, "route not mapped: orders")]
    [InlineData(2, "orders", 2, "route not mapped: orders")]
    public void Hold_is_a_retry_free_failure(int retryCount, string? route, int written, string error)
    {
        var row = Routed(retryCount, route);

        var fail = Policy().Hold(row, FailurePolicy.HoldRoute(route));

        Assert.Equal(42, fail.Id);
        Assert.Equal(retryCount, fail.ReadRetryCount);
        Assert.Equal(written, fail.RetryCount);
        Assert.Equal(TimeSpan.FromMinutes(5), fail.Delay);
        Assert.Equal(error, fail.Error);
    }

    [Fact]
    public void Hold_route_of_null_is_none()
    {
        Assert.Same(FailurePolicy.NoRoute, FailurePolicy.HoldRoute(null));
    }

    [Fact]
    public void Hold_route_of_63_chars_is_the_same_instance()
    {
        var route = new string('a', 63);

        var heldRoute = FailurePolicy.HoldRoute(route);

        Assert.Same(route, heldRoute);
        Assert.Equal(FailurePolicy.HoldPrefix + route, Policy().Hold(Routed(0, route), heldRoute).Error);
    }

    [Fact]
    public void Hold_route_of_64_chars_is_capped()
    {
        var route = new string('a', 63) + "b";

        var heldRoute = FailurePolicy.HoldRoute(route);
        var fail = Policy().Hold(Routed(0, route), heldRoute);

        Assert.Equal(new string('a', 63) + FailurePolicy.HoldRouteTruncated, heldRoute);
        Assert.Equal(FailurePolicy.HoldPrefix + new string('a', 63) + FailurePolicy.HoldRouteTruncated, fail.Error);
        Assert.False(Policy().IsDeadLetterPending(Routed(4, route, fail.Error)));
    }

    [Fact]
    public void Hold_route_cuts_before_a_high_surrogate_at_char_63()
    {
        var route = new string('a', 62) + "\U0001F600" + "tail";
        Assert.True(char.IsHighSurrogate(route[62]));

        Assert.Equal(new string('a', 62) + FailurePolicy.HoldRouteTruncated, FailurePolicy.HoldRoute(route));
    }

    [Theory]
    [InlineData(3, 4)]
    [InlineData(3, 100)]
    [InlineData(0, 1)]
    [InlineData(0, int.MaxValue)]
    public void Held_row_is_never_dead_letter_pending(int max, int retryCount)
    {
        var policy = Policy(max);

        Assert.True(policy.IsDeadLetterPending(Row(retryCount, "boom")));
        Assert.False(policy.IsDeadLetterPending(Row(retryCount, HeldError(null))));
        Assert.False(policy.IsDeadLetterPending(Row(retryCount, HeldError("orders"))));
        Assert.False(policy.IsDeadLetterPending(Row(retryCount, HeldError(new string('x', 80)))));
    }

    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    [InlineData(7)]
    public void Released_row_fault_counts_from_zero(int retryCount)
    {
        var fail = AssertRetry(Policy().OnMessageFault(Row(retryCount, HeldError("orders")), Transient(), Now));

        Assert.Equal(retryCount, fail.ReadRetryCount);
        Assert.Equal(1, fail.RetryCount);
        Assert.Equal(TimeSpan.FromSeconds(10), fail.Delay);
        Assert.Equal("boom", fail.Error);
    }

    [Fact]
    public void Released_row_dead_letter_attempts_count_from_zero()
    {
        var info = AssertDeadLetter(Policy().OnMessageFault(Row(2, HeldError("orders")), Permanent(), Now));

        Assert.Equal(new DeadLetterInfo("too large", 1, Now), info);
    }

    [Fact]
    public void Released_row_block_count_from_zero()
    {
        var row = Row(2, HeldError(null));

        var block = AssertBlock(Blocking().OnMessageFault(row, Permanent(), Now));
        var after = AssertBlock(Blocking(deadLetter: true).AfterDeadLetter(row, "too large"));

        Assert.Equal(new Outcome.Block(42) { ReadRetryCount = 2, RetryCount = 1, Error = "too large" }, block.Outcome);
        Assert.Equal(new Outcome.Block(42) { ReadRetryCount = 2, RetryCount = 1, Error = "too large" }, after.Outcome);
    }

    [Fact]
    public void Released_row_dead_letter_failed_counts_from_zero()
    {
        var fail = Policy().OnDeadLetterFailed(Row(7, HeldError("orders")), "boom").Outcome;

        Assert.Equal(7, fail.ReadRetryCount);
        Assert.Equal(4, fail.RetryCount);
        Assert.Equal(TimeSpan.FromSeconds(10), fail.Delay);
    }

    [Fact]
    public void Fault_text_starting_with_hold_prefix_is_escaped_and_counts_retries()
    {
        var policy = Policy();
        var text = FailurePolicy.HoldPrefix + "orders";
        var escaped = FailurePolicy.FaultEscape + text;

        var a1 = AssertRetry(policy.OnMessageFault(Row(0), Transient(text), Now));
        var a2 = AssertRetry(policy.OnMessageFault(Row(a1.RetryCount, a1.Error), Transient(text), Now));
        var a3 = AssertRetry(policy.OnMessageFault(Row(a2.RetryCount, a2.Error), Transient(text), Now));

        Assert.Equal([1, 2, 3], [a1.RetryCount, a2.RetryCount, a3.RetryCount]);
        Assert.All([a1.Error, a2.Error, a3.Error], e => Assert.Equal(escaped, e));

        var row3 = Row(a3.RetryCount, a3.Error);
        var info = AssertDeadLetter(policy.OnMessageFault(row3, Transient(text), Now));
        Assert.Equal(new DeadLetterInfo(text, 4, Now), info);

        var failed = policy.OnDeadLetterFailed(row3, info.Reason).Outcome;
        Assert.Equal(4, failed.RetryCount);
        Assert.Equal(escaped, failed.Error);
        Assert.True(policy.IsDeadLetterPending(Row(failed.RetryCount, failed.Error)));
    }

    [Fact]
    public void Dead_letter_resent_after_escaped_fault_text_carries_original_reason()
    {
        var policy = Policy();
        var text = FailurePolicy.HoldPrefix + "orders";

        var first = AssertDeadLetter(policy.OnMessageFault(Row(3), Permanent(text), Now));
        var failed = policy.OnDeadLetterFailed(Row(3), first.Reason).Outcome;
        var second = AssertDeadLetter(policy.OnDeadLetterPendingRead(Row(failed.RetryCount, failed.Error), Now));

        Assert.Equal(text, first.Reason);
        Assert.Equal(text, second.Reason);
    }

    [Theory]
    [InlineData("fault: route not mapped: x")]
    [InlineData("fault: other")]
    [InlineData("route not mapped: orders")]
    public void Escaped_fault_text_round_trips_to_the_original_reason(string text)
    {
        var policy = Policy();

        var first = AssertDeadLetter(policy.OnMessageFault(Row(3), Permanent(text), Now));
        var failed = policy.OnDeadLetterFailed(Row(3), first.Reason).Outcome;
        var stored = Row(failed.RetryCount, failed.Error);
        var second = AssertDeadLetter(policy.OnDeadLetterPendingRead(stored, Now));

        Assert.Equal(FailurePolicy.FaultEscape + text, failed.Error);
        Assert.True(policy.IsDeadLetterPending(stored));
        Assert.Equal(text, second.Reason);
    }

    [Fact]
    public void Fault_escape_prefixed_text_is_stored_with_one_more_escape()
    {
        var text = FailurePolicy.FaultEscape + FailurePolicy.HoldPrefix + "x";

        var fail = AssertRetry(Policy().OnMessageFault(Row(0), Transient(text), Now));

        Assert.Equal(FailurePolicy.FaultEscape + FailurePolicy.FaultEscape + FailurePolicy.HoldPrefix + "x", fail.Error);
    }

    [Fact]
    public void Held_row_hold_error_is_never_escaped()
    {
        var policy = Policy();
        var row = Routed(5, "orders");

        var fail = policy.Hold(row, FailurePolicy.HoldRoute("orders"));
        var held = Routed(fail.RetryCount, "orders", fail.Error) with { RetryCount = 5 };

        Assert.Equal(HeldError("orders"), fail.Error);
        Assert.False(policy.IsDeadLetterPending(held));
        Assert.Equal(HeldError("orders"), policy.Hold(held, FailurePolicy.HoldRoute("orders")).Error);
    }

    [Fact]
    public void Block_escapes_error_and_keeps_raw_reason_for_hold_prefixed_fault_text()
    {
        var text = FailurePolicy.HoldPrefix + "orders";

        var block = AssertBlock(Blocking().OnMessageFault(Row(0), Permanent(text), Now));

        Assert.Equal(FailurePolicy.FaultEscape + text, block.Outcome.Error);
        Assert.Equal(text, block.Reason);
    }

    [Fact]
    public void Normal_fault_text_is_the_same_instance()
    {
        var text = new string('x', 8);

        Assert.Same(text, AssertRetry(Policy().OnMessageFault(Row(0), Transient(text), Now)).Error);
    }
}
