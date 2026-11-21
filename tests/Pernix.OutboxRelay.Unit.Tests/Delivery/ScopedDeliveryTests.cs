using System.Text;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Core.Delivery;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Delivery;

public sealed class ScopedDeliveryTests
{
    private static readonly DateTimeOffset T0 = new(2026, 11, 3, 12, 0, 0, TimeSpan.Zero);

    private static OutboxPosition Pos(long id) => new((ulong)id, id);

    [Fact]
    public void PerKey_sends_one_row_per_key_and_keys_in_parallel()
    {
        var h = new Harness(OrderingMode.PerKey, 10);
        h.Insert(1, "a");
        h.Insert(2, "a");
        h.Insert(3, "b");
        h.Insert(4, "b");
        h.Insert(5, "c");

        h.Step();

        Assert.Equal([1L, 3, 5], h.Send());
        Assert.Empty(h.Send());
        h.Deliver(1);
        Assert.Equal([2L], h.Send());
        h.Deliver(3);
        Assert.Equal([4L], h.Send());
    }

    [Fact]
    public void PerKey_null_key_rows_are_unbounded()
    {
        var h = new Harness(OrderingMode.PerKey, 10);
        foreach (var id in Enumerable.Range(1, 5))
        {
            h.Insert(id, null);
        }

        h.Insert(6, "a");
        h.Insert(7, "a");
        h.Step();

        Assert.Equal([1L, 2, 3, 4, 5, 6], h.Send());

        h.Insert(8, null);
        h.Insert(9, null);
        h.Step();

        Assert.Equal([8L, 9], h.Send());
    }

    [Fact]
    public void Per_scope_cap_overflow_moves_cursor_back_to_exact_position_only_at_half_cap()
    {
        var h = new Harness(OrderingMode.PerKey, 4);
        h.Insert(1, "a");
        h.Insert(2, "a");
        h.Insert(3, "a");
        h.Insert(4, "b");
        h.Insert(5, "a");
        h.Insert(6, "a");
        h.Insert(7, "c");
        h.Insert(8, "c");

        Assert.Equal([1L, 2, 3, 4], h.Step());
        Assert.Equal([5L, 7, 8], h.Step());
        Assert.Equal(Pos(8), h.Ordering.After);
        Assert.Equal([1L, 4, 7], h.Send());

        h.Deliver(1);
        Assert.Equal(Pos(8), h.Ordering.After);

        Assert.Equal([2L], h.Send());
        h.Deliver(2);
        Assert.Equal(Pos(5), h.Ordering.After);

        Assert.Equal([6L], h.Step());
        Assert.Equal(Pos(8), h.Ordering.After);
    }

    [Fact]
    public void Overflow_on_first_row_of_a_read_marks_the_read_after()
    {
        var h = new Harness(OrderingMode.PerKey, 2);
        h.Insert(1, "a");
        h.Insert(2, "a");
        h.Insert(3, "a");

        Assert.Equal([1L, 2], h.Step());
        Assert.Empty(h.Step());
        Assert.Equal([1L], h.Send());

        h.Deliver(1);

        Assert.Equal(Pos(2), h.Ordering.After);
        Assert.Equal([3L], h.Step());
    }

    [Fact]
    public void Overflowed_scope_below_cap_but_above_half_does_not_queue_a_later_row()
    {
        var h = new Harness(OrderingMode.PerKey, 4);
        h.Insert(1, "a");
        h.Insert(2, "a");
        h.Insert(3, "a");
        h.Insert(4, "a");
        h.Insert(5, "a");
        h.Insert(6, "b");

        Assert.Equal([1L, 2, 3, 4], h.Step());
        Assert.Equal([6L], h.Step());
        Assert.Equal([1L, 6], h.Send());

        h.Deliver(1);
        h.Insert(7, "a");
        Assert.Empty(h.Step());

        Assert.Equal([2L], h.Send());
        h.Deliver(2);
        Assert.Equal(Pos(4), h.Ordering.After);
        Assert.Equal([5L, 7], h.Step());
    }

    [Theory]
    [InlineData(OrderingMode.PerKey, 100)]
    [InlineData(OrderingMode.Strict, 10)]
    public void Reads_only_when_room_reaches_batch_size_and_always_ask_batch_size(OrderingMode mode, int capacity)
    {
        var ordering = new ScopedDelivery(mode, 10);

        Assert.Equal(capacity, ordering.Capacity);
        Assert.True(ordering.UsesSummary);
        Assert.Null(ordering.NextRead(9, []));
        foreach (var room in new[] { 10, capacity })
        {
            var request = ordering.NextRead(room, [1, 2]);
            Assert.NotNull(request);
            Assert.Equal(10, request.Max);
            Assert.Null(request.After);
            Assert.Empty(request.Exclude);
        }
    }

    [Fact]
    public void Rewind_with_nine_batches_held_reaches_new_rows_within_ten_reads()
    {
        var h = new Harness(OrderingMode.PerKey, 10);
        foreach (var id in Enumerable.Range(1, 90))
        {
            h.Insert(id, $"k{(id - 1) / 10}");
        }

        foreach (var _ in Enumerable.Range(0, 9))
        {
            h.Step();
        }

        Assert.Equal(90, h.Held.Count);
        h.Ordering.Summary(new FailedRowsSummary(0, 0, null));
        h.Ordering.Summary(new FailedRowsSummary(1, 7, null));
        Assert.Empty(h.Step());
        Assert.Null(h.Ordering.After);
        foreach (var id in Enumerable.Range(91, 10))
        {
            h.Insert(id, $"k{(id - 1) / 10}");
        }

        h.Reads.Clear();

        var reads = 0;
        IReadOnlyList<long> queued;
        do
        {
            queued = h.Step();
            reads++;
        }
        while (queued.Count == 0 && reads < 20);

        Assert.Equal(10, reads);
        Assert.Equal(Enumerable.Range(91, 10).Select(i => (long)i), queued);
        Assert.All(h.Reads, r => Assert.Equal(10, r.Max));
        Assert.Equal(
            Enumerable.Range(0, 10).Select(i => i == 0 ? (OutboxPosition?)null : Pos(i * 10)),
            h.Reads.Select(r => r.After));
    }

    [Fact]
    public void Strict_sends_one_row_at_a_time_and_reads_only_with_empty_queue()
    {
        var h = new Harness(OrderingMode.Strict, 3);
        h.Insert(1, "a");
        h.Insert(2, "b");
        h.Insert(3, null);
        h.Insert(4, "c");

        Assert.Equal([1L, 2, 3], h.Step());
        Assert.Null(h.Ordering.NextRead(h.Ordering.Capacity - h.Held.Count, h.Held));
        Assert.Equal([1L], h.Send());
        Assert.Empty(h.Send());

        h.Deliver(1);
        Assert.Equal([2L], h.Send());
        Assert.Null(h.Ordering.NextRead(h.Ordering.Capacity - h.Held.Count, h.Held));
    }

    [Fact]
    public void Strict_rewind_rewalk_is_one_read()
    {
        var h = new Harness(OrderingMode.Strict, 3);
        foreach (var id in Enumerable.Range(1, 5))
        {
            h.Insert(id, "k");
        }

        h.Step();
        Assert.Equal([1L], h.Send());

        Assert.Equal([2L, 3], h.Unpark(1));
        h.Reads.Clear();

        Assert.Equal([1L, 2, 3], h.Step());
        Assert.Single(h.Reads);
        Assert.Null(h.Reads[0].After);
        Assert.Equal([1L], h.Send());
    }

    [Fact]
    public void Strict_halts_at_row_not_due_and_probes_until_it_is_due()
    {
        var h = new Harness(OrderingMode.Strict, 3);
        h.Insert(1, "a");
        h.Insert(2, "a", due: false);
        h.Insert(3, "b");
        h.Insert(4, "c");

        Assert.Equal([1L], h.Step());
        Assert.True(h.Ordering.Halted);
        Assert.Equal(Pos(1), h.Ordering.After);
        Assert.Equal([1L], h.Send());
        h.Deliver(1);

        foreach (var _ in Enumerable.Range(0, 2))
        {
            Assert.Empty(h.Step());
            Assert.True(h.Ordering.Halted);
            Assert.Equal(1, h.Reads[^1].Max);
            Assert.Equal(Pos(1), h.Reads[^1].After);
        }

        h.SetDue(2);

        Assert.Equal([2L], h.Step());
        Assert.False(h.Ordering.Halted);
        Assert.Equal(1, h.Reads[^1].Max);
        Assert.Equal(Pos(2), h.Ordering.After);

        Assert.Equal([2L], h.Send());
        h.Deliver(2);
        Assert.Equal([3L, 4], h.Step());
        Assert.Equal(3, h.Reads[^1].Max);
    }

    [Fact]
    public void Strict_halt_is_cleared_by_rewind()
    {
        var h = new Harness(OrderingMode.Strict, 3);
        h.Insert(1, "a", due: false);
        h.Insert(2, "b");

        Assert.Empty(h.Step());
        Assert.True(h.Ordering.Halted);
        Assert.Null(h.Ordering.After);

        h.Ordering.Summary(new FailedRowsSummary(1, 1, null));
        h.Ordering.Summary(new FailedRowsSummary(0, 0, null));

        Assert.False(h.Ordering.Halted);
        Assert.Equal(3, h.Ordering.NextRead(3, [])!.Max);
    }

    [Fact]
    public void PerKey_safety_rewind_after_sixty_seconds_since_last_rewind_on_the_tick_clock()
    {
        var h = new Harness(OrderingMode.PerKey, 2);
        h.Insert(1, "a");
        h.Insert(2, "b");
        h.Insert(3, "c");
        h.Ordering.Tick(T0);
        h.Step();

        h.Ordering.Tick(T0.AddSeconds(30));
        h.Ordering.Summary(new FailedRowsSummary(0, 0, null));
        h.Ordering.Summary(new FailedRowsSummary(1, 1, null));
        h.Step();
        Assert.Equal([ScopedDelivery.SummaryReason], h.DrainRewinds());
        h.Step();
        Assert.Equal(Pos(2), h.Ordering.After);

        h.Ordering.Tick(T0.AddSeconds(89));
        Assert.Equal(Pos(2), h.Ordering.After);

        h.Ordering.Tick(T0.AddSeconds(90));
        Assert.Null(h.Ordering.After);
        Assert.Equal([ScopedDelivery.SafetyReason], h.DrainRewinds());
    }

    [Fact]
    public void Strict_has_no_safety_rewind()
    {
        var h = new Harness(OrderingMode.Strict, 2);
        h.Insert(1, "a");
        h.Ordering.Tick(T0);
        h.Step();

        h.Ordering.Tick(T0.AddMinutes(5));

        Assert.Equal(Pos(1), h.Ordering.After);
        Assert.Empty(h.DrainRewinds());
    }

    [Fact]
    public void Held_rows_are_skipped_and_the_cursor_passes_them()
    {
        var h = new Harness(OrderingMode.PerKey, 10);
        h.Insert(1, "a");
        h.Insert(2, "b");
        h.Insert(3, "c");
        h.Held.Add(2);

        Assert.Equal([1L, 3], h.Step());
        Assert.Equal(Pos(3), h.Ordering.After);

        h.Insert(4, "d");
        h.Held.Add(4);

        Assert.Empty(h.Step());
        Assert.Equal(Pos(4), h.Ordering.After);
    }

    [Fact]
    public void PerKey_not_due_rows_are_passed_like_held_rows_without_queueing()
    {
        var h = new Harness(OrderingMode.PerKey, 3);
        h.Insert(1, "k", due: false);
        h.Insert(2, "k", due: false);
        h.Insert(3, "a");
        h.Insert(4, "k", due: false);
        h.Insert(5, "k", due: false);
        h.Insert(6, "k", due: false);

        Assert.Equal([3L], h.Step());
        Assert.Equal(Pos(3), h.Ordering.After);

        Assert.Empty(h.Step());
        Assert.Equal(Pos(6), h.Ordering.After);
        Assert.False(h.Ordering.AwaitingMoveBack);
        Assert.False(h.Ordering.Halted);

        Assert.Empty(h.Step());
        Assert.Equal(Pos(6), h.Ordering.After);
        Assert.Equal([3L], h.Send());
    }

    [Fact]
    public void PerKey_row_after_a_passed_not_due_row_of_its_key_is_passed_until_rewind_then_both_queue_in_order()
    {
        var h = new Harness(OrderingMode.PerKey, 2);
        h.Insert(1, "k", due: false);
        h.Insert(2, "a");
        h.Insert(3, "k");
        h.Insert(4, "b");

        Assert.Equal([2L], h.Step());

        h.SetDue(1);
        Assert.Equal([4L], h.Step());
        Assert.Equal(Pos(4), h.Ordering.After);
        Assert.Equal([2L, 4L], h.Send());
        h.Deliver(2);
        h.Deliver(4);

        h.Ordering.Summary(new FailedRowsSummary(1, 1, null));
        h.Ordering.Summary(new FailedRowsSummary(1, 2, null));
        Assert.Empty(h.Step());
        Assert.Null(h.Ordering.After);

        Assert.Equal([1L, 3L], h.Step());
        Assert.Equal([1L], h.Send());
        h.Deliver(1);
        Assert.Equal([3L], h.Send());
    }

    [Fact]
    public void PerKey_later_row_of_a_key_is_passed_in_the_same_read_as_its_not_due_row()
    {
        var h = new Harness(OrderingMode.PerKey, 10);
        h.Insert(1, "k", due: false);
        h.Insert(2, "k");
        h.Insert(3, "a");

        Assert.Equal([3L], h.Step());
        Assert.Equal(Pos(3), h.Ordering.After);
    }

    [Fact]
    public void PerKey_null_key_rows_are_not_passed_after_a_not_due_null_key_row()
    {
        var h = new Harness(OrderingMode.PerKey, 2);
        h.Insert(1, null, due: false);
        h.Insert(2, null);
        h.Insert(3, null);

        Assert.Equal([2L], h.Step());
        Assert.Equal([3L], h.Step());
        Assert.Equal([2L, 3L], h.Send());
    }

    [Theory]
    [InlineData(ScopedDelivery.SummaryReason)]
    [InlineData(ScopedDelivery.FailureReason)]
    [InlineData(ScopedDelivery.UnparkReason)]
    [InlineData(ScopedDelivery.SafetyReason)]
    public void PerKey_passed_keys_are_cleared_by_every_rewind_reason(string reason)
    {
        var h = new Harness(OrderingMode.PerKey, 10);
        h.Ordering.Tick(T0);
        h.Insert(1, "a");
        h.Insert(2, "k", due: false);
        h.Insert(3, "k");
        Assert.Equal([1L], h.Step());
        Assert.Equal([1L], h.Send());

        switch (reason)
        {
            case ScopedDelivery.SummaryReason:
                h.Ordering.Summary(new FailedRowsSummary(0, 0, null));
                h.Ordering.Summary(new FailedRowsSummary(1, 1, null));
                break;
            case ScopedDelivery.FailureReason:
                h.Fail(1);
                h.SetDue(1, false);
                h.Commit(1);
                break;
            case ScopedDelivery.UnparkReason:
                h.Unpark(1);
                break;
            default:
                h.Ordering.Tick(T0 + ScopedDelivery.SafetyRewindInterval);
                break;
        }

        Assert.Equal([reason], h.DrainRewinds());
        h.SetDue(2);

        var queued = h.Step();
        Assert.Equal([2L, 3L], queued.Where(id => id != 1));
        Assert.Contains(2L, h.Send());
    }

    [Fact]
    public void PerKey_overflowed_row_before_a_passed_row_of_its_key_queues_after_move_back()
    {
        var h = new Harness(OrderingMode.PerKey, 2);
        h.Insert(1, "k");
        h.Insert(2, "k");
        h.Insert(3, "k");
        h.Insert(4, "k", due: false);
        h.Insert(5, "k");

        Assert.Equal([1L, 2L], h.Step());
        Assert.Empty(h.Step());
        Assert.Empty(h.Step());
        Assert.Equal(Pos(5), h.Ordering.After);

        Assert.Equal([1L], h.Send());
        h.Deliver(1);
        Assert.Equal(Pos(2), h.Ordering.After);

        Assert.Equal([3L], h.Step());
        Assert.Equal(Pos(4), h.Ordering.After);
        Assert.Empty(h.Step());
        Assert.Equal(Pos(5), h.Ordering.After);
    }

    [Fact]
    public void Read_of_only_overflow_rows_awaits_move_back_and_move_back_ends_it()
    {
        var h = new Harness(OrderingMode.PerKey, 2);
        foreach (var id in Enumerable.Range(1, 6))
        {
            h.Insert(id, "a");
        }

        Assert.Equal([1L, 2], h.Step());
        Assert.False(h.Ordering.AwaitingMoveBack);

        Assert.Empty(h.Step());
        Assert.True(h.Ordering.AwaitingMoveBack);
        Assert.Equal(Pos(4), h.Ordering.After);

        Assert.Equal([1L], h.Send());
        h.Deliver(1);

        Assert.False(h.Ordering.AwaitingMoveBack);
        Assert.Equal(Pos(2), h.Ordering.After);

        Assert.Equal([3L], h.Step());
        Assert.False(h.Ordering.AwaitingMoveBack);
    }

    [Fact]
    public void Fail_evicts_its_scope_queue_and_rewinds()
    {
        var h = new Harness(OrderingMode.PerKey, 10);
        h.Insert(1, "a");
        h.Insert(2, "a");
        h.Insert(3, "a");
        h.Insert(4, "b");
        h.Insert(5, "b");
        h.Step();
        Assert.Equal([1L, 4], h.Send());

        Assert.Equal([2L, 3], h.Fail(1));
        Assert.Null(h.Ordering.After);
        Assert.Equal([ScopedDelivery.FailureReason], h.DrainRewinds());
        Assert.Empty(h.Send());

        h.Commit(1);
        Assert.Empty(h.Send());
        h.Deliver(4);
        Assert.Equal([5L], h.Send());
    }

    [Fact]
    public void Fail_of_null_key_row_does_not_rewind()
    {
        var h = new Harness(OrderingMode.PerKey, 10);
        h.Insert(1, null);
        h.Step();
        Assert.Equal([1L], h.Send());

        Assert.Empty(h.Fail(1));

        Assert.Equal(Pos(1), h.Ordering.After);
        Assert.Empty(h.DrainRewinds());
    }

    [Fact]
    public void Unpark_evicts_its_scope_queue_and_rewinds_so_the_parked_row_goes_first()
    {
        var h = new Harness(OrderingMode.PerKey, 10);
        h.Insert(1, "a");
        h.Insert(2, "a");
        h.Insert(3, "b");
        h.Step();
        Assert.Equal([1L, 3], h.Send());

        Assert.Equal([2L], h.Unpark(1));
        Assert.Null(h.Ordering.After);
        Assert.Equal([ScopedDelivery.UnparkReason], h.DrainRewinds());

        Assert.Equal([1L, 2], h.Step());
        Assert.Equal([1L], h.Send());
    }

    [Fact]
    public void Summary_baseline_does_not_rewind_equal_does_not_and_change_does()
    {
        var h = new Harness(OrderingMode.PerKey, 1);
        h.Insert(1, "a");
        h.Insert(2, "b");
        var first = new FailedRowsSummary(2, 10, null);

        h.Step();
        h.Ordering.Summary(first);
        Assert.Equal(Pos(1), h.Ordering.After);

        h.Ordering.Summary(first with { });
        Assert.Equal(Pos(1), h.Ordering.After);

        h.Ordering.Summary(first with { Checksum = 11 });
        h.Step();
        h.Step();
        Assert.Null(h.Ordering.After);
        Assert.Equal([ScopedDelivery.SummaryReason], h.DrainRewinds());
    }

    [Fact]
    public void Every_rewind_is_drained_even_when_the_reason_repeats()
    {
        var h = new Harness(OrderingMode.PerKey, 1);
        Assert.Empty(h.DrainRewinds());

        h.Ordering.Summary(new FailedRowsSummary(1, 1, null));
        Assert.Empty(h.DrainRewinds());

        h.Ordering.Summary(new FailedRowsSummary(2, 2, null));
        h.Ordering.Summary(new FailedRowsSummary(3, 3, null));

        Assert.Equal([ScopedDelivery.SummaryReason, ScopedDelivery.SummaryReason], h.DrainRewinds());
    }

    [Fact]
    public void DrainRewinds_returns_each_rewind_reason_once_in_order()
    {
        var h = new Harness(OrderingMode.PerKey, 1);
        h.Ordering.Tick(T0);
        h.Ordering.Summary(new FailedRowsSummary(1, 1, null));

        h.Ordering.Tick(T0 + ScopedDelivery.SafetyRewindInterval);
        h.Ordering.Summary(new FailedRowsSummary(2, 2, null));

        var reasons = new List<string>();
        h.Ordering.DrainRewinds(reasons);
        Assert.Equal([ScopedDelivery.SafetyReason, ScopedDelivery.SummaryReason], reasons);

        reasons.Clear();
        h.Ordering.DrainRewinds(reasons);
        Assert.Empty(reasons);
    }

    [Fact]
    public void Summary_next_due_change_rewinds_once()
    {
        var h = new Harness(OrderingMode.PerKey, 1);
        h.Insert(1, "a");
        h.Insert(2, "b");
        var at = new DateTimeOffset(2026, 11, 3, 12, 0, 0, TimeSpan.Zero);

        h.Ordering.Summary(new FailedRowsSummary(2, 10, at));
        h.Step();
        h.Ordering.Summary(new FailedRowsSummary(2, 10, at.AddSeconds(10)));
        h.Step();
        h.Step();
        Assert.Null(h.Ordering.After);

        h.Step();
        h.Ordering.Summary(new FailedRowsSummary(2, 10, at.AddSeconds(10)));
        Assert.Equal(Pos(1), h.Ordering.After);
    }

    [Fact]
    public void PerKey_rewind_asked_mid_walk_waits_for_a_short_read()
    {
        var h = new Harness(OrderingMode.PerKey, 2);
        h.Insert(1, "a");
        h.Insert(2, "k", due: false);
        h.Insert(3, "k");
        h.Insert(4, "b");
        h.Insert(5, "c");
        Assert.Equal([1L], h.Step());

        h.Ordering.Summary(new FailedRowsSummary(0, 0, null));
        h.Ordering.Summary(new FailedRowsSummary(1, 2, null));
        Assert.Equal(Pos(2), h.Ordering.After);
        Assert.Empty(h.DrainRewinds());

        Assert.Equal([4L], h.Step());
        Assert.Equal(Pos(4), h.Ordering.After);
        Assert.Empty(h.DrainRewinds());

        Assert.Equal([5L], h.Step());
        Assert.Null(h.Ordering.After);
        Assert.Equal([ScopedDelivery.SummaryReason], h.DrainRewinds());
    }

    [Fact]
    public void PerKey_rewinds_mid_walk_coalesce_into_one_with_the_first_reason()
    {
        var h = new Harness(OrderingMode.PerKey, 2);
        h.Insert(1, "a");
        h.Insert(2, "b");
        h.Insert(3, "c");
        h.Ordering.Summary(new FailedRowsSummary(0, 0, null));
        h.Step();
        Assert.Equal([1L, 2], h.Send());

        h.Fail(1);
        h.Ordering.Summary(new FailedRowsSummary(1, 1, null));
        h.Ordering.Summary(new FailedRowsSummary(2, 2, null));
        Assert.Equal(Pos(2), h.Ordering.After);
        Assert.Empty(h.DrainRewinds());

        h.Step();
        Assert.Null(h.Ordering.After);
        Assert.Equal([ScopedDelivery.FailureReason], h.DrainRewinds());
    }

    [Fact]
    public void PerKey_rewind_when_not_walking_applies_at_once()
    {
        var h = new Harness(OrderingMode.PerKey, 2);
        h.Insert(1, "a");
        h.Ordering.Summary(new FailedRowsSummary(0, 0, null));
        h.Step();
        Assert.Equal(Pos(1), h.Ordering.After);

        h.Ordering.Summary(new FailedRowsSummary(1, 1, null));
        Assert.Null(h.Ordering.After);
        Assert.Equal([ScopedDelivery.SummaryReason], h.DrainRewinds());

        h.Ordering.Summary(new FailedRowsSummary(2, 2, null));
        Assert.Equal([ScopedDelivery.SummaryReason], h.DrainRewinds());
    }

    [Fact]
    public void PerKey_failed_head_key_is_held_while_its_rewind_waits()
    {
        var h = new Harness(OrderingMode.PerKey, 2);
        h.Insert(1, "a");
        h.Insert(2, "b");
        h.Insert(3, "c");
        h.Insert(4, "a");
        h.Insert(5, "a");
        h.Insert(6, "d");
        Assert.Equal([1L, 2], h.Step());
        Assert.Equal([1L, 2], h.Send());

        Assert.Empty(h.Fail(1));
        h.Commit(1);
        Assert.Empty(h.DrainRewinds());

        Assert.Equal([3L], h.Step());
        Assert.Equal([6L], h.Step());
        Assert.Equal(Pos(6), h.Ordering.After);

        Assert.Empty(h.Step());
        Assert.Null(h.Ordering.After);
        Assert.Equal([ScopedDelivery.FailureReason], h.DrainRewinds());

        Assert.Equal([1L], h.Step());
        Assert.Equal([4L], h.Step());
        Assert.Equal([3L, 6, 1], h.Send());
        h.Deliver(1);
        Assert.Equal([4L], h.Send());
        Assert.Equal([5L], h.Step());
        h.Deliver(4);
        Assert.Equal([5L], h.Send());
    }

    [Fact]
    public void PerKey_unpark_rewinds_at_once_mid_walk()
    {
        var h = new Harness(OrderingMode.PerKey, 2);
        h.Insert(1, "a");
        h.Insert(2, "b");
        h.Insert(3, "c");
        h.Step();
        Assert.Equal([1L, 2], h.Send());

        Assert.Empty(h.Unpark(1));
        Assert.Null(h.Ordering.After);
        Assert.Equal([ScopedDelivery.UnparkReason], h.DrainRewinds());
        Assert.Equal([1L], h.Step());
    }

    [Fact]
    public void PerKey_deferred_rewind_applies_sixty_seconds_after_the_last_progress_of_a_stalled_walk()
    {
        var h = new Harness(OrderingMode.PerKey, 2);
        foreach (var id in Enumerable.Range(1, 5))
        {
            h.Insert(id, $"k{id}");
        }

        var t = T0.AddSeconds(5);
        h.Ordering.Tick(T0);
        h.Step();
        h.Ordering.Tick(t);
        Assert.Equal([3L, 4], h.Step());

        h.Ordering.Tick(t.AddSeconds(10));
        h.Ordering.Summary(new FailedRowsSummary(0, 0, null));
        h.Ordering.Summary(new FailedRowsSummary(1, 1, null));

        h.Ordering.Tick(t.AddSeconds(59));
        Assert.Equal(Pos(4), h.Ordering.After);
        Assert.Empty(h.DrainRewinds());

        h.Ordering.Tick(t.AddSeconds(60));
        Assert.Null(h.Ordering.After);
        Assert.Equal([ScopedDelivery.SummaryReason], h.DrainRewinds());
    }

    [Fact]
    public void PerKey_deferred_rewind_waits_past_sixty_seconds_while_the_walk_progresses()
    {
        var h = new Harness(OrderingMode.PerKey, 2);
        foreach (var id in Enumerable.Range(1, 9))
        {
            h.Insert(id, $"k{id}");
        }

        h.Ordering.Tick(T0);
        Assert.Equal([1L, 2], h.Step());

        h.Ordering.Tick(T0.AddSeconds(1));
        h.Ordering.Summary(new FailedRowsSummary(0, 0, null));
        h.Ordering.Summary(new FailedRowsSummary(1, 1, null));
        Assert.Empty(h.DrainRewinds());

        h.Ordering.Tick(T0.AddSeconds(30));
        Assert.Equal([3L, 4], h.Step());
        h.Ordering.Tick(T0.AddSeconds(60));
        Assert.Equal([5L, 6], h.Step());
        h.Ordering.Tick(T0.AddSeconds(85));
        Assert.Equal([7L, 8], h.Step());

        h.Ordering.Tick(T0.AddSeconds(110));
        Assert.Equal(Pos(8), h.Ordering.After);
        Assert.Empty(h.DrainRewinds());

        Assert.Equal([9L], h.Step());
        Assert.Null(h.Ordering.After);
        Assert.Equal([ScopedDelivery.SummaryReason], h.DrainRewinds());
    }

    [Fact]
    public void PerKey_all_overflow_reads_are_no_progress_and_a_pending_rewind_applies_after_sixty_seconds()
    {
        var h = new Harness(OrderingMode.PerKey, 2);
        foreach (var id in Enumerable.Range(1, 8))
        {
            h.Insert(id, "a");
        }

        h.Ordering.Tick(T0);
        Assert.Equal([1L, 2], h.Step());

        h.Ordering.Tick(T0.AddSeconds(10));
        h.Ordering.Summary(new FailedRowsSummary(0, 0, null));
        h.Ordering.Summary(new FailedRowsSummary(1, 1, null));

        h.Ordering.Tick(T0.AddSeconds(20));
        Assert.Empty(h.Step());
        Assert.True(h.Ordering.AwaitingMoveBack);
        Assert.Equal(Pos(4), h.Ordering.After);

        h.Ordering.Tick(T0.AddSeconds(30));
        Assert.Empty(h.Step());
        Assert.True(h.Ordering.AwaitingMoveBack);
        Assert.Equal(Pos(6), h.Ordering.After);

        h.Ordering.Tick(T0.AddSeconds(40));
        Assert.Equal([1L], h.Send());
        h.Deliver(1);
        Assert.Equal(Pos(2), h.Ordering.After);
        Assert.Equal([3L], h.Step());
        Assert.Equal(Pos(4), h.Ordering.After);

        h.Ordering.Tick(T0.AddSeconds(59));
        Assert.Equal(Pos(4), h.Ordering.After);
        Assert.Empty(h.DrainRewinds());

        h.Ordering.Tick(T0.AddSeconds(60));
        Assert.Null(h.Ordering.After);
        Assert.Equal([ScopedDelivery.SummaryReason], h.DrainRewinds());
    }

    [Fact]
    public void PerKey_held_room_walk_stalls_and_a_pending_rewind_applies_after_sixty_seconds()
    {
        var h = new Harness(OrderingMode.PerKey, 2);
        foreach (var id in Enumerable.Range(1, 30))
        {
            h.Insert(id, $"k{id}");
        }

        h.Ordering.Tick(T0);
        for (var i = 0; i < IDeliveryOrdering.CapacityFactor; i++)
        {
            Assert.Equal(2, h.Step().Count);
        }

        Assert.Equal(Pos(20), h.Ordering.After);
        Assert.Null(h.Ordering.NextRead(h.Ordering.Capacity - h.Held.Count, h.Held));

        h.Ordering.Tick(T0.AddSeconds(10));
        h.Ordering.Summary(new FailedRowsSummary(0, 0, null));
        h.Ordering.Summary(new FailedRowsSummary(1, 1, null));

        h.Ordering.Tick(T0.AddSeconds(59));
        Assert.Equal(Pos(20), h.Ordering.After);
        Assert.Empty(h.DrainRewinds());

        h.Ordering.Tick(T0.AddSeconds(60));
        Assert.Null(h.Ordering.After);
        Assert.Equal([ScopedDelivery.SummaryReason], h.DrainRewinds());

        Assert.Equal(Enumerable.Range(1, 20).Select(i => (long)i), h.Send());
        h.Deliver(1);
        h.Deliver(2);
        for (var i = 1; i < IDeliveryOrdering.CapacityFactor; i++)
        {
            Assert.Empty(h.Step());
        }

        Assert.Equal([21L, 22], h.Step());
        Assert.Equal(Pos(22), h.Ordering.After);
        Assert.Null(h.Ordering.NextRead(h.Ordering.Capacity - h.Held.Count, h.Held));

        h.Ordering.Tick(T0.AddSeconds(119));
        Assert.Equal(Pos(22), h.Ordering.After);
        Assert.Empty(h.DrainRewinds());

        h.Ordering.Tick(T0.AddSeconds(120));
        Assert.Null(h.Ordering.After);
        Assert.Equal([ScopedDelivery.SafetyReason], h.DrainRewinds());
    }

    [Fact]
    public void PerKey_safety_rewind_mid_walk_waits_for_a_short_read()
    {
        var h = new Harness(OrderingMode.PerKey, 2);
        h.Insert(1, "a");
        h.Insert(2, "b");
        h.Insert(3, "c");
        h.Ordering.Tick(T0);
        h.Ordering.Tick(T0.AddSeconds(30));
        h.Step();

        h.Ordering.Tick(T0 + ScopedDelivery.SafetyRewindInterval);
        Assert.Equal(Pos(2), h.Ordering.After);
        Assert.Empty(h.DrainRewinds());

        Assert.Equal([3L], h.Step());
        Assert.Null(h.Ordering.After);
        Assert.Equal([ScopedDelivery.SafetyReason], h.DrainRewinds());
    }

    [Theory]
    [InlineData(ScopedDelivery.SummaryReason)]
    [InlineData(ScopedDelivery.FailureReason)]
    public void Strict_rewinds_at_once_after_a_full_read(string reason)
    {
        var h = new Harness(OrderingMode.Strict, 2);
        h.Insert(1, "a");
        h.Insert(2, "b");
        h.Insert(3, "c");
        Assert.Equal([1L, 2], h.Step());
        Assert.Equal(Pos(2), h.Ordering.After);

        if (reason == ScopedDelivery.SummaryReason)
        {
            h.Ordering.Summary(new FailedRowsSummary(0, 0, null));
            h.Ordering.Summary(new FailedRowsSummary(1, 1, null));
        }
        else
        {
            Assert.Equal([1L], h.Send());
            Assert.Equal([2L], h.Fail(1));
        }

        Assert.Null(h.Ordering.After);
        Assert.Equal([reason], h.DrainRewinds());
    }

    [Fact]
    public void Removed_key_readded_gets_clean_scope()
    {
        var h = new Harness(OrderingMode.PerKey, 10);
        h.Insert(1, "a");
        h.Step();
        Assert.Equal([1L], h.Send());
        h.Deliver(1);
        Assert.Equal(1, h.Ordering.ScopesCreated);

        h.Insert(2, "a");
        Assert.Equal([2L], h.Step());

        Assert.Equal(1, h.Ordering.ScopesCreated);
        Assert.Equal([2L], h.Send());
        Assert.Empty(h.Send());
    }

    [Fact]
    public void Recycled_scope_exposes_no_old_state()
    {
        var h = new Harness(OrderingMode.PerKey, 2);
        h.Insert(1, "a");
        h.Insert(2, "a");
        h.Insert(3, "a");
        Assert.Equal([1L, 2], h.Step());
        Assert.Empty(h.Step());
        Assert.True(h.Ordering.AwaitingMoveBack);
        Assert.Equal([1L], h.Send());

        // Unpark -> Untrack: key a leaves with overflow state (OverflowAfter = position 2) still set.
        Assert.Equal([2L], h.Unpark(1));
        h.Deliver(1);
        h.Deliver(2);
        h.Deliver(3);
        Assert.Equal(1, h.Ordering.ScopesCreated);

        h.Insert(4, "b");
        h.Insert(5, "b");
        Assert.Equal([4L, 5], h.Step());
        Assert.Equal(1, h.Ordering.ScopesCreated);
        Assert.False(h.Ordering.AwaitingMoveBack);
        Assert.Equal(Pos(5), h.Ordering.After);

        Assert.Equal([4L], h.Send());
        Assert.Empty(h.Send());
        h.Deliver(4);
        Assert.Equal(Pos(5), h.Ordering.After);
        Assert.Equal([5L], h.Send());
        Assert.Empty(h.Send());
    }

    [Fact]
    public void Referenced_scope_is_not_recycled()
    {
        var h = new Harness(OrderingMode.PerKey, 10);
        h.Insert(1, "a");
        h.Insert(2, "a");
        h.Insert(3, "a");
        h.Step();

        // a: head 1 in flight, rows 2 and 3 queued and indexed.
        Assert.Equal([1L], h.Send());
        h.Ordering.Release(2);
        h.Ordering.Left(3);

        // c: in ready, never taken.
        h.Insert(4, "c");
        Assert.Equal([4L], h.Step());
        Assert.Equal(2, h.Ordering.ScopesCreated);

        h.Insert(5, "b");
        Assert.Equal([5L], h.Step());
        Assert.Equal(3, h.Ordering.ScopesCreated);

        Assert.Equal([4L, 5], h.Send());
        h.Deliver(1);
        Assert.Equal([2L], h.Send());
        h.Deliver(2);
        Assert.Equal([3L], h.Send());
        h.Deliver(3);
        Assert.Empty(h.Send());
    }

    private sealed class Harness(OrderingMode mode, int batchSize)
    {
        private readonly List<OutboxMessage> table = [];

        public ScopedDelivery Ordering { get; } = new(mode, batchSize);

        public HashSet<long> Held { get; } = [];

        public List<ReadRequest> Reads { get; } = [];

        public void Insert(long id, string? key, bool due = true) => table.Add(new OutboxMessage
        {
            Id = id,
            Position = Pos(id),
            Key = key is null ? null : Encoding.UTF8.GetBytes(key),
            Payload = [1],
            Headers = MessageHeaders.Empty,
            CreatedAt = DateTimeOffset.UnixEpoch,
            RetryCount = 0,
            IsDue = due,
        });

        public void SetDue(long id, bool due = true)
        {
            var index = table.FindIndex(r => r.Id == id);
            table[index] = table[index] with { IsDue = due };
        }

        public IReadOnlyList<long> Step()
        {
            var request = Ordering.NextRead(Ordering.Capacity - Held.Count, Held);
            Assert.NotNull(request);
            Reads.Add(request);
            var rows = table
                .Where(r => request.After is not { } after || r.Position > after)
                .OrderBy(r => r.Position)
                .Take(request.Max)
                .ToList();
            var queued = new List<OutboxMessage>();
            Ordering.Accept(rows, Held.Contains, queued);
            foreach (var row in queued)
            {
                Assert.True(Held.Add(row.Id));
            }

            return [.. queued.Select(r => r.Id)];
        }

        public IReadOnlyList<long> Send()
        {
            var output = new List<OutboxMessage>();
            Ordering.Sendable(output);
            return [.. output.Select(r => r.Id)];
        }

        public void Deliver(long id)
        {
            Ordering.Release(id);
            table.RemoveAll(r => r.Id == id);
            Commit(id);
        }

        public void Commit(long id)
        {
            Held.Remove(id);
            Ordering.Left(id);
        }

        public IReadOnlyList<long> Fail(long id)
        {
            var evicted = new List<long>();
            Ordering.Fail(id, evicted);
            Held.ExceptWith(evicted);
            return evicted;
        }

        public IReadOnlyList<long> Unpark(long id)
        {
            var evicted = new List<long>();
            Held.Remove(id);
            Ordering.Unpark([id], evicted);
            Held.ExceptWith(evicted);
            return evicted;
        }

        public IReadOnlyList<string> DrainRewinds()
        {
            var reasons = new List<string>();
            Ordering.DrainRewinds(reasons);
            return reasons;
        }
    }
}
