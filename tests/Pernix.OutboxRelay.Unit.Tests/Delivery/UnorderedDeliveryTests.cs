using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Delivery;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Delivery;

public sealed class UnorderedDeliveryTests
{
    private static OutboxMessage Row(long id) => new()
    {
        Id = id,
        Position = new OutboxPosition((ulong)id, id),
        Payload = [1],
        Headers = MessageHeaders.Empty,
        CreatedAt = DateTimeOffset.UnixEpoch,
        RetryCount = 0,
        IsDue = true,
    };

    [Fact]
    public void Shape_matches_shipped_None()
    {
        var ordering = new UnorderedDelivery(10);

        Assert.Equal(100, ordering.Capacity);
        Assert.False(ordering.UsesSummary);
        Assert.False(ordering.Halted);
        Assert.False(ordering.AwaitingMoveBack);
    }

    [Fact]
    public void DrainRewinds_returns_no_reasons()
    {
        var ordering = new UnorderedDelivery(10);
        ordering.Summary(new FailedRowsSummary(1, 1, null));
        var reasons = new List<string>();

        ordering.DrainRewinds(reasons);

        Assert.Empty(reasons);
    }

    [Fact]
    public void Read_asks_batch_size_bounded_by_room_from_head_excluding_held()
    {
        var ordering = new UnorderedDelivery(10);
        long[] held = [3, 7, 9];

        var full = ordering.NextRead(100, held);
        var bounded = ordering.NextRead(4, held);

        Assert.NotNull(full);
        Assert.Equal(10, full.Max);
        Assert.Null(full.After);
        Assert.Equal(held, full.Exclude);
        Assert.NotNull(bounded);
        Assert.Equal(4, bounded.Max);
        Assert.Null(bounded.After);
        Assert.Null(ordering.NextRead(0, held));
    }

    [Fact]
    public void Every_accepted_row_is_queued_and_sendable_at_once()
    {
        var ordering = new UnorderedDelivery(10);
        var queued = new List<OutboxMessage>();
        var sendable = new List<OutboxMessage>();

        ordering.Accept([Row(1), Row(2), Row(3), Row(4)], id => id == 2, queued);
        ordering.Sendable(sendable);

        Assert.Equal([1L, 3, 4], queued.Select(r => r.Id));
        Assert.Equal([1L, 3, 4], sendable.Select(r => r.Id));

        sendable.Clear();
        ordering.Sendable(sendable);
        Assert.Empty(sendable);
    }
}
