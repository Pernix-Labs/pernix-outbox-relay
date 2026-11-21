using Pernix.OutboxRelay.Core.Abstractions;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Abstractions;

public sealed class OutboxPositionTests
{
    [Theory]
    [InlineData(1UL, 9L, 2UL, 1L, -1)]
    [InlineData(2UL, 1L, 1UL, 9L, 1)]
    [InlineData(5UL, 1L, 5UL, 2L, -1)]
    [InlineData(5UL, 2L, 5UL, 1L, 1)]
    [InlineData(5UL, 2L, 5UL, 2L, 0)]
    [InlineData(ulong.MaxValue, 1L, 0UL, long.MaxValue, 1)]
    public void Orders_by_order_then_id(ulong leftOrder, long leftId, ulong rightOrder, long rightId, int expected)
    {
        var left = new OutboxPosition(leftOrder, leftId);
        var right = new OutboxPosition(rightOrder, rightId);

        Assert.Equal(expected, Math.Sign(left.CompareTo(right)));
        Assert.Equal(expected < 0, left < right);
        Assert.Equal(expected > 0, left > right);
        Assert.Equal(expected <= 0, left <= right);
        Assert.Equal(expected >= 0, left >= right);
        Assert.Equal(expected == 0, left == right);
    }

    [Fact]
    public void Equal_positions_are_equal()
    {
        Assert.Equal(new OutboxPosition(3, 4), new OutboxPosition(3, 4));
        Assert.NotEqual(new OutboxPosition(3, 4), new OutboxPosition(3, 5));
        Assert.Equal(new OutboxPosition(3, 4).GetHashCode(), new OutboxPosition(3, 4).GetHashCode());
    }

    [Fact]
    public void Sorts_by_order_then_id()
    {
        OutboxPosition[] positions = [new(2, 1), new(1, 3), new(1, 2), new(3, 0)];

        Assert.Equal([new(1, 2), new(1, 3), new(2, 1), new(3, 0)], positions.Order().ToArray());
    }
}
