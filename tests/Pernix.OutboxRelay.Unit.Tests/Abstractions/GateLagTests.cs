using Pernix.OutboxRelay.Core.Abstractions;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Abstractions;

public sealed class GateLagTests
{
    [Fact]
    public void Of_sets_value_only()
    {
        var lag = GateLag.Of(TimeSpan.FromSeconds(3));

        Assert.Equal(TimeSpan.FromSeconds(3), lag.Value);
        Assert.Null(lag.UnavailableReason);
    }

    [Fact]
    public void Unavailable_sets_reason_only()
    {
        var lag = GateLag.Unavailable("role lacks pg_monitor (pg_read_all_stats)");

        Assert.Null(lag.Value);
        Assert.Equal("role lacks pg_monitor (pg_read_all_stats)", lag.UnavailableReason);
    }

    [Fact]
    public void Default_is_zero_lag()
    {
        GateLag lag = default;

        Assert.Equal(GateLag.Of(TimeSpan.Zero), lag);
        Assert.Equal(TimeSpan.Zero, lag.Value);
        Assert.Null(lag.UnavailableReason);
    }

    [Fact]
    public void Negative_lag_throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => GateLag.Of(TimeSpan.FromTicks(-1)));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void Blank_reason_throws(string? reason)
    {
        Assert.ThrowsAny<ArgumentException>(() => GateLag.Unavailable(reason!));
    }
}
