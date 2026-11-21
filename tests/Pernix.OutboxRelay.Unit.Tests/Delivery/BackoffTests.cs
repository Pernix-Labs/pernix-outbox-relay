using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Core.Delivery;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Delivery;

public sealed class BackoffTests
{
    private static RetryOptions Options(double multiplier = 2.0) => new()
    {
        InitialDelay = TimeSpan.FromSeconds(10),
        BackoffMultiplier = multiplier,
        MaxDelay = TimeSpan.FromMinutes(5),
    };

    [Fact]
    public void Delay_grows_by_multiplier_from_initial_delay()
    {
        Assert.Equal(TimeSpan.FromSeconds(10), Backoff.Delay(Options(), 1));
        Assert.Equal(TimeSpan.FromSeconds(20), Backoff.Delay(Options(), 2));
        Assert.Equal(TimeSpan.FromSeconds(40), Backoff.Delay(Options(), 3));
    }

    [Fact]
    public void Delay_saturates_at_max_delay()
    {
        Assert.Equal(TimeSpan.FromSeconds(160), Backoff.Delay(Options(), 5));
        Assert.Equal(TimeSpan.FromMinutes(5), Backoff.Delay(Options(), 6));
        Assert.Equal(TimeSpan.FromMinutes(5), Backoff.Delay(Options(), 50));
    }

    [Fact]
    public void Multiplier_one_keeps_initial_delay()
    {
        Assert.All(
            Enumerable.Range(1, 10).Select(n => Backoff.Delay(Options(multiplier: 1.0), n)),
            d => Assert.Equal(TimeSpan.FromSeconds(10), d));
    }

    [Theory]
    [InlineData(10_000)]
    [InlineData(int.MaxValue)]
    public void Huge_n_returns_max_delay(int n)
    {
        Assert.Equal(TimeSpan.FromMinutes(5), Backoff.Delay(Options(), n));
    }

    [Fact]
    public void NaN_returns_max_delay()
    {
        var options = Options(multiplier: double.NaN);

        Assert.Equal(TimeSpan.FromMinutes(5), Backoff.Delay(options, 2));
    }
}
