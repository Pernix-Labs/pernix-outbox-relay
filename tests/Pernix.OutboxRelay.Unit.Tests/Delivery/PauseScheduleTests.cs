using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Core.Delivery;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Delivery;

public sealed class PauseScheduleTests
{
    private static PauseSchedule Schedule(double multiplier = 2.0) => new(new RetryOptions
    {
        InitialDelay = TimeSpan.FromSeconds(10),
        BackoffMultiplier = multiplier,
        MaxDelay = TimeSpan.FromMinutes(5),
    });

    [Fact]
    public void Pause_n_grows_by_multiplier_from_initial_delay()
    {
        var schedule = Schedule();

        double[] expected = [10, 20, 40, 80, 160];
        Assert.Equal(expected.Select(TimeSpan.FromSeconds), Enumerable.Range(0, 5).Select(_ => schedule.Next()));
    }

    [Fact]
    public void Pause_saturates_at_max_delay_without_overflow()
    {
        var schedule = Schedule();

        var pauses = Enumerable.Range(0, 10_000).Select(_ => schedule.Next()).ToList();

        Assert.Equal(TimeSpan.FromMinutes(5), pauses[5]);
        Assert.All(pauses.Skip(5), p => Assert.Equal(TimeSpan.FromMinutes(5), p));
    }

    [Fact]
    public void Reset_starts_again_at_initial_delay()
    {
        var schedule = Schedule();
        schedule.Next();
        schedule.Next();

        schedule.Reset();

        Assert.Equal(TimeSpan.FromSeconds(10), schedule.Next());
        Assert.Equal(TimeSpan.FromSeconds(20), schedule.Next());
    }

    [Fact]
    public void Multiplier_one_keeps_initial_delay()
    {
        var schedule = Schedule(multiplier: 1.0);

        Assert.All(Enumerable.Range(0, 5).Select(_ => schedule.Next()), p => Assert.Equal(TimeSpan.FromSeconds(10), p));
    }
}
