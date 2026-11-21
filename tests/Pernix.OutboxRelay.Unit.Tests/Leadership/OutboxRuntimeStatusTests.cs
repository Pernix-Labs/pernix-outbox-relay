using Pernix.OutboxRelay.Core.Leadership;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Leadership;

public sealed class OutboxRuntimeStatusTests
{
    private readonly FakeTimeProvider time = new();
    private readonly OutboxRuntimeStatus status = new("Orders");

    [Fact]
    public void Progress_is_not_due_before_any_expectation()
    {
        Assert.Equal(DateTimeOffset.MaxValue, status.ProgressDueAt);
    }

    [Fact]
    public void Due_is_now_plus_bound_plus_10_second_grace()
    {
        var now = time.GetUtcNow();

        status.ExpectProgress(TimeSpan.FromSeconds(5), time);
        Assert.Equal(now + TimeSpan.FromSeconds(15), status.ProgressDueAt);

        time.Advance(TimeSpan.FromSeconds(3));
        status.ExpectProgress(TimeSpan.FromSeconds(1), time);
        Assert.Equal(now + TimeSpan.FromSeconds(14), status.ProgressDueAt);
    }

    [Fact]
    public void Earliest_send_deadline_bounds_due_at_deadline_plus_10_seconds()
    {
        var deadline = time.GetUtcNow() + TimeSpan.FromSeconds(30);
        status.TrackSendDeadline(deadline);

        time.Advance(TimeSpan.FromSeconds(35));
        status.ExpectProgress(TimeSpan.FromSeconds(1), time);
        Assert.Equal(deadline + TimeSpan.FromSeconds(10), status.ProgressDueAt);

        status.TrackSendDeadline(null);
        Assert.Equal(deadline + TimeSpan.FromSeconds(16), status.ProgressDueAt);
    }

    [Fact]
    public void Loop_due_wins_when_earlier_than_send_deadline()
    {
        var deadline = time.GetUtcNow() + TimeSpan.FromSeconds(5);
        status.TrackSendDeadline(deadline);
        status.ExpectProgress(TimeSpan.FromSeconds(1), time);

        Assert.Equal(time.GetUtcNow() + TimeSpan.FromSeconds(11), status.ProgressDueAt);
    }

    [Fact]
    public void Send_deadline_at_max_saturates_instead_of_overflowing()
    {
        status.TrackSendDeadline(DateTimeOffset.MaxValue);
        status.ExpectProgress(TimeSpan.FromSeconds(1), time);

        Assert.Equal(time.GetUtcNow() + TimeSpan.FromSeconds(11), status.ProgressDueAt);
    }

    [Fact]
    public void Huge_bound_saturates_instead_of_overflowing()
    {
        status.ExpectProgress(TimeSpan.MaxValue, time);

        Assert.Equal(DateTimeOffset.MaxValue, status.ProgressDueAt);
    }
}
