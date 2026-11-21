using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Leadership;
using Pernix.OutboxRelay.Core.Stats;
using Microsoft.Extensions.Time.Testing;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Stats;

public sealed class OutboxStatsCacheTests
{
    private static readonly OutboxStats Reading = new(5, 2, 1, TimeSpan.FromSeconds(3), GateLag.Of(TimeSpan.FromSeconds(4)));

    private readonly FakeTimeProvider time = new();
    private readonly OutboxRuntimeStatus leader = new("Orders") { IsLeader = true };

    [Fact]
    public void Empty_cache_has_no_fresh_reading()
    {
        var cache = new OutboxStatsCache(leader, time);

        Assert.False(cache.TryGetFresh(out _));
    }

    [Fact]
    public void Reading_is_fresh_up_to_30_s_and_stale_after()
    {
        var cache = new OutboxStatsCache(leader, time);
        cache.Set(Reading);

        Assert.True(cache.TryGetFresh(out var stats));
        Assert.Equal(Reading, stats);

        time.Advance(TimeSpan.FromSeconds(30));
        Assert.True(cache.TryGetFresh(out _));

        time.Advance(TimeSpan.FromMilliseconds(1));
        Assert.False(cache.TryGetFresh(out _));
    }

    [Fact]
    public void Set_restarts_the_staleness_clock()
    {
        var cache = new OutboxStatsCache(leader, time);
        cache.Set(Reading);
        time.Advance(TimeSpan.FromSeconds(25));
        cache.Set(Reading with { Pending = 9 });
        time.Advance(TimeSpan.FromSeconds(25));

        Assert.True(cache.TryGetFresh(out var stats));
        Assert.Equal(9, stats.Pending);
    }

    [Fact]
    public void Cleared_cache_has_no_fresh_reading()
    {
        var cache = new OutboxStatsCache(leader, time);
        cache.Set(Reading);

        cache.Clear();

        Assert.False(cache.TryGetFresh(out _));
    }

    [Fact]
    public void Tags_are_built_once_with_the_outbox_tag_only()
    {
        var cache = new OutboxStatsCache(leader, time);

        var first = cache.Tags;
        cache.Set(Reading);
        cache.Clear();

        Assert.Same(first, cache.Tags);
        Assert.Equal([new KeyValuePair<string, object?>("outbox", "Orders")], first);
        Assert.Equal("Orders", cache.Outbox);
    }

    [Fact]
    public void Not_leader_has_no_fresh_reading()
    {
        var cache = new OutboxStatsCache(leader, time);
        cache.Set(Reading);

        leader.IsLeader = false;
        Assert.False(cache.TryGetFresh(out _));

        leader.IsLeader = true;
        Assert.True(cache.TryGetFresh(out _));
    }
}
