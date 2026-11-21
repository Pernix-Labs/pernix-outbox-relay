using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Leadership;
using Pernix.OutboxRelay.Core.Stats;
using Pernix.OutboxRelay.Unit.Tests.Leadership;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Stats;

public sealed class OutboxStatsLoopTests : IDisposable
{
    private const string Outbox = "Orders";
    private static readonly TimeSpan Step = TimeSpan.FromSeconds(1);
    private static readonly OutboxStats Reading = new(10, 2, 1, TimeSpan.FromSeconds(4), GateLag.Of(TimeSpan.FromSeconds(1)));

    private readonly WorkerClock time = new();
    private readonly FakeOutboxStore store;
    private readonly OutboxRuntimeStatus status = new(Outbox);
    private readonly OutboxStatsCache cache;
    private readonly RecordingLogger<OutboxStatsService> logger = new();
    private readonly CancellationTokenSource stopping = new();
    private readonly List<TimeSpan> reads = [];
    private Func<CancellationToken, Task<OutboxStats>> stats = static _ => Task.FromResult(Reading);
    private Task? loop;

    public OutboxStatsLoopTests()
    {
        store = new FakeOutboxStore(time);
        store.Stats = ct =>
        {
            lock (reads)
            {
                reads.Add(store.Now);
            }

            return stats(ct);
        };
        cache = new OutboxStatsCache(status, time);
    }

    public void Dispose()
    {
        stopping.Cancel();
        stopping.Dispose();
    }

    [Fact]
    public async Task Not_leader_never_reads()
    {
        await StartAsync();

        await AdvanceAsync(TimeSpan.FromSeconds(40));

        Assert.Equal(0, store.StatsCalls);
        Assert.False(cache.TryGetFresh(out _));
    }

    [Fact]
    public async Task New_leader_reads_on_the_next_tick_then_once_per_cadence()
    {
        await StartAsync();
        await AdvanceAsync(TimeSpan.FromSeconds(5));
        Assert.Equal(0, store.StatsCalls);

        status.IsLeader = true;
        await AdvanceAsync(Step);
        Assert.Equal([TimeSpan.FromSeconds(6)], Reads());
        Assert.True(cache.TryGetFresh(out var cached));
        Assert.Equal(Reading, cached);

        await AdvanceAsync(TimeSpan.FromSeconds(14));
        Assert.Equal(1, store.StatsCalls);

        await AdvanceAsync(TimeSpan.FromSeconds(16));
        Assert.Equal([TimeSpan.FromSeconds(6), TimeSpan.FromSeconds(21), TimeSpan.FromSeconds(36)], Reads());
    }

    [Fact]
    public async Task Lost_leadership_clears_cache_and_stops_reads()
    {
        status.IsLeader = true;
        await StartAsync();
        await AdvanceAsync(Step);
        Assert.True(cache.TryGetFresh(out _));

        status.IsLeader = false;
        await AdvanceAsync(Step);
        Assert.False(cache.TryGetFresh(out _));

        await AdvanceAsync(TimeSpan.FromSeconds(30));
        Assert.Equal(1, store.StatsCalls);
    }

    [Fact]
    public async Task Result_returned_after_leadership_loss_is_dropped()
    {
        var pending = new TaskCompletionSource<OutboxStats>(TaskCreationOptions.RunContinuationsAsynchronously);
        stats = _ => pending.Task;
        status.IsLeader = true;
        await StartAsync();
        await AdvanceAsync(Step);
        Assert.Equal(1, store.StatsCalls);

        var mark = time.Created;
        status.IsLeader = false;
        pending.SetResult(Reading);
        await time.WaitForTimerAsync(OutboxStatsLoop.Tick, mark);

        Assert.False(cache.TryGetFresh(out _));
        Assert.Empty(logger.Entries);
    }

    [Fact]
    public async Task Failure_run_logs_one_130_and_recovery_one_131()
    {
        status.IsLeader = true;
        await StartAsync();
        await AdvanceAsync(Step);
        Assert.True(cache.TryGetFresh(out _));

        stats = _ => Task.FromException<OutboxStats>(new OutboxStatsUnavailableException("Orders", "connection refused"));
        await AdvanceAsync(TimeSpan.FromSeconds(15));
        Assert.False(cache.TryGetFresh(out _));

        await AdvanceAsync(TimeSpan.FromSeconds(30));
        Assert.Equal(4, store.StatsCalls);
        var unavailable = Assert.Single(logger.WithEvent(130));
        Assert.Equal("Outbox stats unavailable for outbox Orders: connection refused", unavailable.Message);
        Assert.Empty(logger.WithEvent(131));

        stats = _ => Task.FromResult(Reading);
        await AdvanceAsync(TimeSpan.FromSeconds(15));
        Assert.True(cache.TryGetFresh(out _));
        var available = Assert.Single(logger.WithEvent(131));
        Assert.Equal("Outbox stats available again for outbox Orders", available.Message);

        await AdvanceAsync(TimeSpan.FromSeconds(30));
        Assert.Equal([130, 131], logger.Entries.Select(e => e.EventId.Id));
    }

    [Fact]
    public async Task First_failure_after_start_logs_130()
    {
        stats = _ => Task.FromException<OutboxStats>(new OutboxStatsUnavailableException("Orders", "read-only standby"));
        status.IsLeader = true;
        await StartAsync();

        await AdvanceAsync(Step);

        var entry = Assert.Single(logger.Entries);
        Assert.Equal(130, entry.EventId.Id);
        Assert.Equal(Microsoft.Extensions.Logging.LogLevel.Warning, entry.Level);
        Assert.False(cache.TryGetFresh(out _));
    }

    [Fact]
    public async Task Unexpected_exception_logs_type_name_only()
    {
        stats = _ => throw new InvalidOperationException("Password=sentinel-secret");
        status.IsLeader = true;
        await StartAsync();

        await AdvanceAsync(Step);

        var entry = Assert.Single(logger.WithEvent(130));
        Assert.Equal("Outbox stats unavailable for outbox Orders: InvalidOperationException", entry.Message);
        Assert.False(Loop.IsCompleted);
    }

    [Fact]
    public async Task Fault_outside_the_tick_logs_130_with_the_exception_type()
    {
        status.IsLeader = true;

        await StartAsync(new FaultingClock());

        Assert.True(Loop.IsCompletedSuccessfully);
        var entry = Assert.Single(logger.Entries);
        Assert.Equal(130, entry.EventId.Id);
        Assert.Equal("Outbox stats unavailable for outbox Orders: stats loop faulted: TimerFaultException", entry.Message);
        Assert.Equal(0, store.StatsCalls);
        Assert.False(cache.TryGetFresh(out _));
    }

    [Fact]
    public async Task Wall_clock_step_back_keeps_the_cadence()
    {
        var stepping = new SteppingClock(time);
        status.IsLeader = true;
        await StartAsync(stepping);
        await AdvanceAsync(Step);
        Assert.Equal([TimeSpan.FromSeconds(1)], Reads());

        stepping.Back = TimeSpan.FromHours(1);
        await AdvanceAsync(TimeSpan.FromSeconds(45));

        Assert.True(stepping.GetUtcNow() < store.Start);
        Assert.Equal([TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(16), TimeSpan.FromSeconds(31), TimeSpan.FromSeconds(46)], Reads());
    }

    [Fact]
    public async Task Missing_gate_lag_logs_132_once_and_caches_other_values()
    {
        var reading = Reading with { GateLag = GateLag.Unavailable("role lacks pg_monitor (pg_read_all_stats)") };
        stats = _ => Task.FromResult(reading);
        status.IsLeader = true;
        await StartAsync();

        await AdvanceAsync(TimeSpan.FromSeconds(46));

        Assert.Equal(4, store.StatsCalls);
        var entry = Assert.Single(logger.Entries);
        Assert.Equal(132, entry.EventId.Id);
        Assert.Equal(Microsoft.Extensions.Logging.LogLevel.Warning, entry.Level);
        Assert.Equal("Outbox gate lag unavailable for outbox Orders: role lacks pg_monitor (pg_read_all_stats)", entry.Message);
        Assert.True(cache.TryGetFresh(out var cached));
        Assert.Equal(reading, cached);
    }

    [Fact]
    public async Task Hung_store_fails_the_tick_after_read_bound()
    {
        stats = _ => new TaskCompletionSource<OutboxStats>().Task;
        status.IsLeader = true;
        await StartAsync();
        await AdvanceAsync(Step);
        Assert.Equal(1, store.StatsCalls);

        await AdvanceAsync(TimeSpan.FromSeconds(9));
        Assert.Empty(logger.Entries);

        await AdvanceAsync(Step);
        var entry = Assert.Single(logger.WithEvent(130));
        Assert.Equal("Outbox stats unavailable for outbox Orders: stats read exceeded 10 s", entry.Message);

        await AdvanceAsync(TimeSpan.FromSeconds(5));
        Assert.Equal([TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(16)], Reads());
    }

    [Fact]
    public async Task Stop_completes_at_once_during_a_hung_read()
    {
        stats = _ => new TaskCompletionSource<OutboxStats>().Task;
        status.IsLeader = true;
        await StartAsync();
        await AdvanceAsync(Step);
        Assert.Equal(1, store.StatsCalls);

        await stopping.CancelAsync();

        await Loop.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        Assert.True(Loop.IsCompletedSuccessfully);
        Assert.Empty(logger.Entries);
        Assert.False(cache.TryGetFresh(out _));
    }

    [Fact]
    public async Task Stop_clears_the_cache()
    {
        status.IsLeader = true;
        await StartAsync();
        await AdvanceAsync(Step);
        Assert.True(cache.TryGetFresh(out _));

        await stopping.CancelAsync();
        await Loop.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.False(cache.TryGetFresh(out _));
    }

    [Fact]
    public async Task Reads_never_touch_health_state()
    {
        var dueBefore = status.ProgressDueAt;
        stats = _ => Task.FromException<OutboxStats>(new OutboxStatsUnavailableException("Orders", "x"));
        status.IsLeader = true;
        await StartAsync();

        await AdvanceAsync(TimeSpan.FromSeconds(20));

        Assert.Equal(dueBefore, status.ProgressDueAt);
        Assert.True(status.DatabaseReachable);
        Assert.Null(status.Loop);
    }

    private Task Loop => loop!;

    private TimeSpan[] Reads()
    {
        lock (reads)
        {
            return [.. reads];
        }
    }

    private async Task StartAsync(TimeProvider? loopTime = null)
    {
        var statsLoop = new OutboxStatsLoop(Outbox, store, status, cache, loopTime ?? time, logger);
        loop = statsLoop.RunAsync(stopping.Token);
        await WorkerClock.WaitUntilAsync(() => time.Active > 0 || Loop.IsCompleted);
    }

    private Task AdvanceAsync(TimeSpan total) => time.AdvanceAsync(total, Step, () => Loop.IsCompleted);

    // Wall clock shifted back by Back. Timers and the monotonic timestamp follow the inner clock.
    private sealed class SteppingClock(WorkerClock inner) : TimeProvider
    {
        public TimeSpan Back { get; set; }

        public override DateTimeOffset GetUtcNow() => inner.GetUtcNow() - Back;

        public override long GetTimestamp() => inner.GetTimestamp();

        public override long TimestampFrequency => inner.TimestampFrequency;

        public override TimeZoneInfo LocalTimeZone => inner.LocalTimeZone;

        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period) =>
            inner.CreateTimer(callback, state, dueTime, period);
    }

    // Fails the loop's own wait, outside the per-tick catch.
    private sealed class FaultingClock : TimeProvider
    {
        public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period) =>
            throw new TimerFaultException();
    }

    private sealed class TimerFaultException : Exception;
}
