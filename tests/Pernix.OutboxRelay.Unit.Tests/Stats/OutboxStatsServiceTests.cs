using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Core.Leadership;
using Pernix.OutboxRelay.Core.Stats;
using Pernix.OutboxRelay.Unit.Tests.Configuration;
using Pernix.OutboxRelay.Unit.Tests.Leadership;
using Pernix.OutboxRelay.Unit.Tests.Telemetry;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Stats;

public sealed class OutboxStatsServiceTests : IDisposable
{
    private static readonly TimeSpan Step = TimeSpan.FromSeconds(1);
    private static readonly OutboxStats Reading = new(10, 2, 1, TimeSpan.FromSeconds(4), GateLag.Of(TimeSpan.FromSeconds(1)));

    private readonly WorkerClock time = new();
    private readonly string prefix = Guid.NewGuid().ToString("N");
    private readonly ValidatedOutboxes outboxes = new();
    private readonly OutboxRuntimeState state = new();
    private readonly FakeLifetime lifetime = new();
    private readonly List<FakeOutboxStore> stores = [];

    public void Dispose() => lifetime.Dispose();

    [Fact]
    public async Task Starts_one_loop_per_outbox()
    {
        AddOutboxes(2);
        using var service = NewService();

        await service.StartAsync(TestContext.Current.CancellationToken);
        await AdvanceAsync(Step);

        Assert.All(stores, s => Assert.Equal(1, s.StatsCalls));
        Assert.Equal(
            [$"{prefix}-0", $"{prefix}-1"],
            OutboxStatsGaugesTests.Collect(prefix).Where(m => m.Instrument == "outbox.pending").Select(m => m.Outbox).Order(StringComparer.Ordinal));

        await service.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Application_stopping_ends_the_loops()
    {
        AddOutboxes(1);
        using var service = NewService();
        await service.StartAsync(TestContext.Current.CancellationToken);
        await AdvanceAsync(Step);
        Assert.NotEmpty(OutboxStatsGaugesTests.Collect(prefix));

        await lifetime.Stopping.CancelAsync();

        await WorkerClock.WaitUntilAsync(() => OutboxStatsGaugesTests.Collect(prefix).Count == 0);
        time.Advance(TimeSpan.FromSeconds(30));
        Assert.Equal(1, stores[0].StatsCalls);

        await service.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Stop_returns_with_a_hung_store_and_unregisters_caches()
    {
        AddOutboxes(2);
        foreach (var store in stores)
        {
            store.Stats = _ => new TaskCompletionSource<OutboxStats>().Task;
        }

        using var service = NewService();
        await service.StartAsync(TestContext.Current.CancellationToken);
        await AdvanceAsync(Step);
        Assert.All(stores, s => Assert.Equal(1, s.StatsCalls));

        await service.StopAsync(CancellationToken.None).WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        AssertUnregistered(service);
    }

    [Fact]
    public async Task Dispose_without_stop_unregisters_caches()
    {
        AddOutboxes(1);
        var service = NewService();
        await service.StartAsync(TestContext.Current.CancellationToken);

        service.Dispose();
        service.Dispose();
        await service.StopAsync(TestContext.Current.CancellationToken);

        AssertUnregistered(service);
    }

    [Fact]
    public async Task Pairs_statuses_by_name_not_position()
    {
        AddOutboxes(2);
        var statuses = state.Outboxes;
        state.SetOutboxes([statuses[1], statuses[0]]);
        statuses[1].IsLeader = false;
        using var service = NewService();

        await service.StartAsync(TestContext.Current.CancellationToken);
        await AdvanceAsync(Step);

        Assert.Equal(1, stores[0].StatsCalls);
        Assert.Equal(0, stores[1].StatsCalls);
        Assert.Equal([$"{prefix}-0", $"{prefix}-1"], service.Caches.Select(c => c.Outbox));

        await service.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Missing_status_name_fails_start_naming_the_outbox()
    {
        AddOutboxes(1);
        state.SetOutboxes([new OutboxRuntimeStatus("Other") { IsLeader = true }]);
        using var service = NewService();

        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => service.StartAsync(TestContext.Current.CancellationToken));

        Assert.Contains($"'{prefix}-0'", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task No_outboxes_starts_no_loops()
    {
        using var service = NewService();

        await service.StartAsync(TestContext.Current.CancellationToken);
        await service.StopAsync(TestContext.Current.CancellationToken);

        Assert.Empty(service.Caches);
    }

    private void AssertUnregistered(OutboxStatsService service)
    {
        Assert.NotEmpty(service.Caches);
        foreach (var cache in service.Caches)
        {
            cache.Set(Reading);
        }

        Assert.Empty(OutboxStatsGaugesTests.Collect(prefix));
    }

    private void AddOutboxes(int count)
    {
        var statuses = new OutboxRuntimeStatus[count];
        for (var i = 0; i < count; i++)
        {
            var name = $"{prefix}-{i}";
            var store = new FakeOutboxStore(time) { Stats = _ => Task.FromResult(Reading) };
            stores.Add(store);
            outboxes.Add(new ValidatedOutbox(new OutboxOptions { Name = name }, store, new DestinationOptions { Name = "Events" }, new FakeDestinationFactory()));
            statuses[i] = new OutboxRuntimeStatus(name) { IsLeader = true };
        }

        state.SetOutboxes(statuses);
    }

    private OutboxStatsService NewService() => new(outboxes, state, lifetime, time, new RecordingLogger<OutboxStatsService>());

    private async Task AdvanceAsync(TimeSpan total)
    {
        await WorkerClock.WaitUntilAsync(() => time.Active >= stores.Count);
        await time.AdvanceAsync(total, Step, static () => false);
        await WorkerClock.WaitUntilAsync(() => time.Active >= stores.Count);
    }

    private sealed class FakeLifetime : IHostApplicationLifetime, IDisposable
    {
        public CancellationTokenSource Started { get; } = new();
        public CancellationTokenSource Stopping { get; } = new();
        public CancellationTokenSource Stopped { get; } = new();

        public CancellationToken ApplicationStarted => Started.Token;
        public CancellationToken ApplicationStopping => Stopping.Token;
        public CancellationToken ApplicationStopped => Stopped.Token;

        public void StopApplication() => Stopping.Cancel();

        public void Dispose()
        {
            Started.Dispose();
            Stopping.Dispose();
            Stopped.Dispose();
        }
    }
}
