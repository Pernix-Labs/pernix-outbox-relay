using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Core.Leadership;
using Pernix.OutboxRelay.Hosting.Health;
using Pernix.OutboxRelay.Unit.Tests.Configuration;
using Pernix.OutboxRelay.Unit.Tests.Leadership;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Health;

public sealed class WorkerLoopLivenessCheckTests
{
    private readonly WorkerClock time = new();

    [Fact]
    public async Task No_outboxes_is_healthy()
    {
        using var lifetime = new FakeLifetime();

        Assert.Equal(HealthStatus.Healthy, await CheckAsync(new OutboxRuntimeState(), lifetime));
    }

    [Fact]
    public async Task Running_loops_are_healthy()
    {
        using var lifetime = new FakeLifetime();
        var running = new TaskCompletionSource();
        var state = StateWith(new OutboxRuntimeStatus("a") { Loop = running.Task }, new OutboxRuntimeStatus("b") { Loop = running.Task });

        Assert.Equal(HealthStatus.Healthy, await CheckAsync(state, lifetime));
    }

    [Fact]
    public async Task Completed_loop_while_not_stopping_is_unhealthy()
    {
        using var lifetime = new FakeLifetime();
        var running = new TaskCompletionSource();
        var state = StateWith(new OutboxRuntimeStatus("a") { Loop = running.Task }, new OutboxRuntimeStatus("b") { Loop = Task.CompletedTask });

        Assert.Equal(HealthStatus.Unhealthy, await CheckAsync(state, lifetime));
    }

    [Fact]
    public async Task Faulted_loop_while_not_stopping_is_unhealthy()
    {
        using var lifetime = new FakeLifetime();
        var faulted = Task.FromException(new InvalidOperationException("boom"));
        var state = StateWith(new OutboxRuntimeStatus("a") { Loop = faulted });

        Assert.Equal(HealthStatus.Unhealthy, await CheckAsync(state, lifetime));

        // Observe the fault so the finalizer does not raise it into another test's UnobservedTaskException hook.
        Assert.IsType<InvalidOperationException>(faulted.Exception?.InnerException);
    }

    [Fact]
    public async Task Completed_loop_while_stopping_is_healthy()
    {
        using var lifetime = new FakeLifetime();
        await lifetime.Stopping.CancelAsync();
        var state = StateWith(new OutboxRuntimeStatus("a") { Loop = Task.CompletedTask });

        Assert.Equal(HealthStatus.Healthy, await CheckAsync(state, lifetime));
    }

    [Fact]
    public async Task Database_unreachable_does_not_affect_liveness()
    {
        using var lifetime = new FakeLifetime();
        var running = new TaskCompletionSource();
        var state = StateWith(new OutboxRuntimeStatus("a") { Loop = running.Task, DatabaseReachable = false });

        Assert.Equal(HealthStatus.Healthy, await CheckAsync(state, lifetime));
    }

    [Fact]
    public async Task Progress_due_passed_is_unhealthy_naming_outbox()
    {
        using var lifetime = new FakeLifetime();
        var running = new TaskCompletionSource();
        var stuck = new OutboxRuntimeStatus("Orders") { Loop = running.Task };
        var fine = new OutboxRuntimeStatus("Payments") { Loop = running.Task };
        var state = StateWith(stuck, fine);
        stuck.ExpectProgress(TimeSpan.FromSeconds(5), time);

        time.Advance(TimeSpan.FromSeconds(15));
        Assert.Equal(HealthStatus.Healthy, await CheckAsync(state, lifetime));

        time.Advance(TimeSpan.FromMilliseconds(1));
        var result = await CheckResultAsync(state, lifetime);

        Assert.Equal(HealthStatus.Unhealthy, result.Status);
        Assert.Equal("Outbox worker progress overdue: Orders", result.Description);
    }

    [Fact]
    public async Task Progress_due_passed_while_stopping_is_healthy()
    {
        using var lifetime = new FakeLifetime();
        var running = new TaskCompletionSource();
        var status = new OutboxRuntimeStatus("Orders") { Loop = running.Task };
        var state = StateWith(status);
        status.ExpectProgress(TimeSpan.FromSeconds(5), time);
        time.Advance(TimeSpan.FromMinutes(1));

        await lifetime.Stopping.CancelAsync();

        Assert.Equal(HealthStatus.Healthy, await CheckAsync(state, lifetime));
    }

    [Fact]
    public async Task Leader_paused_for_five_minutes_stays_healthy()
    {
        using var lifetime = new FakeLifetime();
        using var stopping = new CancellationTokenSource();
        var store = new FakeOutboxStore(time);
        var factory = new FakeDestinationFactory();
        factory.Probes["Events"] = _ => throw new InvalidOperationException("broker down");
        var status = new OutboxRuntimeStatus("Orders");
        var state = StateWith(status);
        var entry = new ValidatedOutbox(new OutboxOptions { Name = "Orders", Ordering = new OrderingOptions { Mode = OrderingMode.None } }, store, new DestinationOptions { Name = "Events" }, factory);
        var worker = new OutboxWorker(entry, status, state, new FixedJitter(TimeSpan.Zero), time, new RecordingLogger<OutboxWorker>());

        var loop = worker.RunAsync(stopping.Token);
        status.Loop = loop;
        await WorkerClock.WaitUntilAsync(() => time.Active > 0 || loop.IsCompleted);
        Assert.True(status.IsLeader);

        // Pauses grow 10 s, 20 s, ... while the worker keeps stamping at least once a second.
        for (var second = 0; second < 300; second++)
        {
            await time.AdvanceAsync(TimeSpan.FromSeconds(1), TimeSpan.FromMilliseconds(100), () => loop.IsCompleted);
            Assert.Equal(HealthStatus.Healthy, await CheckAsync(state, lifetime));
        }

        Assert.True(status.IsLeader);
        Assert.True(factory.ProbeStarted.Count > 1);

        await stopping.CancelAsync();
        await status.Loop.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
    }

    private static OutboxRuntimeState StateWith(params OutboxRuntimeStatus[] statuses)
    {
        var state = new OutboxRuntimeState();
        state.SetOutboxes(statuses);
        return state;
    }

    private async Task<HealthStatus> CheckAsync(OutboxRuntimeState state, IHostApplicationLifetime lifetime) =>
        (await CheckResultAsync(state, lifetime)).Status;

    private async Task<HealthCheckResult> CheckResultAsync(OutboxRuntimeState state, IHostApplicationLifetime lifetime)
    {
        var check = new WorkerLoopLivenessCheck(state, lifetime, time);
        var context = new HealthCheckContext
        {
            Registration = new HealthCheckRegistration("worker-loop", check, HealthStatus.Unhealthy, [OutboxRelayHealthChecks.LiveTag]),
        };

        return await check.CheckHealthAsync(context, TestContext.Current.CancellationToken);
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
