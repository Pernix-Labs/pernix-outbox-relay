using System.Collections.Concurrent;
using System.Runtime.ExceptionServices;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Core.Delivery;
using Pernix.OutboxRelay.Core.Leadership;
using Pernix.OutboxRelay.Unit.Tests.Leadership;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Delivery;

[CollectionDefinition(nameof(DeliveryTermWakeRaceTests), DisableParallelization = true)]
public sealed class DeliveryTermWakeRaceCollection;

// Base 2e8f4c0 aborted (exit 134) when a cancelled WaitToReadAsync(token) was completed again by a send result (dotnet/runtime#132230).
// The exception hooks are process-wide, so this runs alone and counts only exceptions attributable to the term under test.
[Collection(nameof(DeliveryTermWakeRaceTests))]
public sealed class DeliveryTermWakeRaceTests
{
    private const int Lanes = 8;
    private const int TermsPerLane = 60;
    private const int RowsPerTerm = 1_000;
    private static readonly TimeSpan TermBound = TimeSpan.FromSeconds(10);

    private readonly ConcurrentQueue<Exception> unhandled = new();
    private readonly ConcurrentQueue<Exception> unobserved = new();
    private readonly ConcurrentQueue<Exception> invalidOperation = new();

    [Fact]
    public async Task Timer_lost_stopping_and_send_results_racing_the_wait_end_every_term_cleanly()
    {
        // Finalize other tests' unobserved faulted tasks before the hooks go on.
        GC.Collect();
        GC.WaitForPendingFinalizers();
        GC.Collect();

        AppDomain.CurrentDomain.UnhandledException += OnUnhandled;
        TaskScheduler.UnobservedTaskException += OnUnobserved;
        AppDomain.CurrentDomain.FirstChanceException += OnFirstChance;
        int[] counts;
        try
        {
            counts = await Task.WhenAll(Enumerable.Range(0, Lanes).Select(lane => Task.Run(() => RunLaneAsync(lane))));
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
        }
        finally
        {
            AppDomain.CurrentDomain.FirstChanceException -= OnFirstChance;
            TaskScheduler.UnobservedTaskException -= OnUnobserved;
            AppDomain.CurrentDomain.UnhandledException -= OnUnhandled;
        }

        Assert.True(counts.Sum() > Lanes * TermsPerLane, $"only {counts.Sum()} sends");
        Assert.True(unhandled.IsEmpty, Describe("unhandled", unhandled));
        Assert.True(unobserved.IsEmpty, Describe("unobserved", unobserved));
        Assert.True(invalidOperation.IsEmpty, Describe("InvalidOperationException", invalidOperation));
    }

    private static async Task<int> RunLaneAsync(int lane)
    {
        var random = new Random(lane);
        var sends = 0;
        for (var i = 0; i < TermsPerLane; i++)
        {
            sends += await RunTermAsync(lane, i, random).ConfigureAwait(false);
        }

        return sends;
    }

    private static async Task<int> RunTermAsync(int lane, int index, Random random)
    {
        var outbox = $"race-{lane}-{index}-{Guid.NewGuid():N}";

        // A 1 ms poll fires the wait timer about as often as the loop arms a wait.
        var options = new OutboxOptions
        {
            Name = outbox,
            Polling = new PollingOptions { Interval = TimeSpan.FromMilliseconds(1), BatchSize = 10 },
        };
        var time = TimeProvider.System;
        await using var session = new FakeOutboxSession(time);
        session.Insert([.. Enumerable.Range(1, RowsPerTerm).Select(id => (long)id)]);
        var destination = new PoolCompletingDestination();
        using var stopping = new CancellationTokenSource();
        var sut = new DeliveryTerm(
            outbox,
            options,
            "events",
            () => destination,
            new UnorderedDelivery(options.Polling.BatchSize),
            new OutboxRuntimeStatus(outbox),
            time,
            new RecordingLogger<DeliveryTerm>());

        var term = sut.RunAsync(session, stopping.Token);
        await Task.Delay(random.Next(5, 40), TestContext.Current.CancellationToken).ConfigureAwait(false);

        // 0: Lost, 1: stopping, 2: both at once.
        var end = random.Next(3);
        using var go = new ManualResetEventSlim();
        var lost = end != 1 ? Task.Run(() => { go.Wait(); session.SignalLost(); }) : Task.CompletedTask;
        var stop = end != 0 ? Task.Run(() => { go.Wait(); stopping.Cancel(); }) : Task.CompletedTask;
        go.Set();
        await Task.WhenAll(lost, stop).ConfigureAwait(false);

        var result = await term.WaitAsync(TermBound, TestContext.Current.CancellationToken).ConfigureAwait(false);
        switch (end)
        {
            case 0:
                Assert.IsType<DeliveryTermResult.Lost>(result);
                break;
            case 1:
                Assert.IsType<DeliveryTermResult.Stopped>(result);
                break;
            default:
                Assert.True(result is DeliveryTermResult.Lost or DeliveryTermResult.Stopped, $"unexpected {result}");
                break;
        }

        return destination.Sends;
    }

    private static bool FromTermUnderTest(string trace) =>
        trace.Contains(nameof(DeliveryTerm), StringComparison.Ordinal)
        || trace.Contains(nameof(PoolCompletingDestination), StringComparison.Ordinal);

    private static string Describe(string kind, ConcurrentQueue<Exception> seen) =>
        $"{seen.Count} {kind} exception(s) from the term under test:{Environment.NewLine}{string.Join(Environment.NewLine, seen)}";

    private void OnUnhandled(object? sender, UnhandledExceptionEventArgs e)
    {
        var exception = e.ExceptionObject as Exception ?? new InvalidOperationException($"non-exception {e.ExceptionObject}");
        if (FromTermUnderTest(exception.ToString()))
        {
            unhandled.Enqueue(exception);
        }
    }

    private void OnUnobserved(object? sender, UnobservedTaskExceptionEventArgs e)
    {
        if (FromTermUnderTest(e.Exception.ToString()))
        {
            unobserved.Enqueue(e.Exception);
        }
    }

    private void OnFirstChance(object? sender, FirstChanceExceptionEventArgs e)
    {
        if (e.Exception is InvalidOperationException && FromTermUnderTest(Environment.StackTrace))
        {
            invalidOperation.Enqueue(new InvalidOperationException($"{e.Exception.Message}{Environment.NewLine}{Environment.StackTrace}", e.Exception));
        }
    }

    // Results land on pool threads while the loop arms a wait.
    private sealed class PoolCompletingDestination : IDestination
    {
        private static readonly DeliveryResult Delivered = new DeliveryResult.Delivered();
        private int sends;

        public TimeSpan? SendBound => null;

        public IReadOnlySet<string>? Routes => null;

        public int Sends => Volatile.Read(ref sends);

        public Task ValidateAsync(CancellationToken ct) => Task.CompletedTask;

        public ValueTask<DeliveryResult> SendAsync(OutgoingMessage message, CancellationToken ct)
        {
            Interlocked.Increment(ref sends);
            return new ValueTask<DeliveryResult>(Task.Run(static () =>
            {
                Thread.SpinWait(Random.Shared.Next(0, 2_000));
                return Delivered;
            }, CancellationToken.None));
        }

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
