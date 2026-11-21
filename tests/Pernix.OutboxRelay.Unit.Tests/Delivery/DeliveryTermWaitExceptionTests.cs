using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Core.Delivery;
using Pernix.OutboxRelay.Core.Leadership;
using Pernix.OutboxRelay.Unit.Tests.Leadership;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Delivery;

[CollectionDefinition(nameof(DeliveryTermWaitExceptionTests), DisableParallelization = true)]
public sealed class DeliveryTermWaitExceptionCollection;

// First-chance exceptions are process-wide, so this runs alone.
[Collection(nameof(DeliveryTermWaitExceptionTests))]
public sealed class DeliveryTermWaitExceptionTests
{
    private const int Waits = 60;
    private int canceled;

    [Fact]
    public async Task Idle_waits_throw_no_operation_canceled_exception()
    {
        var outbox = $"orders-{Guid.NewGuid():N}";
        var options = new OutboxOptions
        {
            Name = outbox,
            Polling = new PollingOptions { Interval = TimeSpan.FromSeconds(1), BatchSize = 10 },
        };
        var time = new WorkerClock();
        var session = new FakeOutboxSession(time);
        var destination = new FakeDestination();
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

        AppDomain.CurrentDomain.FirstChanceException += Count;
        try
        {
            var term = sut.RunAsync(session, stopping.Token);
            await time.WaitForTimerAsync(TimeSpan.FromSeconds(1), 0);

            await time.AdvanceAsync(TimeSpan.FromSeconds(Waits), TimeSpan.FromSeconds(1), () => term.IsCompleted);
            Assert.False(term.IsCompleted);
            Assert.True(session.Reads.Count >= Waits, $"only {session.Reads.Count} reads");

            await stopping.CancelAsync();
            Assert.IsType<DeliveryTermResult.Stopped>(await term.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken));
        }
        finally
        {
            AppDomain.CurrentDomain.FirstChanceException -= Count;
        }

        Assert.Equal(0, Volatile.Read(ref canceled));
    }

    private void Count(object? sender, System.Runtime.ExceptionServices.FirstChanceExceptionEventArgs e)
    {
        if (e.Exception is OperationCanceledException && Environment.StackTrace.Contains(nameof(DeliveryTerm), StringComparison.Ordinal))
        {
            Interlocked.Increment(ref canceled);
        }
    }
}
