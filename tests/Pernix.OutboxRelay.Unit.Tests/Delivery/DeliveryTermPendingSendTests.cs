using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Delivery;
using Pernix.OutboxRelay.Core.Diagnostics;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Delivery;

public sealed partial class DeliveryTermTests
{
    private readonly ControlledSendDestination controlled = new();
    private DeliveryTerm? controlledTerm;

    public enum SendFailure
    {
        Throw,
        Faulted,
        Null,
        Canceled,
    }

    [Theory]
    [InlineData(SendFailure.Throw, false)]
    [InlineData(SendFailure.Faulted, false)]
    [InlineData(SendFailure.Faulted, true)]
    [InlineData(SendFailure.Null, false)]
    [InlineData(SendFailure.Null, true)]
    [InlineData(SendFailure.Canceled, false)]
    [InlineData(SendFailure.Canceled, true)]
    public async Task Main_send_failure_with_live_term_becomes_destination_fault(SendFailure failure, bool async)
    {
        using var metrics = new MeasurementRecorder(outbox);
        session.Insert(1);
        Exception? error = failure switch
        {
            SendFailure.Throw or SendFailure.Faulted => new InvalidOperationException("secret-payload-text"),
            SendFailure.Canceled => new OperationCanceledException(),
            _ => null,
        };
        var expected = error?.GetType().Name ?? DeliveryTerm.UnknownResultError;
        if (failure == SendFailure.Throw)
        {
            controlled.ThrowOnSend = error;
        }
        else if (!async)
        {
            controlled.CompleteInline = true;
            controlled.InlineResult = null;
            controlled.InlineError = error;
        }

        await StartControlledAsync();
        if (async)
        {
            await WaitUntilAsync(() => controlled.Pending.Count == 1);
            if (error is null)
            {
                controlled.Complete(1, null);
            }
            else
            {
                controlled.Fail(1, error);
            }
        }

        await WaitUntilAsync(() => logger.WithEvent(110).Any());

        Assert.Contains(expected, Assert.Single(logger.WithEvent(113)).Message, StringComparison.Ordinal);
        Assert.Contains($"DestinationFault: {expected}", Assert.Single(logger.WithEvent(110)).Message, StringComparison.Ordinal);
        Assert.DoesNotContain(logger.Entries, e => e.Message.Contains("secret-payload-text", StringComparison.Ordinal));
        Assert.Equal([(DeliveryMetrics.DestinationFaultsName, Destination)], metrics.Tagged);
        Assert.Equal(1, controlled.Sends);
        Assert.Equal(async ? 1 : 0, controlledTerm!.PendingCreated);
        Assert.Equal([1L], session.TableIds);
        Assert.False(Term.IsCompleted);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Delivered_main_send_is_counted_and_committed(bool async)
    {
        using var metrics = new MeasurementRecorder(outbox);
        session.Insert(1);
        controlled.CompleteInline = !async;

        await StartControlledAsync();
        if (async)
        {
            await WaitUntilAsync(() => controlled.Pending.Count == 1);
            controlled.Complete(1, new DeliveryResult.Delivered());
        }

        await WaitUntilAsync(() => session.TableIds.Count == 0);

        Assert.Equal(1, metrics.Delivered);
        Assert.Empty(metrics.Tagged);
        Assert.Empty(logger.WithEvent(113));
        Assert.Equal(async ? 1 : 0, controlledTerm!.PendingCreated);
    }

    [Theory]
    [InlineData("token")]
    [InlineData("oce")]
    [InlineData("other")]
    public async Task Term_cancel_with_sends_in_flight_drops_results(string completion)
    {
        using var metrics = new MeasurementRecorder(outbox);
        session.Insert(1, 2, 3);
        controlled.FailOnCancel = completion == "token";

        await StartControlledAsync();
        await WaitUntilAsync(() => controlled.Pending.Count == 3);
        session.SignalLost();
        Assert.IsType<DeliveryTermResult.Lost>(await WaitForTermAsync());

        foreach (var id in controlled.Pending)
        {
            controlled.Fail(id, completion == "oce" ? new OperationCanceledException() : new InvalidOperationException());
        }

        Assert.Empty(controlled.Pending);
        Assert.Empty(logger.WithEvent(113));
        Assert.Empty(metrics.Tagged);
        Assert.Equal(0, metrics.Delivered);
        Assert.Empty(session.Commits);
        Assert.Equal(3, controlledTerm!.PendingCreated);
    }

    [Fact]
    public async Task Pending_sends_are_reused()
    {
        const int rows = 40;
        const int cap = 4;

        // Waves of cap rows bound concurrency: the next wave is inserted only after the previous one is committed.
        session.Insert([.. Enumerable.Range(1, cap).Select(i => (long)i)]);
        await StartControlledAsync();
        for (var done = 0; done < rows; done += cap)
        {
            if (done > 0)
            {
                session.Insert([.. Enumerable.Range(done + 1, cap).Select(i => (long)i)]);
                await AdvanceAsync(options.Polling.Interval);
            }

            await WaitUntilAsync(() => controlled.Pending.Count == cap);
            CompleteConcurrently([.. controlled.Pending]);
            await WaitUntilAsync(() => session.TableIds.Count == 0);
        }

        Assert.Equal(rows, controlled.Sends);
        Assert.Equal(cap, controlled.PeakPending);
        Assert.Equal(cap, controlledTerm!.PendingCreated);
    }

    [Fact]
    public async Task Loop_does_not_resume_on_completing_thread()
    {
        session.Insert(1);
        var armedAtSend = -1;
        controlled.OnSend = () => Volatile.Write(ref armedAtSend, time.Created);

        await StartControlledAsync();
        // The only re-arm after the send is the one the loop makes as it enters its wait, so the loop is parked.
        await WaitUntilAsync(() => controlled.Pending.Count == 1 && Volatile.Read(ref armedAtSend) >= 0 && time.Created > Volatile.Read(ref armedAtSend));

        using var release = new ManualResetEvent(false);
        try
        {
            var completing = controlled.CompleteOnDedicatedThread(1, new DeliveryResult.Delivered(), release);
            await WaitUntilAsync(() => session.CommitThreads.Count == 1);

            Assert.NotEqual(0, completing);
            Assert.NotEqual(completing, Assert.Single(session.CommitThreads));
        }
        finally
        {
            release.Set();
        }
    }

    private async Task StartControlledAsync()
    {
        controlledTerm = new DeliveryTerm(
            outbox,
            options,
            Destination,
            () => controlled,
            new UnorderedDelivery(options.Polling.BatchSize),
            status,
            time,
            logger);
        term = controlledTerm.RunAsync(session, stopping.Token);
        await WaitUntilAsync(() => time.Active > 0 || Settled());
    }

    private void CompleteConcurrently(long[] ids)
    {
        using var start = new Barrier(ids.Length);
        var threads = new Thread[ids.Length];
        for (var i = 0; i < ids.Length; i++)
        {
            var id = ids[i];
            threads[i] = new Thread(() =>
            {
                start.SignalAndWait();
                controlled.Complete(id, new DeliveryResult.Delivered());
            });
            threads[i].Start();
        }

        foreach (var thread in threads)
        {
            thread.Join();
        }
    }
}
