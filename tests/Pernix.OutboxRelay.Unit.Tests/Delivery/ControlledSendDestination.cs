using System.Collections.Concurrent;
using System.Threading.Tasks.Sources;
using Pernix.OutboxRelay.Core.Abstractions;

namespace Pernix.OutboxRelay.Unit.Tests.Delivery;

// Continuations run synchronously on the completing thread, as on a Kafka delivery-report thread.
internal sealed class ControlledSendDestination : IDestination
{
    private readonly ConcurrentDictionary<long, Source> pending = new();
    private int sends;
    private int peak;

    public TimeSpan? SendBound => null;

    public IReadOnlySet<string>? Routes => null;

    public Exception? ThrowOnSend { get; set; }

    public bool CompleteInline { get; set; }

    public DeliveryResult? InlineResult { get; set; } = new DeliveryResult.Delivered();

    public Exception? InlineError { get; set; }

    public bool FailOnCancel { get; set; }

    public Action? OnSend { get; set; }

    public int Sends => Volatile.Read(ref sends);

    public int PeakPending => Volatile.Read(ref peak);

    public IReadOnlyCollection<long> Pending => [.. pending.Keys];

    public Task ValidateAsync(CancellationToken ct) => Task.CompletedTask;

    public ValueTask<DeliveryResult> SendAsync(OutgoingMessage message, CancellationToken ct)
    {
        Interlocked.Increment(ref sends);
        OnSend?.Invoke();
        if (ThrowOnSend is { } thrown)
        {
            throw thrown;
        }

        var source = new Source();
        if (CompleteInline)
        {
            source.TryComplete(InlineResult, InlineError);
            return new ValueTask<DeliveryResult>(source, source.Version);
        }

        pending[message.Source.Id] = source;
        TrackPeak();
        if (FailOnCancel)
        {
            var id = message.Source.Id;
            ct.UnsafeRegister(s => Fail(id, new OperationCanceledException(ct)), null);
        }

        return new ValueTask<DeliveryResult>(source, source.Version);
    }

    public void Complete(long id, DeliveryResult? result) => Finish(id, result, null);

    public void Fail(long id, Exception error) => Finish(id, null, error);

    public int CompleteOnDedicatedThread(long id, DeliveryResult result, WaitHandle release)
    {
        var threadId = 0;
        using var done = new ManualResetEventSlim();
        var thread = new Thread(() =>
        {
            Volatile.Write(ref threadId, Environment.CurrentManagedThreadId);
            Complete(id, result);
            done.Set();
            release.WaitOne();
        })
        {
            IsBackground = true,
        };
        thread.Start();
        done.Wait(TimeSpan.FromSeconds(5));
        return Volatile.Read(ref threadId);
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;

    private void Finish(long id, DeliveryResult? result, Exception? error)
    {
        if (pending.TryRemove(id, out var source))
        {
            source.TryComplete(result, error);
        }
    }

    private void TrackPeak()
    {
        var now = pending.Count;
        var seen = Volatile.Read(ref peak);
        while (now > seen && Interlocked.CompareExchange(ref peak, now, seen) is var prior && prior != seen)
        {
            seen = prior;
        }
    }

    private sealed class Source : IValueTaskSource<DeliveryResult>
    {
        private ManualResetValueTaskSourceCore<DeliveryResult> core;
        private int completed;

        public short Version => core.Version;

        public void TryComplete(DeliveryResult? result, Exception? error)
        {
            if (Interlocked.Exchange(ref completed, 1) == 1)
            {
                return;
            }

            if (error is not null)
            {
                core.SetException(error);
            }
            else
            {
                core.SetResult(result!);
            }
        }

        public DeliveryResult GetResult(short token) => core.GetResult(token);

        public ValueTaskSourceStatus GetStatus(short token) => core.GetStatus(token);

        public void OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
            core.OnCompleted(continuation, state, token, flags);
    }
}
