using System.Collections.Concurrent;
using Pernix.OutboxRelay.Core.Abstractions;

namespace Pernix.OutboxRelay.Unit.Tests.Delivery;

internal sealed class FakeDestination : IDestination
{
    private readonly ConcurrentDictionary<long, TaskCompletionSource<DeliveryResult>> pending = new();
    private readonly ConcurrentQueue<long> sent = new();
    private readonly ConcurrentQueue<OutgoingMessage> messages = new();
    private int validateCalls;
    private int disposeCalls;

    public TimeSpan? SendBound { get; set; }

    public IReadOnlySet<string>? Routes { get; set; }

    public Func<OutgoingMessage, DeliveryResult>? Respond { get; set; } = _ => new DeliveryResult.Delivered();

    public Exception? SendFailure { get; set; }

    public bool HoldThroughCancellation { get; set; }

    public TaskCompletionSource? DisposeGate { get; set; }

    public Exception? FailPendingOnDispose { get; set; }

    public IReadOnlyList<long> Sent => [.. sent];

    public IReadOnlyList<OutgoingMessage> Messages => [.. messages];

    public Exception? ValidateFailure { get; set; }

    public IReadOnlyCollection<long> Pending => [.. pending.Keys];

    public int ValidateCalls => Volatile.Read(ref validateCalls);

    public int DisposeCalls => Volatile.Read(ref disposeCalls);

    public bool DisposeBlocked => DisposeGate is { Task.IsCompleted: false } && DisposeCalls > 0;

    public Task ValidateAsync(CancellationToken ct)
    {
        Interlocked.Increment(ref validateCalls);
        if (ValidateFailure is { } failure)
        {
            ValidateFailure = null;
            return Task.FromException(failure);
        }

        return Task.CompletedTask;
    }

    public ValueTask<DeliveryResult> SendAsync(OutgoingMessage message, CancellationToken ct)
    {
        sent.Enqueue(message.Source.Id);
        messages.Enqueue(message);
        if (SendFailure is { } failure)
        {
            return ValueTask.FromException<DeliveryResult>(failure);
        }

        if (Respond is { } respond)
        {
            return new ValueTask<DeliveryResult>(respond(message));
        }

        // Inline continuations: the result is in the term's channel when Complete returns.
        var source = new TaskCompletionSource<DeliveryResult>();
        pending[message.Source.Id] = source;
        if (!HoldThroughCancellation)
        {
            ct.Register(() => source.TrySetCanceled(ct));
        }

        return new ValueTask<DeliveryResult>(source.Task);
    }

    public void Complete(long id, DeliveryResult result)
    {
        if (pending.TryRemove(id, out var source))
        {
            source.TrySetResult(result);
        }
    }

    public void CompleteAll(DeliveryResult result)
    {
        foreach (var id in pending.Keys.Order())
        {
            Complete(id, result);
        }
    }

    public async ValueTask DisposeAsync()
    {
        Interlocked.Increment(ref disposeCalls);
        if (FailPendingOnDispose is { } failure)
        {
            foreach (var id in pending.Keys.Order())
            {
                if (pending.TryRemove(id, out var source))
                {
                    source.TrySetException(failure);
                }
            }
        }

        if (DisposeGate is { } gate)
        {
            await gate.Task.ConfigureAwait(false);
        }
    }
}
