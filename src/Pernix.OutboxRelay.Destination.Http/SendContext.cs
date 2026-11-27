using System.Threading.Tasks.Sources;

namespace Pernix.OutboxRelay.Destination.Http;

// State and Generation change only under the gate lock.
internal sealed class SendContext : IValueTaskSource<HttpSendSlot>
{
    internal const int Idle = 0;
    internal const int Waiting = 1;
    internal const int Granted = 2;
    internal const int Cancelled = 3;

    internal ManualResetValueTaskSourceCore<HttpSendSlot> Waiter;
    internal CancellationTokenRegistration CallerLink;
    internal CancellationTokenRegistration WaitLink;
    internal int State;
    internal int Generation;

    internal SendContext(HttpSendGate gate, CancellationTokenSource deadline)
    {
        Gate = gate;
        Deadline = deadline;
        Waiter.RunContinuationsAsynchronously = true;
    }

    internal HttpSendGate Gate { get; }

    public CancellationTokenSource Deadline { get; internal set; }

    public HttpSendSlot GetResult(short token) => Waiter.GetResult(token);

    public ValueTaskSourceStatus GetStatus(short token) => Waiter.GetStatus(token);

    public void OnCompleted(
        Action<object?> continuation,
        object? state,
        short token,
        ValueTaskSourceOnCompletedFlags flags) => Waiter.OnCompleted(continuation, state, token, flags);
}
