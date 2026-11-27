using System.Diagnostics;
using Pernix.OutboxRelay.Core.Abstractions;

namespace Pernix.OutboxRelay.Destination.Http;

// Never cancels a CTS or completes a waiter while holding the lock, so callbacks and continuations never run under it.
internal sealed class HttpSendGate : IDisposable
{
    internal const int HookBeforeLock = 0;
    internal const int HookCancelDecided = 1;

    private const int InitialRingCapacity = 16;

    private static readonly Action<object?> OnWaitCancelled = static state =>
    {
        var context = (SendContext)state!;
        context.Gate.WaitCancelled(context);
    };

    private static readonly Action<object?> OnCallerCancelled = static state => ((SendContext)state!).Deadline.Cancel();

    private readonly Lock sync = new();
    private readonly HttpSendSlot?[] slots;
    private readonly int[] free;
    private readonly Stack<SendContext> pool = new();
    private readonly Func<int, HttpSendSlot> createSlot;
    private readonly TimeProvider time;
    private WaiterEntry[] ring = new WaiterEntry[InitialRingCapacity];
    private int ringHead;
    private int ringCount;
    private int top;
    private int held;
    private long rented;
    private long returned;
    private bool disposed;

    public HttpSendGate(int maxRequestsInFlight, Func<int, HttpSendSlot> createSlot, TimeProvider? time = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxRequestsInFlight, 1);
        ArgumentNullException.ThrowIfNull(createSlot);
        this.createSlot = createSlot;
        this.time = time ?? TimeProvider.System;
        slots = new HttpSendSlot?[maxRequestsInFlight];
        free = new int[maxRequestsInFlight];
        for (var i = 0; i < maxRequestsInFlight; i++)
        {
            free[i] = maxRequestsInFlight - 1 - i;
        }

        top = maxRequestsInFlight;
    }

    internal static DeliveryResult SlotWaitTimeout { get; } =
        new DeliveryResult.DestinationFault("timeout waiting for a request slot");

    internal Action<SendContext, int>? CancelHook { get; set; }

    public int MaxRequestsInFlight => slots.Length;

    internal int FreeCount
    {
        get
        {
            lock (sync)
            {
                return top;
            }
        }
    }

    internal int HeldCount
    {
        get
        {
            lock (sync)
            {
                return held;
            }
        }
    }

    internal long Rented
    {
        get
        {
            lock (sync)
            {
                return rented;
            }
        }
    }

    internal long Returned
    {
        get
        {
            lock (sync)
            {
                return returned;
            }
        }
    }

    internal int PooledCount
    {
        get
        {
            lock (sync)
            {
                return pool.Count;
            }
        }
    }

    internal int QueueCapacity
    {
        get
        {
            lock (sync)
            {
                return ring.Length;
            }
        }
    }

    internal int QueuedEntries
    {
        get
        {
            lock (sync)
            {
                return ringCount;
            }
        }
    }

    public SendContext Rent(TimeSpan timeout, CancellationToken term)
    {
        SendContext? context;
        lock (sync)
        {
            ObjectDisposedException.ThrowIf(disposed, this);
            pool.TryPop(out context);
            rented++;
        }

        context ??= new SendContext(this, NewDeadline());
        context.Deadline.CancelAfter(timeout);
        context.CallerLink = term.UnsafeRegister(OnCallerCancelled, context);
        return context;
    }

    // Await the result exactly once before Return(context).
    public ValueTask<HttpSendSlot> AcquireAsync(SendContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        int index;
        lock (sync)
        {
            ObjectDisposedException.ThrowIf(disposed, this);
            Debug.Assert(context.State == SendContext.Idle, "context acquired twice");
            if (top > 0)
            {
                index = free[--top];
                held++;
                context.State = SendContext.Granted;
            }
            else
            {
                index = -1;
                context.Generation++;
                context.State = SendContext.Waiting;
                Enqueue(new WaiterEntry(context, context.Generation));
            }

            AssertInvariant();
        }

        if (index >= 0)
        {
            var slot = slots[index];
            if (slot is null)
            {
                try
                {
                    slot = slots[index] = createSlot(index);
                }
                catch
                {
                    lock (sync)
                    {
                        context.State = SendContext.Idle;
                    }

                    HandOffUncreated(index);
                    throw;
                }
            }

            return new ValueTask<HttpSendSlot>(slot);
        }

        var version = context.Waiter.Version;
        context.WaitLink = context.Deadline.Token.UnsafeRegister(OnWaitCancelled, context);
        return new ValueTask<HttpSendSlot>(context, version);
    }

    public void Release(HttpSendSlot slot)
    {
        ArgumentNullException.ThrowIfNull(slot);
        SendContext? next = null;
        var disposeSlot = false;
        lock (sync)
        {
            next = DequeueLiveWaiter();
            if (next is null)
            {
                held--;
                if (disposed)
                {
                    disposeSlot = true;
                }
                else
                {
                    free[top++] = slot.Index;
                }
            }

            AssertInvariant();
        }

        if (next is not null)
        {
            next.Waiter.SetResult(slot);
        }
        else if (disposeSlot)
        {
            slot.Dispose();
        }
    }

    public void Return(SendContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        Debug.Assert(context.State != SendContext.Waiting, "context returned before its wait completed");

        // Dispose blocks until a running callback finishes, and WaitCancelled takes the lock: never under the lock.
        context.WaitLink.Dispose();
        context.WaitLink = default;
        context.CallerLink.Dispose();
        context.CallerLink = default;

        if (!context.Deadline.TryReset())
        {
            context.Deadline.Dispose();
            context.Deadline = NewDeadline();
        }

        context.Waiter.Reset();

        bool pooled;
        lock (sync)
        {
            context.State = SendContext.Idle;
            returned++;
            pooled = !disposed;
            if (pooled)
            {
                pool.Push(context);
            }
        }

        if (!pooled)
        {
            context.Deadline.Dispose();
        }
    }

    public void Dispose()
    {
        List<SendContext> waiters = [];
        SendContext[] contexts;
        List<HttpSendSlot> idle = [];
        lock (sync)
        {
            if (disposed)
            {
                return;
            }

            disposed = true;
            while (ringCount > 0)
            {
                var entry = ring[ringHead];
                ring[ringHead] = default;
                ringHead = (ringHead + 1) & (ring.Length - 1);
                ringCount--;
                if (entry.Generation == entry.Context.Generation && entry.Context.State == SendContext.Waiting)
                {
                    entry.Context.State = SendContext.Cancelled;
                    waiters.Add(entry.Context);
                }
            }

            contexts = [.. pool];
            pool.Clear();
            while (top > 0)
            {
                var slot = slots[free[--top]];
                if (slot is not null)
                {
                    idle.Add(slot);
                }
            }
        }

        foreach (var waiter in waiters)
        {
            waiter.Waiter.SetException(new ObjectDisposedException(nameof(HttpSendGate)));
        }

        foreach (var context in contexts)
        {
            context.Deadline.Dispose();
        }

        foreach (var slot in idle)
        {
            slot.Dispose();
        }
    }

    // An index whose createSlot threw goes to the next live waiter (which retries) or back to the free stack.
    private void HandOffUncreated(int index)
    {
        while (true)
        {
            SendContext? next;
            lock (sync)
            {
                next = DequeueLiveWaiter();
                if (next is null)
                {
                    held--;
                    if (!disposed)
                    {
                        free[top++] = index;
                    }

                    AssertInvariant();
                    return;
                }
            }

            HttpSendSlot slot;
            try
            {
                slot = slots[index] ??= createSlot(index);
            }
            catch (Exception exception)
            {
                next.Waiter.SetException(exception);
                continue;
            }

            next.Waiter.SetResult(slot);
            return;
        }
    }

    private SendContext? DequeueLiveWaiter()
    {
        while (ringCount > 0)
        {
            var entry = ring[ringHead];
            ring[ringHead] = default;
            ringHead = (ringHead + 1) & (ring.Length - 1);
            ringCount--;
            if (entry.Generation == entry.Context.Generation && entry.Context.State == SendContext.Waiting)
            {
                entry.Context.State = SendContext.Granted;
                return entry.Context;
            }
        }

        return null;
    }

    private void WaitCancelled(SendContext context)
    {
        CancelHook?.Invoke(context, HookBeforeLock);
        bool cancel;
        lock (sync)
        {
            cancel = context.State == SendContext.Waiting;
            if (cancel)
            {
                // The ring entry stays as a tombstone; Release skips it.
                context.State = SendContext.Cancelled;
            }
        }

        if (!cancel)
        {
            return;
        }

        CancelHook?.Invoke(context, HookCancelDecided);
        context.Waiter.SetException(new OperationCanceledException(context.Deadline.Token));
    }

    private void Enqueue(WaiterEntry entry)
    {
        // Tombstones pile up while slots stay held. Compact first, grow only when live entries fill over half,
        // so capacity tracks live waiters and compaction stays amortized O(1).
        if (ringCount == ring.Length && Compact() > ring.Length / 2)
        {
            var grown = new WaiterEntry[ring.Length * 2];
            for (var i = 0; i < ringCount; i++)
            {
                grown[i] = ring[(ringHead + i) & (ring.Length - 1)];
            }

            ring = grown;
            ringHead = 0;
        }

        ring[(ringHead + ringCount) & (ring.Length - 1)] = entry;
        ringCount++;
    }

    private int Compact()
    {
        var mask = ring.Length - 1;
        var live = 0;
        for (var i = 0; i < ringCount; i++)
        {
            var entry = ring[(ringHead + i) & mask];
            if (entry.Generation == entry.Context.Generation && entry.Context.State == SendContext.Waiting)
            {
                ring[(ringHead + live) & mask] = entry;
                live++;
            }
        }

        for (var i = live; i < ringCount; i++)
        {
            ring[(ringHead + i) & mask] = default;
        }

        ringCount = live;
        return live;
    }

    private CancellationTokenSource NewDeadline() =>
        time == TimeProvider.System ? new CancellationTokenSource() : new CancellationTokenSource(Timeout.InfiniteTimeSpan, time);

    [Conditional("DEBUG")]
    private void AssertInvariant() =>
        Debug.Assert(disposed || top + held == slots.Length, "free + held != MaxRequestsInFlight");

    // Generation makes an entry left by a cancelled, re-rented and re-enqueued context fail the live check.
    private readonly struct WaiterEntry(SendContext context, int generation)
    {
        public SendContext Context { get; } = context;

        public int Generation { get; } = generation;
    }
}
