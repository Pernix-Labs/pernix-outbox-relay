using System.Runtime.CompilerServices;
using Microsoft.Extensions.Time.Testing;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Destination.Http;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Http;

public sealed class HttpSendGateTests
{
    private static readonly TimeSpan SendTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan Bounded = TimeSpan.FromSeconds(5);

    [Fact]
    public async Task Timeout_while_waiting_yields_the_slot_timeout_and_consumes_no_slot()
    {
        var time = new FakeTimeProvider();
        using var gate = Gate(1, time);
        var holder = gate.Rent(TimeSpan.FromMinutes(5), CancellationToken.None);
        var slot = await gate.AcquireAsync(holder);
        var waiter = gate.Rent(SendTimeout, CancellationToken.None);
        var pending = gate.AcquireAsync(waiter);
        Assert.False(pending.IsCompleted);

        time.Advance(SendTimeout);

        var outcome = await Outcome(pending, CancellationToken.None);
        var fault = Assert.IsType<DeliveryResult.DestinationFault>(outcome);
        Assert.Equal("timeout waiting for a request slot", fault.Error);
        Assert.Equal(0, gate.FreeCount);
        Assert.Equal(1, gate.HeldCount);
        gate.Return(waiter);

        gate.Release(slot);
        gate.Return(holder);
        Assert.Equal(1, gate.FreeCount);
        Assert.Equal(0, gate.HeldCount);
        Assert.Equal(gate.Rented, gate.Returned);
    }

    [Fact]
    public async Task Term_cancel_while_waiting_surfaces_cancellation_and_consumes_no_slot()
    {
        using var gate = Gate(1);
        using var term = new CancellationTokenSource();
        var holder = gate.Rent(SendTimeout, term.Token);
        var slot = await gate.AcquireAsync(holder);
        var waiter = gate.Rent(SendTimeout, term.Token);
        var pending = gate.AcquireAsync(waiter);

        await term.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => Outcome(pending, term.Token));
        Assert.True(waiter.Deadline.IsCancellationRequested);
        gate.Return(waiter);
        gate.Release(slot);
        gate.Return(holder);
        Assert.Equal(1, gate.FreeCount);
        Assert.Equal(0, gate.HeldCount);
    }

    [Fact]
    public async Task Hand_off_wins_when_it_runs_before_the_cancel_takes_the_lock()
    {
        using var gate = Gate(1);
        using var entered = new ManualResetEventSlim();
        using var proceed = new ManualResetEventSlim();
        gate.CancelHook = (_, stage) =>
        {
            if (stage == HttpSendGate.HookBeforeLock)
            {
                entered.Set();
                proceed.Wait(Bounded);
            }
        };
        var holder = gate.Rent(SendTimeout, CancellationToken.None);
        var slot = await gate.AcquireAsync(holder);
        var waiter = gate.Rent(SendTimeout, CancellationToken.None);
        var pending = gate.AcquireAsync(waiter);

        var cancel = Task.Run(waiter.Deadline.Cancel, TestContext.Current.CancellationToken);
        Assert.True(entered.Wait(Bounded, TestContext.Current.CancellationToken));
        gate.Release(slot);
        proceed.Set();
        await cancel.WaitAsync(Bounded, TestContext.Current.CancellationToken);

        Assert.Same(slot, await pending);
        Assert.Equal(SendContext.Granted, waiter.State);
        Assert.Equal(1, gate.HeldCount);
        gate.Release(slot);
        gate.Return(waiter);
        gate.Return(holder);
        Assert.Equal(1, gate.FreeCount);
        Assert.Equal(0, gate.HeldCount);
    }

    [Fact]
    public async Task Cancel_wins_when_it_decides_before_the_hand_off_and_the_slot_goes_free()
    {
        using var gate = Gate(1);
        using var entered = new ManualResetEventSlim();
        using var proceed = new ManualResetEventSlim();
        gate.CancelHook = (_, stage) =>
        {
            if (stage == HttpSendGate.HookCancelDecided)
            {
                entered.Set();
                proceed.Wait(Bounded);
            }
        };
        var holder = gate.Rent(SendTimeout, CancellationToken.None);
        var slot = await gate.AcquireAsync(holder);
        var waiter = gate.Rent(SendTimeout, CancellationToken.None);
        var pending = gate.AcquireAsync(waiter);

        var cancel = Task.Run(waiter.Deadline.Cancel, TestContext.Current.CancellationToken);
        Assert.True(entered.Wait(Bounded, TestContext.Current.CancellationToken));
        gate.Release(slot);
        Assert.Equal(1, gate.FreeCount);
        Assert.Equal(0, gate.QueuedEntries);
        proceed.Set();
        await cancel.WaitAsync(Bounded, TestContext.Current.CancellationToken);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await pending);
        gate.Return(waiter);
        gate.Return(holder);
        Assert.Equal(1, gate.FreeCount);
        Assert.Equal(0, gate.HeldCount);
    }

    [Fact]
    public async Task Concurrent_cancel_and_hand_off_has_exactly_one_winner_and_no_leak()
    {
        using var gate = Gate(1);
        var handedOff = 0;
        var cancelled = 0;
        for (var i = 0; i < 500; i++)
        {
            var holder = gate.Rent(SendTimeout, CancellationToken.None);
            var slot = await gate.AcquireAsync(holder);
            var waiter = gate.Rent(SendTimeout, CancellationToken.None);
            var pending = gate.AcquireAsync(waiter);
            using var start = new Barrier(2);
            var cancel = Task.Run(
                () =>
                {
                    start.SignalAndWait(Bounded, TestContext.Current.CancellationToken);
                    waiter.Deadline.Cancel();
                },
                TestContext.Current.CancellationToken);
            start.SignalAndWait(Bounded, TestContext.Current.CancellationToken);
            gate.Release(slot);
            await cancel.WaitAsync(Bounded, TestContext.Current.CancellationToken);

            try
            {
                var granted = await pending;
                Assert.Same(slot, granted);
                Assert.Equal(0, gate.FreeCount);
                handedOff++;
                gate.Release(granted);
            }
            catch (OperationCanceledException)
            {
                Assert.Equal(1, gate.FreeCount);
                cancelled++;
            }

            gate.Return(waiter);
            gate.Return(holder);
            Assert.Equal(1, gate.FreeCount);
            Assert.Equal(0, gate.HeldCount);
            Assert.Equal(0, gate.QueuedEntries);
        }

        Assert.Equal(500, handedOff + cancelled);
    }

    [Fact]
    public async Task Stale_tombstone_of_a_re_enqueued_context_never_gets_a_second_grant()
    {
        using var gate = Gate(1);
        var holder = gate.Rent(SendTimeout, CancellationToken.None);
        var slot = await gate.AcquireAsync(holder);

        var first = gate.Rent(SendTimeout, CancellationToken.None);
        var firstWait = gate.AcquireAsync(first);
        await first.Deadline.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await firstWait);
        gate.Return(first);

        var reused = gate.Rent(SendTimeout, CancellationToken.None);
        Assert.Same(first, reused);
        var reusedWait = gate.AcquireAsync(reused);
        var other = gate.Rent(SendTimeout, CancellationToken.None);
        var otherWait = gate.AcquireAsync(other);
        Assert.Equal(3, gate.QueuedEntries);
        Assert.Equal(SendContext.Waiting, reused.State);

        gate.Release(slot);
        Assert.Same(slot, await reusedWait);
        Assert.False(otherWait.IsCompleted);
        Assert.Equal(1, gate.QueuedEntries);
        Assert.Equal(1, gate.HeldCount);

        gate.Release(slot);
        Assert.Same(slot, await otherWait);
        Assert.Equal(0, gate.QueuedEntries);
        Assert.Equal(1, gate.HeldCount);

        gate.Release(slot);
        gate.Return(reused);
        gate.Return(other);
        gate.Return(holder);
        Assert.Equal(1, gate.FreeCount);
        Assert.Equal(0, gate.HeldCount);
        Assert.Equal(gate.Rented, gate.Returned);
    }

    [Fact]
    public async Task Stale_wait_callback_cannot_fire_after_context_return_on_a_reused_deadline()
    {
        using var gate = Gate(1);
        var callbacks = 0;
        gate.CancelHook = (_, stage) =>
        {
            if (stage == HttpSendGate.HookBeforeLock)
            {
                Interlocked.Increment(ref callbacks);
            }
        };
        var holder = gate.Rent(SendTimeout, CancellationToken.None);
        var slot = await gate.AcquireAsync(holder);
        var context = gate.Rent(SendTimeout, CancellationToken.None);
        var deadline = context.Deadline;
        var wait = gate.AcquireAsync(context);
        gate.Release(slot);
        Assert.Same(slot, await wait);
        gate.Return(holder);
        gate.Release(slot);
        gate.Return(context);
        Assert.Same(deadline, context.Deadline);

        var reused = gate.Rent(SendTimeout, CancellationToken.None);
        Assert.Same(context, reused);
        var granted = await gate.AcquireAsync(reused);
        await reused.Deadline.CancelAsync();
        Assert.Equal(0, Volatile.Read(ref callbacks));
        Assert.Equal(SendContext.Granted, reused.State);
        gate.Release(granted);
        gate.Return(reused);

        var holder2 = gate.Rent(SendTimeout, CancellationToken.None);
        var slot2 = await gate.AcquireAsync(holder2);
        var waiting = gate.Rent(SendTimeout, CancellationToken.None);
        var pending = gate.AcquireAsync(waiting);
        await waiting.Deadline.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () => await pending);
        Assert.Equal(1, Volatile.Read(ref callbacks));
        gate.Return(waiting);
        gate.Release(slot2);
        gate.Return(holder2);
        Assert.Equal(1, gate.FreeCount);
    }

    [Fact]
    public async Task Cancel_firing_concurrently_with_context_return_completes_without_deadlock()
    {
        using var gate = Gate(1);
        using var entered = new ManualResetEventSlim();
        using var proceed = new ManualResetEventSlim();
        gate.CancelHook = (_, stage) =>
        {
            if (stage == HttpSendGate.HookBeforeLock)
            {
                entered.Set();
                proceed.Wait(Bounded);
            }
        };
        var holder = gate.Rent(SendTimeout, CancellationToken.None);
        var slot = await gate.AcquireAsync(holder);
        var context = gate.Rent(SendTimeout, CancellationToken.None);
        var wait = gate.AcquireAsync(context);
        gate.Release(slot);
        Assert.Same(slot, await wait);
        gate.Release(slot);

        var cancel = Task.Run(context.Deadline.Cancel, TestContext.Current.CancellationToken);
        Assert.True(entered.Wait(Bounded, TestContext.Current.CancellationToken));
        var returned = Task.Run(() => gate.Return(context), TestContext.Current.CancellationToken);
        await Task.Delay(50, TestContext.Current.CancellationToken);
        Assert.False(returned.IsCompleted);
        proceed.Set();

        await Task.WhenAll(cancel, returned).WaitAsync(Bounded, TestContext.Current.CancellationToken);
        gate.Return(holder);
        Assert.Equal(1, gate.FreeCount);
        Assert.Equal(gate.Rented, gate.Returned);
    }

    [Fact]
    public async Task Timed_out_waiters_with_slots_held_do_not_grow_the_ring()
    {
        var time = new FakeTimeProvider();
        using var gate = Gate(1, time);
        var holder = gate.Rent(TimeSpan.FromDays(1), CancellationToken.None);
        var slot = await gate.AcquireAsync(holder);
        var live = new (SendContext Context, ValueTask<HttpSendSlot> Wait)[3];
        for (var i = 0; i < live.Length; i++)
        {
            var context = gate.Rent(TimeSpan.FromDays(1), CancellationToken.None);
            live[i] = (context, gate.AcquireAsync(context));
        }

        var initial = gate.QueueCapacity;
        for (var i = 0; i < 1000; i++)
        {
            var waiter = gate.Rent(SendTimeout, CancellationToken.None);
            var pending = gate.AcquireAsync(waiter);
            time.Advance(SendTimeout);
            Assert.Same(HttpSendGate.SlotWaitTimeout, await Outcome(pending, CancellationToken.None));
            gate.Return(waiter);
        }

        Assert.Equal(initial, gate.QueueCapacity);
        Assert.True(gate.QueuedEntries <= initial);

        foreach (var (context, wait) in live)
        {
            gate.Release(slot);
            Assert.Same(slot, await wait);
            gate.Return(context);
        }

        gate.Release(slot);
        gate.Return(holder);
        Assert.Equal(0, gate.QueuedEntries);
        Assert.Equal(1, gate.FreeCount);
        Assert.Equal(gate.Rented, gate.Returned);
    }

    [Fact]
    public async Task Ring_grows_when_live_waiters_fill_it()
    {
        using var gate = Gate(1);
        var holder = gate.Rent(SendTimeout, CancellationToken.None);
        var slot = await gate.AcquireAsync(holder);
        var initial = gate.QueueCapacity;
        var waiters = Enumerable.Range(0, initial + 1)
            .Select(_ => gate.Rent(SendTimeout, CancellationToken.None))
            .Select(context => (Context: context, Wait: gate.AcquireAsync(context)))
            .ToArray();

        Assert.Equal(initial * 2, gate.QueueCapacity);
        Assert.Equal(initial + 1, gate.QueuedEntries);

        foreach (var (context, wait) in waiters)
        {
            gate.Release(slot);
            Assert.Same(slot, await wait);
            gate.Return(context);
        }

        gate.Release(slot);
        gate.Return(holder);
        Assert.Equal(1, gate.FreeCount);
        Assert.Equal(gate.Rented, gate.Returned);
    }

    [Fact]
    public async Task Waiters_are_granted_in_fifo_order()
    {
        using var gate = Gate(1);
        var holder = gate.Rent(SendTimeout, CancellationToken.None);
        var slot = await gate.AcquireAsync(holder);
        var contexts = new SendContext[5];
        var waits = new ValueTask<HttpSendSlot>[5];
        for (var i = 0; i < contexts.Length; i++)
        {
            contexts[i] = gate.Rent(SendTimeout, CancellationToken.None);
            waits[i] = gate.AcquireAsync(contexts[i]);
        }

        for (var i = 0; i < contexts.Length; i++)
        {
            gate.Release(slot);
            for (var j = i + 1; j < contexts.Length; j++)
            {
                Assert.False(waits[j].IsCompleted);
            }

            slot = await waits[i];
            gate.Return(contexts[i]);
        }

        gate.Release(slot);
        gate.Return(holder);
        Assert.Equal(1, gate.FreeCount);
    }

    [Fact]
    public async Task Mixed_outcomes_keep_the_slot_invariant_and_return_every_context()
    {
        const int Max = 4;
        const int Sends = 2000;
        using var gate = Gate(Max);
        var sends = new Task<string>[Sends];
        for (var i = 0; i < Sends; i++)
        {
            var kind = (Kind)(i % 5);
            sends[i] = Task.Run(() => Send(gate, kind), TestContext.Current.CancellationToken);
        }

        var outcomes = await Task.WhenAll(sends).WaitAsync(TimeSpan.FromSeconds(60), TestContext.Current.CancellationToken);

        Assert.Contains("ok", outcomes);
        Assert.Contains("fault", outcomes);
        Assert.Contains("timeout", outcomes);
        Assert.Contains("cancel", outcomes);
        Assert.Contains("mapper", outcomes);
        Assert.Equal(Max, gate.FreeCount + gate.HeldCount);
        Assert.Equal(Max, gate.FreeCount);
        Assert.Equal(Sends, gate.Rented);
        Assert.Equal(gate.Rented, gate.Returned);
        Assert.InRange(gate.PooledCount, 1, Sends);
    }

    [Fact]
    public async Task Contended_wait_allocates_nothing_after_warm_up()
    {
        using var gate = Gate(1);
        using var term = new CancellationTokenSource();
        for (var i = 0; i < 100; i++)
        {
            ContendedCycle(gate, term.Token);
        }

        await Task.Yield();
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++)
        {
            ContendedCycle(gate, term.Token);
        }

        Assert.Equal(0, GC.GetAllocatedBytesForCurrentThread() - before);
        Assert.Equal(1, gate.FreeCount);
    }

    [Fact]
    public async Task Suspended_wait_completed_from_another_thread_allocates_nothing_after_warm_up()
    {
        await Task.Yield();
        using var gate = Gate(1);
        using var releaser = new Releaser(gate);
        for (var i = 0; i < 100; i++)
        {
            SuspendedCycle(gate, releaser);
        }

        releaser.Reset();
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++)
        {
            SuspendedCycle(gate, releaser);
        }

        var awaiting = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.Equal(1000, releaser.Cycles);
        Assert.Equal(0, awaiting);
        Assert.Equal(0, releaser.ReleaseBytes);
        Assert.Equal(0, releaser.ContinuationBytes);
        Assert.Equal(1, gate.FreeCount);
        Assert.Equal(0, gate.HeldCount);
        Assert.Equal(gate.Rented, gate.Returned);
    }

    [Fact]
    public async Task Throwing_slot_factory_returns_the_index_and_the_next_acquire_succeeds()
    {
        var failures = 1;
        using var gate = new HttpSendGate(1, index => failures-- > 0 ? throw new InvalidOperationException("create") : new HttpSendSlot(index));
        var first = gate.Rent(SendTimeout, CancellationToken.None);

        Assert.Throws<InvalidOperationException>(() => gate.AcquireAsync(first));
        Assert.Equal(1, gate.FreeCount);
        Assert.Equal(0, gate.HeldCount);
        gate.Return(first);

        var second = gate.Rent(SendTimeout, CancellationToken.None);
        var slot = await gate.AcquireAsync(second);
        Assert.Equal(0, slot.Index);
        Assert.Equal(0, gate.FreeCount);
        Assert.Equal(1, gate.HeldCount);
        gate.Release(slot);
        gate.Return(second);
        Assert.Equal(1, gate.FreeCount + gate.HeldCount);
        Assert.Equal(gate.Rented, gate.Returned);
    }

    [Fact]
    public async Task Throwing_slot_factory_hands_the_index_to_a_queued_waiter()
    {
        using var entered = new ManualResetEventSlim();
        using var proceed = new ManualResetEventSlim();
        var calls = 0;
        using var gate = new HttpSendGate(1, index =>
        {
            if (Interlocked.Increment(ref calls) == 1)
            {
                entered.Set();
                proceed.Wait(TestContext.Current.CancellationToken);
                throw new InvalidOperationException("create");
            }

            return new HttpSendSlot(index);
        });
        var first = gate.Rent(SendTimeout, CancellationToken.None);
        var failing = Task.Run(() => gate.AcquireAsync(first).AsTask(), TestContext.Current.CancellationToken);
        Assert.True(entered.Wait(Bounded, TestContext.Current.CancellationToken));
        var waiter = gate.Rent(SendTimeout, CancellationToken.None);
        var pending = gate.AcquireAsync(waiter);
        Assert.False(pending.IsCompleted);

        proceed.Set();

        await Assert.ThrowsAsync<InvalidOperationException>(() => failing.WaitAsync(Bounded, TestContext.Current.CancellationToken));
        var slot = await pending.AsTask().WaitAsync(Bounded, TestContext.Current.CancellationToken);
        Assert.Equal(0, gate.FreeCount);
        Assert.Equal(1, gate.HeldCount);
        gate.Release(slot);
        gate.Return(waiter);
        gate.Return(first);
        Assert.Equal(1, gate.FreeCount);
        Assert.Equal(0, gate.HeldCount);
        Assert.Equal(gate.Rented, gate.Returned);
    }

    [Fact]
    public async Task Dispose_fails_pending_waiters_and_later_returns_free_their_resources()
    {
        var gate = Gate(1);
        var holder = gate.Rent(SendTimeout, CancellationToken.None);
        var slot = await gate.AcquireAsync(holder);
        var waiter = gate.Rent(SendTimeout, CancellationToken.None);
        var pending = gate.AcquireAsync(waiter);

        gate.Dispose();

        await Assert.ThrowsAsync<ObjectDisposedException>(async () => await pending);
        Assert.Throws<ObjectDisposedException>(() => gate.Rent(SendTimeout, CancellationToken.None));
        gate.Return(waiter);
        gate.Release(slot);
        gate.Return(holder);
        Assert.Equal(gate.Rented, gate.Returned);
        Assert.Equal(0, gate.PooledCount);
    }

    private static void ContendedCycle(HttpSendGate gate, CancellationToken term)
    {
        var holder = gate.Rent(SendTimeout, term);
        var slot = gate.AcquireAsync(holder).Result;
        var waiter = gate.Rent(SendTimeout, term);
        var pending = gate.AcquireAsync(waiter);
        gate.Release(slot);
        var granted = pending.Result;
        gate.Release(granted);
        gate.Return(waiter);
        gate.Return(holder);
    }

    private static void SuspendedCycle(HttpSendGate gate, Releaser releaser)
    {
        var holder = gate.Rent(SendTimeout, CancellationToken.None);
        var slot = gate.AcquireAsync(holder).Result;
        var waiter = gate.Rent(SendTimeout, CancellationToken.None);
        var pending = gate.AcquireAsync(waiter);
        Assert.False(pending.IsCompleted);
        var run = AwaitAndRelease(gate, pending, releaser);
        Assert.False(run.IsCompleted);
        releaser.Release(slot);
        var spin = default(SpinWait);
        while (!run.IsCompleted)
        {
            spin.SpinOnce(sleep1Threshold: -1);
        }

        run.GetAwaiter().GetResult();
        releaser.WaitDone();
        gate.Return(waiter);
        gate.Return(holder);
    }

    private static ValueTask AwaitAndRelease(HttpSendGate gate, ValueTask<HttpSendSlot> pending, Releaser releaser)
    {
        var machine = new AwaitAndReleaseMachine
        {
            Builder = PoolingAsyncValueTaskMethodBuilder.Create(),
            Gate = gate,
            Pending = pending,
            Releaser = releaser,
        };
        machine.Builder.Start(ref machine);
        return machine.Builder.Task;
    }

    // What Release emits for a pooled async ValueTask: Debug's class state machine would allocate per call.
    private struct AwaitAndReleaseMachine : IAsyncStateMachine
    {
        public PoolingAsyncValueTaskMethodBuilder Builder;
        public HttpSendGate Gate;
        public ValueTask<HttpSendSlot> Pending;
        public Releaser Releaser;
        private ValueTaskAwaiter<HttpSendSlot> awaiter;
        private int state;

        public void MoveNext()
        {
            try
            {
                if (state == 0)
                {
                    awaiter = Pending.GetAwaiter();
                    if (!awaiter.IsCompleted)
                    {
                        state = 1;
                        Builder.AwaitUnsafeOnCompleted(ref awaiter, ref this);
                        return;
                    }
                }

                var granted = awaiter.GetResult();
                awaiter = default;
                var start = GC.GetAllocatedBytesForCurrentThread();
                Gate.Release(granted);
                Releaser.ContinuationBytes += GC.GetAllocatedBytesForCurrentThread() - start;
            }
            catch (Exception exception)
            {
                state = -2;
                Builder.SetException(exception);
                return;
            }

            state = -2;
            Builder.SetResult();
        }

        public void SetStateMachine(IAsyncStateMachine stateMachine) => Builder.SetStateMachine(stateMachine);
    }

    // Spins instead of blocking: a first contended Monitor wait or wake would allocate on the measured thread.
    private sealed class Releaser : IDisposable
    {
        private readonly HttpSendGate gate;
        private readonly Thread thread;
        private HttpSendSlot? slot;
        private int requested;
        private int completed;
        private volatile bool stopping;

        public Releaser(HttpSendGate gate)
        {
            this.gate = gate;
            thread = new Thread(Run) { IsBackground = true };
            thread.Start();
        }

        public long ReleaseBytes { get; private set; }

        public long ContinuationBytes { get; set; }

        public int Cycles { get; private set; }

        public void Reset()
        {
            ReleaseBytes = 0;
            ContinuationBytes = 0;
            Cycles = 0;
        }

        public void Release(HttpSendSlot held)
        {
            slot = held;
            Interlocked.Increment(ref requested);
        }

        public void WaitDone()
        {
            var spin = default(SpinWait);
            while (Volatile.Read(ref completed) < Volatile.Read(ref requested))
            {
                spin.SpinOnce(sleep1Threshold: -1);
            }
        }

        public void Dispose()
        {
            stopping = true;
            thread.Join();
        }

        private void Run()
        {
            var handled = 0;
            var spin = default(SpinWait);
            while (!stopping)
            {
                if (Volatile.Read(ref requested) == handled)
                {
                    spin.SpinOnce(sleep1Threshold: -1);
                    continue;
                }

                var start = GC.GetAllocatedBytesForCurrentThread();
                gate.Release(slot!);
                ReleaseBytes += GC.GetAllocatedBytesForCurrentThread() - start;
                Cycles++;
                handled++;
                Interlocked.Increment(ref completed);
            }
        }
    }

    private static async Task<string> Send(HttpSendGate gate, Kind kind)
    {
        using var term = new CancellationTokenSource();
        var timeout = kind == Kind.Timeout ? TimeSpan.FromMilliseconds(1) : SendTimeout;
        var context = gate.Rent(timeout, term.Token);
        HttpSendSlot? slot = null;
        try
        {
            if (kind == Kind.Cancel)
            {
                term.CancelAfter(1);
            }

            slot = await gate.AcquireAsync(context);
            await Task.Delay(1, context.Deadline.Token);
            return kind switch
            {
                Kind.Fault => throw new HttpRequestException("fault"),
                Kind.Mapper => throw new InvalidOperationException("mapper"),
                Kind.Timeout or Kind.Cancel => await Never(context.Deadline.Token),
                _ => "ok",
            };
        }
        catch (OperationCanceledException) when (term.IsCancellationRequested)
        {
            return "cancel";
        }
        catch (OperationCanceledException)
        {
            return "timeout";
        }
        catch (HttpRequestException)
        {
            return "fault";
        }
        catch (InvalidOperationException)
        {
            return "mapper";
        }
        finally
        {
            if (slot is not null)
            {
                gate.Release(slot);
            }

            gate.Return(context);
        }
    }

    private static async Task<string> Never(CancellationToken token)
    {
        await Task.Delay(Timeout.Infinite, token);
        return "never";
    }

    private static async Task<DeliveryResult> Outcome(ValueTask<HttpSendSlot> pending, CancellationToken term)
    {
        try
        {
            await pending;
            return DeliveryResult.Delivered.Instance;
        }
        catch (OperationCanceledException) when (!term.IsCancellationRequested)
        {
            return HttpSendGate.SlotWaitTimeout;
        }
    }

    private static HttpSendGate Gate(int max, TimeProvider? time = null) => new(max, index => new HttpSendSlot(index), time);

    private enum Kind
    {
        Success,
        Fault,
        Timeout,
        Cancel,
        Mapper,
    }
}
