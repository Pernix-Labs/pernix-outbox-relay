using System.Collections.Concurrent;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Core.Leadership;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;

namespace Pernix.OutboxRelay.Unit.Tests.Leadership;

internal sealed class FakeOutboxStore(TimeProvider timeProvider) : IOutboxStore, IValidatedOutboxStore
{
    public FakeOutboxStore()
        : this(TimeProvider.System)
    {
    }

    public List<FakeOutboxSession> Sessions { get; } = [];

    public List<TimeSpan> OpenAttempts { get; } = [];

    public int ValidateCalls { get; private set; }

    public Exception? ValidateFailure { get; set; }

    public Exception? OpenFailure { get; set; }

    public IValidatedOutboxStore? ValidatedHandle { get; set; }

    public bool LockGranted { get; set; } = true;

    public Exception? NextLockFailure { get; set; }

    // Holds each lock attempt until its token is cancelled, completing it from a callback registered after the worker's.
    public bool LockHangsUntilCancelled { get; set; }

    public Exception? NextCheckFailure { get; set; }

    public DateTimeOffset Start { get; } = timeProvider.GetUtcNow();

    public FakeOutboxSession CurrentSession => Sessions[^1];

    public TimeSpan Now => timeProvider.GetUtcNow() - Start;

    public Task<IValidatedOutboxStore> ValidateAsync(CancellationToken ct)
    {
        ValidateCalls++;
        return ValidateFailure is { } failure
            ? Task.FromException<IValidatedOutboxStore>(failure)
            : Task.FromResult(ValidatedHandle ?? this);
    }

    public Task<IOutboxSession> OpenSessionAsync(CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        OpenAttempts.Add(Now);
        if (OpenFailure is { } failure)
        {
            return Task.FromException<IOutboxSession>(failure);
        }

        var session = new FakeOutboxSession(this);
        Sessions.Add(session);
        return Task.FromResult<IOutboxSession>(session);
    }

    public Func<CancellationToken, Task<OutboxStats>> Stats { get; set; } = static _ => Task.FromResult(default(OutboxStats));

    public int StatsCalls => Volatile.Read(ref statsCalls);

    private int statsCalls;

    public Task<OutboxStats> GetStatsAsync(CancellationToken ct)
    {
        Interlocked.Increment(ref statsCalls);
        return Stats(ct);
    }

    public Exception? TakeLockFailure()
    {
        var failure = NextLockFailure;
        NextLockFailure = null;
        return failure;
    }

    public Exception? TakeCheckFailure()
    {
        var failure = NextCheckFailure;
        NextCheckFailure = null;
        return failure;
    }
}

internal sealed class FakeOutboxSession(FakeOutboxStore store) : IOutboxSession
{
    private readonly CancellationTokenSource lost = new();

    public CancellationToken Lost => lost.Token;

    public List<TimeSpan> LockAttempts { get; } = [];

    public List<TimeSpan> Checks { get; } = [];

    public bool Disposed { get; private set; }

    public bool ReleasePending { get; set; }

    public Action? OnDispose { get; set; }

    public void SignalLost() => lost.Cancel();

    public Task<bool> TryLockAsync(CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        if (store.LockHangsUntilCancelled)
        {
            var pending = new TaskCompletionSource<bool>();
            ct.Register(() => pending.TrySetCanceled(ct));
            LockAttempts.Add(store.Now);
            return pending.Task;
        }

        LockAttempts.Add(store.Now);
        return store.TakeLockFailure() is { } failure
            ? Task.FromException<bool>(failure)
            : Task.FromResult(store.LockGranted);
    }

    public Task CheckAsync(CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        Checks.Add(store.Now);
        return store.TakeCheckFailure() is { } failure ? Task.FromException(failure) : Task.CompletedTask;
    }

    public Task<IReadOnlyList<OutboxMessage>> ReadAsync(ReadRequest request, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        return Task.FromResult<IReadOnlyList<OutboxMessage>>([]);
    }

    public Task CommitAsync(IReadOnlyList<Outcome> outcomes, CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        return Task.CompletedTask;
    }

    public FailedRowsSummary Summary { get; set; } = new(0, 0, null);

    public List<TimeSpan> SummaryReads { get; } = [];

    public Task<FailedRowsSummary> ReadFailedSummaryAsync(CancellationToken ct)
    {
        ct.ThrowIfCancellationRequested();
        SummaryReads.Add(store.Now);
        return Task.FromResult(Summary);
    }

    // Holds the close open until completed, to model a hung connection close.
    public TaskCompletionSource? DisposeGate { get; set; }

    public bool DisposeStarted { get; private set; }

    public int DisposeCalls => Volatile.Read(ref disposeCalls);

    private int disposeCalls;

    public async ValueTask DisposeAsync()
    {
        Interlocked.Increment(ref disposeCalls);
        DisposeStarted = true;
        OnDispose?.Invoke();
        if (DisposeGate is { } gate)
        {
            await gate.Task.ConfigureAwait(false);
        }

        Disposed = true;
    }
}

internal sealed class FakeOutboxStoreProvider(string name, params OrderingMode[] supportedModes) : IOutboxStoreProvider
{
    public FakeOutboxStoreProvider()
        : this("PostgreSql", OrderingMode.None, OrderingMode.PerKey, OrderingMode.Strict)
    {
    }

    public string Name { get; } = name;

    public IReadOnlySet<OrderingMode> SupportedModes { get; } = supportedModes.ToHashSet();

    public Dictionary<string, FakeOutboxStore> Stores { get; } = new(StringComparer.OrdinalIgnoreCase);

    public Action<string, FakeOutboxStore>? Configure { get; set; }

    public Dictionary<string, Exception> CreateFailures { get; } = new(StringComparer.OrdinalIgnoreCase);

    public IOutboxStore CreateStore(OutboxOptions options)
    {
        if (CreateFailures.TryGetValue(options.Name!, out var failure))
        {
            throw failure;
        }

        var store = new FakeOutboxStore();
        Configure?.Invoke(options.Name!, store);
        Stores[options.Name!] = store;
        return store;
    }
}

internal sealed class FixedJitter(TimeSpan delay) : ILeadershipJitter
{
    public ValueTask<TimeSpan> NextStartDelayAsync(string outboxName, CancellationToken ct) => ValueTask.FromResult(delay);
}

internal sealed record LogEntry(LogLevel Level, EventId EventId, string Message, Exception? Exception);

internal sealed class RecordingLogger<T> : ILogger<T>
{
    private readonly ConcurrentQueue<LogEntry> entries = new();

    public IReadOnlyList<LogEntry> Entries => [.. entries];

    public IEnumerable<LogEntry> WithEvent(int eventId) => Entries.Where(e => e.EventId.Id == eventId);

    public IDisposable? BeginScope<TState>(TState state)
        where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => true;

    public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter) =>
        entries.Enqueue(new LogEntry(logLevel, eventId, formatter(state, exception), exception));
}

// Counts timers so a test advances only once the worker is idle on its next wait.
internal sealed class WorkerClock : FakeTimeProvider
{
    private readonly ConcurrentQueue<TimeSpan> dueTimes = new();
    private int created;
    private int active;
    private int fired;

    public int Created => Volatile.Read(ref created);

    public int Active => Volatile.Read(ref active);

    public IReadOnlyList<TimeSpan> DueTimes => [.. dueTimes];

    // Runs before the timer exists, so a WaitAsync promise (the state) can gain a continuation before the worker awaits it.
    public Action<TimeSpan, object?>? TimerCreating { get; set; }

    public override ITimer CreateTimer(TimerCallback callback, object? state, TimeSpan dueTime, TimeSpan period)
    {
        TimerCreating?.Invoke(dueTime, state);
        var tracked = new TrackedTimer(this);
        tracked.Inner = base.CreateTimer(
            s =>
            {
                Interlocked.Increment(ref fired);
                tracked.End();
                callback(s);
            },
            state,
            dueTime,
            period);

        // Count after registration so a test never advances time before this timer is scheduled.
        dueTimes.Enqueue(dueTime);
        Interlocked.Increment(ref created);
        tracked.Activate();
        return tracked;
    }

    public async Task AdvanceAsync(TimeSpan total, TimeSpan step, Func<bool> settled)
    {
        var end = GetUtcNow() + total;
        while (GetUtcNow() < end)
        {
            var before = Created;
            var firedBefore = Volatile.Read(ref fired);
            Advance(step);
            if (Volatile.Read(ref fired) != firedBefore)
            {
                await WaitUntilAsync(() => (Created > before && Active > 0) || settled());
            }
        }
    }

    // Waits for a timer of the given due time created after the mark, so the next advance reaches it.
    public Task WaitForTimerAsync(TimeSpan dueTime, int mark) =>
        WaitUntilAsync(() => DueTimes.Skip(mark).Contains(dueTime));

    public static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (!condition())
        {
            if (DateTime.UtcNow >= deadline)
            {
                throw new TimeoutException("condition not met within 5 s");
            }

            await Task.Delay(5).ConfigureAwait(false);
        }
    }

    private sealed class TrackedTimer(WorkerClock clock) : ITimer
    {
        private const int Pending = 0;
        private const int Running = 1;
        private const int Ended = 2;
        private int phase;

        public ITimer? Inner { get; set; }

        // A timer that fired or was disposed during registration never counts as active.
        public void Activate()
        {
            if (Interlocked.CompareExchange(ref phase, Running, Pending) == Pending)
            {
                Interlocked.Increment(ref clock.active);
            }
        }

        public void End()
        {
            if (Interlocked.Exchange(ref phase, Ended) == Running)
            {
                Interlocked.Decrement(ref clock.active);
            }
        }

        // A finite re-arm counts as a new timer, so a test sees the worker idle on its next wait.
        public bool Change(TimeSpan dueTime, TimeSpan period)
        {
            var finite = dueTime != Timeout.InfiniteTimeSpan;
            if (finite)
            {
                Interlocked.CompareExchange(ref phase, Pending, Ended);
            }

            var changed = Inner!.Change(dueTime, period);
            if (finite)
            {
                clock.dueTimes.Enqueue(dueTime);
                Interlocked.Increment(ref clock.created);
                Activate();
            }

            return changed;
        }

        public void Dispose()
        {
            End();
            Inner?.Dispose();
        }

        public ValueTask DisposeAsync()
        {
            Dispose();
            return ValueTask.CompletedTask;
        }
    }
}
