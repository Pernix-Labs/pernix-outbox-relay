using Pernix.OutboxRelay.Core.Abstractions;

namespace Pernix.OutboxRelay.Benchmarks.Infrastructure;

internal sealed class InMemorySession : IOutboxSession
{
    private static readonly Task<bool> Locked = Task.FromResult(true);
    private static readonly Task<FailedRowsSummary> NoFailedRows = Task.FromResult(new FailedRowsSummary(0, 0, null));

    private readonly CancellationTokenSource lost = new();
    private readonly OutboxMessage[]? rows;
    private readonly long firstId;
    private readonly long total;
    private readonly MessageHeaders headers;
    private readonly string capacityName;
    private readonly List<OutboxMessage> buffer = new(1000);
    private readonly Task<IReadOnlyList<OutboxMessage>> bufferTask;
    private long next;
    private long available;
    private long removed;
    private long target;
    private long reads;
    private long commits;
    private TaskCompletionSource? waiter;

    private InMemorySession(OutboxMessage[]? rows, long firstId, long total, long available, MessageHeaders headers, string capacityName)
    {
        this.rows = rows;
        this.firstId = firstId;
        this.total = total;
        this.available = available;
        this.headers = headers;
        this.capacityName = capacityName;
        bufferTask = Task.FromResult<IReadOnlyList<OutboxMessage>>(buffer);
    }

    public static InMemorySession Preallocated(RowBlock rows) =>
        new(rows.Rows, rows.FirstId, rows.Count, rows.Count, MessageHeaders.Empty, "preallocated");

    public static InMemorySession Gated(RowBlock rows, string suite) =>
        new(rows.Rows, rows.FirstId, rows.Count, 0, MessageHeaders.Empty, suite);

    public static InMemorySession Fresh(long total, int headerCount) =>
        new(null, 1, total, total, Workload.Headers(headerCount), "fresh");

    public static InMemorySession Empty() => new([], 1, 0, 0, MessageHeaders.Empty, "empty");

    public Action<long>? OnRemoved { get; set; }

    public long Reads => Volatile.Read(ref reads);

    public long Commits => Volatile.Read(ref commits);

    public long Removed => Volatile.Read(ref removed);

    public CancellationToken Lost => lost.Token;

    public bool ReleasePending => false;

    public Task ReleaseAsync(int count)
    {
        var limit = Volatile.Read(ref available) + count;
        if (limit > total)
        {
            throw Workload.CapacityExhausted(capacityName);
        }

        var tcs = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        Volatile.Write(ref waiter, tcs);
        Volatile.Write(ref target, limit);
        Volatile.Write(ref available, limit);
        return tcs.Task;
    }

    public Task<bool> TryLockAsync(CancellationToken ct) => Locked;

    public Task CheckAsync(CancellationToken ct) => Task.CompletedTask;

    public Task<FailedRowsSummary> ReadFailedSummaryAsync(CancellationToken ct) => NoFailedRows;

    public Task<IReadOnlyList<OutboxMessage>> ReadAsync(ReadRequest request, CancellationToken ct)
    {
        Interlocked.Increment(ref reads);
        buffer.Clear();
        var limit = Volatile.Read(ref available);
        while (buffer.Count < request.Max && next < limit)
        {
            buffer.Add(rows is not null ? rows[next] : Workload.Row(firstId + next, headers));
            next++;
        }

        return bufferTask;
    }

    public Task CommitAsync(IReadOnlyList<Outcome> outcomes, CancellationToken ct)
    {
        Interlocked.Increment(ref commits);
        var onRemoved = OnRemoved;
        for (var i = 0; i < outcomes.Count; i++)
        {
            if (outcomes[i] is not Outcome.Remove)
            {
                continue;
            }

            var count = Interlocked.Increment(ref removed);
            onRemoved?.Invoke(count);
            if (count == Volatile.Read(ref target))
            {
                Volatile.Read(ref waiter)?.TrySetResult();
            }
        }

        return Task.CompletedTask;
    }

    public ValueTask DisposeAsync() => ValueTask.CompletedTask;
}
