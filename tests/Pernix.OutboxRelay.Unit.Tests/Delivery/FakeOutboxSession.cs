using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;

namespace Pernix.OutboxRelay.Unit.Tests.Delivery;

internal sealed record ReadCall(TimeSpan At, int Max, IReadOnlyCollection<long> Exclude, IReadOnlyList<long> Returned, OutboxPosition? After = null);

internal sealed record CommitCall(TimeSpan At, IReadOnlyList<Outcome> Outcomes)
{
    public IReadOnlyList<long> Ids => [.. Outcomes.Select(o => o.Id)];
}

internal sealed class FakeOutboxSession(TimeProvider time) : IOutboxSession
{
    private readonly object gate = new();
    private readonly SortedDictionary<long, OutboxMessage> table = [];
    private readonly List<ReadCall> reads = [];
    private readonly List<CommitCall> commits = [];
    private readonly List<int> commitThreads = [];
    private readonly List<TimeSpan> checks = [];
    private readonly List<TimeSpan> summaryReads = [];
    private readonly CancellationTokenSource lost = new();
    private readonly DateTimeOffset start = time.GetUtcNow();
    // Gates a session call is waiting on. A released gate stops counting at once, before the waiter resumes.
    private readonly List<TaskCompletionSource> waiting = [];

    public CancellationToken Lost => lost.Token;

    public bool ReleasePending => false;

    public IReadOnlyList<ReadCall> Reads
    {
        get { lock (gate) { return [.. reads]; } }
    }

    public IReadOnlyList<CommitCall> Commits
    {
        get { lock (gate) { return [.. commits]; } }
    }

    public IReadOnlyList<int> CommitThreads
    {
        get { lock (gate) { return [.. commitThreads]; } }
    }

    public IReadOnlyList<TimeSpan> Checks
    {
        get { lock (gate) { return [.. checks]; } }
    }

    public IReadOnlyCollection<long> TableIds
    {
        get { lock (gate) { return [.. table.Keys]; } }
    }

    public OutboxMessage? Get(long id)
    {
        lock (gate)
        {
            return table.GetValueOrDefault(id);
        }
    }

    public IReadOnlyList<TimeSpan> SummaryReads
    {
        get { lock (gate) { return [.. summaryReads]; } }
    }

    public Queue<FailedRowsSummary> SummaryScript { get; } = new();

    public FailedRowsSummary Summary { get; set; } = new(0, 0, null);

    // Reads like the provider's per-mode SQL.
    public OrderingMode Mode { get; set; } = OrderingMode.None;

    public TaskCompletionSource? CheckGate { get; set; }

    public TaskCompletionSource? CommitGate { get; set; }

    public Exception? NextReadFailure { get; set; }

    public Exception? NextCheckFailure { get; set; }

    public bool Blocked
    {
        get
        {
            lock (gate)
            {
                return waiting.Exists(g => !g.Task.IsCompleted);
            }
        }
    }

    public void Insert(params long[] ids)
    {
        foreach (var id in ids)
        {
            Insert(Row(id));
        }
    }

    public void Insert(params OutboxMessage[] rows)
    {
        lock (gate)
        {
            foreach (var row in rows)
            {
                table[row.Id] = row;
            }
        }
    }

    public static OutboxMessage Row(long id, bool malformedHeaders = false, int retryCount = 0, string? lastError = null) => new()
    {
        Id = id,
        Position = new OutboxPosition((ulong)id, id),
        Key = [(byte)id],
        Payload = [1, 2, 3],
        Headers = MessageHeaders.Empty,
        HeadersMalformed = malformedHeaders,
        CreatedAt = DateTimeOffset.UnixEpoch,
        RetryCount = retryCount,
        NextAttemptAt = DateTimeOffset.UnixEpoch,
        LastError = lastError,
        IsDue = true,
    };

    public void SignalLost() => lost.Cancel();

    public Task<bool> TryLockAsync(CancellationToken ct) => Task.FromResult(true);

    public async Task CheckAsync(CancellationToken ct)
    {
        lock (gate)
        {
            checks.Add(Now);
        }

        if (NextCheckFailure is { } failure)
        {
            NextCheckFailure = null;
            throw failure;
        }

        await BlockAsync(CheckGate).ConfigureAwait(false);
    }

    public Task<IReadOnlyList<OutboxMessage>> ReadAsync(ReadRequest request, CancellationToken ct)
    {
        if (NextReadFailure is { } failure)
        {
            NextReadFailure = null;
            return Task.FromException<IReadOnlyList<OutboxMessage>>(failure);
        }

        lock (gate)
        {
            var now = time.GetUtcNow();
            List<OutboxMessage> rows = Mode switch
            {
                OrderingMode.None => [.. table.Values.Where(r => !request.Exclude.Contains(r.Id) && IsDue(r, now)).Take(request.Max)],
                OrderingMode.PerKey => [.. Ordered(request).Take(request.Max).Select(r => r with { IsDue = IsDue(r, now) && !KeyHeldBack(r, now) })],
                _ => [.. Ordered(request).Take(request.Max).Select(r => r with { IsDue = IsDue(r, now) })],
            };
            reads.Add(new ReadCall(Now, request.Max, [.. request.Exclude], [.. rows.Select(r => r.Id)], request.After));
            return Task.FromResult<IReadOnlyList<OutboxMessage>>(rows);
        }
    }

    public async Task CommitAsync(IReadOnlyList<Outcome> outcomes, CancellationToken ct)
    {
        lock (gate)
        {
            commitThreads.Add(Environment.CurrentManagedThreadId);
        }

        await BlockAsync(CommitGate).ConfigureAwait(false);
        lock (gate)
        {
            commits.Add(new CommitCall(Now, [.. outcomes]));
            foreach (var outcome in outcomes)
            {
                switch (outcome)
                {
                    case Outcome.Remove:
                        table.Remove(outcome.Id);
                        break;

                    // Guarded like the provider's UPDATE: an operator change since the read wins.
                    case Outcome.Fail fail when table.TryGetValue(fail.Id, out var row) && row.RetryCount == fail.ReadRetryCount:
                        table[fail.Id] = row with
                        {
                            RetryCount = fail.RetryCount,
                            NextAttemptAt = time.GetUtcNow() + fail.Delay,
                            LastError = fail.Error,
                        };
                        break;

                    case Outcome.Block block when table.TryGetValue(block.Id, out var row) && row.RetryCount == block.ReadRetryCount:
                        table[block.Id] = row with
                        {
                            RetryCount = block.RetryCount,
                            NextAttemptAt = null,
                            LastError = block.Error,
                        };
                        break;
                }
            }
        }
    }

    public Task<FailedRowsSummary> ReadFailedSummaryAsync(CancellationToken ct)
    {
        lock (gate)
        {
            summaryReads.Add(Now);
            return Task.FromResult(SummaryScript.TryDequeue(out var next) ? next : Summary);
        }
    }

    public void Update(long id, Func<OutboxMessage, OutboxMessage> change)
    {
        lock (gate)
        {
            table[id] = change(table[id]);
        }
    }

    public void Delete(long id)
    {
        lock (gate)
        {
            table.Remove(id);
        }
    }

    private static bool IsBlocked(OutboxMessage row) => row.RetryCount > 0 && row.NextAttemptAt is null;

    private static bool IsDue(OutboxMessage row, DateTimeOffset now) => !IsBlocked(row) && (row.NextAttemptAt is null || row.NextAttemptAt <= now);

    private IEnumerable<OutboxMessage> Ordered(ReadRequest request) =>
        table.Values.OrderBy(r => r.Position).Where(r => request.After is not { } after || r.Position > after);

    private bool KeyHeldBack(OutboxMessage row, DateTimeOffset now) =>
        row.Key is { } key && table.Values.Any(b => b.RetryCount > 0 && b.Key is { } other && other.AsSpan().SequenceEqual(key) && !IsDue(b, now));

    public ValueTask DisposeAsync()
    {
        lost.Dispose();
        return ValueTask.CompletedTask;
    }

    private TimeSpan Now => time.GetUtcNow() - start;

    private async Task BlockAsync(TaskCompletionSource? gateSource)
    {
        if (gateSource is null)
        {
            return;
        }

        lock (gate)
        {
            waiting.Add(gateSource);
        }

        try
        {
            await gateSource.Task.ConfigureAwait(false);
        }
        finally
        {
            lock (gate)
            {
                waiting.Remove(gateSource);
            }
        }
    }
}
