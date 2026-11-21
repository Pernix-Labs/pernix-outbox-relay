namespace Pernix.OutboxRelay.Core.Abstractions;

public interface IOutboxSession : IAsyncDisposable
{
    CancellationToken Lost { get; }

    bool ReleasePending { get; }

    Task<bool> TryLockAsync(CancellationToken ct);

    Task CheckAsync(CancellationToken ct);

    // None: due rows only, by id, minus Exclude, After null. PerKey: the next Max rows after After in (tx_id, id) order under the gate,
    // every scanned row, IsDue false (no payload or headers) when the row is a failed row that is not due, or its key has a waiting failed row.
    // A retry_count = 0 row may be sent at once. NULL keys hold nothing.
    // Strict: every row after After in (tx_id, id) order under the gate, IsDue = retry_count = 0 or the row is due. PerKey and Strict: Exclude empty.
    Task<IReadOnlyList<OutboxMessage>> ReadAsync(ReadRequest request, CancellationToken ct);

    Task CommitAsync(IReadOnlyList<Outcome> outcomes, CancellationToken ct);

    Task<FailedRowsSummary> ReadFailedSummaryAsync(CancellationToken ct);
}
