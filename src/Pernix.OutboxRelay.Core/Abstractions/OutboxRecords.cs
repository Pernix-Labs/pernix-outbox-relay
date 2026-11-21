using System.Diagnostics.CodeAnalysis;

namespace Pernix.OutboxRelay.Core.Abstractions;

public sealed record ReadRequest(int Max, OutboxPosition? After, IReadOnlyCollection<long> Exclude);

public readonly record struct OutboxPosition(ulong Order, long Id) : IComparable<OutboxPosition>
{
    public int CompareTo(OutboxPosition other)
    {
        var order = Order.CompareTo(other.Order);
        return order != 0 ? order : Id.CompareTo(other.Id);
    }

    public static bool operator <(OutboxPosition left, OutboxPosition right) => left.CompareTo(right) < 0;

    public static bool operator >(OutboxPosition left, OutboxPosition right) => left.CompareTo(right) > 0;

    public static bool operator <=(OutboxPosition left, OutboxPosition right) => left.CompareTo(right) <= 0;

    public static bool operator >=(OutboxPosition left, OutboxPosition right) => left.CompareTo(right) >= 0;
}

// Rows with retry_count > 0. Checksum and NextDueAt come from the database; compared for equality only.
public readonly record struct FailedRowsSummary(long Count, decimal Checksum, DateTimeOffset? NextDueAt);

public sealed record OutboxMessage
{
    public OutboxMessage()
    {
    }

    // Provider row mapping, one argument per read column. Null headers are malformed, a null payload is empty.
    [SetsRequiredMembers]
    public OutboxMessage(
        long id,
        ulong order,
        byte[]? key,
        byte[]? payload,
        MessageHeaders? headers,
        DateTimeOffset createdAt,
        int retryCount,
        DateTimeOffset? nextAttemptAt,
        bool isDue,
        string? lastError,
        string? route)
    {
        Id = id;
        Position = new OutboxPosition(order, id);
        Key = key;
        Payload = payload ?? [];
        Headers = headers ?? MessageHeaders.Empty;
        HeadersMalformed = headers is null;
        CreatedAt = createdAt;
        RetryCount = retryCount;
        NextAttemptAt = nextAttemptAt;
        IsDue = isDue;
        LastError = lastError;
        Route = route;
    }

    public required long Id { get; init; }

    public required OutboxPosition Position { get; init; }

    public byte[]? Key { get; init; }

    public required byte[] Payload { get; init; }

    public required MessageHeaders Headers { get; init; }

    // Row headers are not a JSON object of string values. Headers is then empty.
    public bool HeadersMalformed { get; init; }

    public required DateTimeOffset CreatedAt { get; init; }

    public required int RetryCount { get; init; }

    public DateTimeOffset? NextAttemptAt { get; init; }

    public string? LastError { get; init; }

    // True when the row may be sent now. "Due" means next_attempt_at <= now() on the database clock. Core never compares row times.
    // On PerKey and Strict a retry_count = 0 row may be sent even when not due, but PerKey sets false behind a waiting failed row of its key.
    public required bool IsDue { get; init; }

    // Producer-written route. Null: the row has no route. The provider fills it from the row's route column. Also null when the provider omits it on a not-due row (IsDue false).
    public string? Route { get; init; }
}

// Closed: only the nested outcomes exist, so a provider can refuse any it does not handle.
public abstract record Outcome
{
    private Outcome(long id) => Id = id;

    public long Id { get; }

    public sealed record Remove(long Id) : Outcome(Id);

    // Provider writes next_attempt_at = now() + Delay from the database clock, guarded by ReadRetryCount.
    public sealed record Fail(long Id) : Outcome(Id)
    {
        public required int ReadRetryCount { get; init; }

        public required int RetryCount { get; init; }

        public required TimeSpan Delay { get; init; }

        public required string Error { get; init; }
    }

    // Provider writes next_attempt_at = 'infinity', guarded by ReadRetryCount. Held until an operator acts.
    public sealed record Block(long Id) : Outcome(Id)
    {
        public required int ReadRetryCount { get; init; }

        public required int RetryCount { get; init; }

        public required string Error { get; init; }
    }
}
