namespace Pernix.OutboxRelay.Client;

/// <summary>Column names of the outbox table, as created by the relay's V001 script.</summary>
public static class OutboxColumns
{
    /// <summary>Row id, <c>bigint</c> identity. Relay-owned.</summary>
    public const string Id = "id";

    /// <summary>Producing transaction id, <c>xid8</c>, defaulted by the database.</summary>
    public const string TxId = "tx_id";

    /// <summary>Message key, <c>bytea</c>, nullable. Written by the producer.</summary>
    public const string MessageKey = "message_key";

    /// <summary>Message payload, <c>bytea</c>. Written by the producer.</summary>
    public const string Payload = "payload";

    /// <summary>Message headers, <c>jsonb</c> object of string values, default <c>{}</c>. Written by the producer.</summary>
    public const string Headers = "headers";

    /// <summary>Route name, <c>text</c>, nullable, no default. Written by the producer. NULL means the row has no route.</summary>
    public const string Route = "route";

    /// <summary>Insert time, <c>timestamptz</c>, defaulted by the database.</summary>
    public const string CreatedAt = "created_at";

    /// <summary>Failed delivery attempts. Relay-owned.</summary>
    public const string RetryCount = "retry_count";

    /// <summary>Earliest next delivery attempt. Relay-owned.</summary>
    public const string NextAttemptAt = "next_attempt_at";

    /// <summary>Last delivery error. Relay-owned.</summary>
    public const string LastError = "last_error";

    /// <summary>Gets the columns a producer writes: <see cref="MessageKey"/>, <see cref="Payload"/>, <see cref="Headers"/>, <see cref="Route"/>.</summary>
    public static IReadOnlyList<string> ProducerWritten { get; } = Array.AsReadOnly(new[] { MessageKey, Payload, Headers, Route });

    /// <summary>Gets every column, in table order.</summary>
    public static IReadOnlyList<string> All { get; } = Array.AsReadOnly(new[]
    {
        Id,
        TxId,
        MessageKey,
        Payload,
        Headers,
        Route,
        CreatedAt,
        RetryCount,
        NextAttemptAt,
        LastError,
    });
}
