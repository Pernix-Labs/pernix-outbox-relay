using System.Buffers;
using Pernix.OutboxRelay.Core.Abstractions;
using Npgsql;

namespace Pernix.OutboxRelay.Outbox.PostgreSql;

// Maps a read row into OutboxMessage by ordinal, columns in ReadColumns order. One per session. Verify once per prepared command.
internal sealed class OutboxRowReader
{
    private const int Id = 0, Order = 1, Key = 2, Payload = 3, Headers = 4, CreatedAt = 5, RetryCount = 6, NextAttemptAt = 7, IsDue = 8,
        LastError = 9, Route = 10;

    private static readonly string[] Names =
        ["Id", "Order", "Key", "Payload", "Headers", "CreatedAt", "RetryCount", "NextAttemptAt", "IsDue", "LastError", "Route"];

    private readonly HeadersParser parser = new();

    public static void Verify(NpgsqlDataReader reader)
    {
        if (reader.FieldCount != Names.Length)
        {
            throw new InvalidOperationException($"Read returned {reader.FieldCount} columns, expected {Names.Length}.");
        }

        for (var i = 0; i < Names.Length; i++)
        {
            var name = reader.GetName(i);
            if (!string.Equals(name, Names[i], StringComparison.Ordinal))
            {
                throw new InvalidOperationException($"Read column {i} is '{name}', expected '{Names[i]}'.");
            }
        }
    }

    // Argument order is evaluation order, so SequentialAccess reads columns in order.
    public OutboxMessage Read(NpgsqlDataReader reader) => new(
        id: reader.GetInt64(Id),
        order: reader.GetFieldValue<ulong>(Order),
        key: reader.IsDBNull(Key) ? null : reader.GetFieldValue<byte[]>(Key),
        payload: reader.IsDBNull(Payload) ? null : reader.GetFieldValue<byte[]>(Payload),
        headers: reader.IsDBNull(Headers) ? null : ReadHeaders(reader),   // null = malformed
        createdAt: reader.GetFieldValue<DateTimeOffset>(CreatedAt),
        retryCount: reader.GetInt32(RetryCount),
        nextAttemptAt: reader.IsDBNull(NextAttemptAt) ? null : reader.GetFieldValue<DateTimeOffset>(NextAttemptAt),
        isDue: reader.GetBoolean(IsDue),
        lastError: reader.IsDBNull(LastError) ? null : reader.GetString(LastError),
        route: reader.IsDBNull(Route) ? null : reader.GetString(Route));

    private MessageHeaders? ReadHeaders(NpgsqlDataReader reader)
    {
        var length = (int)reader.GetBytes(Headers, 0, null, 0, 0);
        var buffer = ArrayPool<byte>.Shared.Rent(length);
        try
        {
            var read = (int)reader.GetBytes(Headers, 0, buffer, 0, length);
            return parser.Parse(buffer.AsSpan(0, read));
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }
}
