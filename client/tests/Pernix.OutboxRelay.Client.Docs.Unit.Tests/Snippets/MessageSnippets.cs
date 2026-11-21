namespace Pernix.OutboxRelay.Client.Docs.Unit.Tests.Snippets;

internal static class MessageSnippets
{
    public static (OutboxMessage Bytes, OutboxMessage Text) Create(Guid orderId, ReadOnlyMemory<byte> payloadBytes, string payloadText, OutboxHeaders headers)
    {
        #region docs:message-create
        var fromBytes = OutboxMessage.Create(orderId.ToString(), payloadBytes, headers);
        var fromText = OutboxMessage.Create(orderId.ToString(), payloadText, headers);
        #endregion
        return (fromBytes, fromText);
    }

    public static OutboxMessage Json(OrderPlaced placed, OutboxHeaders headers)
    {
        #region docs:message-json
        // Source-generated metadata only: AppJson is a JsonSerializerContext with [JsonSerializable(typeof(OrderPlaced))].
        var message = OutboxMessage.Json(placed.OrderId.ToString(), placed, AppJson.Default.OrderPlaced, headers);
        #endregion
        return message;
    }

    public static (OutboxMessage Array, OutboxMessage Memory) FromBytes(
        byte[]? keyArray, ReadOnlyMemory<byte>? keyMemory, ReadOnlyMemory<byte> payload, OutboxHeaders headers)
    {
        #region docs:message-frombytes
        var fromArray = OutboxMessage.FromBytes(keyArray, payload, headers);
        var fromMemory = OutboxMessage.FromBytes(keyMemory, payload, headers);
        #endregion
        return (fromArray, fromMemory);
    }

    public static OutboxHeaders Headers(OrderPlaced placed)
    {
        #region docs:message-headers
        var headers = OutboxHeaders.CreateBuilder(2)
            .Add("idempotency-key", placed.EventId.ToString())
            .Add("event-type", "order-placed")
            .Build();
        #endregion
        return headers;
    }

    public static OutboxMessage FromBytesTrap(bool noKey, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> payload)
    {
        #region docs:frombytes-trap
        // Wrong: the null converts to an empty ReadOnlyMemory<byte>. The key is an empty bytea, not SQL NULL.
        var message = OutboxMessage.FromBytes(noKey ? null : key, payload);
        #endregion
        return message;
    }

    public static OutboxMessage FromBytesNullKey(bool noKey, ReadOnlyMemory<byte> key, ReadOnlyMemory<byte> payload)
    {
        #region docs:frombytes-null-key
        // Right: cast one branch to ReadOnlyMemory<byte>?, so the null stays null and writes SQL NULL.
        var message = OutboxMessage.FromBytes(noKey ? null : (ReadOnlyMemory<byte>?)key, payload);
        #endregion
        return message;
    }
}
