using System.Text.Json;
using System.Text.Json.Serialization.Metadata;

namespace Pernix.OutboxRelay.Client;

/// <summary>
/// One outbox message: key, payload, headers and an optional route (<see cref="WithRoute(string)"/>). Key and payload are bytes written as given. The client checks no size
/// and no key presence. Create one with <see cref="FromBytes(byte[], ReadOnlyMemory{byte}, OutboxHeaders)"/>,
/// <see cref="Create(string, ReadOnlyMemory{byte}, OutboxHeaders)"/> or
/// <see cref="Json{T}(string, T, JsonTypeInfo{T}, OutboxHeaders)"/>. Every factory takes the key first.
/// </summary>
public readonly struct OutboxMessage
{
    private readonly OutboxHeaders? headers;
    private readonly string? route;

    /// <summary>Creates a message from bytes. Internal so that every public entry point takes the key first.</summary>
    /// <param name="payload">The payload bytes.</param>
    /// <param name="key">The key bytes. Null writes SQL NULL. Empty writes an empty <c>bytea</c>.</param>
    /// <param name="headers">The headers. Null means <see cref="OutboxHeaders.Empty"/>.</param>
    internal OutboxMessage(ReadOnlyMemory<byte> payload, ReadOnlyMemory<byte>? key = null, OutboxHeaders? headers = null)
        : this(payload, key, headers, route: null)
    {
    }

    private OutboxMessage(ReadOnlyMemory<byte> payload, ReadOnlyMemory<byte>? key, OutboxHeaders? headers, string? route)
    {
        Payload = payload;
        Key = key;
        this.headers = headers;
        this.route = route;
    }

    /// <summary>Gets the payload bytes.</summary>
    public ReadOnlyMemory<byte> Payload { get; }

    /// <summary>Gets the key bytes. Null is written as SQL NULL. Empty is written as an empty <c>bytea</c>.</summary>
    public ReadOnlyMemory<byte>? Key { get; }

    /// <summary>Gets the headers. Never null: <see cref="OutboxHeaders.Empty"/> when none were given.</summary>
    public OutboxHeaders Headers => headers ?? OutboxHeaders.Empty;

    /// <summary>
    /// Gets the route. Null: the row has no route (taken only by a single-address destination). Otherwise a route name
    /// matching <c>^[a-z0-9][a-z0-9._-]{0,62}$</c>. Broadcast or subset: one message per route, in one transaction.
    /// </summary>
    public string? Route => route;

    /// <summary>
    /// Returns a copy with <see cref="Route"/> = <paramref name="route"/>. <see cref="Key"/>, <see cref="Payload"/> and
    /// <see cref="Headers"/> unchanged; this instance unchanged. Replaces an existing route. Stores
    /// <paramref name="route"/> without copying. Validates without allocating.
    /// </summary>
    /// <param name="route">The route name.</param>
    /// <returns>The routed copy.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="route"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="route"/> is outside <c>^[a-z0-9][a-z0-9._-]{0,62}$</c>.</exception>
    public OutboxMessage WithRoute(string route)
    {
        RouteRules.Validate(route, nameof(route));
        return new(Payload, Key, headers, route);
    }

    /// <summary>Creates a message with a byte-array key and a byte payload.</summary>
    /// <param name="key">
    /// The key bytes. A null array writes SQL NULL. An empty array writes an empty <c>bytea</c>. The array is not copied.
    /// </param>
    /// <param name="payload">The payload bytes.</param>
    /// <param name="headers">The headers. Null means <see cref="OutboxHeaders.Empty"/>.</param>
    /// <returns>The message.</returns>
    public static OutboxMessage FromBytes(byte[]? key, ReadOnlyMemory<byte> payload, OutboxHeaders? headers = null)
    {
        // A branch, not a conditional: in "key is null ? null : new ReadOnlyMemory<byte>(key)" the null converts through
        // the array conversion to an empty ReadOnlyMemory, not to a null key.
        if (key is null)
        {
            return new(payload, null, headers);
        }

        return new(payload, new ReadOnlyMemory<byte>(key), headers);
    }

    /// <summary>Creates a message with a byte-memory key and a byte payload.</summary>
    /// <remarks>
    /// A literal <c>null</c> key and a <c>byte[]</c> key bind to
    /// <see cref="FromBytes(byte[], ReadOnlyMemory{byte}, OutboxHeaders)"/>, so a null array still writes SQL NULL.
    /// </remarks>
    /// <param name="key">The key bytes. Null writes SQL NULL. Empty writes an empty <c>bytea</c>.</param>
    /// <param name="payload">The payload bytes.</param>
    /// <param name="headers">The headers. Null means <see cref="OutboxHeaders.Empty"/>.</param>
    /// <returns>The message.</returns>
    public static OutboxMessage FromBytes(ReadOnlyMemory<byte>? key, ReadOnlyMemory<byte> payload, OutboxHeaders? headers = null) =>
        new(payload, key, headers);

    /// <summary>Creates a message with a UTF-8 key and a byte payload.</summary>
    /// <param name="key">The key. Null writes SQL NULL. Otherwise its UTF-8 bytes.</param>
    /// <param name="payload">The payload bytes.</param>
    /// <param name="headers">The headers. Null means <see cref="OutboxHeaders.Empty"/>.</param>
    /// <returns>The message.</returns>
    /// <exception cref="ArgumentException"><paramref name="key"/> contains a lone surrogate.</exception>
    public static OutboxMessage Create(string? key, ReadOnlyMemory<byte> payload, OutboxHeaders? headers = null) =>
        new(payload, EncodeKey(key), headers);

    /// <summary>Creates a message with a UTF-8 key and a UTF-8 text payload.</summary>
    /// <param name="key">The key. Null writes SQL NULL. Otherwise its UTF-8 bytes.</param>
    /// <param name="payload">The payload text, written as its UTF-8 bytes.</param>
    /// <param name="headers">The headers. Null means <see cref="OutboxHeaders.Empty"/>.</param>
    /// <returns>The message.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="payload"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="key"/> or <paramref name="payload"/> contains a lone surrogate.</exception>
    public static OutboxMessage Create(string? key, string payload, OutboxHeaders? headers = null)
    {
        ArgumentNullException.ThrowIfNull(payload);
        var keyBytes = EncodeKey(key);
        return new(StrictUtf8.Encode(payload, nameof(payload)), keyBytes, headers);
    }

    /// <summary>Creates a message whose payload is the UTF-8 JSON of <paramref name="value"/>, written by source-generated metadata.</summary>
    /// <typeparam name="T">The payload type.</typeparam>
    /// <param name="key">The key. Null writes SQL NULL. Otherwise its UTF-8 bytes.</param>
    /// <param name="value">The value to serialize.</param>
    /// <param name="typeInfo">The source-generated JSON metadata for <typeparamref name="T"/>.</param>
    /// <param name="headers">The headers. Null means <see cref="OutboxHeaders.Empty"/>.</param>
    /// <returns>The message.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="typeInfo"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="key"/> contains a lone surrogate.</exception>
    public static OutboxMessage Json<T>(string? key, T value, JsonTypeInfo<T> typeInfo, OutboxHeaders? headers = null)
    {
        ArgumentNullException.ThrowIfNull(typeInfo);
        var keyBytes = EncodeKey(key);
        return new(JsonSerializer.SerializeToUtf8Bytes(value, typeInfo), keyBytes, headers);
    }

    // A null byte[] converts to an empty ReadOnlyMemory, so null must be returned before any conversion.
    private static ReadOnlyMemory<byte>? EncodeKey(string? key)
    {
        if (key is null)
        {
            return null;
        }

        return StrictUtf8.Encode(key, nameof(key));
    }
}
