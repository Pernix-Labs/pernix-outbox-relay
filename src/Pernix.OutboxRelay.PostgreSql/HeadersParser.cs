using System.Buffers;
using System.Text.Json;
using Pernix.OutboxRelay.Core.Abstractions;

namespace Pernix.OutboxRelay.PostgreSql;

// One per `OutboxRowReader`, so one per session. Not thread-safe.
internal sealed class HeadersParser
{
    private const int RetainedCapacity = 64;
    private const int NameCacheSize = 32;
    private const int MaxCachedNameBytes = 64;

    private readonly List<MessageHeader> headers = [];
    private readonly Dictionary<string, int> positions = new(StringComparer.Ordinal);

    // The first 32 distinct cacheable names, kept for the parser's life. Never evicted, never grown.
    private readonly byte[][] nameBytes = new byte[NameCacheSize][];
    private readonly string[] names = new string[NameCacheSize];
    private int nameCount;
    private int cachedNameBytes;

    internal int ScratchCapacity => Math.Max(headers.Capacity, positions.Capacity);

    internal int CachedNameCount => nameCount;

    internal int CachedNameBytes => cachedNameBytes;

    // Null when the text is not a JSON object of string values or does not decode, such as a lone surrogate.
    // A repeated name keeps its first position and last value.
    public MessageHeaders? Parse(ReadOnlySpan<byte> json)
    {
        try
        {
            return ParseObject(json);
        }
        catch (Exception ex) when (ex is JsonException or InvalidOperationException)
        {
            return null;
        }
        finally
        {
            Reset();
        }
    }

    private MessageHeaders? ParseObject(ReadOnlySpan<byte> json)
    {
        var reader = new Utf8JsonReader(json);
        if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
        {
            return null;
        }

        while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
        {
            var name = NameOf(ref reader);
            if (!reader.Read() || reader.TokenType != JsonTokenType.String)
            {
                return null;
            }

            Add(name, ValueOf(ref reader));
        }

        if (reader.Read())
        {
            return null;
        }

        return headers.Count == 0 ? MessageHeaders.Empty : new MessageHeaders([.. headers]);
    }

    private string NameOf(ref Utf8JsonReader reader)
    {
        if (reader.ValueIsEscaped || reader.HasValueSequence || reader.ValueSpan.Length > MaxCachedNameBytes)
        {
            return reader.GetString()!;
        }

        var span = reader.ValueSpan;
        for (var i = 0; i < nameCount; i++)
        {
            if (span.SequenceEqual(nameBytes[i]))
            {
                return names[i];
            }
        }

        var name = reader.GetString()!;
        if (nameCount < NameCacheSize)
        {
            nameBytes[nameCount] = span.ToArray();
            names[nameCount] = name;
            nameCount++;
            cachedNameBytes += span.Length;
        }

        return name;
    }

    private static byte[] ValueOf(ref Utf8JsonReader reader)
    {
        if (!reader.ValueIsEscaped)
        {
            return reader.ValueSpan.ToArray();
        }

        var buffer = ArrayPool<byte>.Shared.Rent(reader.ValueSpan.Length);
        try
        {
            var written = reader.CopyString(buffer);
            return buffer.AsSpan(0, written).ToArray();
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private void Add(string name, byte[] value)
    {
        if (positions.TryGetValue(name, out var at))
        {
            headers[at] = new MessageHeader(headers[at].Name, value);
            return;
        }

        positions.Add(name, headers.Count);
        headers.Add(new MessageHeader(name, value));
    }

    private void Reset()
    {
        headers.Clear();
        positions.Clear();
        if (headers.Capacity > RetainedCapacity)
        {
            headers.Capacity = RetainedCapacity;
        }

        positions.TrimExcess(RetainedCapacity);
    }
}
