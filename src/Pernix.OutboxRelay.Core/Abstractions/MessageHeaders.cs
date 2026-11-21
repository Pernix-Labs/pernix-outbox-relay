using System.Collections;

namespace Pernix.OutboxRelay.Core.Abstractions;

// A plain struct, not a record: equality on Value would compare array references, not bytes.
public readonly struct MessageHeader(string name, byte[] value)
{
    public string Name { get; } = name;

    public byte[] Value { get; } = value;
}

// Values are UTF-8 bytes, decoded once at read, so a transport sends them without re-encoding. Names are unique (ordinal).
// Shared between the read row and every send of it: neither the items nor the value bytes may change.
public sealed class MessageHeaders : IReadOnlyList<MessageHeader>
{
    private readonly MessageHeader[] items;

    public MessageHeaders(MessageHeader[] items)
    {
        ArgumentNullException.ThrowIfNull(items);
        this.items = items;
    }

    public static MessageHeaders Empty { get; } = new([]);

    public int Count => items.Length;

    public MessageHeader this[int index] => items[index];

    public IEnumerator<MessageHeader> GetEnumerator() => ((IEnumerable<MessageHeader>)items).GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
}
