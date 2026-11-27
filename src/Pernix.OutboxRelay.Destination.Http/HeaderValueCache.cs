using System.Text;

namespace Pernix.OutboxRelay.Destination.Http;

// One per send slot. Not thread-safe.
internal sealed class HeaderValueCache
{
    internal const int Capacity = 16;

    private readonly string[] names = new string[Capacity];
    private readonly byte[][] values = new byte[Capacity][];
    private readonly string[] strings = new string[Capacity];
    private int count;

    internal int Count => count;

    public string GetString(string name, byte[] value)
    {
        var index = IndexOf(name);
        if (index < 0)
        {
            var decoded = Encoding.UTF8.GetString(value);
            if (count < Capacity)
            {
                names[count] = name;
                values[count] = value;
                strings[count] = decoded;
                count++;
            }

            return decoded;
        }

        var cached = values[index];
        if (ReferenceEquals(cached, value)
            || (cached.Length == value.Length && cached.AsSpan().SequenceEqual(value)))
        {
            return strings[index];
        }

        var miss = Encoding.UTF8.GetString(value);
        values[index] = value;
        strings[index] = miss;
        return miss;
    }

    private int IndexOf(string name)
    {
        for (var i = 0; i < count; i++)
        {
            if (ReferenceEquals(names[i], name))
            {
                return i;
            }
        }

        for (var i = 0; i < count; i++)
        {
            if (string.Equals(names[i], name, StringComparison.Ordinal))
            {
                return i;
            }
        }

        return -1;
    }
}
