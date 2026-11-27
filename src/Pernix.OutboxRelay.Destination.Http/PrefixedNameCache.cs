namespace Pernix.OutboxRelay.Destination.Http;

// One per send slot. Not thread-safe. Capacity matches HeadersParser.NameCacheSize.
internal sealed class PrefixedNameCache(string prefix)
{
    internal const int Capacity = 32;

    private readonly string[] names = new string[Capacity];
    private readonly string[] prefixed = new string[Capacity];
    private int count;

    public string Prefix { get; } = prefix;

    internal int Count => count;

    public string Get(string name)
    {
        for (var i = 0; i < count; i++)
        {
            if (ReferenceEquals(names[i], name))
            {
                return prefixed[i];
            }
        }

        for (var i = 0; i < count; i++)
        {
            if (string.Equals(names[i], name, StringComparison.Ordinal))
            {
                return prefixed[i];
            }
        }

        var value = string.Concat(Prefix, name);
        if (count < Capacity)
        {
            names[count] = name;
            prefixed[count] = value;
            count++;
        }

        return value;
    }
}
