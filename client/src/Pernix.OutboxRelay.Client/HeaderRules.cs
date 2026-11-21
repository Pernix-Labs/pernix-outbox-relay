namespace Pernix.OutboxRelay.Client;

// Header rules checked by OutboxHeaders.Builder.Build. Messages name the rule and the zero-based position, never a name or value.
internal static class HeaderRules
{
    // Pairwise ordinal compare up to this count, a set above it.
    internal const int PairwiseDuplicateLimit = 8;

    public static void Validate(KeyValuePair<string, string>[] entries, int count)
    {
        HashSet<string>? seen = count > PairwiseDuplicateLimit ? new HashSet<string>(count, StringComparer.Ordinal) : null;

        for (var i = 0; i < count; i++)
        {
            var name = entries[i].Key;
            var value = entries[i].Value;

            if (string.IsNullOrEmpty(name))
            {
                throw Fail(i, "name is null or empty");
            }

            if (value is null)
            {
                throw Fail(i, "value is null");
            }

            if (name.Contains('\0'))
            {
                throw Fail(i, "name contains U+0000");
            }

            if (value.Contains('\0'))
            {
                throw Fail(i, "value contains U+0000");
            }

            if (StrictUtf8.HasLoneSurrogate(name))
            {
                throw Fail(i, "name contains a lone surrogate");
            }

            if (StrictUtf8.HasLoneSurrogate(value))
            {
                throw Fail(i, "value contains a lone surrogate");
            }

            if (seen is null)
            {
                for (var j = 0; j < i; j++)
                {
                    if (string.Equals(entries[j].Key, name, StringComparison.Ordinal))
                    {
                        throw Duplicate(i, j);
                    }
                }
            }
            else if (!seen.Add(name))
            {
                throw Duplicate(i, FirstIndexOf(entries, i, name));
            }
        }
    }

    private static int FirstIndexOf(KeyValuePair<string, string>[] entries, int end, string name)
    {
        for (var j = 0; j < end; j++)
        {
            if (string.Equals(entries[j].Key, name, StringComparison.Ordinal))
            {
                return j;
            }
        }

        return -1;
    }

    private static ArgumentException Duplicate(int index, int earlier) =>
        Fail(index, $"name duplicates header {earlier} (ordinal)");

    private static ArgumentException Fail(int index, string rule) => new($"Header {index}: {rule}.");
}
