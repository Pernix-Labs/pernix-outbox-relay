using System.Collections;

namespace Pernix.OutboxRelay.Client;

/// <summary>
/// Immutable, validated message headers in <see cref="Builder.Add"/> order. Every instance satisfies the header rules,
/// so publish never re-checks it. Create one with <see cref="CreateBuilder"/>.
/// </summary>
public sealed class OutboxHeaders : IReadOnlyList<KeyValuePair<string, string>>
{
    private readonly KeyValuePair<string, string>[] entries;

    private OutboxHeaders(KeyValuePair<string, string>[] entries, string json)
    {
        this.entries = entries;
        Json = json;
    }

    /// <summary>Gets the shared instance with no headers. Its <see cref="Json"/> is <c>{}</c>.</summary>
    public static OutboxHeaders Empty { get; } = new([], HeadersJsonWriter.EmptyObject);

    /// <summary>Gets the number of headers.</summary>
    public int Count => entries.Length;

    /// <summary>Gets the header at <paramref name="index"/>.</summary>
    /// <param name="index">The zero-based position, in <see cref="Builder.Add"/> order.</param>
    /// <returns>The header name and value.</returns>
    /// <exception cref="IndexOutOfRangeException"><paramref name="index"/> is outside <c>[0, Count)</c>.</exception>
    public KeyValuePair<string, string> this[int index] => entries[index];

    /// <summary>
    /// Gets the headers as one JSON object text (name to string value), written once when the instance was built and
    /// reused for every publish. <c>{}</c> for <see cref="Empty"/>.
    /// </summary>
    public string Json { get; }

    /// <summary>Creates a builder for a new <see cref="OutboxHeaders"/> instance.</summary>
    /// <param name="capacity">The expected number of headers.</param>
    /// <returns>A new, empty builder.</returns>
    /// <exception cref="ArgumentOutOfRangeException"><paramref name="capacity"/> is negative.</exception>
    public static Builder CreateBuilder(int capacity = 4)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(capacity);
        return new Builder(capacity);
    }

    /// <summary>Returns an enumerator over the headers in <see cref="Builder.Add"/> order.</summary>
    /// <returns>The enumerator.</returns>
    public IEnumerator<KeyValuePair<string, string>> GetEnumerator() => ((IEnumerable<KeyValuePair<string, string>>)entries).GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    /// <summary>
    /// Collects headers for <see cref="Build"/>. <see cref="Build"/> leaves the builder unchanged, so it can add more
    /// headers and build again.
    /// </summary>
    public sealed class Builder
    {
        private KeyValuePair<string, string>[] items;
        private int count;

        internal Builder(int capacity)
        {
            items = capacity == 0 ? [] : new KeyValuePair<string, string>[capacity];
        }

        /// <summary>Appends a header. Checks run in <see cref="Build"/>, which reports the header's position.</summary>
        /// <param name="name">The header name.</param>
        /// <param name="value">The header value. Empty is allowed.</param>
        /// <returns>This builder.</returns>
        public Builder Add(string name, string value)
        {
            if (count == items.Length)
            {
                Array.Resize(ref items, items.Length == 0 ? 4 : items.Length * 2);
            }

            items[count++] = new KeyValuePair<string, string>(name, value);
            return this;
        }

        /// <summary>
        /// Validates the added headers and returns an immutable instance. Rules per header, in order: name not null or
        /// empty, value not null, no U+0000 in name or value, no lone surrogate in name or value, name not
        /// ordinal-equal to an earlier name. Names that differ only by case are distinct.
        /// </summary>
        /// <returns>The headers, or <see cref="Empty"/> when none were added.</returns>
        /// <exception cref="ArgumentException">
        /// A header breaks a rule. The message names the rule and the zero-based position, never a name or value.
        /// </exception>
        public OutboxHeaders Build()
        {
            if (count == 0)
            {
                return Empty;
            }

            HeaderRules.Validate(items, count);

            var entries = new KeyValuePair<string, string>[count];
            Array.Copy(items, entries, count);
            return new OutboxHeaders(entries, HeadersJsonWriter.Write(entries));
        }
    }
}
