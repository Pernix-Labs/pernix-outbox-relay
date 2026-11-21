using System.Collections.Frozen;

namespace Pernix.OutboxRelay.Client;

/// <summary>
/// The outbox table name, as <c>name</c> or <c>schema.name</c>, with the same text semantics as the relay's
/// <c>Database:Table</c> setting. Create one with <see cref="Parse"/>. <c>default(OutboxTable)</c> is not a valid table.
/// </summary>
public readonly struct OutboxTable
{
    private const int MaxPartLength = 63;

    // PostgreSQL 17 keywords of category R ("reserved") and T ("reserved (can be function or type)"), from
    // pg_get_keywords(). The client writes the table text unquoted, so such a part is a syntax error in the INSERT and
    // the DDL, while the relay quotes it and resolves the table.
    private static readonly FrozenSet<string> ReservedKeywords = FrozenSet.ToFrozenSet(
        [
            // R
            "all", "analyse", "analyze", "and", "any", "array", "as", "asc", "asymmetric", "both", "case", "cast", "check",
            "collate", "column", "constraint", "create", "current_catalog", "current_date", "current_role", "current_time",
            "current_timestamp", "current_user", "default", "deferrable", "desc", "distinct", "do", "else", "end", "except",
            "false", "fetch", "for", "foreign", "from", "grant", "group", "having", "in", "initially", "intersect", "into",
            "lateral", "leading", "limit", "localtime", "localtimestamp", "not", "null", "offset", "on", "only", "or",
            "order", "placing", "primary", "references", "returning", "select", "session_user", "some", "symmetric",
            "system_user", "table", "then", "to", "trailing", "true", "union", "unique", "user", "using", "variadic",
            "when", "where", "window", "with",
            // T
            "authorization", "binary", "collation", "concurrently", "cross", "current_schema", "freeze", "full", "ilike",
            "inner", "is", "isnull", "join", "left", "like", "natural", "notnull", "outer", "overlaps", "right", "similar",
            "tablesample", "verbose",
        ],
        StringComparer.OrdinalIgnoreCase);

    private readonly string? text;
    private readonly string? name;

    private OutboxTable(string text, string? schema, string name)
    {
        this.text = text;
        Schema = schema;
        this.name = name;
    }

    /// <summary>Gets the schema part, or null when the text has no schema.</summary>
    public string? Schema { get; }

    /// <summary>Gets the table name part. Empty for <c>default(OutboxTable)</c>.</summary>
    public string Name => name ?? string.Empty;

    /// <summary>
    /// Parses <c>name</c> or <c>schema.name</c>. Each part matches <c>[A-Za-z_][A-Za-z0-9_]*</c>, has at most 63
    /// characters and is not a PostgreSQL reserved keyword (any case). The text is kept verbatim: no quoting, no case
    /// change.
    /// </summary>
    /// <param name="text">The table text.</param>
    /// <returns>The table.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="text"/> is null, empty, has more than two parts, an empty part, a part outside the grammar, or a
    /// part that is a PostgreSQL reserved keyword.
    /// </exception>
    public static OutboxTable Parse(string text)
    {
        if (string.IsNullOrEmpty(text))
        {
            throw new ArgumentException("Outbox table text is null or empty. Expected 'name' or 'schema.name'.", nameof(text));
        }

        var dot = text.IndexOf('.');
        if (dot < 0)
        {
            if (!IsValidPart(text))
            {
                throw Invalid(text);
            }

            ThrowIfReserved(text, text);
            return new OutboxTable(text, null, text);
        }

        var schema = text.AsSpan(0, dot);
        var name = text.AsSpan(dot + 1);
        if (!IsValidPart(schema) || !IsValidPart(name))
        {
            throw Invalid(text);
        }

        var schemaText = schema.ToString();
        var nameText = name.ToString();
        ThrowIfReserved(text, schemaText);
        ThrowIfReserved(text, nameText);
        return new OutboxTable(text, schemaText, nameText);
    }

    /// <summary>Returns the parsed text verbatim, or an empty string for <c>default(OutboxTable)</c>.</summary>
    /// <returns>The table text as given to <see cref="Parse"/>.</returns>
    public override string ToString() => text ?? string.Empty;

    // A second dot lands in the name part and fails the grammar there.
    private static bool IsValidPart(ReadOnlySpan<char> part)
    {
        if (part.Length is 0 or > MaxPartLength || !IsStart(part[0]))
        {
            return false;
        }

        for (var i = 1; i < part.Length; i++)
        {
            if (!IsStart(part[i]) && !char.IsAsciiDigit(part[i]))
            {
                return false;
            }
        }

        return true;
    }

    private static bool IsStart(char c) => char.IsAsciiLetter(c) || c == '_';

    private static void ThrowIfReserved(string text, string part)
    {
        if (ReservedKeywords.Contains(part))
        {
            throw new ArgumentException(
                $"Outbox table '{text}' is invalid. '{part}' is a PostgreSQL reserved keyword, and the client writes the table text unquoted. Use another name.",
                nameof(text));
        }
    }

    private static ArgumentException Invalid(string text) =>
        new($"Outbox table '{text}' is invalid. Expected 'name' or 'schema.name', each part [A-Za-z_][A-Za-z0-9_]* with at most {MaxPartLength} characters.", nameof(text));
}
