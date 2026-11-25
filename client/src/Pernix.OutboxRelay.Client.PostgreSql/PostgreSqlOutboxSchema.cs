using System.Text.RegularExpressions;

namespace Pernix.OutboxRelay.Client.PostgreSql;

/// <summary>
/// The relay's outbox DDL (<c>sql/postgresql/V*.sql</c>), embedded in this package and rendered for a table name.
/// The package returns text only and never executes SQL: the producer's migration tool applies the scripts.
/// </summary>
public static partial class PostgreSqlOutboxSchema
{
    /// <summary>
    /// Gets the highest script number this package ships. A property, not a constant, so the value is read from the
    /// package at run time and not compiled into the caller.
    /// </summary>
    public static int Version { get; } = 1;

    private const string ResourcePrefix = "Pernix.OutboxRelay.Client.PostgreSql.Sql.";
    private const int MaxIdentifierBytes = 63;
    private const int TokenLength = 6; // "outbox"

    private static readonly OutboxSchemaScript[] Templates = LoadTemplates();

    // Longest "_..." tail after a name token, for example "_header_values_are_strings" (26) in V001.
    private static readonly int LongestSuffix = ComputeLongestSuffix(Templates);

    private static readonly int MaxPrefixBytes = MaxIdentifierBytes - LongestSuffix;

    /// <summary>
    /// Returns the outbox scripts <c>V001</c> to <c>V</c><see cref="Version"/> in version order, rendered for
    /// <paramref name="table"/>. The table token is replaced with <c>table.ToString()</c> and every index and
    /// constraint name starts with <paramref name="namePrefix"/> instead of <c>outbox</c>. Returns a new list on every call.
    /// </summary>
    /// <param name="table">The outbox table, as given to the relay's <c>Database:Table</c> setting.</param>
    /// <param name="namePrefix">
    /// The index and constraint name prefix, <c>[A-Za-z_][A-Za-z0-9_]*</c>. Defaults to <c>table.Name</c>.
    /// Index and constraint names are unique per schema, so two outbox tables in one schema need two prefixes.
    /// </param>
    /// <returns>The rendered scripts in version order.</returns>
    /// <exception cref="ArgumentException">
    /// <paramref name="table"/> is <c>default(OutboxTable)</c>, the prefix is outside the grammar, or the prefix plus
    /// the longest index or constraint name suffix exceeds 63 bytes (PostgreSQL would truncate the name).
    /// </exception>
    public static IReadOnlyList<OutboxSchemaScript> Scripts(OutboxTable table, string? namePrefix = null)
    {
        var tableText = table.ToString();
        if (tableText.Length == 0)
        {
            throw new ArgumentException("Outbox table is default(OutboxTable). Create it with OutboxTable.Parse.", nameof(table));
        }

        var prefix = namePrefix ?? table.Name;
        var source = namePrefix is null ? " (defaulted from the table name)" : string.Empty;
        if (!IsIdentifier(prefix))
        {
            throw new ArgumentException(
                $"namePrefix '{prefix}'{source} is invalid. Expected [A-Za-z_][A-Za-z0-9_]*.",
                nameof(namePrefix));
        }

        if (prefix.Length > MaxPrefixBytes)
        {
            throw new ArgumentException(
                $"namePrefix '{prefix}'{source} is {prefix.Length} bytes. Index and constraint names allow at most {MaxPrefixBytes} bytes of prefix ({MaxIdentifierBytes} minus the longest suffix of {LongestSuffix}). Pass a shorter namePrefix.",
                nameof(namePrefix));
        }

        // One pass for both tokens: a second pass would rescan the inserted table text.
        var nameText = prefix + "_";
        var scripts = new OutboxSchemaScript[Templates.Length];
        for (var i = 0; i < Templates.Length; i++)
        {
            var template = Templates[i];
            var sql = TokenRegex().Replace(template.Sql, match => match.Groups["name"].Success ? nameText : tableText);
            scripts[i] = template with { Sql = sql };
        }

        return scripts;
    }

    private static OutboxSchemaScript[] LoadTemplates()
    {
        var assembly = typeof(PostgreSqlOutboxSchema).Assembly;
        var templates = new List<OutboxSchemaScript>();
        foreach (var resource in assembly.GetManifestResourceNames())
        {
            if (!resource.StartsWith(ResourcePrefix, StringComparison.Ordinal))
            {
                continue;
            }

            var fileName = resource.Substring(ResourcePrefix.Length);
            var match = ScriptFileRegex().Match(fileName);
            if (!match.Success)
            {
                throw new InvalidOperationException($"Embedded script '{fileName}' does not match Vnnn__name.sql.");
            }

            using var stream = assembly.GetManifestResourceStream(resource)!;
            using var reader = new StreamReader(stream);
            var sql = CommentRegex().Replace(reader.ReadToEnd(), string.Empty).Trim();
            templates.Add(new OutboxSchemaScript(int.Parse(match.Groups[1].ValueSpan), fileName[..^".sql".Length], sql));
        }

        if (templates.Count == 0)
        {
            throw new InvalidOperationException($"No embedded outbox scripts found under '{ResourcePrefix}'.");
        }

        templates.Sort(static (a, b) => a.Version.CompareTo(b.Version));
        return templates.ToArray();
    }

    private static int ComputeLongestSuffix(OutboxSchemaScript[] templates)
    {
        var longest = 0;
        foreach (var template in templates)
        {
            var sql = template.Sql;
            for (var match = TokenRegex().Match(sql); match.Success; match = match.NextMatch())
            {
                if (!match.Groups["name"].Success)
                {
                    continue;
                }

                var start = match.Index + TokenLength;
                var end = start;
                while (end < sql.Length && IsIdentifierChar(sql[end]))
                {
                    end++;
                }

                longest = Math.Max(longest, end - start);
            }
        }

        return longest;
    }

    private static bool IsIdentifier(string text)
    {
        if (text.Length == 0 || !(char.IsAsciiLetter(text[0]) || text[0] == '_'))
        {
            return false;
        }

        for (var i = 1; i < text.Length; i++)
        {
            if (!(char.IsAsciiLetterOrDigit(text[i]) || text[i] == '_'))
            {
                return false;
            }
        }

        return true;
    }

    private static bool IsIdentifierChar(char c) => char.IsAsciiLetterOrDigit(c) || c == '_' || c == '$';

    // Table token "outbox", or name token "outbox_" before a lowercase letter (group "name"). "x_outbox" never matches.
    [GeneratedRegex(@"(?<![A-Za-z0-9_$])outbox(?:(?<name>_)(?=[a-z])|(?![A-Za-z0-9_$]))", RegexOptions.CultureInvariant)]
    private static partial Regex TokenRegex();

    // "--" to end of line, with the whitespace before it. Scripts carry no "--" inside literals (unit-tested).
    [GeneratedRegex(@"[ \t]*--[^\r\n]*", RegexOptions.CultureInvariant)]
    private static partial Regex CommentRegex();

    [GeneratedRegex(@"^V(\d{3})__(.+)\.sql$", RegexOptions.CultureInvariant)]
    private static partial Regex ScriptFileRegex();
}
