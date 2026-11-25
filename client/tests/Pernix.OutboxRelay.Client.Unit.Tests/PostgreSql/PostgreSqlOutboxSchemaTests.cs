using System.Text.RegularExpressions;
using Pernix.OutboxRelay.Client.PostgreSql;
using Xunit;

namespace Pernix.OutboxRelay.Client.Unit.Tests.PostgreSql;

public sealed partial class PostgreSqlOutboxSchemaTests
{
    private const string ResourcePrefix = "Pernix.OutboxRelay.Client.PostgreSql.Sql.";

    [Fact]
    public void Version_is_the_highest_embedded_script_number_and_numbers_are_contiguous()
    {
        var numbers = EmbeddedScripts().Select(s => s.Number).Order().ToArray();

        Assert.Equal(Enumerable.Range(1, numbers.Length), numbers);
        Assert.Equal(PostgreSqlOutboxSchema.Version, numbers[^1]);
    }

    [Fact]
    public void Scripts_returns_v001_first_and_every_version_in_order()
    {
        var scripts = PostgreSqlOutboxSchema.Scripts(OutboxTable.Parse("outbox"));

        Assert.Equal(Enumerable.Range(1, PostgreSqlOutboxSchema.Version), scripts.Select(s => s.Version));
        Assert.Equal(1, scripts[0].Version);
        Assert.Equal("V001__create_outbox", scripts[0].Name);
    }

    [Fact]
    public void Scripts_returns_a_new_list_on_every_call()
    {
        var table = OutboxTable.Parse("outbox");

        Assert.NotSame(PostgreSqlOutboxSchema.Scripts(table), PostgreSqlOutboxSchema.Scripts(table));
    }

    [Fact]
    public void Rendered_scripts_carry_no_comments()
    {
        var sql = PostgreSqlOutboxSchema.Scripts(OutboxTable.Parse("outbox"))[0].Sql;

        Assert.DoesNotContain("--", sql, StringComparison.Ordinal);
        Assert.StartsWith("CREATE TABLE outbox (", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void No_embedded_script_has_a_comment_marker_inside_a_literal_or_a_dollar_quoted_body()
    {
        foreach (var (name, _, sql) in EmbeddedScripts())
        {
            Assert.Null(FindLexProblem(sql) is { } problem ? $"{name}: {problem}" : null);
        }
    }

    [Theory]
    [InlineData("SELECT 'a--b'", "'--' inside a literal")]
    [InlineData("SELECT \"a--b\"", "'--' inside a literal")]
    [InlineData("DO $$ BEGIN END $$", "dollar-quoted body")]
    [InlineData("DO $fn$ BEGIN END $fn$", "dollar-quoted body")]
    public void Lexer_flags_unsafe_scripts(string sql, string expected)
    {
        Assert.StartsWith(expected, FindLexProblem(sql));
    }

    [Theory]
    [InlineData("SELECT 'it''s' -- 'comment")]
    [InlineData("CHECK (NOT jsonb_path_exists(headers, '$.* ? (@.type() != \"string\")'))")]
    [InlineData("SELECT a$b FROM t")]
    public void Lexer_accepts_safe_scripts(string sql)
    {
        Assert.Null(FindLexProblem(sql));
    }

    [Fact]
    public void Render_with_schema_table_and_prefix_leaves_no_outbox_identifier()
    {
        foreach (var script in PostgreSqlOutboxSchema.Scripts(OutboxTable.Parse("s.t"), "p"))
        {
            Assert.Empty(UnprefixedOutboxRegex().Matches(script.Sql));
        }

        var v001 = PostgreSqlOutboxSchema.Scripts(OutboxTable.Parse("s.t"), "p")[0].Sql;
        Assert.Contains("CREATE TABLE s.t (", v001, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE s.t SET", v001, StringComparison.Ordinal);
        Assert.Equal(2, Count(v001, " ON s.t ("));
        Assert.Contains("CONSTRAINT p_header_values_are_strings ", v001, StringComparison.Ordinal);
    }

    [Fact]
    public void Prefix_defaults_to_the_table_name_without_schema()
    {
        var sql = PostgreSqlOutboxSchema.Scripts(OutboxTable.Parse("risk.orders"))[0].Sql;

        Assert.Contains("CONSTRAINT orders_headers_is_object ", sql, StringComparison.Ordinal);
        Assert.Contains("CREATE INDEX orders_tx_id_id_idx ON risk.orders (", sql, StringComparison.Ordinal);
    }

    // Sequential replacement would rescan "risk.outbox_x" and render "risk.outbox_x_x".
    [Fact]
    public void Table_name_that_contains_the_name_token_renders_intact()
    {
        var sql = PostgreSqlOutboxSchema.Scripts(OutboxTable.Parse("risk.outbox_x"))[0].Sql;

        Assert.Contains("CREATE TABLE risk.outbox_x (", sql, StringComparison.Ordinal);
        Assert.Contains("ALTER TABLE risk.outbox_x SET", sql, StringComparison.Ordinal);
        Assert.Equal(2, Count(sql, " ON risk.outbox_x ("));
        Assert.Contains("CONSTRAINT outbox_x_header_values_are_strings ", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("outbox_x_x", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("risk.risk", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Prefix_of_37_bytes_renders_names_of_at_most_63_bytes()
    {
        var prefix = new string('p', 37);

        var sql = PostgreSqlOutboxSchema.Scripts(OutboxTable.Parse("s.t"), prefix)[0].Sql;

        Assert.Contains($"CONSTRAINT {prefix}_header_values_are_strings ", sql, StringComparison.Ordinal);
        Assert.Equal(63, (prefix + "_header_values_are_strings").Length);
    }

    [Fact]
    public void Prefix_of_38_bytes_throws_naming_the_limit()
    {
        var ex = Assert.Throws<ArgumentException>("namePrefix", () => PostgreSqlOutboxSchema.Scripts(OutboxTable.Parse("s.t"), new string('p', 38)));

        Assert.Contains("37", ex.Message, StringComparison.Ordinal);
        Assert.Contains("pass a shorter namePrefix", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Default_prefix_from_a_long_table_name_throws()
    {
        var table = OutboxTable.Parse("s." + new string('t', 38));

        var ex = Assert.Throws<ArgumentException>("namePrefix", () => PostgreSqlOutboxSchema.Scripts(table));

        Assert.Contains("pass a shorter namePrefix", ex.Message, StringComparison.OrdinalIgnoreCase);
    }

    [Fact]
    public void Long_table_name_with_a_short_prefix_renders()
    {
        var name = new string('t', 63);

        var sql = PostgreSqlOutboxSchema.Scripts(OutboxTable.Parse("s." + name), "p")[0].Sql;

        Assert.Contains($"CREATE TABLE s.{name} (", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("")]
    [InlineData("1p")]
    [InlineData("p-q")]
    [InlineData("p.q")]
    [InlineData("p q")]
    [InlineData("p$")]
    [InlineData("ä")]
    public void Prefix_outside_the_grammar_throws(string prefix)
    {
        Assert.Throws<ArgumentException>("namePrefix", () => PostgreSqlOutboxSchema.Scripts(OutboxTable.Parse("s.t"), prefix));
    }

    [Fact]
    public void Default_table_throws()
    {
        Assert.Throws<ArgumentException>("table", () => PostgreSqlOutboxSchema.Scripts(default));
    }

    [Fact]
    public void Version_is_1_and_scripts_return_v001_only()
    {
        var scripts = PostgreSqlOutboxSchema.Scripts(OutboxTable.Parse("outbox"));

        Assert.Equal(1, PostgreSqlOutboxSchema.Version);
        Assert.Equal(["V001__create_outbox"], scripts.Select(s => s.Name));
    }

    [Fact]
    public void Rendered_v001_has_exactly_the_outbox_columns()
    {
        var sql = PostgreSqlOutboxSchema.Scripts(OutboxTable.Parse("outbox"))[0].Sql.ReplaceLineEndings("\n");
        var columns = sql.Split('\n')
            .Select(line => line.Trim())
            .Where(line => line.Length > 0 && char.IsAsciiLetterLower(line[0]))
            .Select(line => line[..line.IndexOf(' ')])
            .ToArray();

        Assert.Equal(OutboxColumns.All, columns);
    }

    [Fact]
    public void Rendered_v001_has_the_route_column_and_the_prefixed_route_format_check()
    {
        var sql = PostgreSqlOutboxSchema.Scripts(OutboxTable.Parse("s.t"), "p")[0].Sql;

        Assert.Contains("    route                 text,\n", sql.ReplaceLineEndings("\n"), StringComparison.Ordinal);
        Assert.Contains("CONSTRAINT p_route_format              CHECK (route ~ '^[a-z0-9][a-z0-9._-]{0,62}$')", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("route_idx", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Prefix_of_37_bytes_renders_the_route_format_name()
    {
        var prefix = new string('p', 37);

        var sql = PostgreSqlOutboxSchema.Scripts(OutboxTable.Parse("s.t"), prefix)[0].Sql;

        Assert.Contains($"CONSTRAINT {prefix}_route_format ", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void No_sql_file_exists_under_client()
    {
        var clientDir = Path.Combine(FindRepoRoot(), "client");

        Assert.Empty(Directory.GetFiles(clientDir, "*.sql", SearchOption.AllDirectories));
    }

    private static List<(string Name, int Number, string Sql)> EmbeddedScripts()
    {
        var assembly = typeof(PostgreSqlOutboxSchema).Assembly;
        var scripts = new List<(string, int, string)>();
        foreach (var resource in assembly.GetManifestResourceNames().Where(r => r.StartsWith(ResourcePrefix, StringComparison.Ordinal)))
        {
            var fileName = resource[ResourcePrefix.Length..];
            var match = ScriptFileRegex().Match(fileName);
            Assert.True(match.Success, $"Embedded script '{fileName}' does not match Vnnn__name.sql.");

            using var reader = new StreamReader(assembly.GetManifestResourceStream(resource)!);
            scripts.Add((fileName, int.Parse(match.Groups[1].Value), reader.ReadToEnd()));
        }

        Assert.NotEmpty(scripts);
        return scripts;
    }

    // Mini lexer: line comments, '...' literals ('' escape), "..." identifiers ("" escape), dollar quotes.
    private static string? FindLexProblem(string sql)
    {
        var i = 0;
        while (i < sql.Length)
        {
            var c = sql[i];
            if (c == '-' && i + 1 < sql.Length && sql[i + 1] == '-')
            {
                while (i < sql.Length && sql[i] != '\n')
                {
                    i++;
                }
            }
            else if (c is '\'' or '"')
            {
                var start = i++;
                while (true)
                {
                    if (i >= sql.Length)
                    {
                        return $"unterminated literal at {start}";
                    }

                    if (sql[i] == c)
                    {
                        if (i + 1 < sql.Length && sql[i + 1] == c)
                        {
                            i += 2;
                            continue;
                        }

                        i++;
                        break;
                    }

                    if (sql[i] == '-' && i + 1 < sql.Length && sql[i + 1] == '-')
                    {
                        return $"'--' inside a literal at {i}";
                    }

                    i++;
                }
            }
            else if (c == '$' && (i == 0 || !IsIdentifierChar(sql[i - 1])) && DollarTagRegex().IsMatch(sql.AsSpan(i)))
            {
                return $"dollar-quoted body at {i}";
            }
            else
            {
                i++;
            }
        }

        return null;
    }

    private static bool IsIdentifierChar(char c) => char.IsAsciiLetterOrDigit(c) || c == '_' || c == '$';

    private static int Count(string text, string value)
    {
        var count = 0;
        for (var i = text.IndexOf(value, StringComparison.Ordinal); i >= 0; i = text.IndexOf(value, i + value.Length, StringComparison.Ordinal))
        {
            count++;
        }

        return count;
    }

    private static string FindRepoRoot()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            if (File.Exists(Path.Combine(dir.FullName, "OutboxRelay.slnx")))
            {
                return dir.FullName;
            }
        }

        Assert.Fail("Repo root with OutboxRelay.slnx not found above " + AppContext.BaseDirectory);
        return null!;
    }

    [GeneratedRegex(@"(?<![A-Za-z0-9_$])outbox", RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)]
    private static partial Regex UnprefixedOutboxRegex();

    [GeneratedRegex(@"^V(\d{3})__(.+)\.sql$", RegexOptions.CultureInvariant)]
    private static partial Regex ScriptFileRegex();

    [GeneratedRegex(@"^\$[A-Za-z_]*\$", RegexOptions.CultureInvariant)]
    private static partial Regex DollarTagRegex();
}
