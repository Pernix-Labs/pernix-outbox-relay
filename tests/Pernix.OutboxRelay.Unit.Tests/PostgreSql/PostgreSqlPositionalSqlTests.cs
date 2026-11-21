using Pernix.OutboxRelay.PostgreSql;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.PostgreSql;

public sealed class PostgreSqlPositionalSqlTests
{
    private const string Table = "\"public\".\"outbox\"";

    [Fact]
    public void None_read_binds_max_then_exclude()
    {
        var sql = PostgreSqlOutboxSession.Positional(PostgreSqlOutboxSession.NoneReadSql(Table), PostgreSqlOutboxSession.NoneNames);

        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $1", sql, StringComparison.Ordinal);
        Assert.Contains("id <> ALL($2)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("$3", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void PerKey_read_binds_documented_order(bool cursor)
    {
        AssertOrdered(PostgreSqlOutboxSession.PerKeyReadSql(Table, cursor), cursor);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Strict_read_binds_documented_order(bool cursor)
    {
        AssertOrdered(PostgreSqlOutboxSession.StrictReadSql(Table, cursor), cursor);
    }

    [Fact]
    public void Summary_has_no_parameter()
    {
        var sql = PostgreSqlOutboxSession.SummarySql(Table);

        Assert.Equal(sql, PostgreSqlOutboxSession.Positional(sql, []));
        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
    }

    [Fact]
    public void Leftover_named_parameter_throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(
            () => PostgreSqlOutboxSession.Positional(PostgreSqlOutboxSession.NoneReadSql(Table), PostgreSqlOutboxSession.HeadNames));

        Assert.Equal("SQL parameter '@exclude' has no position.", ex.Message);
    }

    [Theory]
    [InlineData("\"public\".\"t@max\"")]
    [InlineData("\"public\".\"orders@2024\"")]
    [InlineData("\"public\".\"a\"\"@exclude\"\"b\"")]
    public void None_read_keeps_quoted_table_with_at_sign(string table)
    {
        var sql = PostgreSqlOutboxSession.Positional(PostgreSqlOutboxSession.NoneReadSql(table), PostgreSqlOutboxSession.NoneNames);

        Assert.Contains($"FROM {table}", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("@", sql.Replace(table, "", StringComparison.Ordinal), StringComparison.Ordinal);
        Assert.Contains("LIMIT $1", sql, StringComparison.Ordinal);
        Assert.Contains("id <> ALL($2)", sql, StringComparison.Ordinal);
        Assert.DoesNotContain("$3", sql, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("\"public\".\"t@max\"", false)]
    [InlineData("\"public\".\"t@max\"", true)]
    [InlineData("\"public\".\"orders@2024\"", false)]
    [InlineData("\"public\".\"orders@2024\"", true)]
    [InlineData("\"public\".\"a\"\"@after_tx\"\"b\"", true)]
    public void PerKey_and_Strict_reads_keep_quoted_table_with_at_sign(string table, bool cursor)
    {
        var names = cursor ? PostgreSqlOutboxSession.CursorNames : PostgreSqlOutboxSession.HeadNames;

        foreach (var named in new[] { PostgreSqlOutboxSession.PerKeyReadSql(table, cursor), PostgreSqlOutboxSession.StrictReadSql(table, cursor) })
        {
            var sql = PostgreSqlOutboxSession.Positional(named, names);

            Assert.Contains(table, sql, StringComparison.Ordinal);
            Assert.DoesNotContain("@", sql.Replace(table, "", StringComparison.Ordinal), StringComparison.Ordinal);
            Assert.Contains("LIMIT $1", sql, StringComparison.Ordinal);
            if (cursor)
            {
                Assert.Contains("(tx_id, id) > (CAST($2 AS text)::xid8, $3)", sql, StringComparison.Ordinal);
            }
            else
            {
                Assert.DoesNotContain("$2", sql, StringComparison.Ordinal);
            }
        }
    }

    private static void AssertOrdered(string named, bool cursor)
    {
        var sql = PostgreSqlOutboxSession.Positional(named, cursor ? PostgreSqlOutboxSession.CursorNames : PostgreSqlOutboxSession.HeadNames);

        Assert.DoesNotContain("@", sql, StringComparison.Ordinal);
        Assert.Contains("LIMIT $1", sql, StringComparison.Ordinal);
        if (cursor)
        {
            Assert.Contains("(tx_id, id) > (CAST($2 AS text)::xid8, $3)", sql, StringComparison.Ordinal);
        }
        else
        {
            Assert.DoesNotContain("$2", sql, StringComparison.Ordinal);
        }
    }
}
