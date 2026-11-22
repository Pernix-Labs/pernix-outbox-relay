using System.Reflection;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Outbox.PostgreSql;
using Npgsql;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.PostgreSql;

// The measurement harness runs the builders, so each builder must produce exactly the SQL the session and store execute.
public sealed class PostgreSqlSqlBuildersTests
{
    private const string Table = "\"public\".\"outbox\"";

    [Fact]
    public void None_read_sql_is_what_the_session_reads()
    {
        using var connection = new NpgsqlConnection();
        var session = new PostgreSqlOutboxSession("Orders", Table, OrderingMode.None, connection);

        Assert.Equal(PostgreSqlOutboxSession.NoneReadSql(Table), Field(session, "readSql"));
        Assert.Null(Field(session, "cursorReadSql"));
    }

    [Fact]
    public void PerKey_read_sql_is_what_the_session_reads()
    {
        using var connection = new NpgsqlConnection();
        var session = new PostgreSqlOutboxSession("Orders", Table, OrderingMode.PerKey, connection);

        Assert.Equal(PostgreSqlOutboxSession.PerKeyReadSql(Table, cursor: false), Field(session, "readSql"));
        Assert.Equal(PostgreSqlOutboxSession.PerKeyReadSql(Table, cursor: true), Field(session, "cursorReadSql"));
    }

    [Fact]
    public void Strict_read_sql_is_what_the_session_reads()
    {
        using var connection = new NpgsqlConnection();
        var session = new PostgreSqlOutboxSession("Orders", Table, OrderingMode.Strict, connection);

        Assert.Equal(PostgreSqlOutboxSession.StrictReadSql(Table, cursor: false), Field(session, "readSql"));
        Assert.Equal(PostgreSqlOutboxSession.StrictReadSql(Table, cursor: true), Field(session, "cursorReadSql"));
    }

    [Theory]
    [InlineData(OrderingMode.None)]
    [InlineData(OrderingMode.PerKey)]
    [InlineData(OrderingMode.Strict)]
    public void Summary_sql_is_what_the_session_reads(OrderingMode mode)
    {
        using var connection = new NpgsqlConnection();
        var session = new PostgreSqlOutboxSession("Orders", Table, mode, connection);

        Assert.Equal(PostgreSqlOutboxSession.SummarySql(Table), Field(session, "summarySql"));
    }

    [Theory]
    [InlineData(OrderingMode.None, false)]
    [InlineData(OrderingMode.None, true)]
    [InlineData(OrderingMode.PerKey, false)]
    [InlineData(OrderingMode.PerKey, true)]
    [InlineData(OrderingMode.Strict, false)]
    [InlineData(OrderingMode.Strict, true)]
    public void Stats_sql_is_what_the_store_runs(OrderingMode mode, bool partitioned)
    {
        var store = new PostgreSqlValidatedOutboxStore(
            "Orders", Table, mode, "Host=db", "Host=db", partitioned, inRecovery: recovering => recovering);

        Assert.Equal(PostgreSqlValidatedOutboxStore.BuildStatsSql(Table, mode, partitioned), store.StatsSql);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void PerKey_and_Strict_reads_treat_a_fresh_row_as_due(bool cursor)
    {
        Assert.Contains("(o.retry_count = 0 OR o.next_attempt_at <= pg_catalog.now())", PostgreSqlOutboxSession.PerKeyReadSql(Table, cursor), StringComparison.Ordinal);
        Assert.Contains("(retry_count = 0 OR next_attempt_at <= pg_catalog.now()) AS \"IsDue\"", PostgreSqlOutboxSession.StrictReadSql(Table, cursor), StringComparison.Ordinal);
    }

    [Fact]
    public void None_read_and_summary_keep_the_clock_rule()
    {
        var none = PostgreSqlOutboxSession.NoneReadSql(Table);
        var summary = PostgreSqlOutboxSession.SummarySql(Table);

        Assert.Contains("WHERE next_attempt_at <= pg_catalog.now()", none, StringComparison.Ordinal);
        Assert.DoesNotContain("retry_count = 0", none, StringComparison.Ordinal);
        Assert.DoesNotContain("retry_count = 0", summary, StringComparison.Ordinal);
        Assert.Contains("(next_attempt_at <= pg_catalog.now())::int", summary, StringComparison.Ordinal);
    }

    [Fact]
    public void No_builder_uses_clock_timestamp()
    {
        string[] sql =
        [
            PostgreSqlOutboxSession.NoneReadSql(Table),
            PostgreSqlOutboxSession.PerKeyReadSql(Table, cursor: false),
            PostgreSqlOutboxSession.PerKeyReadSql(Table, cursor: true),
            PostgreSqlOutboxSession.StrictReadSql(Table, cursor: false),
            PostgreSqlOutboxSession.StrictReadSql(Table, cursor: true),
            PostgreSqlOutboxSession.SummarySql(Table),
            .. from mode in Enum.GetValues<OrderingMode>()
               from partitioned in new[] { false, true }
               select PostgreSqlValidatedOutboxStore.BuildStatsSql(Table, mode, partitioned),
        ];

        Assert.All(sql, text => Assert.DoesNotContain("clock_timestamp", text, StringComparison.Ordinal));
    }

    private static string? Field(PostgreSqlOutboxSession session, string name) =>
        (string?)typeof(PostgreSqlOutboxSession).GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(session);
}
