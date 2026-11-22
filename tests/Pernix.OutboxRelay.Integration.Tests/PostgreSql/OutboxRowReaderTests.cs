using System.Data;
using Pernix.OutboxRelay.Outbox.PostgreSql;
using Npgsql;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.PostgreSql;

public sealed class OutboxRowReaderTests(PostgreSqlFixture postgres)
{
    private const string TenColumns = """
        SELECT 1::bigint AS "Id", '1'::xid8 AS "Order", NULL::bytea AS "Key", NULL::bytea AS "Payload", NULL::bytea AS "Headers",
               now() AS "CreatedAt", 0 AS "RetryCount", now() AS "NextAttemptAt", true AS "IsDue", NULL::text AS "LastError"
        """;

    private const string RenamedColumn = """
        SELECT 1::bigint AS "Id", '1'::xid8 AS "Order", NULL::bytea AS "Key", NULL::bytea AS "Payload", NULL::bytea AS "Headers",
               now() AS "CreatedAt", 0 AS "RetryCount", now() AS "NextAttemptAt", true AS "Due", NULL::text AS "LastError", NULL::text AS "Route"
        """;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Verify_accepts_the_session_read_columns()
    {
        var schema = await postgres.CreateOutboxAsync();

        await VerifyAsync(PostgreSqlOutboxSession.Positional(
            PostgreSqlOutboxSession.NoneReadSql($"{schema}.outbox"), PostgreSqlOutboxSession.NoneNames), new List<long>());
    }

    [Fact]
    public async Task Verify_rejects_a_wrong_field_count()
    {
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => VerifyAsync(TenColumns));

        Assert.Equal("Read returned 10 columns, expected 11.", ex.Message);
    }

    [Fact]
    public async Task Verify_rejects_a_renamed_column()
    {
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => VerifyAsync(RenamedColumn));

        Assert.Equal("Read column 8 is 'Due', expected 'IsDue'.", ex.Message);
    }

    private async Task VerifyAsync(string sql, List<long>? exclude = null)
    {
        await using var connection = new NpgsqlConnection(postgres.ConnectionString);
        await connection.OpenAsync(Ct);
        await using var command = new NpgsqlCommand(sql, connection);
        if (exclude is not null)
        {
            command.Parameters.Add(new NpgsqlParameter<int> { TypedValue = 1 });
            command.Parameters.Add(new NpgsqlParameter<List<long>> { TypedValue = exclude });
        }

        await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, Ct);
        OutboxRowReader.Verify(reader);
    }
}
