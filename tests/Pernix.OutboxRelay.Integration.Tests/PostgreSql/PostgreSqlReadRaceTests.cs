using System.Data;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Outbox.PostgreSql;
using Dapper;
using Npgsql;
using Testcontainers.PostgreSql;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.PostgreSql;

// The read gate is cluster-wide, so any other backend holding an xid would hide the raced row. Own container, autovacuum off, nothing else connects.
public sealed class PostgreSqlReadRaceFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer container = TestContainer.PostgreSql("postgres:17.11")
        .WithCommand("-c", "autovacuum=off")
        .Build();

    public string ConnectionString => container.GetConnectionString();

    public async ValueTask InitializeAsync() => await container.StartBoundedAsync();

    public async ValueTask DisposeAsync() => await container.DisposeAsync();
}

// A held READ COMMITTED reader turns the read's now()-then-snapshot order into explicit steps.
public sealed class PostgreSqlReadRaceTests(PostgreSqlReadRaceFixture postgres) : IClassFixture<PostgreSqlReadRaceFixture>
{
    private const string Key = "'\\x6b'::bytea";
    private static readonly long[] NoExclude = [];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PerKey_row_committed_after_reader_now_reads_due(bool cursor)
    {
        var table = await TableAsync();

        var row = await RaceReadAsync(table, PostgreSqlOutboxSession.PerKeyReadSql(table, cursor), Parameters(cursor));

        Assert.True(row.IsDue);
        Assert.NotNull(row.Payload);
        Assert.Equal("payload"u8.ToArray(), row.Payload);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Strict_row_committed_after_reader_now_reads_due(bool cursor)
    {
        var table = await TableAsync();

        var row = await RaceReadAsync(table, PostgreSqlOutboxSession.StrictReadSql(table, cursor), Parameters(cursor));

        Assert.True(row.IsDue);
    }

    [Fact]
    public async Task PerKey_raced_row_behind_a_waiting_failed_row_of_its_key_stays_not_due()
    {
        var table = await TableAsync();
        var failed = await InsertAsync(table, retryCount: 1, nextAttemptAt: "now() + interval '1 hour'");

        var row = await RaceReadAsync(table, PostgreSqlOutboxSession.PerKeyReadSql(table, cursor: false), Parameters(cursor: false), failed);

        Assert.False(row.IsDue);
    }

    [Fact]
    public async Task None_raced_row_is_returned_by_the_next_read()
    {
        var table = await TableAsync();
        var sql = PostgreSqlOutboxSession.NoneReadSql(table);
        var parameters = new { max = 100, exclude = NoExclude };

        var (raced, inRace) = await RaceAsync(table, sql, parameters);
        await using var connection = await OpenAsync();
        var next = await QueryRowsAsync(connection, sql, parameters);

        Assert.DoesNotContain(inRace, m => m.Id == raced);
        Assert.Contains(next, m => m.Id == raced);
    }

    [Fact]
    public async Task Failed_row_due_rules_are_unchanged()
    {
        var table = await TableAsync();
        var waiting = await InsertAsync(table, "'\\x61'::bytea", 1, "now() + interval '1 hour'");
        var waitingNext = await InsertAsync(table, "'\\x61'::bytea");
        var blocked = await InsertAsync(table, "'\\x62'::bytea", 1, "'infinity'");
        var blockedNext = await InsertAsync(table, "'\\x62'::bytea");
        var deadLetterPending = await InsertAsync(table, "'\\x63'::bytea", 4, "now() + interval '1 hour'");
        var deadLetterNext = await InsertAsync(table, "'\\x63'::bytea");
        var duePast = await InsertAsync(table, "'\\x64'::bytea", 1, "now() - interval '1 second'");
        var dueNext = await InsertAsync(table, "'\\x64'::bytea");
        await using var connection = await OpenAsync();

        var perKey = await ReadDueAsync(connection, PostgreSqlOutboxSession.PerKeyReadSql(table, cursor: false));
        var strict = await ReadDueAsync(connection, PostgreSqlOutboxSession.StrictReadSql(table, cursor: false));

        Assert.Equal(
            [(waiting, false), (waitingNext, false), (blocked, false), (blockedNext, false),
             (deadLetterPending, false), (deadLetterNext, false), (duePast, true), (dueNext, true)],
            perKey);
        Assert.Equal(
            [(waiting, false), (waitingNext, true), (blocked, false), (blockedNext, true),
             (deadLetterPending, false), (deadLetterNext, true), (duePast, true), (dueNext, true)],
            strict);
    }

    private static object Parameters(bool cursor) =>
        cursor ? new { max = 100, after_tx = "0", after_id = 0L } : new { max = 100 };

    private async Task<OutboxMessage> RaceReadAsync(string table, string sql, object parameters, long? failedRow = null)
    {
        var (raced, rows) = await RaceAsync(table, sql, parameters);

        if (failedRow is { } failed)
        {
            Assert.Contains(rows, m => m.Id == failed);
        }

        return Assert.Single(rows, m => m.Id == raced);
    }

    private async Task<(long Raced, List<OutboxMessage> Rows)> RaceAsync(string table, string sql, object parameters)
    {
        await using var reader = await OpenAsync();
        await using var tx = await reader.BeginTransactionAsync(IsolationLevel.ReadCommitted, Ct);
        var readerNow = await reader.ExecuteScalarAsync<DateTime>(new CommandDefinition("SELECT pg_catalog.now()", transaction: tx, cancellationToken: Ct));

        await using var producer = await OpenAsync();
        var (raced, nextAttemptAt) = await producer.QuerySingleAsync<(long, DateTime)>(new CommandDefinition(
            $"INSERT INTO {table} (message_key, payload) VALUES ({Key}, '\\x7061796c6f6164'::bytea) RETURNING id, next_attempt_at",
            cancellationToken: Ct));

        Assert.True(nextAttemptAt > readerNow, $"precondition: next_attempt_at {nextAttemptAt:O} must follow reader now() {readerNow:O}");
        var visible = await reader.ExecuteScalarAsync<bool>(new CommandDefinition(
            $"SELECT tx_id < pg_catalog.pg_snapshot_xmin(pg_catalog.pg_current_snapshot()) FROM {table} WHERE id = @raced",
            new { raced }, tx, cancellationToken: Ct));
        Assert.True(visible, "precondition: the raced row must pass the read gate tx_id < pg_snapshot_xmin, else another backend holds an xid");

        var rows = await QueryRowsAsync(reader, sql, parameters, tx);
        await tx.RollbackAsync(Ct);
        return (raced, rows);
    }

    private static async Task<List<(long, bool)>> ReadDueAsync(NpgsqlConnection connection, string sql)
    {
        var rows = await QueryRowsAsync(connection, sql, new { max = 100 });
        return rows.Select(m => (m.Id, m.IsDue)).ToList();
    }

    private static async Task<List<OutboxMessage>> QueryRowsAsync(
        NpgsqlConnection connection, string sql, object parameters, NpgsqlTransaction? transaction = null)
    {
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        foreach (var property in parameters.GetType().GetProperties())
        {
            command.Parameters.AddWithValue(property.Name, property.GetValue(parameters)!);
        }

        await using var reader = await command.ExecuteReaderAsync(CommandBehavior.SequentialAccess, Ct);
        OutboxRowReader.Verify(reader);
        var rowReader = new OutboxRowReader();
        var rows = new List<OutboxMessage>();
        while (await reader.ReadAsync(Ct))
        {
            rows.Add(rowReader.Read(reader));
        }

        return rows;
    }

    private async Task<string> TableAsync()
    {
        var schema = PostgreSqlFixture.NewSchema();
        await PostgreSqlFixture.CreateOutboxSchemaAsync(postgres.ConnectionString, schema);
        return $"{schema}.outbox";
    }

    private async Task<long> InsertAsync(string table, string key = Key, int retryCount = 0, string nextAttemptAt = "now()")
    {
        await using var connection = await OpenAsync();
        return await connection.ExecuteScalarAsync<long>(new CommandDefinition($"""
            INSERT INTO {table} (message_key, payload, retry_count, next_attempt_at, last_error)
            VALUES ({key}, '\x7061796c6f6164'::bytea, {retryCount}, {nextAttemptAt}, {(retryCount > 0 ? "'failed'" : "NULL")})
            RETURNING id
            """, cancellationToken: Ct));
    }

    private async Task<NpgsqlConnection> OpenAsync()
    {
        var connection = new NpgsqlConnection(postgres.ConnectionString);
        await connection.OpenAsync(Ct);
        return connection;
    }
}
