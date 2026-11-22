using System.Data;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Outbox.PostgreSql;
using Npgsql;
using Testcontainers.PostgreSql;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.PostgreSql;

public sealed class PostgreSqlPreparedSessionTests(PostgreSqlFixture postgres)
{
    private const string BusyLost = "Outboxes[Orders]: session lost (NpgsqlOperationInProgressException)";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Read_and_commit_after_a_caller_cancelled_read_are_lost_while_it_runs()
    {
        await using var container = new PostgreSqlBuilder("postgres:17.11").Build();
        var store = await ValidatedStoreAsync(container);
        var session = await store.OpenSessionAsync(Ct);
        await container.PauseAsync(Ct);
        try
        {
            using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
            var read = session.ReadAsync(new ReadRequest(10, null, []), cts.Token);
            await Task.Delay(500, Ct);
            await cts.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => read);
            Assert.False(session.Lost.IsCancellationRequested);

            await AssertBusyAsync(session);
        }
        finally
        {
            await session.DisposeAsync();
            await container.UnpauseAsync(Ct);
        }
    }

    [Fact]
    public async Task Read_and_commit_after_a_deadline_abandoned_read_are_lost_while_it_runs()
    {
        await using var container = new PostgreSqlBuilder("postgres:17.11").Build();
        var store = await ValidatedStoreAsync(container);
        var session = await store.OpenSessionAsync(Ct);
        await container.PauseAsync(Ct);
        try
        {
            // The 5 s session deadline races Npgsql's 5 s command timeout, so only the prefix is fixed.
            var abandoned = await Assert.ThrowsAsync<OutboxSessionLostException>(() => session.ReadAsync(new ReadRequest(10, null, []), Ct));
            Assert.StartsWith("Outboxes[Orders]: session lost (", abandoned.Message, StringComparison.Ordinal);
            Assert.True(session.Lost.IsCancellationRequested);

            await AssertBusyAsync(session);
        }
        finally
        {
            await session.DisposeAsync();
            await container.UnpauseAsync(Ct);
        }
    }

    [Fact]
    public async Task Dispose_after_busy_call_waits_for_the_abandoned_owner()
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema);
        var id = await postgres.ScalarAsync<long>(
            $"INSERT INTO {schema}.outbox (message_key, payload, headers) VALUES (NULL, '\\x70'::bytea, '{{}}'::jsonb) RETURNING id");
        await using var blocker = new NpgsqlConnection(postgres.ConnectionString);
        await blocker.OpenAsync(Ct);
        await using var transaction = await blocker.BeginTransactionAsync(Ct);
        await using (var lockRow = new NpgsqlCommand($"SELECT id FROM {schema}.outbox WHERE id = {id} FOR UPDATE", blocker, transaction))
        {
            await lockRow.ExecuteScalarAsync(Ct);
        }

        var session = (PostgreSqlOutboxSession)await store.OpenSessionAsync(Ct);
        var pid = session.Connection.ProcessID;
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var commit = session.CommitAsync([new Outcome.Remove(id)], cts.Token);
        var deadline = DateTime.UtcNow.AddSeconds(3);
        while (await postgres.ScalarAsync<long>($"SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE pid = {pid} AND wait_event_type = 'Lock'") == 0)
        {
            Assert.True(DateTime.UtcNow < deadline, "commit never waited on the row lock");
            await Task.Delay(20, Ct);
        }

        await cts.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => commit);
        var busy = await Assert.ThrowsAsync<OutboxSessionLostException>(() => session.CommitAsync([new Outcome.Remove(id)], Ct));
        Assert.Equal(BusyLost, busy.Message);

        await session.DisposeAsync();

        Assert.NotEqual(ConnectionState.Closed, session.Connection.State);
        Assert.True(await postgres.BackendExistsAsync(pid));
        Assert.True(session.ReleasePending);

        await transaction.RollbackAsync(Ct);

        deadline = DateTime.UtcNow.AddSeconds(5);
        while (await postgres.ScalarAsync<long>($"SELECT count(*) FROM {schema}.outbox") != 0 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(50, Ct);
        }

        Assert.Equal(0L, await postgres.ScalarAsync<long>($"SELECT count(*) FROM {schema}.outbox"));
        deadline = DateTime.UtcNow.AddSeconds(5);
        while (await postgres.BackendExistsAsync(pid) && DateTime.UtcNow < deadline)
        {
            await Task.Delay(50, Ct);
        }

        Assert.False(await postgres.BackendExistsAsync(pid));
    }

    [Fact]
    public async Task Read_after_a_read_column_type_change_is_lost_and_a_new_session_reads()
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema);
        var id = await postgres.ScalarAsync<long>(
            $"INSERT INTO {schema}.outbox (message_key, payload, next_attempt_at, last_error) VALUES (NULL, '\\x70'::bytea, now() - interval '1 second', 'e') RETURNING id");
        var session = await store.OpenSessionAsync(Ct);
        try
        {
            Assert.Equal([id], (await session.ReadAsync(new ReadRequest(10, null, []), Ct)).Select(m => m.Id));

            await postgres.ExecuteAsync($"ALTER TABLE {schema}.outbox ALTER COLUMN last_error TYPE varchar(1000)");

            var lost = await Assert.ThrowsAsync<OutboxSessionLostException>(() => session.ReadAsync(new ReadRequest(10, null, []), Ct));
            Assert.Equal("Outboxes[Orders]: session lost (PostgresException 0A000)", lost.Message);
            Assert.True(session.Lost.IsCancellationRequested);
        }
        finally
        {
            await session.DisposeAsync();
        }

        await using var next = await store.OpenSessionAsync(Ct);
        var rows = await next.ReadAsync(new ReadRequest(10, null, []), Ct);

        Assert.Equal([id], rows.Select(m => m.Id));
        Assert.Equal("e", rows[0].LastError);
        Assert.False(next.Lost.IsCancellationRequested);
    }

    [Fact]
    public async Task Read_result_is_unchanged_by_a_later_read_and_an_empty_read_is_the_shared_empty_array()
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema);
        var first = await postgres.ScalarAsync<long>(
            $"INSERT INTO {schema}.outbox (message_key, payload, next_attempt_at) VALUES (NULL, '\\x70'::bytea, now() - interval '1 second') RETURNING id");
        var second = await postgres.ScalarAsync<long>(
            $"INSERT INTO {schema}.outbox (message_key, payload, next_attempt_at) VALUES (NULL, '\\x70'::bytea, now() - interval '1 second') RETURNING id");
        await using var session = await store.OpenSessionAsync(Ct);

        var held = await session.ReadAsync(new ReadRequest(1, null, []), Ct);
        var later = await session.ReadAsync(new ReadRequest(10, null, [first]), Ct);

        var heldArray = Assert.IsType<OutboxMessage[]>(held);
        Assert.Equal(first, Assert.Single(heldArray).Id);
        Assert.Equal([second], later.Select(m => m.Id));

        var empty = await session.ReadAsync(new ReadRequest(10, null, [first, second]), Ct);
        var emptyAgain = await session.ReadAsync(new ReadRequest(10, null, [first, second]), Ct);

        Assert.Same(Array.Empty<OutboxMessage>(), empty);
        Assert.Same(empty, emptyAgain);
        Assert.Equal(first, Assert.Single(heldArray).Id);
        Assert.False(session.Lost.IsCancellationRequested);
    }

    [Fact]
    public async Task Command_on_a_connection_whose_backend_ended_throws_InvalidOperationException()
    {
        await using var connection = new NpgsqlConnection(postgres.ConnectionString);
        await connection.OpenAsync(Ct);
        await postgres.TerminateAsync(connection.ProcessID);

        await using (var first = new NpgsqlCommand("SELECT 1", connection))
        {
            await Assert.ThrowsAnyAsync<Exception>(() => first.ExecuteScalarAsync(Ct));
        }

        await using var second = new NpgsqlCommand("SELECT 1", connection);
        var ex = await Assert.ThrowsAsync<InvalidOperationException>(() => second.ExecuteScalarAsync(Ct));
        Assert.Equal("InvalidOperationException", PostgreSqlErrors.Describe(ex));
    }

    [Fact]
    public async Task Commit_cached_commands_run_in_the_session_transaction()
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema);
        await postgres.ExecuteAsync($"""
            CREATE TABLE {schema}.xact_log (op text, id bigint, xid xid8);
            CREATE FUNCTION {schema}.log_xact() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                INSERT INTO {schema}.xact_log VALUES (TG_OP, OLD.id, pg_catalog.pg_current_xact_id());
                RETURN NULL;
            END $$;
            CREATE TRIGGER log_xact AFTER DELETE OR UPDATE ON {schema}.outbox FOR EACH ROW EXECUTE FUNCTION {schema}.log_xact();
            """);
        var r1 = await InsertAsync(schema);
        var r2 = await InsertAsync(schema);
        var f = await InsertAsync(schema);
        var b = await InsertAsync(schema);
        await using var session = await store.OpenSessionAsync(Ct);

        await session.CommitAsync([new Outcome.Remove(r1)], Ct);
        await session.CommitAsync(
            [
                new Outcome.Remove(r2),
                new Outcome.Fail(f) { ReadRetryCount = 0, RetryCount = 1, Delay = TimeSpan.Zero, Error = "e" },
                new Outcome.Block(b) { ReadRetryCount = 0, RetryCount = 1, Error = "e" },
            ],
            Ct);

        var r1Xid = await XidAsync(schema, "DELETE", r1);
        var r2Xid = await XidAsync(schema, "DELETE", r2);
        Assert.Equal(r2Xid, await XidAsync(schema, "UPDATE", f));
        Assert.Equal(r2Xid, await XidAsync(schema, "UPDATE", b));
        Assert.NotEqual(r1Xid, r2Xid);
        Assert.Equal(4L, await postgres.ScalarAsync<long>($"SELECT count(*) FROM {schema}.xact_log"));
        Assert.False(session.Lost.IsCancellationRequested);
    }

    [Theory]
    [InlineData(OrderingMode.None)]
    [InlineData(OrderingMode.PerKey)]
    public async Task Table_name_with_at_sign_reads_and_commits(OrderingMode mode)
    {
        var schema = PostgreSqlFixture.NewSchema();
        await postgres.CreateOutboxSchemaAsync(schema);
        await postgres.ExecuteAsync($"ALTER TABLE {schema}.outbox RENAME TO \"orders@max\"");
        var table = $"{schema}.\"orders@max\"";
        var removed = await postgres.ScalarAsync<long>(
            $"INSERT INTO {table} (message_key, payload, next_attempt_at) VALUES ('k1', '\\x70'::bytea, now() - interval '1 second') RETURNING id");
        var failed = await postgres.ScalarAsync<long>(
            $"INSERT INTO {table} (message_key, payload, next_attempt_at) VALUES ('k2', '\\x70'::bytea, now() - interval '1 second') RETURNING id");
        var store = await PostgreSqlOutboxStoreTests.Provider(("Db", postgres.ConnectionString))
            .CreateStore(PostgreSqlOutboxStoreTests.Outbox("Orders", "Db", table, mode))
            .ValidateAsync(Ct);
        await using var session = await store.OpenSessionAsync(Ct);

        var rows = await PostgreSqlOutboxSessionTests.ReadUntilAsync(session, new ReadRequest(10, null, []), 2);
        Assert.Equal([removed, failed], rows.Select(m => m.Id).Order());

        await session.CommitAsync(
            [
                new Outcome.Remove(removed),
                new Outcome.Fail(failed) { ReadRetryCount = 0, RetryCount = 1, Delay = TimeSpan.FromHours(1), Error = "e" },
            ],
            Ct);

        Assert.Equal(0L, await postgres.ScalarAsync<long>($"SELECT count(*) FROM {table} WHERE id = {removed}"));
        Assert.Equal(1, await postgres.ScalarAsync<int>($"SELECT retry_count FROM {table} WHERE id = {failed}"));
        Assert.False(session.Lost.IsCancellationRequested);
    }

    private Task<long> InsertAsync(string schema) =>
        postgres.ScalarAsync<long>($"INSERT INTO {schema}.outbox (message_key, payload, headers) VALUES (NULL, '\\x70'::bytea, '{{}}'::jsonb) RETURNING id");

    private async Task<string> XidAsync(string schema, string op, long id) =>
        Assert.IsType<string>(await postgres.ScalarAsync<string>($"SELECT xid::text FROM {schema}.xact_log WHERE op = '{op}' AND id = {id}"));

    private static async Task AssertBusyAsync(IOutboxSession session)
    {
        var read = await Assert.ThrowsAsync<OutboxSessionLostException>(() => session.ReadAsync(new ReadRequest(10, null, []), Ct));
        Assert.Equal(BusyLost, read.Message);
        Assert.True(session.Lost.IsCancellationRequested);

        var commit = await Assert.ThrowsAsync<OutboxSessionLostException>(() => session.CommitAsync([new Outcome.Remove(1)], Ct));
        Assert.Equal(BusyLost, commit.Message);
    }

    private static async Task<IValidatedOutboxStore> ValidatedStoreAsync(PostgreSqlContainer container)
    {
        await container.StartAsync(Ct);
        var schema = PostgreSqlFixture.NewSchema();
        await PostgreSqlFixture.CreateOutboxSchemaAsync(container.GetConnectionString(), schema);
        var store = PostgreSqlOutboxStoreTests.Provider(("Db", container.GetConnectionString()))
            .CreateStore(PostgreSqlOutboxStoreTests.Outbox("Orders", "Db", $"{schema}.outbox"));
        return await store.ValidateAsync(Ct);
    }

    private async Task<IValidatedOutboxStore> ValidatedStoreAsync(string schema)
    {
        await postgres.CreateOutboxSchemaAsync(schema);
        var store = PostgreSqlOutboxStoreTests.Provider(("Db", postgres.ConnectionString))
            .CreateStore(PostgreSqlOutboxStoreTests.Outbox("Orders", "Db", $"{schema}.outbox"));
        return await store.ValidateAsync(Ct);
    }
}
