using System.Diagnostics;
using System.Globalization;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Pernix.OutboxRelay.Outbox.PostgreSql;
using Npgsql;
using Testcontainers.PostgreSql;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.PostgreSql;

public sealed class PostgreSqlOutboxSessionTests(PostgreSqlFixture postgres)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Session_sets_server_side_keepalives()
    {
        var store = await ValidatedStoreAsync();
        await using var session = (PostgreSqlOutboxSession)await store.OpenSessionAsync(Ct);

        Assert.Equal("5", await ShowAsync(session.Connection, "tcp_keepalives_idle"));
        Assert.Equal("2", await ShowAsync(session.Connection, "tcp_keepalives_interval"));
        Assert.Equal("3", await ShowAsync(session.Connection, "tcp_keepalives_count"));
        // show_tcp_user_timeout prints raw milliseconds, not "11s".
        Assert.Equal("11000", await ShowAsync(session.Connection, "tcp_user_timeout"));
    }

    [Fact]
    public async Task Lock_is_exclusive_keyed_by_qualified_table_and_lost_on_terminate()
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema);
        await using var first = (PostgreSqlOutboxSession)await store.OpenSessionAsync(Ct);
        await using var second = await store.OpenSessionAsync(Ct);

        Assert.True(await first.TryLockAsync(Ct));
        Assert.False(await second.TryLockAsync(Ct));
        Assert.Equal(first.Connection.ProcessID, await postgres.FindLockHolderPidAsync($"{schema}.outbox"));

        await postgres.TerminateAsync(first.Connection.ProcessID);

        var ex = await Assert.ThrowsAsync<OutboxSessionLostException>(() => first.CheckAsync(Ct));
        Assert.StartsWith("Outboxes[Orders]: session lost (", ex.Message, StringComparison.Ordinal);
        Assert.True(first.Lost.IsCancellationRequested);
        Assert.False(second.Lost.IsCancellationRequested);
    }

    [Fact]
    public async Task Dispose_closes_backend_and_releases_lock()
    {
        var store = await ValidatedStoreAsync();
        var first = (PostgreSqlOutboxSession)await store.OpenSessionAsync(Ct);
        await using var second = await store.OpenSessionAsync(Ct);
        var pid = first.Connection.ProcessID;
        Assert.True(await first.TryLockAsync(Ct));

        await first.DisposeAsync();
        await first.DisposeAsync();

        // Backend exit after socket close is asynchronous on the server.
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (await postgres.BackendExistsAsync(pid) && DateTime.UtcNow < deadline)
        {
            await Task.Delay(50, Ct);
        }

        Assert.False(await postgres.BackendExistsAsync(pid));
        Assert.True(await second.TryLockAsync(Ct));
    }

    [Fact]
    public async Task Dispose_leaves_no_lock_row_when_it_reports_released()
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema);
        await using var observer = new NpgsqlConnection(postgres.ConnectionString);
        await observer.OpenAsync(Ct);
        await using var lockRows = new NpgsqlCommand(
            """
            SELECT count(*) FROM pg_catalog.pg_locks
            WHERE locktype = 'advisory' AND classid = hashtext('outbox-relay')::oid AND objid = hashtext(@table)::oid AND objsubid = 2
            """,
            observer);
        lockRows.Parameters.AddWithValue("table", $"{schema}.outbox");

        for (var i = 0; i < 50; i++)
        {
            var session = (PostgreSqlOutboxSession)await store.OpenSessionAsync(Ct);
            Assert.True(await session.TryLockAsync(Ct));

            // Exit drops temp tables before session locks, so a close alone leaves the lock row visible for a while.
            await using (var slowExit = new NpgsqlCommand(
                "DO 'BEGIN FOR i IN 1..200 LOOP EXECUTE format(''CREATE TEMP TABLE t%s (i int)'', i); END LOOP; END'", session.Connection))
            {
                await slowExit.ExecuteNonQueryAsync(Ct);
            }

            await session.DisposeAsync();

            // No polling: released must already be true on the server when dispose returns.
            Assert.False(session.ReleasePending, $"iteration {i}");
            Assert.Equal(0L, (long)(await lockRows.ExecuteScalarAsync(Ct))!);
        }
    }

    [Fact]
    public async Task Dispose_while_a_command_sleeps_reports_release_pending_until_the_sleep_ends()
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema);
        await postgres.ExecuteAsync(
            $"""
            CREATE FUNCTION {schema}.sleep() RETURNS trigger LANGUAGE plpgsql AS 'BEGIN PERFORM pg_catalog.pg_sleep(3); RETURN OLD; END';
            CREATE TRIGGER sleep BEFORE DELETE ON {schema}.outbox FOR EACH ROW EXECUTE FUNCTION {schema}.sleep();
            """);
        var id = await InsertAsync(schema, "NULL", "'{}'::jsonb");
        var session = (PostgreSqlOutboxSession)await store.OpenSessionAsync(Ct);
        var pid = session.Connection.ProcessID;
        Assert.True(await session.TryLockAsync(Ct));
        var commit = session.CommitAsync([new Outcome.Remove(id)], Ct);
        var deadline = DateTime.UtcNow.AddSeconds(3);
        while (await postgres.ScalarAsync<long>($"SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE pid = {pid} AND wait_event = 'PgSleep'") == 0)
        {
            Assert.True(DateTime.UtcNow < deadline, "commit never reached pg_sleep");
            await Task.Delay(20, Ct);
        }

        await session.DisposeAsync();

        Assert.True(session.ReleasePending);
        Assert.Equal(1L, await postgres.CountLockHoldersAsync($"{schema}.outbox"));
        await commit.WaitAsync(TimeSpan.FromSeconds(8), Ct);
        deadline = DateTime.UtcNow.AddSeconds(5);
        while (await postgres.CountLockHoldersAsync($"{schema}.outbox") != 0)
        {
            Assert.True(DateTime.UtcNow < deadline, "lock still held after the sleep ended");
            await Task.Delay(50, Ct);
        }
    }

    [Fact]
    public async Task Dispose_after_backend_terminated_does_not_throw_and_reports_release_pending()
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema);
        var session = (PostgreSqlOutboxSession)await store.OpenSessionAsync(Ct);
        Assert.True(await session.TryLockAsync(Ct));
        await postgres.TerminateAsync(session.Connection.ProcessID);

        await session.DisposeAsync();

        // The unlock met a broken connection, not a live backend, so release is not proven.
        Assert.True(session.ReleasePending);
        Assert.Equal(0L, await postgres.CountLockHoldersAsync($"{schema}.outbox"));
    }

    [Fact]
    public async Task Lost_callbacks_do_not_run_inline_on_the_state_change_thread()
    {
        var store = await ValidatedStoreAsync();
        await using var session = (PostgreSqlOutboxSession)await store.OpenSessionAsync(Ct);
        var closingThread = -1;
        var ranInline = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var registration = session.Lost.Register(
            () => ranInline.TrySetResult(Volatile.Read(ref closingThread) == Environment.CurrentManagedThreadId));

        await Task.Run(
            () =>
            {
                Volatile.Write(ref closingThread, Environment.CurrentManagedThreadId);
                session.Connection.Close();
                Volatile.Write(ref closingThread, -1);
            },
            Ct);

        Assert.True(session.Lost.IsCancellationRequested);
        Assert.False(await ranInline.Task.WaitAsync(TimeSpan.FromSeconds(5), Ct));
    }

    [Fact]
    public async Task Closed_session_connection_is_never_reopened_by_try_lock()
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema);
        await using var session = (PostgreSqlOutboxSession)await store.OpenSessionAsync(Ct);

        await session.Connection.CloseAsync();

        var ex = await Assert.ThrowsAsync<OutboxSessionLostException>(() => session.TryLockAsync(Ct));
        Assert.Equal("Outboxes[Orders]: session lost (connection not open)", ex.Message);
        Assert.True(session.Lost.IsCancellationRequested);
        Assert.Equal(System.Data.ConnectionState.Closed, session.Connection.State);
        Assert.Equal(0, await postgres.CountLockHoldersAsync($"{schema}.outbox"));
    }

    [Fact]
    public async Task Lock_and_check_agree_for_table_with_negative_hash()
    {
        var schema = PostgreSqlFixture.NewSchema();
        await postgres.CreateOutboxSchemaAsync(schema);
        var table = await postgres.ScalarAsync<string>(
            $"""
            SELECT name FROM (SELECT 't' || i AS name FROM generate_series(1, 1000) AS i) AS candidates
            WHERE hashtext(format('%I.%I', '{schema}', name)) < 0
            ORDER BY name LIMIT 1
            """);
        Assert.NotNull(table);
        await postgres.ExecuteAsync($"CREATE TABLE {schema}.{table} (LIKE {schema}.outbox)");
        var store = PostgreSqlOutboxStoreTests.Provider(("Db", postgres.ConnectionString))
            .CreateStore(PostgreSqlOutboxStoreTests.Outbox("Orders", "Db", $"{schema}.{table}"));
        var validated = await store.ValidateAsync(Ct);
        await using var session = await validated.OpenSessionAsync(Ct);

        Assert.True(await session.TryLockAsync(Ct));
        await session.CheckAsync(Ct);

        Assert.False(session.Lost.IsCancellationRequested);
    }

    [Fact]
    public async Task Check_succeeds_while_session_holds_lock()
    {
        var store = await ValidatedStoreAsync();
        await using var session = await store.OpenSessionAsync(Ct);
        Assert.True(await session.TryLockAsync(Ct));

        await session.CheckAsync(Ct);

        Assert.False(session.Lost.IsCancellationRequested);
    }

    [Fact]
    public async Task Check_reports_loss_once_session_no_longer_holds_lock()
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema);
        await using var session = (PostgreSqlOutboxSession)await store.OpenSessionAsync(Ct);
        Assert.True(await session.TryLockAsync(Ct));

        await using (var unlock = new NpgsqlCommand("SELECT pg_advisory_unlock(hashtext('outbox-relay'), hashtext(@table))", session.Connection))
        {
            unlock.Parameters.AddWithValue("table", $"{schema}.outbox");
            Assert.Equal(true, await unlock.ExecuteScalarAsync(Ct));
        }

        var ex = await Assert.ThrowsAsync<OutboxSessionLostException>(() => session.CheckAsync(Ct));
        Assert.Equal("Outboxes[Orders]: session lost (lock not held)", ex.Message);
        Assert.True(session.Lost.IsCancellationRequested);
    }

    [Fact]
    public async Task Check_fails_within_command_deadline_when_server_stops_answering()
    {
        await using var container = TestContainer.PostgreSql("postgres:17.11").Build();
        var store = await ValidatedStoreAsync(container);
        var session = await store.OpenSessionAsync(Ct);
        await container.PauseAsync(Ct);

        var elapsed = Stopwatch.StartNew();
        var ex = await Assert.ThrowsAsync<OutboxSessionLostException>(() => session.CheckAsync(Ct));
        var failedAfter = elapsed.Elapsed;
        await session.DisposeAsync();
        var disposedAfter = elapsed.Elapsed - failedAfter;
        await container.UnpauseAsync(Ct);

        // The 5 s session deadline races Npgsql's 5 s command timeout, so only the prefix is fixed.
        Assert.StartsWith("Outboxes[Orders]: session lost (", ex.Message, StringComparison.Ordinal);
        Assert.InRange(failedAfter, TimeSpan.FromSeconds(4.5), TimeSpan.FromSeconds(5.5));
        Assert.True(disposedAfter < TimeSpan.FromSeconds(1), $"dispose took {disposedAfter}");
    }

    [Fact]
    public async Task Caller_cancellation_mid_command_surfaces_as_cancellation()
    {
        await using var container = TestContainer.PostgreSql("postgres:17.11").Build();
        var store = await ValidatedStoreAsync(container);
        var session = await store.OpenSessionAsync(Ct);
        await container.PauseAsync(Ct);
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);

        var check = session.CheckAsync(cts.Token);
        await Task.Delay(500, Ct);
        var elapsed = Stopwatch.StartNew();
        await cts.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => check);
        var cancelledAfter = elapsed.Elapsed;
        await session.DisposeAsync();
        await container.UnpauseAsync(Ct);

        Assert.True(cancelledAfter < TimeSpan.FromSeconds(1), $"cancellation took {cancelledAfter}");
    }

    [Fact]
    public async Task Read_returns_due_rows_in_id_order_up_to_max_skipping_excluded()
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema);
        var ids = new List<long>();
        for (var i = 0; i < 6; i++)
        {
            ids.Add(await InsertAsync(schema, "'\\x6b'::bytea", "'{}'::jsonb"));
        }

        var future = await InsertAsync(schema, "NULL", "'{}'::jsonb", nextAttemptAt: "now() + interval '1 hour'");
        await using var session = await store.OpenSessionAsync(Ct);

        var all = await session.ReadAsync(new ReadRequest(100, null, []), Ct);
        var limited = await session.ReadAsync(new ReadRequest(3, null, [ids[0], ids[2]]), Ct);

        Assert.Equal(ids, all.Select(m => m.Id));
        Assert.DoesNotContain(all, m => m.Id == future);
        Assert.Equal([ids[1], ids[3], ids[4]], limited.Select(m => m.Id));
    }

    [Fact]
    public async Task Read_maps_row_columns()
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema);
        var keyed = await InsertAsync(schema, "'\\x6b6579'::bytea", """'{"trace":"t-1","type":"created"}'::jsonb""", retryCount: 2);
        var unkeyed = await InsertAsync(schema, "NULL", "'{}'::jsonb");
        await using var session = await store.OpenSessionAsync(Ct);

        var messages = await session.ReadAsync(new ReadRequest(10, null, []), Ct);

        var first = messages.Single(m => m.Id == keyed);
        Assert.Equal("key"u8.ToArray(), first.Key);
        Assert.Equal("payload"u8.ToArray(), first.Payload);
        Assert.Equal(new Dictionary<string, string> { ["trace"] = "t-1", ["type"] = "created" }, first.Headers.ToStrings());
        Assert.False(first.HeadersMalformed);
        Assert.Equal(2, first.RetryCount);
        Assert.Equal(new OutboxPosition(ulong.Parse((await postgres.ScalarAsync<string>($"SELECT tx_id::text FROM {schema}.outbox WHERE id = {keyed}"))!, CultureInfo.InvariantCulture), keyed), first.Position);
        Assert.Equal(new DateTimeOffset(await postgres.ScalarAsync<DateTime>($"SELECT created_at FROM {schema}.outbox WHERE id = {keyed}")), first.CreatedAt);
        Assert.Equal(TimeSpan.Zero, first.CreatedAt.Offset);
        Assert.Equal(new DateTimeOffset(await postgres.ScalarAsync<DateTime>($"SELECT next_attempt_at FROM {schema}.outbox WHERE id = {keyed}")), first.NextAttemptAt);

        var second = messages.Single(m => m.Id == unkeyed);
        Assert.Null(second.Key);
        Assert.Empty(second.Headers);
        Assert.False(second.HeadersMalformed);
    }

    [Theory]
    [InlineData("""'{"attempt":1}'::jsonb""")]
    [InlineData("""'{"a":"x","b":null}'::jsonb""")]
    [InlineData("""'["a"]'::jsonb""")]
    public async Task Read_flags_malformed_headers_without_failing(string headers)
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema);
        await postgres.ExecuteAsync(
            $"ALTER TABLE {schema}.outbox DROP CONSTRAINT outbox_headers_is_object, DROP CONSTRAINT outbox_header_values_are_strings");
        var bad = await InsertAsync(schema, "NULL", headers);
        var good = await InsertAsync(schema, "NULL", """'{"a":"x"}'::jsonb""");
        await using var session = await store.OpenSessionAsync(Ct);

        var messages = await session.ReadAsync(new ReadRequest(10, null, []), Ct);

        var malformed = messages.Single(m => m.Id == bad);
        Assert.True(malformed.HeadersMalformed);
        Assert.Empty(malformed.Headers);
        Assert.False(messages.Single(m => m.Id == good).HeadersMalformed);
        Assert.False(session.Lost.IsCancellationRequested);
    }

    [Fact]
    public async Task Read_flags_lone_surrogate_headers_without_failing()
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema);
        await postgres.ExecuteAsync(
            $"""
            ALTER TABLE {schema}.outbox DROP CONSTRAINT outbox_headers_is_object, DROP CONSTRAINT outbox_header_values_are_strings,
                ALTER COLUMN headers DROP DEFAULT, ALTER COLUMN headers TYPE text USING headers::text
            """);
        var bad = await InsertAsync(schema, "NULL", """'{"a":"\uD800"}'""");
        var good = await InsertAsync(schema, "NULL", """'{"a":"x"}'""");
        await using var session = await store.OpenSessionAsync(Ct);

        var messages = await session.ReadAsync(new ReadRequest(10, null, []), Ct);

        var malformed = messages.Single(m => m.Id == bad);
        Assert.True(malformed.HeadersMalformed);
        Assert.Empty(malformed.Headers);
        Assert.False(messages.Single(m => m.Id == good).HeadersMalformed);
        Assert.False(session.Lost.IsCancellationRequested);
    }

    [Fact]
    public async Task Read_with_position_cursor_is_rejected()
    {
        var store = await ValidatedStoreAsync();
        await using var session = await store.OpenSessionAsync(Ct);

        await Assert.ThrowsAsync<ArgumentException>(() => session.ReadAsync(new ReadRequest(10, new OutboxPosition(1, 1), []), Ct));
    }

    [Fact]
    public async Task Commit_deletes_exactly_the_given_ids_in_one_statement()
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema);
        var ids = new List<long>();
        for (var i = 0; i < 4; i++)
        {
            ids.Add(await InsertAsync(schema, "NULL", "'{}'::jsonb"));
        }

        await postgres.ExecuteAsync(
            $"""
            CREATE TABLE {schema}.delete_statements (n int);
            CREATE FUNCTION {schema}.count_delete() RETURNS trigger LANGUAGE plpgsql AS
                'BEGIN INSERT INTO {schema}.delete_statements VALUES (1); RETURN NULL; END';
            CREATE TRIGGER count_delete AFTER DELETE ON {schema}.outbox FOR EACH STATEMENT EXECUTE FUNCTION {schema}.count_delete();
            """);
        await using var session = await store.OpenSessionAsync(Ct);

        await session.CommitAsync([new Outcome.Remove(ids[3]), new Outcome.Remove(ids[1])], Ct);

        Assert.Equal(1L, await postgres.ScalarAsync<long>($"SELECT count(*) FROM {schema}.delete_statements"));
        var remaining = await session.ReadAsync(new ReadRequest(10, null, []), Ct);
        Assert.Equal([ids[0], ids[2]], remaining.Select(m => m.Id));
    }

    [Fact]
    public async Task Commit_refuses_an_outcome_it_does_not_handle_and_deletes_nothing()
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema);
        var id = await InsertAsync(schema, "NULL", "'{}'::jsonb");
        await using var session = await store.OpenSessionAsync(Ct);

        await Assert.ThrowsAsync<ArgumentException>(() => session.CommitAsync([new Outcome.Remove(id), null!], Ct));

        Assert.Equal(1L, await postgres.ScalarAsync<long>($"SELECT count(*) FROM {schema}.outbox"));
    }

    [Fact]
    public async Task Dispose_during_running_commit_closes_after_it_ends()
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema);
        var id = await InsertAsync(schema, "NULL", "'{}'::jsonb");
        await using var blocker = new NpgsqlConnection(postgres.ConnectionString);
        await blocker.OpenAsync(Ct);
        await using var transaction = await blocker.BeginTransactionAsync(Ct);
        await using (var lockRow = new NpgsqlCommand($"SELECT id FROM {schema}.outbox WHERE id = {id} FOR UPDATE", blocker, transaction))
        {
            await lockRow.ExecuteScalarAsync(Ct);
        }

        var session = (PostgreSqlOutboxSession)await store.OpenSessionAsync(Ct);
        var pid = session.Connection.ProcessID;
        var commit = session.CommitAsync([new Outcome.Remove(id)], Ct);
        var deadline = DateTime.UtcNow.AddSeconds(3);
        while (await postgres.ScalarAsync<long>($"SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE pid = {pid} AND wait_event_type = 'Lock'") == 0)
        {
            Assert.True(DateTime.UtcNow < deadline, "commit never waited on the row lock");
            await Task.Delay(20, Ct);
        }

        await session.DisposeAsync();
        Assert.True(session.ReleasePending);
        Assert.True(await postgres.BackendExistsAsync(pid));

        await transaction.RollbackAsync(Ct);
        await commit.WaitAsync(TimeSpan.FromSeconds(5), Ct);

        Assert.Equal(0L, await postgres.ScalarAsync<long>($"SELECT count(*) FROM {schema}.outbox"));
        deadline = DateTime.UtcNow.AddSeconds(5);
        while (await postgres.BackendExistsAsync(pid) && DateTime.UtcNow < deadline)
        {
            await Task.Delay(50, Ct);
        }

        Assert.False(await postgres.BackendExistsAsync(pid));
    }

    [Fact]
    public async Task Empty_commit_makes_no_round_trip()
    {
        var store = await ValidatedStoreAsync();
        await using var session = (PostgreSqlOutboxSession)await store.OpenSessionAsync(Ct);
        await session.Connection.CloseAsync();

        await session.CommitAsync([], Ct);

        Assert.Equal(System.Data.ConnectionState.Closed, session.Connection.State);
    }

    [Fact]
    public async Task Read_and_commit_report_loss_after_terminate_and_never_reconnect()
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema);
        var id = await InsertAsync(schema, "NULL", "'{}'::jsonb");
        await using var session = (PostgreSqlOutboxSession)await store.OpenSessionAsync(Ct);
        var pid = session.Connection.ProcessID;

        await postgres.TerminateAsync(pid);

        var read = await Assert.ThrowsAsync<OutboxSessionLostException>(() => session.ReadAsync(new ReadRequest(10, null, []), Ct));
        Assert.StartsWith("Outboxes[Orders]: session lost (", read.Message, StringComparison.Ordinal);
        Assert.True(session.Lost.IsCancellationRequested);
        var commit = await Assert.ThrowsAsync<OutboxSessionLostException>(() => session.CommitAsync([new Outcome.Remove(id)], Ct));
        Assert.Equal("Outboxes[Orders]: session lost (connection not open)", commit.Message);
        Assert.NotEqual(System.Data.ConnectionState.Open, session.Connection.State);
        Assert.Equal(1L, await postgres.ScalarAsync<long>($"SELECT count(*) FROM {schema}.outbox"));
    }

    [Fact]
    public async Task Read_fails_within_command_deadline_when_server_stops_answering()
    {
        await using var container = TestContainer.PostgreSql("postgres:17.11").Build();
        var store = await ValidatedStoreAsync(container);
        var session = await store.OpenSessionAsync(Ct);
        await container.PauseAsync(Ct);

        var elapsed = Stopwatch.StartNew();
        var ex = await Assert.ThrowsAsync<OutboxSessionLostException>(() => session.ReadAsync(new ReadRequest(10, null, []), Ct));
        var failedAfter = elapsed.Elapsed;
        await session.DisposeAsync();
        await container.UnpauseAsync(Ct);

        Assert.StartsWith("Outboxes[Orders]: session lost (", ex.Message, StringComparison.Ordinal);
        Assert.True(failedAfter < TimeSpan.FromSeconds(6), $"read failed after {failedAfter}");
        Assert.True(session.Lost.IsCancellationRequested);
    }

    [Fact]
    public async Task Commit_fail_writes_retry_state_on_database_clock_and_read_returns_last_error()
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema);
        var later = await InsertAsync(schema, "NULL", "'{}'::jsonb");
        var due = await InsertAsync(schema, "NULL", "'{}'::jsonb", retryCount: 2);
        await using var session = await store.OpenSessionAsync(Ct);
        var delay = TimeSpan.FromMinutes(10);

        var before = await postgres.ScalarAsync<DateTime>("SELECT now()");
        await session.CommitAsync(
            [
                new Outcome.Fail(later) { ReadRetryCount = 0, RetryCount = 1, Delay = delay, Error = "first error" },
                new Outcome.Fail(due) { ReadRetryCount = 2, RetryCount = 3, Delay = TimeSpan.Zero, Error = "second error" },
            ],
            Ct);
        var after = await postgres.ScalarAsync<DateTime>("SELECT now()");

        Assert.Equal("1:first error", await RowStateAsync(schema, later));
        var nextAttemptAt = await postgres.ScalarAsync<DateTime>($"SELECT next_attempt_at FROM {schema}.outbox WHERE id = {later}");
        Assert.InRange(nextAttemptAt, before + delay - TimeSpan.FromSeconds(1), after + delay + TimeSpan.FromSeconds(1));
        var read = await session.ReadAsync(new ReadRequest(10, null, []), Ct);
        var message = Assert.Single(read);
        Assert.Equal(due, message.Id);
        Assert.Equal(3, message.RetryCount);
        Assert.Equal("second error", message.LastError);
        Assert.False(session.Lost.IsCancellationRequested);
    }

    [Fact]
    public async Task Commit_mixed_group_removes_and_fails_together()
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema);
        var ids = new List<long>();
        for (var i = 0; i < 4; i++)
        {
            ids.Add(await InsertAsync(schema, "NULL", "'{}'::jsonb"));
        }

        await using var session = await store.OpenSessionAsync(Ct);

        await session.CommitAsync(
            [
                new Outcome.Remove(ids[0]),
                new Outcome.Fail(ids[1]) { ReadRetryCount = 0, RetryCount = 1, Delay = TimeSpan.Zero, Error = "e1" },
                new Outcome.Remove(ids[2]),
                new Outcome.Fail(ids[3]) { ReadRetryCount = 0, RetryCount = 1, Delay = TimeSpan.Zero, Error = "e3" },
            ],
            Ct);

        var remaining = await session.ReadAsync(new ReadRequest(10, null, []), Ct);
        Assert.Equal([ids[1], ids[3]], remaining.Select(m => m.Id));
        Assert.Equal(["e1", "e3"], remaining.Select(m => m.LastError));
        Assert.All(remaining, m => Assert.Equal(1, m.RetryCount));
    }

    [Fact]
    public async Task Commit_fail_after_operator_reset_or_delete_is_a_no_op_and_succeeds()
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema);
        var reset = await InsertAsync(schema, "NULL", "'{}'::jsonb", retryCount: 5);
        var deleted = await InsertAsync(schema, "NULL", "'{}'::jsonb", retryCount: 5);
        var removed = await InsertAsync(schema, "NULL", "'{}'::jsonb");
        await using var session = await store.OpenSessionAsync(Ct);
        Assert.Equal(3, (await session.ReadAsync(new ReadRequest(10, null, []), Ct)).Count);

        await postgres.ExecuteAsync(
            $"UPDATE {schema}.outbox SET retry_count = 0 WHERE id = {reset}; DELETE FROM {schema}.outbox WHERE id = {deleted};");

        await session.CommitAsync(
            [
                new Outcome.Fail(reset) { ReadRetryCount = 5, RetryCount = 6, Delay = TimeSpan.FromHours(1), Error = "late" },
                new Outcome.Fail(deleted) { ReadRetryCount = 5, RetryCount = 6, Delay = TimeSpan.FromHours(1), Error = "late" },
                new Outcome.Remove(removed),
            ],
            Ct);

        Assert.Equal("0:<null>", await RowStateAsync(schema, reset));
        var read = await session.ReadAsync(new ReadRequest(10, null, []), Ct);
        Assert.Equal([reset], read.Select(m => m.Id));
        Assert.Equal(0L, await postgres.ScalarAsync<long>($"SELECT count(*) FROM {schema}.outbox WHERE id IN ({deleted}, {removed})"));
        Assert.False(session.Lost.IsCancellationRequested);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Commit_transaction_lost_mid_update_applies_no_outcome(bool pastDeadline)
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema);
        var removed = await InsertAsync(schema, "NULL", "'{}'::jsonb");
        var failed = await InsertAsync(schema, "NULL", "'{}'::jsonb");
        var blocked = await InsertAsync(schema, "NULL", "'{}'::jsonb");
        await using var blocker = new NpgsqlConnection(postgres.ConnectionString);
        await blocker.OpenAsync(Ct);
        await using var transaction = await blocker.BeginTransactionAsync(Ct);
        await using (var lockRow = new NpgsqlCommand($"SELECT id FROM {schema}.outbox WHERE id = {blocked} FOR UPDATE", blocker, transaction))
        {
            await lockRow.ExecuteScalarAsync(Ct);
        }

        var session = (PostgreSqlOutboxSession)await store.OpenSessionAsync(Ct);
        var pid = session.Connection.ProcessID;
        var elapsed = Stopwatch.StartNew();
        var commit = session.CommitAsync(
            [
                new Outcome.Remove(removed),
                new Outcome.Fail(failed) { ReadRetryCount = 0, RetryCount = 1, Delay = TimeSpan.Zero, Error = "e" },
                new Outcome.Fail(blocked) { ReadRetryCount = 0, RetryCount = 1, Delay = TimeSpan.Zero, Error = "e" },
            ],
            Ct);
        var deadline = DateTime.UtcNow.AddSeconds(3);
        while (await postgres.ScalarAsync<long>(
            $"SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE pid = {pid} AND wait_event_type = 'Lock' AND query LIKE 'UPDATE%'") == 0)
        {
            Assert.True(DateTime.UtcNow < deadline, "commit never waited in UPDATE on the row lock");
            await Task.Delay(20, Ct);
        }

        if (pastDeadline)
        {
            var timedOut = await Assert.ThrowsAsync<OutboxSessionLostException>(() => commit);
            Assert.True(elapsed.Elapsed < TimeSpan.FromSeconds(6), $"commit failed after {elapsed.Elapsed}");
            Assert.StartsWith("Outboxes[Orders]: session lost (", timedOut.Message, StringComparison.Ordinal);
        }

        await postgres.TerminateAsync(pid);
        await transaction.RollbackAsync(Ct);

        var ex = await Assert.ThrowsAsync<OutboxSessionLostException>(() => commit);
        Assert.StartsWith("Outboxes[Orders]: session lost (", ex.Message, StringComparison.Ordinal);
        Assert.True(session.Lost.IsCancellationRequested);
        await session.DisposeAsync();
        Assert.Equal(3L, await postgres.ScalarAsync<long>($"SELECT count(*) FROM {schema}.outbox"));
        Assert.Equal("0:<null>", await RowStateAsync(schema, failed));
        Assert.Equal("0:<null>", await RowStateAsync(schema, blocked));
    }

    [Theory]
    [InlineData(OrderingMode.PerKey)]
    [InlineData(OrderingMode.Strict)]
    public async Task Ordered_read_orders_by_tx_id_then_id_and_cursor_is_exclusive(OrderingMode mode)
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema, mode);
        await using var producer = new NpgsqlConnection(postgres.ConnectionString);
        await producer.OpenAsync(Ct);
        await using var transaction = await producer.BeginTransactionAsync(Ct);
        var a1 = await InsertInAsync(producer, transaction, schema);
        var b1 = await InsertAsync(schema, "'\\x62'::bytea", "'{}'::jsonb");
        var a2 = await InsertInAsync(producer, transaction, schema);
        await transaction.CommitAsync(Ct);
        var c1 = await InsertAsync(schema, "'\\x63'::bytea", "'{}'::jsonb");
        await using var session = await store.OpenSessionAsync(Ct);

        var all = await ReadUntilAsync(session, new ReadRequest(100, null, []), 4);

        Assert.Equal([a1, a2, b1, c1], all.Select(m => m.Id));
        Assert.Equal(all.Select(m => m.Position).Order(), all.Select(m => m.Position));
        Assert.Equal([b1, c1], (await session.ReadAsync(new ReadRequest(100, all[1].Position, []), Ct)).Select(m => m.Id));
        Assert.Equal([a2], (await session.ReadAsync(new ReadRequest(1, all[0].Position, []), Ct)).Select(m => m.Id));
        Assert.Empty(await session.ReadAsync(new ReadRequest(100, all[3].Position, []), Ct));
        Assert.Equal([a1, a2], (await session.ReadAsync(new ReadRequest(2, null, []), Ct)).Select(m => m.Id));
    }

    [Theory]
    [InlineData(OrderingMode.PerKey, true)]
    [InlineData(OrderingMode.PerKey, false)]
    [InlineData(OrderingMode.Strict, true)]
    [InlineData(OrderingMode.Strict, false)]
    public async Task Ordered_read_hides_rows_behind_an_open_transaction_until_it_ends(OrderingMode mode, bool commit)
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema, mode);
        await using var producer = new NpgsqlConnection(postgres.ConnectionString);
        await producer.OpenAsync(Ct);
        await using var transaction = await producer.BeginTransactionAsync(Ct);
        var held = await InsertInAsync(producer, transaction, schema);
        var later = await InsertAsync(schema, "NULL", "'{}'::jsonb");
        await using var session = await store.OpenSessionAsync(Ct);

        Assert.Empty(await session.ReadAsync(new ReadRequest(100, null, []), Ct));

        if (commit)
        {
            await transaction.CommitAsync(Ct);
            var read = await ReadUntilAsync(session, new ReadRequest(100, null, []), 2);
            Assert.Equal([held, later], read.Select(m => m.Id));
        }
        else
        {
            await transaction.RollbackAsync(Ct);
            var read = await ReadUntilAsync(session, new ReadRequest(100, null, []), 1);
            Assert.Equal([later], read.Select(m => m.Id));
        }
    }

    [Theory]
    [InlineData(OrderingMode.None)]
    [InlineData(OrderingMode.PerKey)]
    [InlineData(OrderingMode.Strict)]
    public async Task Read_maps_route_column(OrderingMode mode)
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema, mode);
        var unrouted = await InsertAsync(schema, "'\\x61'::bytea", "'{}'::jsonb");
        var routed = await InsertAsync(schema, "'\\x62'::bytea", "'{}'::jsonb", route: "'eu-west.orders_v2'");
        await using var session = await store.OpenSessionAsync(Ct);

        var read = await ReadUntilAsync(session, new ReadRequest(100, null, []), 2);

        Assert.Equal([unrouted, routed], read.Select(m => m.Id));
        Assert.Null(read[0].Route);
        Assert.Equal("eu-west.orders_v2", read[1].Route);
    }

    // A not-due PerKey row reads no route, like its payload and headers.
    [Fact]
    public async Task PerKey_read_gives_not_due_row_no_route()
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema, OrderingMode.PerKey);
        var waitingHead = await InsertAsync(schema, "'\\x61'::bytea", "'{}'::jsonb", retryCount: 1, nextAttemptAt: "now() + interval '1 hour'", route: "'r1'");
        var waitingNext = await InsertAsync(schema, "'\\x61'::bytea", "'{}'::jsonb", route: "'r1'");
        var due = await InsertAsync(schema, "'\\x62'::bytea", "'{}'::jsonb", route: "'r1'");
        await using var session = await store.OpenSessionAsync(Ct);

        var read = await ReadUntilAsync(session, new ReadRequest(100, null, []), 3);

        Assert.Equal([waitingHead, waitingNext, due], read.Select(m => m.Id));
        Assert.Equal([false, false, true], read.Select(m => m.IsDue));
        Assert.Equal([null, null, "r1"], read.Select(m => m.Route));
    }

    [Fact]
    public async Task PerKey_read_returns_every_scanned_row_and_marks_keys_held_by_a_waiting_or_blocked_failed_row_not_due()
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema, OrderingMode.PerKey);
        const string empty = "'{}'::jsonb";
        var waitingHead = await InsertAsync(schema, "'\\x61'::bytea", empty, retryCount: 1, nextAttemptAt: "now() + interval '1 hour'");
        var waitingNext = await InsertAsync(schema, "'\\x61'::bytea", """'{"h":"v"}'::jsonb""");
        var blockedHead = await InsertAsync(schema, "'\\x62'::bytea", empty, retryCount: 1, nextAttemptAt: "'infinity'");
        var blockedNext = await InsertAsync(schema, "'\\x62'::bytea", empty);
        var dueHead = await InsertAsync(schema, "'\\x63'::bytea", empty, retryCount: 2);
        var dueNext = await InsertAsync(schema, "'\\x63'::bytea", empty);
        var blockedUnkeyed = await InsertAsync(schema, "NULL", empty, retryCount: 1, nextAttemptAt: "'infinity'");
        var unkeyed = await InsertAsync(schema, "NULL", empty);
        await using var session = await store.OpenSessionAsync(Ct);

        var read = await ReadUntilAsync(session, new ReadRequest(100, null, []), 8);

        Assert.Equal([waitingHead, waitingNext, blockedHead, blockedNext, dueHead, dueNext, blockedUnkeyed, unkeyed], read.Select(m => m.Id));
        Assert.Equal([false, false, false, false, true, true, false, true], read.Select(m => m.IsDue));
        Assert.All(read.Where(m => !m.IsDue), m =>
        {
            Assert.Empty(m.Payload);
            Assert.Empty(m.Headers);
            Assert.False(m.HeadersMalformed);
        });
        Assert.All(read.Where(m => m.IsDue), m => Assert.Equal("payload"u8.ToArray(), m.Payload));
        Assert.NotNull(read[0].NextAttemptAt);
        Assert.Null(read[2].NextAttemptAt);
        Assert.Equal(1, read[2].RetryCount);
        Assert.Equal(2, read[4].RetryCount);
        Assert.NotNull(read[4].NextAttemptAt);
    }

    [Fact]
    public async Task PerKey_read_is_a_bounded_scan_that_walks_a_blocked_backlog_and_returns_nothing_past_its_tail()
    {
        const int max = 5;
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema, OrderingMode.PerKey);
        var blocked = await InsertAsync(schema, "'\\x6b'::bytea", "'{}'::jsonb", retryCount: 1, nextAttemptAt: "'infinity'");
        var other = await InsertAsync(schema, "'\\x61'::bytea", "'{}'::jsonb");
        await postgres.ExecuteAsync($"INSERT INTO {schema}.outbox (message_key, payload) SELECT '\\x6b'::bytea, '\\x70'::bytea FROM generate_series(1, {3 * max})");
        const int total = 2 + (3 * max);
        await using var session = await store.OpenSessionAsync(Ct);
        Assert.Equal(total, (await ReadUntilAsync(session, new ReadRequest(100, null, []), total)).Count);

        var first = await session.ReadAsync(new ReadRequest(max, null, []), Ct);
        Assert.Equal([blocked, other], first.Take(2).Select(m => m.Id));
        Assert.Equal([false, true, false, false, false], first.Select(m => m.IsDue));

        var after = first[^1].Position;
        var scanned = first.Count;
        var reads = 1;
        while (true)
        {
            var rows = await session.ReadAsync(new ReadRequest(max, after, []), Ct);
            Assert.InRange(rows.Count, 0, max);
            if (rows.Count == 0)
            {
                break;
            }

            Assert.All(rows, m => Assert.False(m.IsDue));
            Assert.True(rows[0].Position > after);
            after = rows[^1].Position;
            scanned += rows.Count;
            reads++;
        }

        Assert.Equal(total, scanned);
        Assert.Equal((total + max - 1) / max, reads);
        Assert.Empty(await session.ReadAsync(new ReadRequest(max, after, []), Ct));
        Assert.False(session.Lost.IsCancellationRequested);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PerKey_read_plan_reads_at_most_max_outbox_index_entries_behind_a_blocked_key(bool cursor)
    {
        const int max = 5;
        var schema = PostgreSqlFixture.NewSchema();
        await postgres.CreateOutboxSchemaAsync(schema);
        var table = $"{schema}.outbox";
        await InsertAsync(schema, "'\\x6b'::bytea", "'{}'::jsonb", retryCount: 1, nextAttemptAt: "'infinity'");
        await postgres.ExecuteAsync($"INSERT INTO {table} (message_key, payload) SELECT '\\x6b'::bytea, '\\x70'::bytea FROM generate_series(1, 2000); ANALYZE {table};");
        await using var connection = new NpgsqlConnection(postgres.ConnectionString);
        await connection.OpenAsync(Ct);
        await using var command = new NpgsqlCommand($"EXPLAIN (ANALYZE, FORMAT JSON) {PostgreSqlOutboxSession.PerKeyReadSql(table, cursor)}", connection);
        command.Parameters.AddWithValue("max", max);
        if (cursor)
        {
            command.Parameters.AddWithValue("after_tx", "0");
            command.Parameters.AddWithValue("after_id", 0L);
        }

        using var plan = System.Text.Json.JsonDocument.Parse((string)(await command.ExecuteScalarAsync(Ct))!);

        var scans = Nodes(plan.RootElement[0].GetProperty("Plan"))
            .Where(n => n.TryGetProperty("Relation Name", out var relation) && relation.GetString() == "outbox")
            .ToList();
        static bool IsMainScan(System.Text.Json.JsonElement n) => n.TryGetProperty("Index Name", out var index) && index.GetString() == "outbox_tx_id_id_idx";
        var main = Assert.Single(scans, IsMainScan);
        Assert.InRange(main.GetProperty("Actual Rows").GetDouble() * main.GetProperty("Actual Loops").GetDouble(), 0, max);
        Assert.False(main.TryGetProperty("Rows Removed by Filter", out var removed) && removed.GetDouble() > 0);
        Assert.All(scans.Where(n => !IsMainScan(n)), probe => Assert.InRange(probe.GetProperty("Actual Loops").GetDouble(), 0, max));
    }

    [Fact]
    public async Task Summary_changes_when_an_unblock_only_sets_next_attempt_at_and_survives_max_retry_count()
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema, OrderingMode.PerKey);
        var blocked = await InsertAsync(schema, "'\\x6b'::bytea", "'{}'::jsonb", retryCount: 1, nextAttemptAt: "'infinity'");
        await InsertAsync(schema, "NULL", "'{}'::jsonb", retryCount: int.MaxValue, nextAttemptAt: "'infinity'");
        await using var session = await store.OpenSessionAsync(Ct);

        var before = await session.ReadFailedSummaryAsync(Ct);
        await postgres.ExecuteAsync($"UPDATE {schema}.outbox SET next_attempt_at = now() WHERE id = {blocked}");
        var after = await session.ReadFailedSummaryAsync(Ct);

        Assert.Equal(2, before.Count);
        Assert.Equal(before.Count, after.Count);
        Assert.Null(before.NextDueAt);
        Assert.Null(after.NextDueAt);
        Assert.NotEqual(before.Checksum, after.Checksum);
        Assert.False(session.Lost.IsCancellationRequested);
    }

    [Fact]
    public async Task Summary_changes_when_next_attempt_at_alone_is_set_to_now_on_a_waiting_row_that_is_not_the_earliest()
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema, OrderingMode.PerKey);
        var earliest = await InsertAsync(schema, "'\\x6b'::bytea", "'{}'::jsonb", retryCount: 1, nextAttemptAt: "now() + interval '1 hour'");
        var waiting = await InsertAsync(schema, "'\\x6c'::bytea", "'{}'::jsonb", retryCount: 1, nextAttemptAt: "now() + interval '2 hours'");
        await using var session = await store.OpenSessionAsync(Ct);

        var before = await session.ReadFailedSummaryAsync(Ct);
        await postgres.ExecuteAsync($"UPDATE {schema}.outbox SET next_attempt_at = now() WHERE id = {waiting}");
        var after = await session.ReadFailedSummaryAsync(Ct);

        Assert.Equal(2, before.Count);
        Assert.Equal(before.Count, after.Count);
        Assert.Equal(await NextAttemptAtAsync(schema, earliest), before.NextDueAt);
        Assert.Equal(before.NextDueAt, after.NextDueAt);
        Assert.NotEqual(before.Checksum, after.Checksum);
        Assert.False(session.Lost.IsCancellationRequested);
    }

    [Fact]
    public async Task Summary_checksum_changes_when_a_waiting_failed_row_becomes_due()
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema, OrderingMode.PerKey);
        var waiting = await InsertAsync(schema, "'\\x6b'::bytea", "'{}'::jsonb", retryCount: 1, nextAttemptAt: "now() + interval '1 second'");
        await using var session = await store.OpenSessionAsync(Ct);

        var before = await session.ReadFailedSummaryAsync(Ct);
        var dueAt = await NextAttemptAtAsync(schema, waiting);
        while (await postgres.ScalarAsync<bool>($"SELECT next_attempt_at > now() FROM {schema}.outbox WHERE id = {waiting}"))
        {
            await Task.Delay(100, Ct);
        }

        var after = await session.ReadFailedSummaryAsync(Ct);

        Assert.Equal(dueAt, before.NextDueAt);
        Assert.Null(after.NextDueAt);
        Assert.Equal(before.Count, after.Count);
        Assert.NotEqual(before.Checksum, after.Checksum);
        Assert.False(session.Lost.IsCancellationRequested);
    }

    [Fact]
    public async Task Strict_read_returns_every_row_with_due_state_and_blocked_row_has_no_next_attempt()
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema, OrderingMode.Strict);
        var blocked = await InsertAsync(schema, "NULL", "'{}'::jsonb", retryCount: 1, nextAttemptAt: "'infinity'");
        var waiting = await InsertAsync(schema, "NULL", "'{}'::jsonb", retryCount: 1, nextAttemptAt: "now() + interval '1 hour'");
        var due = await InsertAsync(schema, "NULL", "'{}'::jsonb");
        await using var session = await store.OpenSessionAsync(Ct);

        var read = await ReadUntilAsync(session, new ReadRequest(100, null, []), 3);

        Assert.Equal([blocked, waiting, due], read.Select(m => m.Id));
        Assert.Null(read[0].NextAttemptAt);
        Assert.False(read[0].IsDue);
        Assert.Equal(1, read[0].RetryCount);
        Assert.NotNull(read[1].NextAttemptAt);
        Assert.False(read[1].IsDue);
        Assert.True(read[2].IsDue);
        Assert.False(session.Lost.IsCancellationRequested);
    }

    [Theory]
    [InlineData(OrderingMode.PerKey)]
    [InlineData(OrderingMode.Strict)]
    public async Task Ordered_read_with_exclude_list_is_rejected(OrderingMode mode)
    {
        var store = await ValidatedStoreAsync(mode: mode);
        await using var session = await store.OpenSessionAsync(Ct);

        await Assert.ThrowsAsync<ArgumentException>(() => session.ReadAsync(new ReadRequest(10, null, [1]), Ct));
        await Assert.ThrowsAsync<ArgumentException>(() => session.ReadAsync(new ReadRequest(10, new OutboxPosition(1, 1), [1]), Ct));
    }

    [Fact]
    public async Task Summary_covers_failed_rows_only_and_changes_on_retry_update_delete_and_reset()
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema, OrderingMode.Strict);
        await using var session = await store.OpenSessionAsync(Ct);

        Assert.Equal(new FailedRowsSummary(0, 0, null), await session.ReadFailedSummaryAsync(Ct));

        await InsertAsync(schema, "NULL", "'{}'::jsonb");
        await InsertAsync(schema, "NULL", "'{}'::jsonb", retryCount: 1, nextAttemptAt: "'infinity'");
        await InsertAsync(schema, "NULL", "'{}'::jsonb", retryCount: 2);
        var onlyBlockedAndDue = await session.ReadFailedSummaryAsync(Ct);
        Assert.Equal(2, onlyBlockedAndDue.Count);
        Assert.Null(onlyBlockedAndDue.NextDueAt);

        await InsertAsync(schema, "NULL", "'{}'::jsonb");
        Assert.Equal(onlyBlockedAndDue, await session.ReadFailedSummaryAsync(Ct));

        var soon = await InsertAsync(schema, "NULL", "'{}'::jsonb", retryCount: 1, nextAttemptAt: "now() + interval '1 hour'");
        var later = await InsertAsync(schema, "NULL", "'{}'::jsonb", retryCount: 1, nextAttemptAt: "now() + interval '2 hours'");
        var waiting = await session.ReadFailedSummaryAsync(Ct);
        Assert.Equal(4, waiting.Count);
        Assert.Equal(await NextAttemptAtAsync(schema, soon), waiting.NextDueAt);

        await postgres.ExecuteAsync($"UPDATE {schema}.outbox SET retry_count = 2 WHERE id = {soon}");
        var retried = await session.ReadFailedSummaryAsync(Ct);
        Assert.Equal(4, retried.Count);
        Assert.NotEqual(waiting.Checksum, retried.Checksum);
        Assert.Equal(waiting.NextDueAt, retried.NextDueAt);

        await postgres.ExecuteAsync($"DELETE FROM {schema}.outbox WHERE id = {soon}");
        var deleted = await session.ReadFailedSummaryAsync(Ct);
        Assert.Equal(3, deleted.Count);
        Assert.Equal(await NextAttemptAtAsync(schema, later), deleted.NextDueAt);

        await postgres.ExecuteAsync($"UPDATE {schema}.outbox SET retry_count = 0, next_attempt_at = now() WHERE id = {later}");
        Assert.Equal(onlyBlockedAndDue, await session.ReadFailedSummaryAsync(Ct));
        Assert.False(session.Lost.IsCancellationRequested);
    }

    [Fact]
    public async Task Commit_block_writes_infinity_and_retry_state_and_operator_change_wins()
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema, OrderingMode.Strict);
        var blocked = await InsertAsync(schema, "NULL", "'{}'::jsonb");
        var reset = await InsertAsync(schema, "NULL", "'{}'::jsonb", retryCount: 3);
        var deleted = await InsertAsync(schema, "NULL", "'{}'::jsonb", retryCount: 3);
        await using var session = await store.OpenSessionAsync(Ct);
        Assert.Equal(3, (await ReadUntilAsync(session, new ReadRequest(10, null, []), 3)).Count);

        await postgres.ExecuteAsync(
            $"UPDATE {schema}.outbox SET retry_count = 0 WHERE id = {reset}; DELETE FROM {schema}.outbox WHERE id = {deleted};");

        await session.CommitAsync(
            [
                new Outcome.Block(blocked) { ReadRetryCount = 0, RetryCount = 1, Error = "too large" },
                new Outcome.Block(reset) { ReadRetryCount = 3, RetryCount = 4, Error = "late" },
                new Outcome.Block(deleted) { ReadRetryCount = 3, RetryCount = 4, Error = "late" },
            ],
            Ct);

        Assert.Equal("1:too large", await RowStateAsync(schema, blocked));
        Assert.True(await IsInfinityAsync(schema, blocked));
        Assert.Equal("0:<null>", await RowStateAsync(schema, reset));
        Assert.False(await IsInfinityAsync(schema, reset));
        Assert.Equal(0L, await postgres.ScalarAsync<long>($"SELECT count(*) FROM {schema}.outbox WHERE id = {deleted}"));
        Assert.False(session.Lost.IsCancellationRequested);
    }

    [Fact]
    public async Task Commit_mixed_group_removes_fails_and_blocks_together()
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema, OrderingMode.PerKey);
        var removed = await InsertAsync(schema, "NULL", "'{}'::jsonb");
        var failed = await InsertAsync(schema, "NULL", "'{}'::jsonb");
        var blocked = await InsertAsync(schema, "NULL", "'{}'::jsonb");
        await using var session = await store.OpenSessionAsync(Ct);

        await session.CommitAsync(
            [
                new Outcome.Remove(removed),
                new Outcome.Fail(failed) { ReadRetryCount = 0, RetryCount = 1, Delay = TimeSpan.FromHours(1), Error = "e1" },
                new Outcome.Block(blocked) { ReadRetryCount = 0, RetryCount = 1, Error = "e2" },
            ],
            Ct);

        Assert.Equal(0L, await postgres.ScalarAsync<long>($"SELECT count(*) FROM {schema}.outbox WHERE id = {removed}"));
        Assert.Equal("1:e1", await RowStateAsync(schema, failed));
        Assert.False(await IsInfinityAsync(schema, failed));
        Assert.Equal("1:e2", await RowStateAsync(schema, blocked));
        Assert.True(await IsInfinityAsync(schema, blocked));
    }

    [Fact]
    public async Task Commit_transaction_lost_mid_block_applies_no_outcome()
    {
        var schema = PostgreSqlFixture.NewSchema();
        var store = await ValidatedStoreAsync(schema, OrderingMode.Strict);
        var removed = await InsertAsync(schema, "NULL", "'{}'::jsonb");
        var failed = await InsertAsync(schema, "NULL", "'{}'::jsonb");
        var blocked = await InsertAsync(schema, "NULL", "'{}'::jsonb");
        await using var blocker = new NpgsqlConnection(postgres.ConnectionString);
        await blocker.OpenAsync(Ct);
        await using var transaction = await blocker.BeginTransactionAsync(Ct);
        await using (var lockRow = new NpgsqlCommand($"SELECT id FROM {schema}.outbox WHERE id = {blocked} FOR UPDATE", blocker, transaction))
        {
            await lockRow.ExecuteScalarAsync(Ct);
        }

        var session = (PostgreSqlOutboxSession)await store.OpenSessionAsync(Ct);
        var pid = session.Connection.ProcessID;
        var commit = session.CommitAsync(
            [
                new Outcome.Remove(removed),
                new Outcome.Fail(failed) { ReadRetryCount = 0, RetryCount = 1, Delay = TimeSpan.Zero, Error = "e" },
                new Outcome.Block(blocked) { ReadRetryCount = 0, RetryCount = 1, Error = "e" },
            ],
            Ct);
        var deadline = DateTime.UtcNow.AddSeconds(3);
        while (await postgres.ScalarAsync<long>(
            $"SELECT count(*) FROM pg_catalog.pg_stat_activity WHERE pid = {pid} AND wait_event_type = 'Lock' AND query LIKE '%''infinity''%'") == 0)
        {
            Assert.True(DateTime.UtcNow < deadline, "commit never waited in the block UPDATE on the row lock");
            await Task.Delay(20, Ct);
        }

        await postgres.TerminateAsync(pid);
        await transaction.RollbackAsync(Ct);

        var ex = await Assert.ThrowsAsync<OutboxSessionLostException>(() => commit);
        Assert.StartsWith("Outboxes[Orders]: session lost (", ex.Message, StringComparison.Ordinal);
        Assert.True(session.Lost.IsCancellationRequested);
        await session.DisposeAsync();
        Assert.Equal(3L, await postgres.ScalarAsync<long>($"SELECT count(*) FROM {schema}.outbox"));
        Assert.Equal("0:<null>", await RowStateAsync(schema, failed));
        Assert.Equal("0:<null>", await RowStateAsync(schema, blocked));
        Assert.False(await IsInfinityAsync(schema, blocked));
    }

    private static IEnumerable<System.Text.Json.JsonElement> Nodes(System.Text.Json.JsonElement node)
    {
        yield return node;
        if (node.TryGetProperty("Plans", out var children))
        {
            foreach (var child in children.EnumerateArray().SelectMany(Nodes))
            {
                yield return child;
            }
        }
    }

    private async Task<bool> IsInfinityAsync(string schema, long id) =>
        await postgres.ScalarAsync<bool>($"SELECT next_attempt_at = 'infinity' FROM {schema}.outbox WHERE id = {id}");

    private async Task<DateTimeOffset> NextAttemptAtAsync(string schema, long id) =>
        new(await postgres.ScalarAsync<DateTime>($"SELECT next_attempt_at FROM {schema}.outbox WHERE id = {id}"));

    private static async Task<long> InsertInAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string schema)
    {
        await using var command = new NpgsqlCommand(
            $"INSERT INTO {schema}.outbox (message_key, payload) VALUES ('\\x61'::bytea, '\\x70'::bytea) RETURNING id", connection, transaction);
        return (long)(await command.ExecuteScalarAsync(Ct))!;
    }

    // The gate is cluster-wide: an open transaction in a parallel test holds rows back for its duration.
    internal static async Task<IReadOnlyList<OutboxMessage>> ReadUntilAsync(IOutboxSession session, ReadRequest request, int count)
    {
        var deadline = DateTime.UtcNow.AddSeconds(15);
        while (true)
        {
            var rows = await session.ReadAsync(request, Ct);
            if (rows.Count >= count || DateTime.UtcNow > deadline)
            {
                return rows;
            }

            await Task.Delay(50, Ct);
        }
    }

    private Task<string?> RowStateAsync(string schema, long id) =>
        postgres.ScalarAsync<string>($"SELECT retry_count || ':' || coalesce(last_error, '<null>') FROM {schema}.outbox WHERE id = {id}");

    private Task<long> InsertAsync(
        string schema, string key, string headers, int retryCount = 0, string nextAttemptAt = "now() - interval '1 second'", string route = "NULL") =>
        postgres.ScalarAsync<long>(
            $"""
            INSERT INTO {schema}.outbox (message_key, payload, headers, retry_count, next_attempt_at, route)
            VALUES ({key}, '\x7061796c6f6164'::bytea, {headers}, {retryCount}, {nextAttemptAt}, {route})
            RETURNING id
            """);

    private static async Task<IValidatedOutboxStore> ValidatedStoreAsync(PostgreSqlContainer container)
    {
        await container.StartBoundedAsync(Ct);
        var schema = PostgreSqlFixture.NewSchema();
        await PostgreSqlFixture.CreateOutboxSchemaAsync(container.GetConnectionString(), schema);
        var store = PostgreSqlOutboxStoreTests.Provider(("Db", container.GetConnectionString()))
            .CreateStore(PostgreSqlOutboxStoreTests.Outbox("Orders", "Db", $"{schema}.outbox"));
        return await store.ValidateAsync(Ct);
    }

    private async Task<IValidatedOutboxStore> ValidatedStoreAsync(string? schema = null, OrderingMode mode = OrderingMode.None)
    {
        schema ??= PostgreSqlFixture.NewSchema();
        await postgres.CreateOutboxSchemaAsync(schema);
        var store = PostgreSqlOutboxStoreTests.Provider(("Db", postgres.ConnectionString))
            .CreateStore(PostgreSqlOutboxStoreTests.Outbox("Orders", "Db", $"{schema}.outbox", mode));
        return await store.ValidateAsync(Ct);
    }

    private static async Task<string?> ShowAsync(NpgsqlConnection connection, string setting)
    {
        await using var command = new NpgsqlCommand($"SHOW {setting}", connection);
        return (string?)await command.ExecuteScalarAsync(Ct);
    }
}
