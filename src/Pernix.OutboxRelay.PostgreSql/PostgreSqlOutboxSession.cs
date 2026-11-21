using System.Data;
using System.Globalization;
using System.Text;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Npgsql;

namespace Pernix.OutboxRelay.PostgreSql;

public sealed class PostgreSqlOutboxSession : IOutboxSession
{
    private const string TryLockSql = "SELECT pg_catalog.pg_try_advisory_lock(pg_catalog.hashtext('outbox-relay'), pg_catalog.hashtext(@table))";

    // Proves this backend still holds the lock, so a lock stranded on a pooled backend reads as lost.
    private const string CheckSql = """
        SELECT EXISTS (
            SELECT 1 FROM pg_catalog.pg_locks
            WHERE locktype = 'advisory' AND pid = pg_catalog.pg_backend_pid() AND granted
              AND classid = pg_catalog.hashtext('outbox-relay')::oid AND objid = pg_catalog.hashtext($1)::oid AND objsubid = 2)
        """;

    private const string UnlockSql = "SELECT pg_catalog.pg_advisory_unlock_all()";

    private const string NoHeadersSql = "pg_catalog.convert_to('{}', 'UTF8')";

    internal static readonly string[] NoneNames = ["max", "exclude"];
    internal static readonly string[] HeadNames = ["max"];
    internal static readonly string[] CursorNames = ["max", "after_tx", "after_id"];

    private readonly string outboxName;
    private readonly string qualifiedTable;
    private readonly OrderingMode mode;
    private readonly string readSql;
    private readonly string? cursorReadSql;
    private readonly string summarySql;
    private readonly string deleteSql;
    private readonly string failSql;
    private readonly string blockSql;
    private readonly NpgsqlConnection connection;
    private readonly CancellationTokenSource lost = new();
    private readonly CancellationToken lostToken;
    private readonly List<long> exclude = [];
    private readonly NpgsqlParameter<string> checkTable;
    private readonly OutboxRowReader rowReader = new();
    private readonly List<OutboxMessage> rows = [];
    private readonly NpgsqlParameter<int> readMax = new();
    private readonly NpgsqlParameter<int> cursorMax = new();
    private readonly NpgsqlParameter<List<long>> readExclude;
    private readonly NpgsqlParameter<string> afterTx = new();
    private readonly NpgsqlParameter<long> afterId = new();
    private readonly NpgsqlParameter<long[]> deleteIds = new();
    private readonly NpgsqlParameter<long[]> failIds = new();
    private readonly NpgsqlParameter<long[]> blockIds = new();
    private readonly NpgsqlParameter<int[]> failReadRetryCounts = new();
    private readonly NpgsqlParameter<int[]> failRetryCounts = new();
    private readonly NpgsqlParameter<int[]> blockReadRetryCounts = new();
    private readonly NpgsqlParameter<int[]> blockRetryCounts = new();
    private readonly NpgsqlParameter<TimeSpan[]> failDelays = new();
    private readonly NpgsqlParameter<string[]> failErrors = new();
    private readonly NpgsqlParameter<string[]> blockErrors = new();
    private NpgsqlCommand? checkCommand;
    private NpgsqlCommand? readCommand;
    private NpgsqlCommand? cursorCommand;
    private NpgsqlCommand? deleteCommand;
    private NpgsqlCommand? failCommand;
    private NpgsqlCommand? blockCommand;
    private NpgsqlCommand? summaryCommand;
    private bool readVerified;
    private bool cursorVerified;
    private Task? abandoned;
    private Task? inFlight;
    private int disposed;
    private volatile bool releasePending;

    internal PostgreSqlOutboxSession(string outboxName, string qualifiedTable, OrderingMode mode, NpgsqlConnection connection)
    {
        this.outboxName = outboxName;
        this.qualifiedTable = qualifiedTable;
        this.mode = mode;
        this.connection = connection;
        checkTable = new NpgsqlParameter<string> { TypedValue = qualifiedTable };
        readExclude = new NpgsqlParameter<List<long>> { TypedValue = exclude };

        // The table is format('%I.%I') output from validation, so it is safe to splice.
        switch (mode)
        {
            case OrderingMode.None:
                readSql = NoneReadSql(qualifiedTable);
                break;
            case OrderingMode.PerKey:
                readSql = PerKeyReadSql(qualifiedTable, cursor: false);
                cursorReadSql = PerKeyReadSql(qualifiedTable, cursor: true);
                break;
            case OrderingMode.Strict:
                readSql = StrictReadSql(qualifiedTable, cursor: false);
                cursorReadSql = StrictReadSql(qualifiedTable, cursor: true);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unknown Ordering.Mode.");
        }

        summarySql = SummarySql(qualifiedTable);
        deleteSql = $"DELETE FROM {qualifiedTable} WHERE id = ANY($1)";

        // Set-based and guarded by the read retry_count, so an operator reset or delete wins. now() is the
        // transaction start on the database clock.
        failSql = $"""
            UPDATE {qualifiedTable} AS o
            SET retry_count = f.retry_count,
                next_attempt_at = pg_catalog.now() + f.delay,
                last_error = f.error
            FROM unnest($1, $2, $3, $4, $5)
                 AS f(id, read_retry_count, retry_count, delay, error)
            WHERE o.id = f.id AND o.retry_count = f.read_retry_count
            """;

        blockSql = $"""
            UPDATE {qualifiedTable} AS o
            SET retry_count = b.retry_count, next_attempt_at = 'infinity', last_error = b.error
            FROM unnest($1, $2, $3, $4)
                 AS b(id, read_retry_count, retry_count, error)
            WHERE o.id = b.id AND o.retry_count = b.read_retry_count
            """;
        lostToken = lost.Token;
        connection.StateChange += OnStateChange;
    }

    public CancellationToken Lost => lostToken;

    public bool ReleasePending => releasePending;

    // Test hook: the session's own physical connection (SHOW, pid).
    internal NpgsqlConnection Connection => connection;

    public Task<bool> TryLockAsync(CancellationToken ct) =>
        RunAsync(static (c, table) => ExecuteLockCommandAsync(c, TryLockSql, table), qualifiedTable, ct);

    public async Task CheckAsync(CancellationToken ct)
    {
        if (!await RunAsync(static (_, s) => s.CheckLockAsync(), this, ct).ConfigureAwait(false))
        {
            SignalLost();
            throw SessionLost("lock not held");
        }
    }

    public Task<IReadOnlyList<OutboxMessage>> ReadAsync(ReadRequest request, CancellationToken ct)
    {
        if (request is null)
        {
            return Task.FromException<IReadOnlyList<OutboxMessage>>(new ArgumentNullException(nameof(request)));
        }

        if (mode == OrderingMode.None)
        {
            if (request.After is not null)
            {
                return Task.FromException<IReadOnlyList<OutboxMessage>>(
                    new ArgumentException("A position cursor is not supported for Ordering.Mode None.", nameof(request)));
            }
        }
        else if (request.Exclude is { Count: > 0 })
        {
            return Task.FromException<IReadOnlyList<OutboxMessage>>(
                new ArgumentException($"An exclude list is not supported for Ordering.Mode {mode}.", nameof(request)));
        }

        // A null Exclude faults with
        // ArgumentNullException in every mode, after the base mode checks.
        if (request.Exclude is null)
        {
            return Task.FromException<IReadOnlyList<OutboxMessage>>(
                new ArgumentNullException(nameof(request), "ReadRequest.Exclude must not be null."));
        }

        return RunAsync(static (_, s) => s.Session.ReadRowsAsync(s.Request), (Session: this, Request: request), ct);
    }

    public Task<FailedRowsSummary> ReadFailedSummaryAsync(CancellationToken ct) => RunAsync(static (_, s) => s.SummaryAsync(), this, ct);

    public Task CommitAsync(IReadOnlyList<Outcome> outcomes, CancellationToken ct)
    {
        if (outcomes is null)
        {
            return Task.FromException(new ArgumentNullException(nameof(outcomes)));
        }

        int removes = 0, fails = 0, blocks = 0;
        for (var i = 0; i < outcomes.Count; i++)
        {
            switch (outcomes[i])
            {
                case Outcome.Remove:
                    removes++;
                    break;
                case Outcome.Fail:
                    fails++;
                    break;
                case Outcome.Block:
                    blocks++;
                    break;
                case var outcome:
                    return Task.FromException(
                        new ArgumentException($"Outcome type '{outcome?.GetType().Name ?? "null"}' is not supported.", nameof(outcomes)));
            }
        }

        if (fails == 0 && blocks == 0)
        {
            if (removes == 0)
            {
                return Task.CompletedTask;
            }

            // Per call, not reused: an abandoned commit can still read its ids after the next commit starts.
            var ids = RemoveIds(outcomes, removes);
            return RunAsync(static (_, s) => s.Session.DeleteAsync(s.Ids), (Session: this, Ids: ids), ct);
        }

        var batch = new FailureBatch(outcomes, removes, fails, blocks);

        // One transaction under the one deadline. A lost connection rolls back on the server.
        return RunAsync(static (_, s) => s.Session.CommitTransactionAsync(s.Batch), (Session: this, Batch: batch), ct);
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref disposed, 1) != 0)
        {
            return;
        }

        connection.StateChange -= OnStateChange;
        try
        {
            // An abandoned or still-running command owns the connection, so the close waits for Npgsql to let go.
            var owner = abandoned is { IsCompleted: false } ? abandoned : Volatile.Read(ref inFlight);
            var unlocked = false;
            if (owner is not { IsCompleted: false } && connection.State == ConnectionState.Open)
            {
                (owner, unlocked) = await UnlockAsync().ConfigureAwait(false);
            }

            releasePending = await PostgreSqlCommands.DisposeAsync(connection, owner).ConfigureAwait(false) || !unlocked;

            // Never dispose a cached command mid-execution.
            if (owner is { IsCompleted: false })
            {
                _ = PostgreSqlCommands.Abandon(owner.ContinueWith(
                    static (_, s) => ((PostgreSqlOutboxSession)s!).DisposeCommands(),
                    this,
                    CancellationToken.None,
                    TaskContinuationOptions.None,
                    TaskScheduler.Default));
            }
            else
            {
                DisposeCommands();
            }
        }
        catch (Exception)
        {
            // A failed close leaves the backend's fate unknown, so the lock may still be held.
            releasePending = true;
        }
        finally
        {
            lost.Dispose();
        }
    }

    // A closed backend drops its locks only on exit, after the close returns, so released must mean unlocked first. Any failure
    // (server error such as a cancel or statement timeout, deadline, I/O failure, FATAL) means the unlock did not run and the lock may stay
    // held until the backend exits. A still-running unlock owns the connection, so the close waits for it.
    private async Task<(Task? Owner, bool Unlocked)> UnlockAsync()
    {
        var unlock = ExecuteLockCommandAsync(connection, UnlockSql, table: null);
        try
        {
            await PostgreSqlCommands.WithDeadline(unlock, CancellationToken.None).ConfigureAwait(false);
            return (null, true);
        }
        catch (Exception)
        {
            return (PostgreSqlCommands.Abandon(unlock), false);
        }
    }

    private void DisposeCommands()
    {
        checkCommand?.Dispose();
        readCommand?.Dispose();
        cursorCommand?.Dispose();
        deleteCommand?.Dispose();
        failCommand?.Dispose();
        blockCommand?.Dispose();
        summaryCommand?.Dispose();
    }

    private async Task<T> RunAsync<TState, T>(Func<NpgsqlConnection, TState, Task<T>> command, TState state, CancellationToken ct)
    {
        // A closed session connection must never be reopened: a fresh backend would take the lock and drop it on close.
        if (connection.State != ConnectionState.Open)
        {
            SignalLost();
            throw SessionLost("connection not open");
        }

        // An earlier execution may still use a cached command or scratch. inFlight and abandoned stay, so dispose waits for it.
        if (Volatile.Read(ref inFlight) is { IsCompleted: false })
        {
            SignalLost();
            throw SessionLost(nameof(NpgsqlOperationInProgressException));
        }

        // No await between the guards and the command: NpgsqlCommand refuses a closed connection and never reopens it.
        var running = command(connection, state);
        Volatile.Write(ref inFlight, running);
        try
        {
            return await PostgreSqlCommands.WithDeadline(running, ct).ConfigureAwait(false);
        }
        catch (Exception) when (ct.IsCancellationRequested)
        {
            // Caller cancellation (shutdown) surfaces as cancellation, whatever Npgsql threw. No inner exception.
            abandoned = PostgreSqlCommands.Abandon(running);
            throw new OperationCanceledException(ct);
        }
        catch (Exception ex)
        {
            var deadline = PostgreSqlCommands.IsDeadline(ex, running);
            abandoned = PostgreSqlCommands.Abandon(running);
            SignalLost();
            throw SessionLost(deadline ? PostgreSqlCommands.DeadlineReason : PostgreSqlErrors.Describe(ex));
        }
    }

    private static long[] RemoveIds(IReadOnlyList<Outcome> outcomes, int count)
    {
        var ids = new long[count];
        var next = 0;
        for (var i = 0; i < outcomes.Count; i++)
        {
            if (outcomes[i] is Outcome.Remove remove)
            {
                ids[next++] = remove.Id;
            }
        }

        return ids;
    }

    private async Task<int> DeleteAsync(long[] ids)
    {
        deleteIds.TypedValue = ids;
        var command = deleteCommand ??= Create(deleteSql, deleteIds);
        await PrepareOnceAsync(command).ConfigureAwait(false);
        return await command.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
    }

    // Cached commands never set Transaction. After BEGIN every statement on the connection runs in the transaction.
    private async Task<int> CommitTransactionAsync(FailureBatch batch)
    {
        deleteIds.TypedValue = batch.RemoveIds;
        failIds.TypedValue = batch.FailIds;
        failReadRetryCounts.TypedValue = batch.FailReadRetryCounts;
        failRetryCounts.TypedValue = batch.FailRetryCounts;
        failDelays.TypedValue = batch.FailDelays;
        failErrors.TypedValue = batch.FailErrors;
        blockIds.TypedValue = batch.BlockIds;
        blockReadRetryCounts.TypedValue = batch.BlockReadRetryCounts;
        blockRetryCounts.TypedValue = batch.BlockRetryCounts;
        blockErrors.TypedValue = batch.BlockErrors;

        await using var transaction = await connection.BeginTransactionAsync(CancellationToken.None).ConfigureAwait(false);
        if (batch.RemoveIds.Length > 0)
        {
            var delete = deleteCommand ??= Create(deleteSql, deleteIds);
            await PrepareOnceAsync(delete).ConfigureAwait(false);
            await delete.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
        }

        // Fewer rows than outcomes means an operator acted on purpose. Not an error.
        var updated = 0;
        if (batch.FailIds.Length > 0)
        {
            var fail = failCommand ??= Create(failSql, failIds, failReadRetryCounts, failRetryCounts, failDelays, failErrors);
            await PrepareOnceAsync(fail).ConfigureAwait(false);
            updated += await fail.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
        }

        if (batch.BlockIds.Length > 0)
        {
            var block = blockCommand ??= Create(blockSql, blockIds, blockReadRetryCounts, blockRetryCounts, blockErrors);
            await PrepareOnceAsync(block).ConfigureAwait(false);
            updated += await block.ExecuteNonQueryAsync(CancellationToken.None).ConfigureAwait(false);
        }

        await transaction.CommitAsync(CancellationToken.None).ConfigureAwait(false);
        return updated;
    }

    private async Task<bool> CheckLockAsync()
    {
        var command = checkCommand ??= Create(CheckSql, checkTable);
        await PrepareOnceAsync(command).ConfigureAwait(false);
        return await command.ExecuteScalarAsync(CancellationToken.None).ConfigureAwait(false) is true;
    }

    private async Task<IReadOnlyList<OutboxMessage>> ReadRowsAsync(ReadRequest request)
    {
        NpgsqlCommand command;
        if (mode == OrderingMode.None)
        {
            exclude.Clear();
            exclude.AddRange(request.Exclude);
            readMax.TypedValue = request.Max;
            command = readCommand ??= Create(Positional(readSql, NoneNames), readMax, readExclude);
        }
        else if (request.After is { } after)
        {
            cursorMax.TypedValue = request.Max;

            // xid8 travels as text, so Npgsql never infers a type for the ulong.
            afterTx.TypedValue = after.Order.ToString(CultureInfo.InvariantCulture);
            afterId.TypedValue = after.Id;
            command = cursorCommand ??= Create(Positional(cursorReadSql!, CursorNames), cursorMax, afterTx, afterId);
        }
        else
        {
            readMax.TypedValue = request.Max;
            command = readCommand ??= Create(Positional(readSql, HeadNames), readMax);
        }

        await PrepareOnceAsync(command).ConfigureAwait(false);
        try
        {
            var reader = await command.ExecuteReaderAsync(
                CommandBehavior.SequentialAccess | CommandBehavior.SingleResult, CancellationToken.None).ConfigureAwait(false);
            await using (reader.ConfigureAwait(false))
            {
                VerifyOnce(command, reader);
                while (await reader.ReadAsync(CancellationToken.None).ConfigureAwait(false))
                {
                    rows.Add(rowReader.Read(reader));
                }
            }

            return rows.Count == 0 ? Array.Empty<OutboxMessage>() : rows.ToArray();
        }
        finally
        {
            rows.Clear();
        }
    }

    private void VerifyOnce(NpgsqlCommand command, NpgsqlDataReader reader)
    {
        if (ReferenceEquals(command, cursorCommand))
        {
            if (!cursorVerified)
            {
                OutboxRowReader.Verify(reader);
                cursorVerified = true;
            }
        }
        else if (!readVerified)
        {
            OutboxRowReader.Verify(reader);
            readVerified = true;
        }
    }

    private async Task<FailedRowsSummary> SummaryAsync()
    {
        var command = summaryCommand ??= Create(summarySql);
        await PrepareOnceAsync(command).ConfigureAwait(false);
        var reader = await command.ExecuteReaderAsync(
            CommandBehavior.SequentialAccess | CommandBehavior.SingleResult, CancellationToken.None).ConfigureAwait(false);
        await using (reader.ConfigureAwait(false))
        {
            if (!await reader.ReadAsync(CancellationToken.None).ConfigureAwait(false))
            {
                throw new InvalidOperationException("Sequence contains no elements");
            }

            return new FailedRowsSummary(reader.GetInt64(0), reader.GetDecimal(1), reader.IsDBNull(2) ? null : reader.GetFieldValue<DateTimeOffset>(2));
        }
    }

    private NpgsqlCommand Create(string sql, params ReadOnlySpan<NpgsqlParameter> parameters)
    {
        var command = new NpgsqlCommand(sql, connection) { CommandTimeout = PostgreSqlSessionSettings.DeadlineSeconds };
        foreach (var parameter in parameters)
        {
            command.Parameters.Add(parameter);
        }

        return command;
    }

    private static Task PrepareOnceAsync(NpgsqlCommand command) =>
        command.IsPrepared ? Task.CompletedTask : command.PrepareAsync(CancellationToken.None);

    // Quoted identifiers ("" toggles twice) may hold '@', so they are copied unchanged.
    internal static string Positional(string sql, string[] names)
    {
        var result = new StringBuilder(sql.Length);
        var quoted = false;
        var i = 0;
        while (i < sql.Length)
        {
            var c = sql[i];
            if (c == '"')
            {
                quoted = !quoted;
                result.Append(c);
                i++;
                continue;
            }

            if (quoted || c != '@')
            {
                result.Append(c);
                i++;
                continue;
            }

            var position = PositionAt(sql, i + 1, names);
            if (position > 0)
            {
                result.Append('$').Append(position.ToString(CultureInfo.InvariantCulture));
                i += 1 + names[position - 1].Length;
                continue;
            }

            var end = i + 1;
            while (end < sql.Length && (char.IsAsciiLetterOrDigit(sql[end]) || sql[end] == '_'))
            {
                end++;
            }

            if (end > i + 1)
            {
                throw new InvalidOperationException($"SQL parameter '{sql[i..end]}' has no position.");
            }

            result.Append(c);
            i++;
        }

        return result.ToString();
    }

    private static int PositionAt(string sql, int start, string[] names)
    {
        for (var n = 0; n < names.Length; n++)
        {
            if (start + names[n].Length <= sql.Length && string.CompareOrdinal(sql, start, names[n], 0, names[n].Length) == 0)
            {
                return n + 1;
            }
        }

        return 0;
    }

    // NpgsqlCommand refuses a closed connection instead of opening it.
    private static async Task<bool> ExecuteLockCommandAsync(NpgsqlConnection connection, string sql, string? table)
    {
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = PostgreSqlSessionSettings.DeadlineSeconds };
        if (table is not null)
        {
            command.Parameters.AddWithValue("table", table);
        }

        return await command.ExecuteScalarAsync(CancellationToken.None).ConfigureAwait(false) is true;
    }

    // Read order per mode. The stats head probe uses the same value, so outbox.head_age measures the row read next.
    private const string IdColumn = "id";
    private const string TxColumn = "tx_id";
    private const string IdReadOrder = IdColumn;
    private const string TxReadOrder = $"{TxColumn}, {IdColumn}";

    internal static string ReadOrder(OrderingMode mode) => mode switch
    {
        OrderingMode.None => IdReadOrder,
        OrderingMode.PerKey or OrderingMode.Strict => TxReadOrder,
        _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unknown Ordering.Mode."),
    };

    internal static string NoneReadSql(string table) => $"""
        SELECT {ReadColumns(alias: "", payload: "payload", headers: "headers", nextAttemptAt: "next_attempt_at", isDue: "true", route: "route")}
        FROM {table}
        WHERE next_attempt_at <= pg_catalog.now()
          AND id <> ALL(@exclude)
        ORDER BY {IdReadOrder}
        LIMIT @max
        """;

    // Earliest future finite attempt changes the summary once. The checksum folds in the blocked and due flags,
    // so any failed row becoming due or an operator next_attempt_at edit changes it.
    internal static string SummarySql(string table) => $"""
        SELECT count(*) AS "Count",
               coalesce(sum(pg_catalog.hashint8extended(id,
                   retry_count::bigint * 4 + (next_attempt_at = 'infinity')::int * 2 + (next_attempt_at <= pg_catalog.now())::int)), 0) AS "Checksum",
               min(next_attempt_at) FILTER (WHERE next_attempt_at > pg_catalog.now() AND next_attempt_at <> 'infinity') AS "NextDueAt"
        FROM {table}
        WHERE retry_count > 0
        """;

    // One statement, so gate and rows share a snapshot. 'infinity' becomes NULL in SQL.
    // A retry_count = 0 row is due. now() precedes the statement snapshot, so a row committed in between carries a later next_attempt_at.
    internal static string StrictReadSql(string table, bool cursor) => $"""
        SELECT {ReadColumns(
            alias: "",
            payload: "payload",
            headers: "headers",
            nextAttemptAt: "CASE WHEN next_attempt_at = 'infinity' THEN NULL ELSE next_attempt_at END",
            isDue: "(retry_count = 0 OR next_attempt_at <= pg_catalog.now())",
            route: "route")}
        FROM {table}
        WHERE tx_id < pg_catalog.pg_snapshot_xmin(pg_catalog.pg_current_snapshot())
        {(cursor ? "  AND (tx_id, id) > (CAST(@after_tx AS text)::xid8, @after_id)" : string.Empty)}
        ORDER BY {TxReadOrder}
        LIMIT @max
        """;

    // Inner LIMIT bounds the scan to @max entries, all returned. OFFSET 0 keeps one NOT EXISTS probe per row. NULL keys hold nothing.
    // A retry_count = 0 row is due. now() precedes the statement snapshot, so a row committed in between carries a later next_attempt_at.
    internal static string PerKeyReadSql(string table, bool cursor) => $"""
        SELECT {ReadColumns(
            alias: "s.",
            payload: "CASE WHEN s.is_due THEN s.payload END",
            headers: "CASE WHEN s.is_due THEN s.headers END",
            nextAttemptAt: "CASE WHEN s.next_attempt_at = 'infinity' THEN NULL ELSE s.next_attempt_at END",
            isDue: "s.is_due",
            route: "CASE WHEN s.is_due THEN s.route END")}
        FROM (SELECT o.*,
                     (o.retry_count = 0 OR o.next_attempt_at <= pg_catalog.now())
                     AND NOT EXISTS (SELECT 1 FROM {table} b
                                     WHERE b.retry_count > 0 AND b.message_key = o.message_key
                                       AND b.next_attempt_at > pg_catalog.now()
                                     OFFSET 0) AS is_due
              FROM (SELECT id, tx_id, message_key, payload, headers, created_at, retry_count, next_attempt_at, last_error, route
                    FROM {table}
                    WHERE tx_id < pg_catalog.pg_snapshot_xmin(pg_catalog.pg_current_snapshot())
                    {(cursor ? "  AND (tx_id, id) > (CAST(@after_tx AS text)::xid8, @after_id)" : string.Empty)}
                    ORDER BY {TxReadOrder}
                    LIMIT @max) o
              OFFSET 0) s
        ORDER BY {TxReadOrder}
        """;

    // Every read's columns, named and ordered as the OutboxMessage row constructor parameters, which `OutboxRowReader` reads by ordinal, checked by `Verify`.
    private static string ReadColumns(string alias, string payload, string headers, string nextAttemptAt, string isDue, string route) => $"""
        {alias}id AS "Id", {alias}tx_id AS "Order", {alias}message_key AS "Key", {payload} AS "Payload", {HeadersSql(headers, isDue)} AS "Headers",
        {alias}created_at AS "CreatedAt", {alias}retry_count AS "RetryCount", {nextAttemptAt} AS "NextAttemptAt", {isDue} AS "IsDue",
        {alias}last_error AS "LastError", {route} AS "Route"
        """;

    // NULL headers on a not-due row read as no headers, so only a due NULL or an unparsable value is malformed.
    private static string HeadersSql(string headers, string isDue) =>
        $"CASE WHEN ({headers}) IS NULL AND ({isDue}) IS NOT TRUE THEN {NoHeadersSql} ELSE pg_catalog.convert_to(({headers})::text, 'UTF8') END";

    private OutboxSessionLostException SessionLost(string reason) => new($"Outboxes[{outboxName}]: session lost ({reason})");

    private void OnStateChange(object sender, StateChangeEventArgs e)
    {
        if (e.CurrentState is ConnectionState.Closed or ConnectionState.Broken)
        {
            SignalLost();
        }
    }

    private void SignalLost()
    {
        if (Volatile.Read(ref disposed) != 0)
        {
            return;
        }

        // CancelAsync runs callbacks on the thread pool, so worker teardown never inlines inside Npgsql's break path.
        try
        {
            _ = lost.CancelAsync();
        }
        catch (ObjectDisposedException)
        {
        }
    }

    // Per call, never reused: an abandoned commit can still read its arrays after the next commit starts.
    private sealed class FailureBatch
    {
        public FailureBatch(IReadOnlyList<Outcome> outcomes, int removes, int fails, int blocks)
        {
            RemoveIds = removes == 0 ? [] : new long[removes];
            FailIds = fails == 0 ? [] : new long[fails];
            FailReadRetryCounts = fails == 0 ? [] : new int[fails];
            FailRetryCounts = fails == 0 ? [] : new int[fails];
            FailDelays = fails == 0 ? [] : new TimeSpan[fails];
            FailErrors = fails == 0 ? [] : new string[fails];
            BlockIds = blocks == 0 ? [] : new long[blocks];
            BlockReadRetryCounts = blocks == 0 ? [] : new int[blocks];
            BlockRetryCounts = blocks == 0 ? [] : new int[blocks];
            BlockErrors = blocks == 0 ? [] : new string[blocks];
            int r = 0, f = 0, b = 0;
            for (var i = 0; i < outcomes.Count; i++)
            {
                switch (outcomes[i])
                {
                    case Outcome.Remove remove:
                        RemoveIds[r++] = remove.Id;
                        break;
                    case Outcome.Fail fail:
                        FailIds[f] = fail.Id;
                        FailReadRetryCounts[f] = fail.ReadRetryCount;
                        FailRetryCounts[f] = fail.RetryCount;
                        FailDelays[f] = fail.Delay;
                        FailErrors[f++] = fail.Error;
                        break;
                    case Outcome.Block block:
                        BlockIds[b] = block.Id;
                        BlockReadRetryCounts[b] = block.ReadRetryCount;
                        BlockRetryCounts[b] = block.RetryCount;
                        BlockErrors[b++] = block.Error;
                        break;
                }
            }
        }

        public long[] RemoveIds { get; }

        public long[] FailIds { get; }

        public int[] FailReadRetryCounts { get; }

        public int[] FailRetryCounts { get; }

        public TimeSpan[] FailDelays { get; }

        public string[] FailErrors { get; }

        public long[] BlockIds { get; }

        public int[] BlockReadRetryCounts { get; }

        public int[] BlockRetryCounts { get; }

        public string[] BlockErrors { get; }
    }
}
