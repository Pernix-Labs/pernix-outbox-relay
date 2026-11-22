using System.Collections.Concurrent;
using System.Diagnostics;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Dapper;
using Npgsql;

namespace Pernix.OutboxRelay.Outbox.PostgreSql;

public sealed class PostgreSqlOutboxStore : IOutboxStore
{
    private static readonly string[] RequiredColumns = OutboxColumnCheck.Names;

    private const string ValidateSql = """
        SELECT pg_catalog.pg_is_in_recovery() AS "InRecovery",
               pg_catalog.current_database() AS "Database",
               pg_catalog.host(pg_catalog.inet_server_addr()) AS "ServerAddress",
               pg_catalog.inet_server_port() AS "ServerPort",
               t."QualifiedTable", t."RelKind", t."Columns", t."MissingPrivileges",
               t."RelayColumns", t."RelayTypeOids", t."RelayTypmods", t."RelayTypes", t."RelayNotNull",
               pg_catalog.hashtext(t."QualifiedTable") AS "LockKey"
        FROM (SELECT 1) AS one
        LEFT JOIN LATERAL (
            SELECT pg_catalog.format('%I.%I', n.nspname, c.relname) AS "QualifiedTable",
                   c.relkind::text AS "RelKind",
                   array(SELECT a.attname::text FROM pg_catalog.pg_attribute a
                         WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped) AS "Columns",
                   array(SELECT p.privilege FROM unnest(ARRAY['SELECT', 'UPDATE', 'DELETE']) WITH ORDINALITY AS p(privilege, ord)
                         WHERE NOT pg_catalog.has_table_privilege(c.oid, p.privilege) ORDER BY p.ord) AS "MissingPrivileges",
                   r."RelayColumns", r."RelayTypeOids", r."RelayTypmods", r."RelayTypes", r."RelayNotNull"
            FROM pg_catalog.pg_class c
            JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
            CROSS JOIN LATERAL (
                SELECT pg_catalog.array_agg(a.attname::pg_catalog.text ORDER BY a.attnum) AS "RelayColumns",
                       pg_catalog.array_agg(a.atttypid::pg_catalog.int8 ORDER BY a.attnum) AS "RelayTypeOids",
                       pg_catalog.array_agg(a.atttypmod ORDER BY a.attnum) AS "RelayTypmods",
                       pg_catalog.array_agg(pg_catalog.format_type(a.atttypid, a.atttypmod) ORDER BY a.attnum) AS "RelayTypes",
                       pg_catalog.array_agg((a.attnotnull AND NOT EXISTS (
                                                 SELECT 1 FROM pg_catalog.pg_constraint k
                                                 WHERE k.conrelid = c.oid AND k.contype = 'n' AND NOT k.convalidated
                                                   AND a.attnum = ANY(k.conkey)))
                                            OR ty.typnotnull ORDER BY a.attnum) AS "RelayNotNull"
                FROM pg_catalog.pg_attribute a
                JOIN pg_catalog.pg_type ty ON ty.oid = a.atttypid
                WHERE a.attrelid = c.oid AND a.attnum > 0 AND NOT a.attisdropped
                  AND a.attname::pg_catalog.text = ANY(@columns)) AS r
            WHERE c.oid = pg_catalog.to_regclass(@table)) AS t ON true;
        """;

    private readonly string outboxName;
    private readonly string configuredTable;
    private readonly OrderingMode mode;
    private readonly string sessionConnectionString;
    private readonly string validationConnectionString;
    private readonly string statsConnectionString;
    private readonly ConcurrentDictionary<TableIdentity, string> tableOwners;
    private readonly ConcurrentDictionary<LockKeyIdentity, string> lockKeyOwners;

    internal PostgreSqlOutboxStore(
        string outboxName,
        string configuredTable,
        OrderingMode mode,
        string sessionConnectionString,
        string validationConnectionString,
        string statsConnectionString,
        ConcurrentDictionary<TableIdentity, string> tableOwners,
        ConcurrentDictionary<LockKeyIdentity, string> lockKeyOwners)
    {
        this.outboxName = outboxName;
        this.configuredTable = configuredTable;
        this.mode = mode;
        this.sessionConnectionString = sessionConnectionString;
        this.validationConnectionString = validationConnectionString;
        this.statsConnectionString = statsConnectionString;
        this.tableOwners = tableOwners;
        this.lockKeyOwners = lockKeyOwners;
    }

    // Test hook: qualified catalog calls cannot be shadowed, so tests stand in for a standby here.
    internal Func<bool, bool> InRecovery { get; set; } = static inRecovery => inRecovery;

    public async Task<IValidatedOutboxStore> ValidateAsync(CancellationToken ct)
    {
        var row = await QueryValidationRowAsync(ct).ConfigureAwait(false);

        if (InRecovery(row.InRecovery))
        {
            throw new OutboxValidationException($"Outboxes[{outboxName}]: database is a read-only standby");
        }

        if (row.QualifiedTable is null || row.LockKey is null)
        {
            throw Invalid("not found");
        }

        if (row.RelKind is not ("r" or "p"))
        {
            throw Invalid("not a table");
        }

        var present = new HashSet<string>(row.Columns ?? [], StringComparer.Ordinal);
        var missing = RequiredColumns.Where(c => !present.Contains(c)).ToArray();
        if (missing.Length > 0)
        {
            throw Invalid($"missing columns: {string.Join(", ", missing)}");
        }

        if (OutboxColumnCheck.Problem(row.RelayColumns ?? [], row.RelayTypeOids ?? [], row.RelayTypmods ?? [], row.RelayTypes ?? [], row.RelayNotNull ?? []) is { } problem)
        {
            throw Invalid(problem);
        }

        if (row.MissingPrivileges is { Length: > 0 } privileges)
        {
            throw Invalid($"role lacks privileges: {string.Join(", ", privileges)}");
        }

        var database = DatabaseIdentity.Of(row.ServerAddress, row.ServerPort, row.Database, validationConnectionString);
        var tableOwner = tableOwners.GetOrAdd(new TableIdentity(database, row.QualifiedTable), outboxName);
        if (!string.Equals(tableOwner, outboxName, StringComparison.Ordinal))
        {
            throw Invalid($"same table as outbox '{tableOwner}'");
        }

        var lockKeyOwner = lockKeyOwners.GetOrAdd(new LockKeyIdentity(database, row.LockKey.Value), outboxName);
        if (!string.Equals(lockKeyOwner, outboxName, StringComparison.Ordinal))
        {
            throw Invalid($"shares its advisory lock key with outbox '{lockKeyOwner}'");
        }

        // The hook is read at each open, so a test can stand in for a standby after validation.
        return new PostgreSqlValidatedOutboxStore(
            outboxName,
            row.QualifiedTable,
            mode,
            sessionConnectionString,
            statsConnectionString,
            partitioned: row.RelKind == "p",
            inRecovery => InRecovery(inRecovery));
    }

    private async Task<ValidationRow> QueryValidationRowAsync(CancellationToken ct)
    {
        var (connection, row) = await OpenAndRunAsync(
            validationConnectionString,
            c => c.QuerySingleAsync<ValidationRow>(new CommandDefinition(
                ValidateSql, new { table = configuredTable, columns = RequiredColumns }, commandTimeout: PostgreSqlSessionSettings.DeadlineSeconds)),
            ValidationFailure,
            ct).ConfigureAwait(false);
        await connection.DisposeAsync().ConfigureAwait(false);
        return row;
    }

    internal static async Task<(NpgsqlConnection Connection, T Result)> OpenAndRunAsync<T>(
        string connectionString,
        Func<NpgsqlConnection, Task<T>> command,
        Func<Exception, Task, Exception> failure,
        CancellationToken ct)
    {
        // Open and command share one wall-clock deadline.
        var started = Stopwatch.GetTimestamp();
        var connection = new NpgsqlConnection(connectionString);
        Task inFlight = connection.OpenAsync(CancellationToken.None);
        try
        {
            await inFlight.WaitAsync(PostgreSqlSessionSettings.Deadline, ct).ConfigureAwait(false);
            var running = command(connection);
            inFlight = running;
            var remaining = PostgreSqlSessionSettings.Deadline - Stopwatch.GetElapsedTime(started);
            return (connection, await PostgreSqlCommands.WithDeadline(running, remaining, ct).ConfigureAwait(false));
        }
        catch (Exception ex)
        {
            await PostgreSqlCommands.DisposeAsync(connection, PostgreSqlCommands.Abandon(inFlight)).ConfigureAwait(false);

            if (ct.IsCancellationRequested)
            {
                throw new OperationCanceledException(ct);
            }

            throw failure(ex, inFlight);
        }
    }

    // A server error such as a rejected password or a table name PostgreSQL refuses is a setting to fix, not an outage.
    private Exception ValidationFailure(Exception exception, Task inFlight) =>
        PostgreSqlErrors.IsServerRefusal(exception)
            ? Invalid($"could not be validated ({PostgreSqlErrors.Describe(exception)})")
            : Unreachable(Reason(exception, inFlight));

    internal static string Reason(Exception exception, Task inFlight) =>
        PostgreSqlCommands.IsDeadline(exception, inFlight)
            ? PostgreSqlCommands.DeadlineReason
            : PostgreSqlErrors.Describe(exception);

    private OutboxDatabaseUnreachableException Unreachable(string reason) =>
        new($"Outboxes[{outboxName}]: database unreachable ({reason})");

    private OutboxValidationException Invalid(string problem) =>
        new($"Outboxes[{outboxName}]: table '{configuredTable}' {problem}");

    private sealed class ValidationRow
    {
        public bool InRecovery { get; set; }
        public string Database { get; set; } = string.Empty;
        public string? ServerAddress { get; set; }
        public int? ServerPort { get; set; }
        public string? QualifiedTable { get; set; }
        public string? RelKind { get; set; }
        public string[]? Columns { get; set; }
        public string[]? MissingPrivileges { get; set; }
        public string[]? RelayColumns { get; set; }
        public long[]? RelayTypeOids { get; set; }
        public int[]? RelayTypmods { get; set; }
        public string[]? RelayTypes { get; set; }
        public bool[]? RelayNotNull { get; set; }
        public int? LockKey { get; set; }
    }
}
