using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Dapper;
using Npgsql;

namespace Pernix.OutboxRelay.PostgreSql;

public sealed class PostgreSqlValidatedOutboxStore : IValidatedOutboxStore
{
    private static readonly string OpenSql = PostgreSqlSessionSettings.SetSql + " SELECT pg_catalog.pg_is_in_recovery();";

    private const string GateLagGrantReason = "role lacks pg_monitor (pg_read_all_stats)";

    private readonly string outboxName;
    private readonly string qualifiedTable;
    private readonly OrderingMode mode;
    private readonly string sessionConnectionString;
    private readonly string statsConnectionString;
    private readonly Func<bool, bool> inRecovery;
    private readonly CommandDefinition statsCommand;

    internal PostgreSqlValidatedOutboxStore(
        string outboxName,
        string qualifiedTable,
        OrderingMode mode,
        string sessionConnectionString,
        string statsConnectionString,
        bool partitioned,
        Func<bool, bool> inRecovery)
    {
        this.outboxName = outboxName;
        this.qualifiedTable = qualifiedTable;
        this.mode = mode;
        this.sessionConnectionString = sessionConnectionString;
        this.statsConnectionString = statsConnectionString;
        this.inRecovery = inRecovery;
        StatsSql = BuildStatsSql(qualifiedTable, mode, partitioned);
        statsCommand = new CommandDefinition(
            StatsSql, commandTimeout: PostgreSqlSessionSettings.DeadlineSeconds, cancellationToken: CancellationToken.None);
    }

    internal string StatsSql { get; }

    public async Task<IOutboxSession> OpenSessionAsync(CancellationToken ct)
    {
        var (connection, recovering) = await PostgreSqlOutboxStore.OpenAndRunAsync(
            sessionConnectionString,
            c => c.ExecuteScalarAsync<bool>(new CommandDefinition(OpenSql, commandTimeout: PostgreSqlSessionSettings.DeadlineSeconds)),
            (ex, inFlight) => Unreachable(PostgreSqlOutboxStore.Reason(ex, inFlight)),
            ct).ConfigureAwait(false);

        if (inRecovery(recovering))
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw Unreachable("read-only standby");
        }

        return new PostgreSqlOutboxSession(outboxName, qualifiedTable, mode, connection);
    }

    public async Task<OutboxStats> GetStatsAsync(CancellationToken ct)
    {
        var (connection, row) = await PostgreSqlOutboxStore.OpenAndRunAsync(
            statsConnectionString,
            c => c.QuerySingleAsync<StatsRow>(statsCommand),
            (ex, inFlight) =>
            {
                ClearStatsPool();
                return StatsUnavailable(PostgreSqlOutboxStore.Reason(ex, inFlight));
            },
            ct).ConfigureAwait(false);

        if (inRecovery(row.InRecovery))
        {
            NpgsqlConnection.ClearPool(connection);
            await connection.DisposeAsync().ConfigureAwait(false);
            throw StatsUnavailable("read-only standby");
        }

        await connection.DisposeAsync().ConfigureAwait(false);   // back to the stats pool

        return new OutboxStats(
            row.Pending,
            row.Retrying,
            row.Blocked,
            TimeSpan.FromSeconds(row.HeadAgeSeconds),
            row.GateLagVisible ? GateLag.Of(TimeSpan.FromSeconds(row.GateLagSeconds)) : GateLag.Unavailable(GateLagGrantReason));
    }

    internal static string BuildStatsSql(string table, OrderingMode mode, bool partitioned)
    {
        var literal = table.Replace("'", "''", StringComparison.Ordinal);

        var (blocked, blockedAggregates) = mode switch
        {
            OrderingMode.None => ("0::bigint", string.Empty),
            OrderingMode.PerKey => (
                "f.blocked_keys + f.blocked_null_keys",
                """
                ,
                             count(DISTINCT message_key) FILTER (WHERE next_attempt_at = 'infinity') AS blocked_keys,
                             count(*) FILTER (WHERE next_attempt_at = 'infinity' AND message_key IS NULL) AS blocked_null_keys
                """),
            OrderingMode.Strict => (
                "f.blocked_rows",
                """
                ,
                             count(*) FILTER (WHERE next_attempt_at = 'infinity') AS blocked_rows
                """),
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, null),
        };

        var order = PostgreSqlOutboxSession.ReadOrder(mode);

        // A partitioned parent's own live-tuple count is always 0, so pending sums the leaf partitions.
        var live = partitioned
            ? $"(SELECT COALESCE(sum(pg_catalog.pg_stat_get_live_tuples(pt.relid)), 0)::bigint FROM pg_catalog.pg_partition_tree('{literal}'::pg_catalog.regclass) AS pt WHERE pt.isleaf)"
            : $"pg_catalog.pg_stat_get_live_tuples('{literal}'::pg_catalog.regclass)";

        return $"""
            SELECT pg_catalog.pg_is_in_recovery() AS "InRecovery",
                   f.retrying AS "Retrying",
                   {blocked} AS "Blocked",
                   GREATEST({live} - f.failed, 0) AS "Pending",
                   h.head_age AS "HeadAgeSeconds",
                   g.visible AS "GateLagVisible",
                   g.lag AS "GateLagSeconds"
            FROM (SELECT count(*) AS failed,
                         count(*) FILTER (WHERE next_attempt_at <> 'infinity') AS retrying{blockedAggregates}
                  FROM {table} WHERE retry_count > 0) AS f
            CROSS JOIN LATERAL (
                  SELECT COALESCE((SELECT GREATEST(EXTRACT(EPOCH FROM pg_catalog.now() - created_at), 0)
                                   FROM {table} ORDER BY {order} LIMIT 1), 0)::float8 AS head_age) AS h
            CROSS JOIN LATERAL (
                  SELECT pg_catalog.pg_has_role('pg_read_all_stats', 'USAGE') AS visible,
                         COALESCE(GREATEST(EXTRACT(EPOCH FROM pg_catalog.now() - LEAST(
                             (SELECT min(a.xact_start) FROM pg_catalog.pg_stat_activity a
                              WHERE a.backend_xid IS NOT NULL AND a.pid <> pg_catalog.pg_backend_pid()),
                             (SELECT min(p.prepared) FROM pg_catalog.pg_prepared_xacts p))), 0), 0)::float8 AS lag) AS g;
            """;
    }

    // A failed or standby read must not leave its connection pooled. Clearing also closes one returned later.
    private void ClearStatsPool()
    {
        using var connection = new NpgsqlConnection(statsConnectionString);
        NpgsqlConnection.ClearPool(connection);
    }

    private OutboxDatabaseUnreachableException Unreachable(string reason) =>
        new($"Outboxes[{outboxName}]: database unreachable ({reason})");

    private OutboxStatsUnavailableException StatsUnavailable(string reason) =>
        new(outboxName, reason);

    private sealed class StatsRow
    {
        public bool InRecovery { get; set; }
        public long Pending { get; set; }
        public long Retrying { get; set; }
        public long Blocked { get; set; }
        public double HeadAgeSeconds { get; set; }
        public double GateLagSeconds { get; set; }
        public bool GateLagVisible { get; set; }
    }
}
