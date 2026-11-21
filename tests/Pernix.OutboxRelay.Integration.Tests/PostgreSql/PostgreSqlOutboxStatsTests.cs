using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.PostgreSql;
using Npgsql;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.PostgreSql;

public sealed class PostgreSqlOutboxStatsTests(PostgreSqlStatsFixture stats) : IClassFixture<PostgreSqlStatsFixture>
{
    // Seed: 50 fresh rows (head 120 s old), 7 retrying, 6 blocked over 3 keys, 2 blocked NULL-key rows.
    private const long Seeded = 65;
    private const long Failed = 15;
    private const long Retrying = 7;
    private const long BlockedRows = 8;
    private const long BlockedKeys = 3 + 2;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(OrderingMode.None)]
    [InlineData(OrderingMode.PerKey)]
    [InlineData(OrderingMode.Strict)]
    public async Task Seeded_rows_give_exact_values(OrderingMode mode)
    {
        var schema = await stats.CreateOutboxAsync();
        var table = $"{schema}.outbox";
        await stats.SeedAndAnalyzeAsync(SeedSql(table), table);
        var store = await ValidatedAsync(stats.ConnectionString, "Seeded", table, mode);

        var result = await store.GetStatsAsync(Ct);

        AssertSeeded(result, mode);
        Assert.NotNull(result.GateLag.Value);
        Assert.Null(result.GateLag.UnavailableReason);
    }

    [Theory]
    [InlineData(OrderingMode.None)]
    [InlineData(OrderingMode.PerKey)]
    [InlineData(OrderingMode.Strict)]
    public async Task Empty_table_gives_zero(OrderingMode mode)
    {
        var schema = await stats.CreateOutboxAsync();
        var store = await ValidatedAsync(stats.ConnectionString, "Empty", $"{schema}.outbox", mode);

        var result = await store.GetStatsAsync(Ct);

        Assert.Equal(0, result.Pending);
        Assert.Equal(0, result.Retrying);
        Assert.Equal(0, result.Blocked);
        Assert.Equal(TimeSpan.Zero, result.HeadAge);
    }

    [Theory]
    [InlineData(OrderingMode.None)]
    [InlineData(OrderingMode.PerKey)]
    [InlineData(OrderingMode.Strict)]
    public async Task Partitioned_table_sums_leaf_partitions(OrderingMode mode)
    {
        var schema = PostgreSqlFixture.NewSchema();
        var table = $"{schema}.outbox";
        await stats.ExecuteAsync(
            $"""
            CREATE SCHEMA {schema};
            CREATE TABLE {table} (
                id                    bigint       GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                tx_id                 xid8         NOT NULL DEFAULT pg_current_xact_id(),
                message_key           bytea,
                payload               bytea        NOT NULL,
                headers               jsonb        NOT NULL DEFAULT '{"{}"}'::jsonb,
                route                 text,
                created_at            timestamptz  NOT NULL DEFAULT now(),
                retry_count           integer      NOT NULL DEFAULT 0,
                next_attempt_at       timestamptz  NOT NULL DEFAULT now(),
                last_error            text,
                source_outbox         text,
                source_id             bigint,
                source_destination    text,
                dead_lettered_at      timestamptz,
                dead_letter_reason    text,
                dead_letter_attempts  integer,
                CONSTRAINT outbox_headers_is_object         CHECK (jsonb_typeof(headers) = 'object'),
                CONSTRAINT outbox_header_values_are_strings CHECK (NOT jsonb_path_exists(headers, '$.* ? (@.type() != "string")')),
                CONSTRAINT outbox_retry_count_non_negative  CHECK (retry_count >= 0),
                CONSTRAINT outbox_source_complete           CHECK ((source_outbox IS NULL) = (source_id IS NULL))
            ) PARTITION BY RANGE (id);
            CREATE INDEX outbox_tx_id_id_idx ON {table} (tx_id, id);
            CREATE INDEX outbox_failed_message_key_idx ON {table} (message_key, next_attempt_at) WHERE retry_count > 0;
            CREATE TABLE {schema}.outbox_p1 PARTITION OF {table} FOR VALUES FROM (MINVALUE) TO (1000);
            CREATE TABLE {schema}.outbox_p2 PARTITION OF {table} FOR VALUES FROM (1000) TO (2000);
            CREATE TABLE {schema}.outbox_p3 PARTITION OF {table} FOR VALUES FROM (2000) TO (MAXVALUE) PARTITION BY RANGE (id);
            CREATE TABLE {schema}.outbox_p3a PARTITION OF {schema}.outbox_p3 FOR VALUES FROM (2000) TO (3000);
            CREATE TABLE {schema}.outbox_p3b PARTITION OF {schema}.outbox_p3 FOR VALUES FROM (3000) TO (MAXVALUE);
            """);
        await stats.SeedAndAnalyzeAsync(SeedSql(table), table);
        Assert.Equal(0, await stats.ScalarAsync<long>($"SELECT pg_catalog.pg_stat_get_live_tuples('{table}'::regclass)"));
        Assert.Equal(4, await stats.ScalarAsync<long>(
            $"SELECT count(*) FROM pg_catalog.pg_partition_tree('{table}'::regclass) AS pt WHERE pt.isleaf AND pg_catalog.pg_stat_get_live_tuples(pt.relid) > 0"));
        var store = await ValidatedAsync(stats.ConnectionString, "Partitioned", table, mode);

        var result = await store.GetStatsAsync(Ct);

        AssertSeeded(result, mode);
    }

    [Fact]
    public async Task Gate_lag_reports_open_and_prepared_transactions_in_another_database()
    {
        var schema = await stats.CreateOutboxAsync();
        var store = await ValidatedAsync(stats.ConnectionString, "Gate", $"{schema}.outbox", OrderingMode.None);
        var database = $"gate_{Guid.NewGuid():N}";
        await stats.ExecuteAsync($"CREATE DATABASE {database}");
        var otherBuilder = stats.Builder();
        otherBuilder.Database = database;
        otherBuilder.Pooling = false;
        await using var holder = new NpgsqlConnection(otherBuilder.ConnectionString);
        await holder.OpenAsync(Ct);
        var gid = $"g_{Guid.NewGuid():N}";
        try
        {
            Assert.Equal(TimeSpan.Zero, (await store.GetStatsAsync(Ct)).GateLag.Value);

            await RunAsync(holder, "BEGIN");
            await RunAsync(holder, "SELECT pg_catalog.pg_current_xact_id()");
            var open = Stopwatch.StartNew();
            await Task.Delay(TimeSpan.FromSeconds(3), Ct);
            var openAge = open.Elapsed;
            var whileOpen = await store.GetStatsAsync(Ct);
            Assert.True(whileOpen.GateLag.Value >= openAge, $"gate lag {whileOpen.GateLag.Value} < {openAge}");

            await RunAsync(holder, "COMMIT");
            Assert.Equal(TimeSpan.Zero, (await store.GetStatsAsync(Ct)).GateLag.Value);

            await RunAsync(holder, "BEGIN");
            await RunAsync(holder, "SELECT pg_catalog.pg_current_xact_id()");
            await RunAsync(holder, $"PREPARE TRANSACTION '{gid}'");
            var prepared = Stopwatch.StartNew();
            await Task.Delay(TimeSpan.FromSeconds(1), Ct);
            var preparedAge = prepared.Elapsed;
            var whilePrepared = await store.GetStatsAsync(Ct);
            Assert.True(whilePrepared.GateLag.Value >= preparedAge, $"gate lag {whilePrepared.GateLag.Value} < {preparedAge}");

            await RunAsync(holder, $"ROLLBACK PREPARED '{gid}'");
            Assert.Equal(TimeSpan.Zero, (await store.GetStatsAsync(Ct)).GateLag.Value);
        }
        finally
        {
            await using var cleanup = new NpgsqlCommand(
                $"SELECT count(*) FROM pg_catalog.pg_prepared_xacts WHERE gid = '{gid}'", holder);
            if ((long)(await cleanup.ExecuteScalarAsync(CancellationToken.None))! > 0)
            {
                await RunAsync(holder, $"ROLLBACK PREPARED '{gid}'");
            }
        }
    }

    [Fact]
    public async Task Role_without_pg_monitor_has_no_gate_lag_until_granted()
    {
        var schema = await stats.CreateOutboxAsync();
        var table = $"{schema}.outbox";
        await stats.SeedAndAnalyzeAsync(SeedSql(table), table);
        var role = $"r_{Guid.NewGuid():N}";
        await stats.ExecuteAsync(
            $"CREATE ROLE {role} LOGIN PASSWORD 'relay'; GRANT USAGE ON SCHEMA {schema} TO {role}; GRANT SELECT, UPDATE, DELETE ON {table} TO {role}");
        var roleBuilder = stats.Builder();
        roleBuilder.Username = role;
        roleBuilder.Password = "relay";
        var store = await ValidatedAsync(roleBuilder.ConnectionString, "Unprivileged", table, OrderingMode.PerKey);

        var withoutGrant = await store.GetStatsAsync(Ct);

        Assert.Null(withoutGrant.GateLag.Value);
        Assert.NotNull(withoutGrant.GateLag.UnavailableReason);
        Assert.Contains("pg_monitor", withoutGrant.GateLag.UnavailableReason, StringComparison.Ordinal);
        Assert.Contains("pg_read_all_stats", withoutGrant.GateLag.UnavailableReason, StringComparison.Ordinal);
        AssertSeeded(withoutGrant, OrderingMode.PerKey);

        await stats.ExecuteAsync($"GRANT pg_monitor TO {role}");
        var withGrant = await store.GetStatsAsync(Ct);

        Assert.NotNull(withGrant.GateLag.Value);
        Assert.True(withGrant.GateLag.Value >= TimeSpan.Zero);
        Assert.Null(withGrant.GateLag.UnavailableReason);
        AssertSeeded(withGrant, OrderingMode.PerKey);
    }

    [Fact]
    public async Task Exclusive_lock_fails_with_deadline_reason_within_deadline()
    {
        var schema = await stats.CreateOutboxAsync();
        var table = $"{schema}.outbox";
        var store = await ValidatedAsync(stats.ConnectionString, "Locked", table, OrderingMode.PerKey);
        var lockerBuilder = stats.Builder();
        lockerBuilder.Pooling = false;
        await using var locker = new NpgsqlConnection(lockerBuilder.ConnectionString);
        await locker.OpenAsync(Ct);
        await RunAsync(locker, "BEGIN");
        await RunAsync(locker, $"LOCK TABLE {table} IN ACCESS EXCLUSIVE MODE");
        try
        {
            var elapsed = Stopwatch.StartNew();

            var ex = await Assert.ThrowsAsync<OutboxStatsUnavailableException>(() => store.GetStatsAsync(Ct));

            Assert.True(elapsed.Elapsed < TimeSpan.FromSeconds(6), $"took {elapsed.Elapsed}");
            Assert.Equal($"Outboxes[Locked]: stats unavailable ({PostgreSqlCommands.DeadlineReason})", ex.Message);
            Assert.Equal(PostgreSqlCommands.DeadlineReason, ex.Reason);
            AssertSanitized(ex, stats.ConnectionString);
        }
        finally
        {
            await RunAsync(locker, "ROLLBACK");
        }
    }

    [Fact]
    public async Task Standby_fails_stats()
    {
        var schema = await stats.CreateOutboxAsync();
        var store = (PostgreSqlOutboxStore)PostgreSqlOutboxStoreTests.Provider(("Db", stats.ConnectionString))
            .CreateStore(PostgreSqlOutboxStoreTests.Outbox("Standby", "Db", $"{schema}.outbox"));
        var validated = await store.ValidateAsync(Ct);

        store.InRecovery = _ => true;

        var ex = await Assert.ThrowsAsync<OutboxStatsUnavailableException>(() => validated.GetStatsAsync(Ct));
        Assert.Equal("Outboxes[Standby]: stats unavailable (read-only standby)", ex.Message);
        Assert.Equal("read-only standby", ex.Reason);
        AssertSanitized(ex, stats.ConnectionString);
    }

    [Fact]
    public async Task Standby_reading_drops_the_pooled_stats_connection()
    {
        var schema = await stats.CreateOutboxAsync();
        var name = $"StandbyPool{Guid.NewGuid():N}";
        var store = (PostgreSqlOutboxStore)PostgreSqlOutboxStoreTests.Provider(("Db", stats.ConnectionString))
            .CreateStore(PostgreSqlOutboxStoreTests.Outbox(name, "Db", $"{schema}.outbox"));
        var validated = await store.ValidateAsync(Ct);
        await validated.GetStatsAsync(Ct);
        var pooled = await StatsBackendsAsync(name);
        Assert.Single(pooled);

        store.InRecovery = _ => true;
        await Assert.ThrowsAsync<OutboxStatsUnavailableException>(() => validated.GetStatsAsync(Ct));

        var deadline = Stopwatch.StartNew();
        while ((await StatsBackendsAsync(name)).Length != 0 && deadline.Elapsed < TimeSpan.FromSeconds(5))
        {
            await Task.Delay(50, Ct);
        }

        Assert.Empty(await StatsBackendsAsync(name));

        store.InRecovery = static inRecovery => inRecovery;
        await validated.GetStatsAsync(Ct);
        var fresh = await StatsBackendsAsync(name);
        Assert.Single(fresh);
        Assert.NotEqual(pooled[0], fresh[0]);
    }

    // A server error leaves the connection usable, so only the pool clear stops the next read reusing it.
    [Fact]
    public async Task Failed_reading_drops_the_pooled_stats_connection()
    {
        var schema = await stats.CreateOutboxAsync();
        var table = $"{schema}.outbox";
        var role = $"r_{Guid.NewGuid():N}";
        await stats.ExecuteAsync(
            $"CREATE ROLE {role} LOGIN PASSWORD 'relay'; GRANT USAGE ON SCHEMA {schema} TO {role}; GRANT SELECT, UPDATE, DELETE ON {table} TO {role}");
        var roleBuilder = stats.Builder();
        roleBuilder.Username = role;
        roleBuilder.Password = "relay";
        var name = $"FailedPool{Guid.NewGuid():N}";
        var store = await ValidatedAsync(roleBuilder.ConnectionString, name, table, OrderingMode.PerKey);
        await store.GetStatsAsync(Ct);
        var pooled = await StatsBackendsAsync(name);
        Assert.Single(pooled);

        await stats.ExecuteAsync($"REVOKE SELECT ON {table} FROM {role}");
        var ex = await Assert.ThrowsAsync<OutboxStatsUnavailableException>(() => store.GetStatsAsync(Ct));
        Assert.Contains("42501", ex.Message, StringComparison.Ordinal);

        var deadline = Stopwatch.StartNew();
        while ((await StatsBackendsAsync(name)).Length != 0 && deadline.Elapsed < TimeSpan.FromSeconds(5))
        {
            await Task.Delay(50, Ct);
        }

        Assert.Empty(await StatsBackendsAsync(name));
        await stats.ExecuteAsync($"GRANT SELECT ON {table} TO {role}");

        await store.GetStatsAsync(Ct);
        var fresh = await StatsBackendsAsync(name);
        Assert.Single(fresh);
        Assert.NotEqual(pooled[0], fresh[0]);
    }

    [Fact]
    public async Task Stats_connection_sets_server_keepalives()
    {
        var name = $"KeepAlive{Guid.NewGuid():N}";
        await using var connection = new NpgsqlConnection(PostgreSqlConnectionStrings.ForStats(stats.ConnectionString, name));
        await connection.OpenAsync(Ct);

        Assert.Equal("5", await ShowAsync(connection, "tcp_keepalives_idle"));
        Assert.Equal("2", await ShowAsync(connection, "tcp_keepalives_interval"));
        Assert.Equal("3", await ShowAsync(connection, "tcp_keepalives_count"));
        NpgsqlConnection.ClearPool(connection);
    }

    [Fact]
    public async Task Rejected_password_fails_stats_without_secrets_in_message()
    {
        var schema = await stats.CreateOutboxAsync();
        var table = $"{schema}.outbox";
        var role = $"r_{Guid.NewGuid():N}";
        await stats.ExecuteAsync(
            $"CREATE ROLE {role} LOGIN PASSWORD 'secret-5e2a'; GRANT USAGE ON SCHEMA {schema} TO {role}; GRANT SELECT, UPDATE, DELETE ON {table} TO {role}");
        var roleBuilder = stats.Builder();
        roleBuilder.Username = role;
        roleBuilder.Password = "secret-5e2a";
        var store = await ValidatedAsync(roleBuilder.ConnectionString, "Rejected", table, OrderingMode.None);
        await stats.ExecuteAsync($"ALTER ROLE {role} PASSWORD 'changed-9b7f'");

        var ex = await Assert.ThrowsAsync<OutboxStatsUnavailableException>(() => store.GetStatsAsync(Ct));

        Assert.StartsWith("Outboxes[Rejected]: stats unavailable (", ex.Message, StringComparison.Ordinal);
        Assert.Contains("28P01", ex.Message, StringComparison.Ordinal);
        AssertSanitized(ex, roleBuilder.ConnectionString);
    }

    [Fact]
    public async Task Plan_has_no_sequential_scan_of_the_outbox()
    {
        var schema = await stats.CreateOutboxAsync();
        var table = $"{schema}.outbox";
        await stats.SeedAndAnalyzeAsync(
            $"""
            INSERT INTO {table} (message_key, payload, retry_count, next_attempt_at)
            SELECT pg_catalog.convert_to('k' || (i % 5000), 'UTF8'), '\x00'::bytea,
                   CASE WHEN i % 10 = 0 THEN 3 ELSE 0 END,
                   CASE WHEN i % 100 = 0 THEN 'infinity'::timestamptz
                        WHEN i % 10 = 0 THEN now() + interval '1 h'
                        ELSE now() END
            FROM generate_series(1, 100000) AS i
            """,
            table);

        foreach (var mode in new[] { OrderingMode.None, OrderingMode.PerKey, OrderingMode.Strict })
        {
            var store = await ValidatedAsync(stats.ConnectionString, $"Plan{mode}", table, mode);
            var plan = await stats.ScalarAsync<string>($"EXPLAIN (FORMAT JSON) {store.StatsSql}");
            using var json = JsonDocument.Parse(plan!);
            var outboxNodes = new List<string>();
            CollectOutboxNodes(json.RootElement, outboxNodes);

            Assert.NotEmpty(outboxNodes);
            Assert.DoesNotContain("Seq Scan", outboxNodes);
        }
    }

    [Fact]
    public async Task Stats_use_one_pooled_backend_never_the_session()
    {
        var schema = await stats.CreateOutboxAsync();
        var table = $"{schema}.outbox";
        var name = $"Pool{Guid.NewGuid():N}";
        var store = await ValidatedAsync(stats.ConnectionString, name, table, OrderingMode.PerKey);
        await using var session = await store.OpenSessionAsync(Ct);
        Assert.True(await session.TryLockAsync(Ct));
        var sessionPid = await stats.ScalarAsync<int>(
            $"""
            SELECT pid FROM pg_catalog.pg_locks
            WHERE locktype = 'advisory' AND granted
              AND classid = hashtext('outbox-relay')::oid
              AND objid = hashtext('{table}')::oid
              AND objsubid = 2
            """);
        Assert.NotEqual(0, sessionPid);

        for (var i = 0; i < 20; i++)
        {
            await store.GetStatsAsync(Ct);

            var pids = await stats.ScalarAsync<int[]>(
                $"""
                SELECT COALESCE(array_agg(pid), '{"{}"}') FROM pg_catalog.pg_stat_activity
                WHERE application_name = '{PostgreSqlConnectionStrings.StatsApplicationNamePrefix}{name}'
                """);
            Assert.NotNull(pids);
            Assert.True(pids.Length <= 1, $"call {i}: {pids.Length} stats backends");
            Assert.DoesNotContain(sessionPid, pids);
        }
    }

    private async Task<int[]> StatsBackendsAsync(string name) =>
        await stats.ScalarAsync<int[]>(
            $"""
            SELECT COALESCE(array_agg(pid), '{"{}"}') FROM pg_catalog.pg_stat_activity
            WHERE application_name = '{PostgreSqlConnectionStrings.StatsApplicationNamePrefix}{name}'
            """) ?? [];

    private static async Task<PostgreSqlValidatedOutboxStore> ValidatedAsync(
        string connectionString, string name, string table, OrderingMode mode)
    {
        var store = PostgreSqlOutboxStoreTests.Provider(("Db", connectionString))
            .CreateStore(PostgreSqlOutboxStoreTests.Outbox(name, "Db", table, mode));
        return (PostgreSqlValidatedOutboxStore)await store.ValidateAsync(Ct);
    }

    // Explicit ids reach every leaf of the partitioned table. One transaction, so id 1 is the head in both orders.
    private static string SeedSql(string table)
    {
        var sql = new StringBuilder(
            $"INSERT INTO {table} (id, message_key, payload, created_at, retry_count, next_attempt_at) OVERRIDING SYSTEM VALUE VALUES\n");
        sql.Append("(1, NULL, '\\x00', now() - interval '120 s', 0, now())");
        foreach (var (from, count) in new[] { (2, 12), (1000, 12), (2000, 12), (3000, 13) })
        {
            for (var id = from; id < from + count; id++)
            {
                sql.Append($",\n({id}, pg_catalog.convert_to('f{id}', 'UTF8'), '\\x00', now(), 0, now())");
            }
        }

        foreach (var (id, key) in new[] { (20, "'r0'"), (21, "'r1'"), (22, "NULL"), (1020, "'r2'"), (1021, "'r0'"), (3020, "'r3'"), (3021, "NULL") })
        {
            sql.Append($",\n({id}, {KeyOf(key)}, '\\x00', now(), 2, now() + interval '1 h')");
        }

        foreach (var (id, key) in new[] { (2020, "'b0'"), (2021, "'b0'"), (2022, "'b1'"), (2023, "'b1'"), (2024, "'b2'"), (2025, "'b2'"), (30, "NULL"), (3030, "NULL") })
        {
            sql.Append($",\n({id}, {KeyOf(key)}, '\\x00', now(), 4, 'infinity')");
        }

        return sql.ToString();

        static string KeyOf(string key) => key == "NULL" ? "NULL" : $"pg_catalog.convert_to({key}, 'UTF8')";
    }

    private static void AssertSeeded(OutboxStats result, OrderingMode mode)
    {
        Assert.Equal(Retrying, result.Retrying);
        Assert.Equal(
            mode switch
            {
                OrderingMode.PerKey => BlockedKeys,
                OrderingMode.Strict => BlockedRows,
                _ => 0,
            },
            result.Blocked);
        Assert.InRange(result.HeadAge.TotalSeconds, 119, 121);
        Assert.Equal(Seeded - Failed, result.Pending);
    }

    private static void AssertSanitized(Exception ex, string connectionString)
    {
        var builder = new NpgsqlConnectionStringBuilder(connectionString);
        Assert.DoesNotContain(connectionString, ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(builder.Password!, ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(builder.Host!, ex.Message, StringComparison.Ordinal);
        Assert.Null(ex.InnerException);
    }

    private static void CollectOutboxNodes(JsonElement element, List<string> nodeTypes)
    {
        switch (element.ValueKind)
        {
            case JsonValueKind.Object:
                if (element.TryGetProperty("Relation Name", out var relation) && relation.GetString() == "outbox")
                {
                    nodeTypes.Add(element.GetProperty("Node Type").GetString()!);
                }

                foreach (var property in element.EnumerateObject())
                {
                    CollectOutboxNodes(property.Value, nodeTypes);
                }

                break;
            case JsonValueKind.Array:
                foreach (var item in element.EnumerateArray())
                {
                    CollectOutboxNodes(item, nodeTypes);
                }

                break;
        }
    }

    private static async Task<string?> ShowAsync(NpgsqlConnection connection, string setting)
    {
        await using var command = new NpgsqlCommand($"SHOW {setting}", connection);
        return (string?)await command.ExecuteScalarAsync(Ct);
    }

    private static async Task RunAsync(NpgsqlConnection connection, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(CancellationToken.None);
    }
}
