using System.Globalization;
using System.Text;
using System.Text.RegularExpressions;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Npgsql;

namespace Pernix.OutboxRelay.Integration.Tests.Pilot;

// Datasets and the server-time harness (method: plpgsql EXECUTE ... USING, clock_timestamp() deltas).
internal static partial class PilotDatasets
{
    public const int FreshRows = 1_000_000;

    private const string Payload = "repeat('x', 200)::bytea";

    public readonly record struct Timing(double P50, double P95, double Max);

    public readonly record struct Walk(int Reads, long Rows, Timing Timing);

    // A V001 table in a fresh schema, with the bench functions beside it. Autovacuum is off, so state B stays unvacuumed while it is measured.
    public static async Task<string> CreateAsync(PostgreSqlFixture postgres)
    {
        var schema = PostgreSqlFixture.NewSchema();
        await postgres.CreateOutboxSchemaAsync(schema);
        await ExecuteAsync(postgres, $"""
            ALTER TABLE {schema}.outbox SET (autovacuum_enabled = false);

            CREATE FUNCTION {schema}.pilot_bench(p_sql text, p_runs int, p_ids bigint[], p_max int)
            RETURNS TABLE (p50_ms float8, p95_ms float8, max_ms float8)
            LANGUAGE plpgsql AS $fn$
            DECLARE
                t0 timestamptz;
                d float8[] := ARRAY[]::float8[];
                r record;
            BEGIN
                FOR i IN 1 .. p_runs LOOP
                    t0 := pg_catalog.clock_timestamp();
                    FOR r IN EXECUTE p_sql USING p_ids, p_max LOOP
                    END LOOP;
                    d := d || (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp() - t0) * 1000)::float8;
                END LOOP;
                RETURN QUERY
                    SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY x), percentile_cont(0.95) WITHIN GROUP (ORDER BY x), max(x)
                    FROM unnest(d) AS x;
            END
            $fn$;

            CREATE FUNCTION {schema}.pilot_walk(p_sql text, p_max int, p_walks int)
            RETURNS TABLE (reads int, total_rows bigint, p50_ms float8, p95_ms float8, max_ms float8)
            LANGUAGE plpgsql AS $fn$
            DECLARE
                t0 timestamptz;
                d float8[] := ARRAY[]::float8[];
                r record;
                after_tx text;
                after_id bigint;
                got int;
                walk_reads int;
                walk_rows bigint;
            BEGIN
                FOR w IN 1 .. p_walks LOOP
                    after_tx := '0';
                    after_id := 0;
                    walk_reads := 0;
                    walk_rows := 0;
                    LOOP
                        got := 0;
                        t0 := pg_catalog.clock_timestamp();
                        FOR r IN EXECUTE p_sql USING p_max, after_tx, after_id LOOP
                            got := got + 1;
                            after_tx := r."Order"::text;
                            after_id := r."Id";
                        END LOOP;
                        d := d || (EXTRACT(EPOCH FROM pg_catalog.clock_timestamp() - t0) * 1000)::float8;
                        walk_reads := walk_reads + 1;
                        walk_rows := walk_rows + got;
                        EXIT WHEN got < p_max;
                    END LOOP;
                END LOOP;
                RETURN QUERY
                    SELECT walk_reads, walk_rows,
                           percentile_cont(0.5) WITHIN GROUP (ORDER BY x), percentile_cont(0.95) WITHIN GROUP (ORDER BY x), max(x)
                    FROM unnest(d) AS x;
            END
            $fn$;
            """);
        return schema;
    }

    public static Task DropAsync(PostgreSqlFixture postgres, string schema) => ExecuteAsync(postgres, $"DROP SCHEMA {schema} CASCADE");

    public static Task SeedBlockedKeyAsync(PostgreSqlFixture postgres, string schema, int n) => SeedAsync(postgres, schema, $"""
        INSERT INTO {schema}.outbox (message_key, payload, retry_count, next_attempt_at, last_error)
        VALUES ('blocked'::bytea, {Payload}, 1, 'infinity', 'blocked');
        INSERT INTO {schema}.outbox (message_key, payload)
        SELECT CASE WHEN g % 2 = 1 THEN 'blocked'::bytea ELSE convert_to('k' || (g / 2) % 1000, 'UTF8') END, {Payload}
        FROM generate_series(1, {2L * n}) AS g;
        """);

    public static Task SeedFailedRowsAsync(PostgreSqlFixture postgres, string schema, int f) => SeedAsync(postgres, schema, $"""
        INSERT INTO {schema}.outbox (message_key, payload, retry_count, next_attempt_at, last_error)
        SELECT CASE WHEN g <= 10 THEN NULL ELSE convert_to('k' || g % 50000, 'UTF8') END,
               {Payload}, 4,
               CASE WHEN g <= {f / 100} THEN 'infinity'::timestamptz ELSE now() + interval '1 hour' END,
               'failed'
        FROM generate_series(1, {f}) AS g;
        INSERT INTO {schema}.outbox (message_key, payload)
        SELECT convert_to('k' || g % 50000, 'UTF8'), {Payload}
        FROM generate_series(1, {FreshRows}) AS g;
        """);

    // Big-endian Unix-ms insert time, then filler to the 200 bytes the server-time datasets use.
    public const int PayloadSize = 200;
    public const int PayloadTimeBytes = 8;

    private const string TimedPayload = "int8send((extract(epoch FROM pg_catalog.clock_timestamp()) * 1000)::bigint) || repeat('x', 192)::bytea";

    // Backlog rows carry their seed sequence g after the insert time. ORDER BY g makes g follow id, so delivered rows of one key can be checked for id order.
    public const int PayloadSequenceOffset = PayloadTimeBytes;

    private const string SequencedPayload = "int8send((extract(epoch FROM pg_catalog.clock_timestamp()) * 1000)::bigint) || int8send(g) || repeat('x', 184)::bytea";

    public const string ResetSingleKey = "reset-single";
    public const string ResetChurnKey = "reset-churn";
    public const string ChurnKey = "churn";
    public const string MarkerSingleKey = "marker-single";
    public const string MarkerChurnKey = "marker-churn";
    public const string ResetChurnManyKey = "reset-churn-many";
    public const string MarkerChurnManyKey = "marker-churn-many";
    public const string BlockedKey = "blocked";

    public const int ChurnManyKeys = 10;

    public const int RewalkExtraRows = 8 + ChurnManyKeys;

    // Markers are waiting failed rows, so the cursor passes them while their own row holds the key and only a re-walk delivers them.
    public static Task SeedRewalkAsync(PostgreSqlFixture postgres, string schema, int n, string tagHeader, string churnTag, string churnManyTag) => SeedAsync(postgres, schema, $"""
        INSERT INTO {schema}.outbox (message_key, payload, retry_count, next_attempt_at, last_error)
        VALUES (convert_to('{BlockedKey}', 'UTF8'), {TimedPayload}, 1, 'infinity', 'blocked'),
               (convert_to('{ResetSingleKey}', 'UTF8'), {TimedPayload}, 1, 'infinity', 'blocked'),
               (convert_to('{ResetChurnKey}', 'UTF8'), {TimedPayload}, 1, 'infinity', 'blocked'),
               (convert_to('{ResetChurnManyKey}', 'UTF8'), {TimedPayload}, 1, 'infinity', 'blocked');
        INSERT INTO {schema}.outbox (message_key, payload, headers, retry_count, next_attempt_at, last_error)
        VALUES (convert_to('{ChurnKey}', 'UTF8'), {TimedPayload}, jsonb_build_object('{tagHeader}', '{churnTag}'), 1, now() + interval '1 day', 'churn');
        INSERT INTO {schema}.outbox (message_key, payload, headers, retry_count, next_attempt_at, last_error)
        SELECT convert_to('churn-m' || g, 'UTF8'), {TimedPayload}, jsonb_build_object('{tagHeader}', '{churnManyTag}'), 1, now() + interval '1 day', 'churn'
        FROM generate_series(0, {ChurnManyKeys - 1}) AS g
        ORDER BY g;
        INSERT INTO {schema}.outbox (message_key, payload)
        SELECT CASE WHEN g % 2 = 1 THEN convert_to('{BlockedKey}', 'UTF8') ELSE convert_to('k' || (g / 2) % 1000, 'UTF8') END, {SequencedPayload}
        FROM generate_series(1, {2L * n}) AS g
        ORDER BY g;
        INSERT INTO {schema}.outbox (message_key, payload, retry_count, next_attempt_at, last_error)
        VALUES (convert_to('{MarkerSingleKey}', 'UTF8'), {TimedPayload}, 1, now() + interval '1 day', 'marker'),
               (convert_to('{MarkerChurnKey}', 'UTF8'), {TimedPayload}, 1, now() + interval '1 day', 'marker'),
               (convert_to('{MarkerChurnManyKey}', 'UTF8'), {TimedPayload}, 1, now() + interval '1 day', 'marker');
        """);

    // docs/delivery.md operator action "Retry from scratch" on the reset row and on the marker, in one transaction.
    public static Task ReleaseAsync(PostgreSqlFixture postgres, string schema, string resetKey, string markerKey) => ExecuteAsync(postgres, $"""
        BEGIN;
        UPDATE {schema}.outbox SET retry_count = 0, next_attempt_at = now(), last_error = NULL WHERE message_key = convert_to('{resetKey}', 'UTF8');
        UPDATE {schema}.outbox SET retry_count = 0, next_attempt_at = now(), last_error = NULL WHERE message_key = convert_to('{markerKey}', 'UTF8');
        COMMIT;
        """);

    // Once due, every failed retry of the churn row changes the summary.
    public static Task StartChurnAsync(PostgreSqlFixture postgres, string schema) =>
        ExecuteAsync(postgres, $"UPDATE {schema}.outbox SET next_attempt_at = now() WHERE message_key = convert_to('{ChurnKey}', 'UTF8')");

    // Churn-many: spreads only the first retries over one Polling.Interval (1 s). A fail commit sets now() + 1 s for its whole commit group,
    // so later phases stay distinct only because the harness delays each churn-m{g} fault by g x 100 ms (PilotDeliveryDelayTests.ChurnManyDelay).
    public static Task StartChurnManyAsync(PostgreSqlFixture postgres, string schema) => ExecuteAsync(postgres, $"""
        UPDATE {schema}.outbox AS o SET next_attempt_at = now() + (g * interval '{1000 / ChurnManyKeys} milliseconds')
        FROM generate_series(0, {ChurnManyKeys - 1}) AS g
        WHERE o.message_key = convert_to('churn-m' || g, 'UTF8')
        """);

    // Distinct retry phases across the churn-many rows: about ChurnManyKeys when the faults keep separate commits.
    public static async Task<long> ChurnManyPhasesAsync(PostgreSqlFixture postgres, string schema)
    {
        await using var connection = await OpenAsync(postgres);
        await using var command = new NpgsqlCommand($"""
            SELECT count(DISTINCT next_attempt_at) FROM {schema}.outbox
            WHERE retry_count > 0 AND message_key IN (SELECT convert_to('churn-m' || g, 'UTF8') FROM generate_series(0, {ChurnManyKeys - 1}) AS g)
            """, connection);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    public static Task SkipChurnAsync(PostgreSqlFixture postgres, string schema) =>
        ExecuteAsync(postgres, $"DELETE FROM {schema}.outbox WHERE retry_count > 0 AND message_key = convert_to('{ChurnKey}', 'UTF8')");

    // docs/delivery.md operator action "Skip a message" on the blocked head and the churn-many rows (final drain).
    public static Task SkipBlockedAndChurnManyAsync(PostgreSqlFixture postgres, string schema) => ExecuteAsync(postgres, $"""
        DELETE FROM {schema}.outbox
        WHERE retry_count > 0 AND (message_key = convert_to('{BlockedKey}', 'UTF8') OR message_key IN (SELECT convert_to('churn-m' || g, 'UTF8') FROM generate_series(0, {ChurnManyKeys - 1}) AS g))
        """);

    // docs/delivery.md operator action "Skip a message" on every remaining row of the blocked key (N = 1M). Returns the rows deleted.
    public static async Task<int> SkipBlockedKeyAsync(PostgreSqlFixture postgres, string schema)
    {
        await using var connection = await OpenAsync(postgres);
        await using var command = new NpgsqlCommand($"DELETE FROM {schema}.outbox WHERE message_key = convert_to('{BlockedKey}', 'UTF8')", connection) { CommandTimeout = 0 };
        return await command.ExecuteNonQueryAsync();
    }

    public static Task<long> CountBlockedKeyAsync(PostgreSqlFixture postgres, string schema) =>
        ScalarAsync(postgres, $"SELECT count(*) FROM {schema}.outbox WHERE message_key = convert_to('{BlockedKey}', 'UTF8')");

    public static Task<long> CountOtherThanBlockedKeyAsync(PostgreSqlFixture postgres, string schema) =>
        ScalarAsync(postgres, $"SELECT count(*) FROM {schema}.outbox WHERE message_key IS DISTINCT FROM convert_to('{BlockedKey}', 'UTF8')");

    public static Task SeedHotKeyAsync(PostgreSqlFixture postgres, string schema, byte[] key, int h) => ExecuteAsync(postgres, $"""
        INSERT INTO {schema}.outbox (message_key, payload)
        SELECT '\x{Convert.ToHexString(key)}'::bytea, {TimedPayload}
        FROM generate_series(1, {h}) AS g
        ORDER BY g
        """);

    public static async Task<long> CountAsync(PostgreSqlFixture postgres, string schema)
    {
        await using var connection = await OpenAsync(postgres);
        await using var command = new NpgsqlCommand($"SELECT count(*) FROM {schema}.outbox", connection);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    public static Task TouchFailedRowsAsync(PostgreSqlFixture postgres, string schema) =>
        ExecuteAsync(postgres, $"UPDATE {schema}.outbox SET last_error = last_error WHERE retry_count > 0");

    public static Task SeedNotDueAsync(PostgreSqlFixture postgres, string schema, int n) => SeedAsync(postgres, schema, $"""
        INSERT INTO {schema}.outbox (message_key, payload, retry_count, next_attempt_at, last_error)
        SELECT convert_to('k' || g % 50000, 'UTF8'), {Payload}, 4, now() + interval '1 hour', 'failed'
        FROM generate_series(1, {n}) AS g;
        INSERT INTO {schema}.outbox (message_key, payload)
        SELECT convert_to('k' || g % 50000, 'UTF8'), {Payload}
        FROM generate_series(1, {FreshRows}) AS g;
        """);

    public static async Task<long[]> FirstDueIdsAsync(PostgreSqlFixture postgres, string schema, int count)
    {
        await using var connection = await OpenAsync(postgres);
        await using var command = new NpgsqlCommand(
            $"SELECT array_agg(id ORDER BY id) FROM (SELECT id FROM {schema}.outbox WHERE retry_count = 0 ORDER BY id LIMIT {count}) AS h",
            connection);
        return (long[])(await command.ExecuteScalarAsync())!;
    }

    // The shipped text with its named parameters bound by position, the only change EXECUTE ... USING needs.
    public static string Positional(string sql, params string[] names)
    {
        for (var i = 0; i < names.Length; i++)
        {
            sql = Regex.Replace(sql, $@"@{names[i]}\b", $"${i + 1}", RegexOptions.CultureInvariant);
        }

        if (ParameterPattern().IsMatch(sql))
        {
            throw new InvalidOperationException($"Unbound parameter left in: {sql}");
        }

        return sql.TrimEnd().TrimEnd(';');
    }

    // p_sql binds $1 = ids, $2 = max. A statement that uses neither ignores them.
    public static async Task<Timing> BenchAsync(PostgreSqlFixture postgres, string schema, string sql, int runs, long[]? ids = null, int? max = null)
    {
        await using var connection = await OpenAsync(postgres);
        await using var command = new NpgsqlCommand($"SELECT * FROM {schema}.pilot_bench(@sql, @runs, @ids, @max)", connection) { CommandTimeout = 0 };
        command.Parameters.AddWithValue("sql", sql);
        command.Parameters.AddWithValue("runs", runs);
        command.Parameters.Add(new NpgsqlParameter<long[]>("ids", NpgsqlTypes.NpgsqlDbType.Array | NpgsqlTypes.NpgsqlDbType.Bigint) { TypedValue = ids ?? [] });
        command.Parameters.Add(new NpgsqlParameter<int>("max", max ?? 0));
        await using var reader = await command.ExecuteReaderAsync();
        await reader.ReadAsync();
        return new Timing(reader.GetDouble(0), reader.GetDouble(1), reader.GetDouble(2));
    }

    // p_sql binds $1 = max, $2 = after_tx (text), $3 = after_id. Walks from the head until a read returns fewer than max rows.
    public static async Task<Walk> WalkAsync(PostgreSqlFixture postgres, string schema, string sql, int max, int walks)
    {
        await using var connection = await OpenAsync(postgres);
        await using var command = new NpgsqlCommand($"SELECT * FROM {schema}.pilot_walk(@sql, @max, @walks)", connection) { CommandTimeout = 0 };
        command.Parameters.AddWithValue("sql", sql);
        command.Parameters.AddWithValue("max", max);
        command.Parameters.AddWithValue("walks", walks);
        await using var reader = await command.ExecuteReaderAsync();
        await reader.ReadAsync();
        return new Walk(reader.GetInt32(0), reader.GetInt64(1), new Timing(reader.GetDouble(2), reader.GetDouble(3), reader.GetDouble(4)));
    }

    public static async Task<string> ExplainAsync(PostgreSqlFixture postgres, string sql, string parameterTypes = "", string values = "")
    {
        await using var connection = await OpenAsync(postgres);
        var explain = new StringBuilder();
        var target = sql;
        if (parameterTypes.Length > 0)
        {
            await using var prepare = new NpgsqlCommand($"PREPARE pilot_explain({parameterTypes}) AS {sql}", connection);
            await prepare.ExecuteNonQueryAsync();
            target = $"EXECUTE pilot_explain({values})";
        }

        await using var command = new NpgsqlCommand($"EXPLAIN (ANALYZE, BUFFERS) {target}", connection) { CommandTimeout = 0 };
        await using var reader = await command.ExecuteReaderAsync();
        while (await reader.ReadAsync())
        {
            var line = reader.GetString(0);
            explain.AppendLine(line.Length > 240 ? string.Concat(line.AsSpan(0, 240), " ...") : line);
        }

        return explain.ToString();
    }

    public static async Task<string> ServerAsync(PostgreSqlFixture postgres)
    {
        await using var connection = await OpenAsync(postgres);
        await using var command = new NpgsqlCommand(
            "SELECT version() || ' | shared_buffers ' || current_setting('shared_buffers') || ', work_mem ' || current_setting('work_mem')"
            + " || ', max_parallel_workers_per_gather ' || current_setting('max_parallel_workers_per_gather')",
            connection);
        return (string)(await command.ExecuteScalarAsync())!;
    }

    public static string Ms(double value) => value.ToString("0.00", CultureInfo.InvariantCulture);

    // VACUUM refuses to run inside a multi-statement batch.
    private static async Task SeedAsync(PostgreSqlFixture postgres, string schema, string sql)
    {
        await ExecuteAsync(postgres, sql);
        await ExecuteAsync(postgres, $"VACUUM ANALYZE {schema}.outbox");
    }

    private static async Task<long> ScalarAsync(PostgreSqlFixture postgres, string sql)
    {
        await using var connection = await OpenAsync(postgres);
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 0 };
        return (long)(await command.ExecuteScalarAsync())!;
    }

    private static async Task ExecuteAsync(PostgreSqlFixture postgres, string sql)
    {
        await using var connection = await OpenAsync(postgres);
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 0 };
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<NpgsqlConnection> OpenAsync(PostgreSqlFixture postgres)
    {
        var connection = new NpgsqlConnection(postgres.ConnectionString);
        await connection.OpenAsync();
        return connection;
    }

    [GeneratedRegex(@"@[a-z_]+\b", RegexOptions.CultureInvariant)]
    private static partial Regex ParameterPattern();
}
