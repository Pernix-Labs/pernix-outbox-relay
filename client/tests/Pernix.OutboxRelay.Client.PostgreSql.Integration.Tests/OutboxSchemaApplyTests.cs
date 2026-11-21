using Testcontainers.PostgreSql;
using Xunit;

namespace Pernix.OutboxRelay.Client.PostgreSql.Integration.Tests;

public sealed class OutboxSchemaApplyTests(PostgreSqlFixture postgres)
{
    // Final PostgreSQL 13.x release (13 is the oldest supported version, V001 needs xid8).
    private const string OldestImage = "postgres:13.23";

    [Fact]
    public async Task Rendered_v001_creates_the_columns_of_outbox_columns_all_in_order()
    {
        var table = await postgres.CreateOutboxAsync("t", "p");

        var columns = await PostgreSqlFixture.QueryStringsAsync(
            postgres.ConnectionString,
            $"SELECT attname::text FROM pg_attribute WHERE attrelid = '{table}'::regclass AND attnum > 0 AND NOT attisdropped ORDER BY attnum");

        Assert.Equal(OutboxColumns.All, columns);
    }

    [Fact]
    public Task Two_tables_with_two_prefixes_apply_in_one_schema_on_postgres_17() =>
        AssertTwoTablesApplyAsync(postgres.ConnectionString);

    [Fact]
    public async Task Two_tables_with_two_prefixes_apply_in_one_schema_on_postgres_13()
    {
        await using var container = new PostgreSqlBuilder(OldestImage).Build();
        await container.StartAsync(TestContext.Current.CancellationToken);

        await AssertTwoTablesApplyAsync(container.GetConnectionString());
    }

    private static async Task AssertTwoTablesApplyAsync(string connectionString)
    {
        var schema = PostgreSqlFixture.NewSchema();
        await PostgreSqlFixture.ExecuteAsync(connectionString, $"CREATE SCHEMA {schema}");
        var first = OutboxTable.Parse($"{schema}.outbox");
        var second = OutboxTable.Parse($"{schema}.orders_outbox");

        await PostgreSqlFixture.ApplyScriptsAsync(connectionString, first);
        await PostgreSqlFixture.ApplyScriptsAsync(connectionString, second, "orders");

        // V002 drops <prefix>_source_complete and <prefix>_source_uq with their columns, keeps the rest.
        Assert.Equal(
            ["outbox_header_values_are_strings", "outbox_headers_is_object", "outbox_retry_count_non_negative", "outbox_route_format"],
            await CheckConstraintsAsync(connectionString, first));
        Assert.Equal(
            ["orders_header_values_are_strings", "orders_headers_is_object", "orders_retry_count_non_negative", "orders_route_format"],
            await CheckConstraintsAsync(connectionString, second));
        Assert.Equal(
            ["outbox_failed_message_key_idx", "outbox_pkey", "outbox_tx_id_id_idx"],
            await IndexesAsync(connectionString, schema, "outbox"));
        Assert.Equal(
            ["orders_failed_message_key_idx", "orders_outbox_pkey", "orders_tx_id_id_idx"],
            await IndexesAsync(connectionString, schema, "orders_outbox"));

        foreach (var table in new[] { first, second })
        {
            Assert.Equal(
                ["{autovacuum_vacuum_scale_factor=0,autovacuum_vacuum_threshold=1000}"],
                await PostgreSqlFixture.QueryStringsAsync(connectionString, $"SELECT reloptions::text FROM pg_class WHERE oid = '{table}'::regclass"));
            Assert.Equal(
                ["text|nullable|no default"],
                await PostgreSqlFixture.QueryStringsAsync(
                    connectionString,
                    $"SELECT format_type(a.atttypid, a.atttypmod) || CASE WHEN a.attnotnull THEN '|not null' ELSE '|nullable' END || CASE WHEN d.adrelid IS NULL THEN '|no default' ELSE '|default' END FROM pg_attribute a LEFT JOIN pg_attrdef d ON d.adrelid = a.attrelid AND d.adnum = a.attnum WHERE a.attrelid = '{table}'::regclass AND a.attname = 'route'"));
            Assert.Empty(await PostgreSqlFixture.QueryStringsAsync(
                connectionString,
                $"SELECT i.indexrelid::regclass::text FROM pg_index i JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY (i.indkey) WHERE i.indrelid = '{table}'::regclass AND a.attname = 'route'"));
        }
    }

    [Theory]
    [InlineData("INSERT INTO {0} (message_key, payload) VALUES ('\\x6b'::bytea, '\\x70'::bytea)")]
    [InlineData("INSERT INTO {0} (message_key, payload, headers) VALUES ('\\x6b'::bytea, '\\x70'::bytea, '{{\"a\": \"b\"}}'::jsonb)")]
    public async Task Insert_naming_only_the_1_0_0_columns_leaves_route_null(string insert)
    {
        var table = await postgres.CreateOutboxAsync();

        await postgres.ExecuteAsync(string.Format(System.Globalization.CultureInfo.InvariantCulture, insert, table));

        Assert.Equal(
            ["1|null"],
            await PostgreSqlFixture.QueryStringsAsync(
                postgres.ConnectionString,
                $"SELECT count(*)::text || '|' || CASE WHEN bool_and(route IS NULL) THEN 'null' ELSE 'set' END FROM {table}"));
    }

    private static Task<List<string>> CheckConstraintsAsync(string connectionString, OutboxTable table) =>
        PostgreSqlFixture.QueryStringsAsync(
            connectionString,
            $"SELECT conname::text FROM pg_constraint WHERE conrelid = '{table}'::regclass AND contype = 'c' ORDER BY conname::text COLLATE \"C\"");

    private static Task<List<string>> IndexesAsync(string connectionString, string schema, string table) =>
        PostgreSqlFixture.QueryStringsAsync(
            connectionString,
            $"SELECT indexname::text FROM pg_indexes WHERE schemaname = '{schema}' AND tablename = '{table}' ORDER BY indexname::text COLLATE \"C\"");
}
