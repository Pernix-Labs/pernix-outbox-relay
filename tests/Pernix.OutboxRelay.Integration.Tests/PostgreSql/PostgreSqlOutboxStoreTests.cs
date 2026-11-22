using System.Diagnostics;
using System.Reflection;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Pernix.OutboxRelay.Outbox.PostgreSql;
using Microsoft.Extensions.Configuration;
using Npgsql;
using Testcontainers.PostgreSql;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.PostgreSql;

public sealed class PostgreSqlOutboxStoreTests(PostgreSqlFixture postgres)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Valid_table_passes()
    {
        var schema = PostgreSqlFixture.NewSchema();
        await postgres.CreateOutboxSchemaAsync(schema);
        var provider = Provider(("Db", postgres.ConnectionString));

        var validated = await provider.CreateStore(Outbox("Orders", "Db", $"{schema}.outbox")).ValidateAsync(Ct);

        await using var session = await validated.OpenSessionAsync(Ct);
        Assert.True(await session.TryLockAsync(Ct));
    }

    [Fact]
    public async Task V001_plus_V002_table_validates()
    {
        var schema = PostgreSqlFixture.NewSchema();
        await postgres.CreateOutboxSchemaAsync(schema);
        await postgres.ApplyV002Async(schema);
        var provider = Provider(("Db", postgres.ConnectionString));

        var validated = await provider.CreateStore(Outbox("Orders", "Db", $"{schema}.outbox")).ValidateAsync(Ct);

        await using var session = await validated.OpenSessionAsync(Ct);
        Assert.True(await session.TryLockAsync(Ct));
    }

    [Fact]
    public void Store_keeps_no_mutable_table_state()
    {
        var fields = typeof(PostgreSqlOutboxStore).GetFields(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);

        Assert.All(fields.Where(f => f.FieldType == typeof(string)), f => Assert.True(f.IsInitOnly, f.Name));
        Assert.All(
            typeof(PostgreSqlValidatedOutboxStore).GetFields(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public),
            f => Assert.True(f.IsInitOnly, f.Name));
    }

    [Fact]
    public async Task Missing_table_fails()
    {
        var schema = PostgreSqlFixture.NewSchema();
        await postgres.ExecuteAsync($"CREATE SCHEMA {schema}");
        var provider = Provider(("Db", postgres.ConnectionString));

        var ex = await Assert.ThrowsAsync<OutboxValidationException>(
            () => provider.CreateStore(Outbox("Orders", "Db", $"{schema}.outbox")).ValidateAsync(Ct));

        Assert.Equal($"Outboxes[Orders]: table '{schema}.outbox' not found", ex.Message);
    }

    [Fact]
    public async Task Missing_columns_are_all_listed()
    {
        var schema = PostgreSqlFixture.NewSchema();
        await postgres.CreateOutboxSchemaAsync(schema);
        await postgres.ExecuteAsync($"ALTER TABLE {schema}.outbox DROP COLUMN last_error, DROP COLUMN tx_id");
        var provider = Provider(("Db", postgres.ConnectionString));

        var ex = await Assert.ThrowsAsync<OutboxValidationException>(
            () => provider.CreateStore(Outbox("Orders", "Db", $"{schema}.outbox")).ValidateAsync(Ct));

        Assert.Equal($"Outboxes[Orders]: table '{schema}.outbox' missing columns: tx_id, last_error", ex.Message);
    }

    [Fact]
    public async Task Missing_route_column_fails()
    {
        var schema = PostgreSqlFixture.NewSchema();
        await postgres.CreateOutboxSchemaAsync(schema);
        await postgres.ExecuteAsync($"ALTER TABLE {schema}.outbox DROP COLUMN route");
        var provider = Provider(("Db", postgres.ConnectionString));

        var ex = await Assert.ThrowsAsync<OutboxValidationException>(
            () => provider.CreateStore(Outbox("Orders", "Db", $"{schema}.outbox")).ValidateAsync(Ct));

        Assert.Equal($"Outboxes[Orders]: table '{schema}.outbox' missing columns: route", ex.Message);
    }

    [Fact]
    public async Task Wrong_column_type_fails()
    {
        var schema = PostgreSqlFixture.NewSchema();
        await postgres.CreateOutboxSchemaAsync(schema);
        await postgres.ExecuteAsync($"ALTER TABLE {schema}.outbox ALTER COLUMN retry_count TYPE bigint");
        var provider = Provider(("Db", postgres.ConnectionString));

        var ex = await Assert.ThrowsAsync<OutboxValidationException>(
            () => provider.CreateStore(Outbox("Orders", "Db", $"{schema}.outbox")).ValidateAsync(Ct));

        Assert.Equal(
            $"Outboxes[Orders]: table '{schema}.outbox' columns differ from V001: retry_count bigint NOT NULL (expected integer NOT NULL)",
            ex.Message);
    }

    [Fact]
    public async Task Timestamp_without_time_zone_fails()
    {
        var schema = PostgreSqlFixture.NewSchema();
        await postgres.CreateOutboxSchemaAsync(schema);
        await postgres.ExecuteAsync($"ALTER TABLE {schema}.outbox ALTER COLUMN created_at TYPE timestamp");
        var provider = Provider(("Db", postgres.ConnectionString));

        var ex = await Assert.ThrowsAsync<OutboxValidationException>(
            () => provider.CreateStore(Outbox("Orders", "Db", $"{schema}.outbox")).ValidateAsync(Ct));

        Assert.Equal(
            $"Outboxes[Orders]: table '{schema}.outbox' columns differ from V001: created_at timestamp without time zone NOT NULL (expected timestamp with time zone NOT NULL)",
            ex.Message);
    }

    [Fact]
    public async Task Nullable_column_that_V001_declares_not_null_fails()
    {
        var schema = PostgreSqlFixture.NewSchema();
        await postgres.CreateOutboxSchemaAsync(schema);
        await postgres.ExecuteAsync($"ALTER TABLE {schema}.outbox ALTER COLUMN tx_id DROP NOT NULL");
        var provider = Provider(("Db", postgres.ConnectionString));

        var ex = await Assert.ThrowsAsync<OutboxValidationException>(
            () => provider.CreateStore(Outbox("Orders", "Db", $"{schema}.outbox")).ValidateAsync(Ct));

        Assert.Equal(
            $"Outboxes[Orders]: table '{schema}.outbox' columns differ from V001: tx_id xid8 NULL (expected xid8 NOT NULL)",
            ex.Message);
    }

    [Fact]
    public async Task Not_null_column_that_V001_declares_nullable_fails()
    {
        var schema = PostgreSqlFixture.NewSchema();
        await postgres.CreateOutboxSchemaAsync(schema);
        await postgres.ExecuteAsync($"ALTER TABLE {schema}.outbox ALTER COLUMN last_error SET NOT NULL");
        var provider = Provider(("Db", postgres.ConnectionString));

        var ex = await Assert.ThrowsAsync<OutboxValidationException>(
            () => provider.CreateStore(Outbox("Orders", "Db", $"{schema}.outbox")).ValidateAsync(Ct));

        Assert.Equal(
            $"Outboxes[Orders]: table '{schema}.outbox' columns differ from V001: last_error text NOT NULL (expected text NULL)",
            ex.Message);
    }

    // {s} is replaced by the test schema in both the DDL and the expected problem.
    [Theory]
    [InlineData(
        "ALTER TABLE {s}.outbox ALTER COLUMN route TYPE varchar",
        "route character varying NULL (expected text NULL)")]
    [InlineData(
        "ALTER TABLE {s}.outbox ALTER COLUMN last_error TYPE varchar(200)",
        "last_error character varying(200) NULL (expected text NULL)")]
    [InlineData(
        "ALTER TABLE {s}.outbox DROP CONSTRAINT outbox_headers_is_object, DROP CONSTRAINT outbox_header_values_are_strings; ALTER TABLE {s}.outbox ALTER COLUMN headers TYPE json",
        "headers json NOT NULL (expected jsonb NOT NULL)")]
    [InlineData(
        "ALTER TABLE {s}.outbox ALTER COLUMN message_key SET NOT NULL",
        "message_key bytea NOT NULL (expected bytea NULL)")]
    [InlineData(
        "ALTER TABLE {s}.outbox ALTER COLUMN route SET NOT NULL",
        "route text NOT NULL (expected text NULL)")]
    [InlineData(
        "CREATE DOMAIN {s}.outbox_text AS text; ALTER TABLE {s}.outbox ALTER COLUMN last_error TYPE {s}.outbox_text",
        "last_error {s}.outbox_text NULL (expected text NULL)")]
    [InlineData(
        "ALTER TABLE {s}.outbox ALTER COLUMN next_attempt_at TYPE timestamptz(3)",
        "next_attempt_at timestamp(3) with time zone NOT NULL (expected timestamp with time zone NOT NULL)")]
    public async Task Refused_candidates_fail(string ddl, string problem)
    {
        var schema = PostgreSqlFixture.NewSchema();
        await postgres.CreateOutboxSchemaAsync(schema);
        await postgres.ExecuteAsync(ddl.Replace("{s}", schema, StringComparison.Ordinal));
        var provider = Provider(("Db", postgres.ConnectionString));

        var ex = await Assert.ThrowsAsync<OutboxValidationException>(
            () => provider.CreateStore(Outbox("Orders", "Db", $"{schema}.outbox")).ValidateAsync(Ct));

        Assert.Equal(
            $"Outboxes[Orders]: table '{schema}.outbox' columns differ from V001: {problem.Replace("{s}", schema, StringComparison.Ordinal)}",
            ex.Message);
    }

    [Fact]
    public async Task Mismatched_columns_are_all_listed()
    {
        var schema = PostgreSqlFixture.NewSchema();
        await postgres.CreateOutboxSchemaAsync(schema);
        await postgres.ExecuteAsync(
            $"ALTER TABLE {schema}.outbox ALTER COLUMN tx_id DROP NOT NULL, ALTER COLUMN route TYPE varchar(63), ALTER COLUMN retry_count TYPE bigint");
        var provider = Provider(("Db", postgres.ConnectionString));

        var ex = await Assert.ThrowsAsync<OutboxValidationException>(
            () => provider.CreateStore(Outbox("Orders", "Db", $"{schema}.outbox")).ValidateAsync(Ct));

        Assert.Equal(
            $"Outboxes[Orders]: table '{schema}.outbox' columns differ from V001: tx_id xid8 NULL (expected xid8 NOT NULL); route character varying(63) NULL (expected text NULL); retry_count bigint NOT NULL (expected integer NOT NULL)",
            ex.Message);
    }

    [Fact]
    public async Task Partitioned_v001_table_passes()
    {
        var schema = PostgreSqlFixture.NewSchema();
        await postgres.CreateOutboxSchemaAsync(schema);
        await postgres.ExecuteAsync(
            $"CREATE TABLE {schema}.p (LIKE {schema}.outbox INCLUDING DEFAULTS) PARTITION BY RANGE (id); CREATE TABLE {schema}.p1 PARTITION OF {schema}.p DEFAULT;");
        var provider = Provider(("Db", postgres.ConnectionString));

        await provider.CreateStore(Outbox("Orders", "Db", $"{schema}.p")).ValidateAsync(Ct);
    }

    [Fact]
    public async Task Role_without_update_privilege_fails()
    {
        var schema = PostgreSqlFixture.NewSchema();
        await postgres.CreateOutboxSchemaAsync(schema);
        var role = $"r_{Guid.NewGuid():N}";
        await postgres.ExecuteAsync(
            $"CREATE ROLE {role} LOGIN PASSWORD 'relay'; GRANT USAGE ON SCHEMA {schema} TO {role}; GRANT SELECT, DELETE ON {schema}.outbox TO {role}");
        var connection = new NpgsqlConnectionStringBuilder(postgres.ConnectionString) { Username = role, Password = "relay" }.ConnectionString;
        var provider = Provider(("Db", connection));

        var ex = await Assert.ThrowsAsync<OutboxValidationException>(
            () => provider.CreateStore(Outbox("Orders", "Db", $"{schema}.outbox")).ValidateAsync(Ct));

        Assert.Equal($"Outboxes[Orders]: table '{schema}.outbox' role lacks privileges: UPDATE", ex.Message);
    }

    [Fact]
    public async Task Unqualified_and_qualified_name_of_one_table_fail_second_outbox()
    {
        var schema = PostgreSqlFixture.NewSchema();
        await postgres.CreateOutboxSchemaAsync(schema);
        var connection = new NpgsqlConnectionStringBuilder(postgres.ConnectionString) { SearchPath = schema }.ConnectionString;
        var provider = Provider(("Db", connection));

        await provider.CreateStore(Outbox("First", "Db", "outbox")).ValidateAsync(Ct);
        var ex = await Assert.ThrowsAsync<OutboxValidationException>(
            () => provider.CreateStore(Outbox("Second", "Db", $"{schema}.outbox")).ValidateAsync(Ct));

        Assert.Equal($"Outboxes[Second]: table '{schema}.outbox' same table as outbox 'First'", ex.Message);
    }

    [Fact]
    public async Task Same_table_via_two_connection_names_fails_second_outbox()
    {
        var schema = PostgreSqlFixture.NewSchema();
        await postgres.CreateOutboxSchemaAsync(schema);
        var other = new NpgsqlConnectionStringBuilder(postgres.ConnectionString) { ApplicationName = "other" }.ConnectionString;
        var provider = Provider(("DbA", postgres.ConnectionString), ("DbB", other));

        await provider.CreateStore(Outbox("First", "DbA", $"{schema}.outbox")).ValidateAsync(Ct);
        var ex = await Assert.ThrowsAsync<OutboxValidationException>(
            () => provider.CreateStore(Outbox("Second", "DbB", $"{schema}.outbox")).ValidateAsync(Ct));

        Assert.Contains("same table as outbox 'First'", ex.Message, StringComparison.Ordinal);
        Assert.StartsWith("Outboxes[Second]:", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Same_table_twice_in_one_provider_fails_second_outbox()
    {
        var schema = PostgreSqlFixture.NewSchema();
        await postgres.CreateOutboxSchemaAsync(schema);
        var provider = Provider(("Db", postgres.ConnectionString));

        await provider.CreateStore(Outbox("First", "Db", $"{schema}.outbox")).ValidateAsync(Ct);
        var ex = await Assert.ThrowsAsync<OutboxValidationException>(
            () => provider.CreateStore(Outbox("Second", "Db", $"{schema}.outbox")).ValidateAsync(Ct));

        Assert.Equal($"Outboxes[Second]: table '{schema}.outbox' same table as outbox 'First'", ex.Message);
    }

    [Fact]
    public async Task Same_table_with_and_without_database_setting_fails_second_outbox()
    {
        var schema = PostgreSqlFixture.NewSchema();
        await postgres.CreateOutboxSchemaAsync(schema);
        var builder = new NpgsqlConnectionStringBuilder(postgres.ConnectionString);
        Assert.Equal(builder.Username, builder.Database);
        builder.Remove("Database");
        var provider = Provider(("WithDatabase", postgres.ConnectionString), ("WithoutDatabase", builder.ConnectionString));

        await provider.CreateStore(Outbox("First", "WithDatabase", $"{schema}.outbox")).ValidateAsync(Ct);
        var ex = await Assert.ThrowsAsync<OutboxValidationException>(
            () => provider.CreateStore(Outbox("Second", "WithoutDatabase", $"{schema}.outbox")).ValidateAsync(Ct));

        Assert.Equal($"Outboxes[Second]: table '{schema}.outbox' same table as outbox 'First'", ex.Message);
    }

    [Fact]
    public async Task Two_tables_with_colliding_lock_keys_fail_second_outbox()
    {
        var schema = PostgreSqlFixture.NewSchema();
        await postgres.CreateOutboxSchemaAsync(schema);
        var names = await postgres.ScalarAsync<string[]>(
            $"""
            SELECT array_agg(name ORDER BY name) FROM (
                SELECT 't' || i AS name, hashtext(format('%I.%I', '{schema}', 't' || i)) AS key
                FROM generate_series(1, 300000) AS i) AS candidates
            GROUP BY key HAVING count(*) > 1 LIMIT 1
            """);
        Assert.NotNull(names);
        var (first, second) = (names[0], names[1]);
        await postgres.ExecuteAsync($"CREATE TABLE {schema}.{first} (LIKE {schema}.outbox); CREATE TABLE {schema}.{second} (LIKE {schema}.outbox);");
        var provider = Provider(("Db", postgres.ConnectionString));

        await provider.CreateStore(Outbox("First", "Db", $"{schema}.{first}")).ValidateAsync(Ct);
        var ex = await Assert.ThrowsAsync<OutboxValidationException>(
            () => provider.CreateStore(Outbox("Second", "Db", $"{schema}.{second}")).ValidateAsync(Ct));

        Assert.Equal($"Outboxes[Second]: table '{schema}.{second}' shares its advisory lock key with outbox 'First'", ex.Message);
    }

    [Fact]
    public async Task Standby_fails_validation_and_session_open()
    {
        var schema = await postgres.CreateOutboxAsync();
        var store = (PostgreSqlOutboxStore)Provider(("Db", postgres.ConnectionString)).CreateStore(Outbox("Orders", "Db", $"{schema}.outbox"));
        var validated = await store.ValidateAsync(Ct);

        store.InRecovery = _ => true;

        var open = await Assert.ThrowsAsync<OutboxDatabaseUnreachableException>(() => validated.OpenSessionAsync(Ct));
        Assert.Equal("Outboxes[Orders]: database unreachable (read-only standby)", open.Message);
        var validating = (PostgreSqlOutboxStore)Provider(("Db", postgres.ConnectionString)).CreateStore(Outbox("Orders", "Db", $"{schema}.outbox"));
        validating.InRecovery = _ => true;
        var validate = await Assert.ThrowsAsync<OutboxValidationException>(() => validating.ValidateAsync(Ct));
        Assert.Equal("Outboxes[Orders]: database is a read-only standby", validate.Message);
    }

    [Fact]
    public async Task Search_path_function_does_not_shadow_catalog_calls()
    {
        var schema = await postgres.CreateOutboxAsync();
        var stub = PostgreSqlFixture.NewSchema();
        await postgres.ExecuteAsync(
            $"""
            CREATE SCHEMA {stub};
            CREATE FUNCTION {stub}.pg_is_in_recovery() RETURNS boolean LANGUAGE sql AS 'SELECT true';
            CREATE FUNCTION {stub}.pg_try_advisory_lock(integer, integer) RETURNS boolean LANGUAGE sql AS 'SELECT true';
            CREATE FUNCTION {stub}.hashtext(text) RETURNS integer LANGUAGE sql AS 'SELECT 1';
            """);
        var connection = new NpgsqlConnectionStringBuilder(postgres.ConnectionString) { SearchPath = $"{stub},pg_catalog" }.ConnectionString;
        var store = Provider(("Db", connection)).CreateStore(Outbox("Orders", "Db", $"{schema}.outbox"));

        var validated = await store.ValidateAsync(Ct);
        await using var session = await validated.OpenSessionAsync(Ct);
        Assert.True(await session.TryLockAsync(Ct));
        await session.CheckAsync(Ct);

        Assert.NotNull(await postgres.FindLockHolderPidAsync($"{schema}.outbox"));
    }

    [Fact]
    public async Task Search_path_type_does_not_shadow_column_check()
    {
        var schema = await postgres.CreateOutboxAsync();
        var stub = PostgreSqlFixture.NewSchema();
        await postgres.ExecuteAsync(
            $"""
            CREATE SCHEMA {stub};
            CREATE DOMAIN {stub}.text AS pg_catalog.text;
            """);
        var connection = new NpgsqlConnectionStringBuilder(postgres.ConnectionString) { SearchPath = $"{stub},pg_catalog" }.ConnectionString;

        await Provider(("Db", connection)).CreateStore(Outbox("Orders", "Db", $"{schema}.outbox")).ValidateAsync(Ct);
    }

    [Fact]
    public async Task Expected_type_names_match_catalog()
    {
        foreach (var column in OutboxColumnCheck.V001)
        {
            Assert.Equal(column.Type, await postgres.ScalarAsync<string>($"SELECT pg_catalog.format_type({column.TypeOid}, NULL)"));
        }
    }

    [Fact]
    public async Task Rejected_password_fails_validation_as_invalid_setting()
    {
        var schema = await postgres.CreateOutboxAsync();
        var connection = new NpgsqlConnectionStringBuilder(postgres.ConnectionString) { Password = "wrong-secret-4c1d" }.ConnectionString;
        var provider = Provider(("Db", connection));

        var ex = await Assert.ThrowsAsync<OutboxValidationException>(
            () => provider.CreateStore(Outbox("Orders", "Db", $"{schema}.outbox")).ValidateAsync(Ct));

        Assert.Equal($"Outboxes[Orders]: table '{schema}.outbox' could not be validated (PostgresException 28P01)", ex.Message);
        Assert.DoesNotContain("wrong-secret", ex.Message, StringComparison.Ordinal);
        Assert.Null(ex.InnerException);
    }

    [Fact]
    public async Task Catalog_query_error_fails_validation_as_invalid_setting()
    {
        var provider = Provider(("Db", postgres.ConnectionString));

        var ex = await Assert.ThrowsAsync<OutboxValidationException>(
            () => provider.CreateStore(Outbox("Orders", "Db", "orders.public.outbox")).ValidateAsync(Ct));

        Assert.Equal("Outboxes[Orders]: table 'orders.public.outbox' could not be validated (PostgresException 0A000)", ex.Message);
    }

    [Fact]
    public async Task Caller_cancellation_surfaces_as_plain_cancellation()
    {
        await using var container = new PostgreSqlBuilder("postgres:17.11").Build();
        await container.StartAsync(Ct);
        var schema = PostgreSqlFixture.NewSchema();
        await PostgreSqlFixture.CreateOutboxSchemaAsync(container.GetConnectionString(), schema);
        var store = Provider(("Db", container.GetConnectionString())).CreateStore(Outbox("Orders", "Db", $"{schema}.outbox"));
        var validated = await store.ValidateAsync(Ct);
        await container.PauseAsync(Ct);

        var open = await CancelledAfterAsync(ct => validated.OpenSessionAsync(ct));
        var validate = await CancelledAfterAsync(ct => store.ValidateAsync(ct));
        await container.UnpauseAsync(Ct);

        foreach (var ex in new[] { open, validate })
        {
            Assert.Equal(typeof(OperationCanceledException), ex.GetType());
            Assert.Null(ex.InnerException);
        }
    }

    [Fact]
    public async Task View_fails_as_not_a_table()
    {
        var schema = PostgreSqlFixture.NewSchema();
        await postgres.CreateOutboxSchemaAsync(schema);
        await postgres.ExecuteAsync($"CREATE VIEW {schema}.outbox_view AS SELECT * FROM {schema}.outbox");
        var provider = Provider(("Db", postgres.ConnectionString));

        var ex = await Assert.ThrowsAsync<OutboxValidationException>(
            () => provider.CreateStore(Outbox("Orders", "Db", $"{schema}.outbox_view")).ValidateAsync(Ct));

        Assert.Equal($"Outboxes[Orders]: table '{schema}.outbox_view' not a table", ex.Message);
    }

    [Fact]
    public async Task Unreachable_database_fails_fast_without_host_in_message()
    {
        var provider = Provider(("Db", "Host=127.0.0.1;Port=1;Database=orders;Username=relay;Password=secret"));
        var store = provider.CreateStore(Outbox("Orders", "Db", "outbox"));
        var stopwatch = Stopwatch.StartNew();

        var ex = await Assert.ThrowsAsync<OutboxDatabaseUnreachableException>(() => store.ValidateAsync(Ct));

        Assert.True(stopwatch.Elapsed < TimeSpan.FromSeconds(6), $"took {stopwatch.Elapsed}");
        Assert.StartsWith("Outboxes[Orders]: database unreachable (", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("127.0.0.1", ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("secret", ex.Message, StringComparison.Ordinal);
        Assert.Null(ex.InnerException);
    }

    private static async Task<OperationCanceledException> CancelledAfterAsync(Func<CancellationToken, Task> action)
    {
        using var cts = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var running = action(cts.Token);
        await Task.Delay(500, Ct);
        var elapsed = Stopwatch.StartNew();
        await cts.CancelAsync();
        var ex = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => running);
        Assert.True(elapsed.Elapsed < TimeSpan.FromSeconds(1), $"cancellation took {elapsed.Elapsed}");
        Assert.Equal(cts.Token, ex.CancellationToken);
        return ex;
    }

    internal static PostgreSqlOutboxStoreProvider Provider(params (string Name, string Value)[] connections) =>
        new(new RelayConfiguration(new ConfigurationBuilder()
            .AddInMemoryCollection(connections.Select(c => new KeyValuePair<string, string?>($"ConnectionStrings:{c.Name}", c.Value)))
            .Build()));

    internal static OutboxOptions Outbox(string name, string connection, string table, OrderingMode mode = OrderingMode.None) => new()
    {
        Name = name,
        Destination = "Topic",
        Database = new DatabaseOptions { Provider = PostgreSqlOutboxStoreProvider.ProviderName, Connection = connection, Table = table },
        Ordering = new OrderingOptions { Mode = mode },
    };
}
