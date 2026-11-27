using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Npgsql;
using Testcontainers.PostgreSql;
using Xunit;

[assembly: AssemblyFixture(typeof(PostgreSqlFixture))]

namespace Pernix.OutboxRelay.Integration.Tests.PostgreSql;

public sealed class PostgreSqlFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer container = TestContainer.PostgreSql("postgres:17.11").Build();

    public string ConnectionString => container.GetConnectionString();

    public async ValueTask InitializeAsync() => await container.StartBoundedAsync();

    public async ValueTask DisposeAsync() => await container.DisposeAsync();

    public Task CreateOutboxSchemaAsync(string schema) => CreateOutboxSchemaAsync(ConnectionString, schema);

    // For tests that run their own container.
    public static async Task CreateOutboxSchemaAsync(string connectionString, string schema)
    {
        var script = await File.ReadAllTextAsync(Path.Combine(AppContext.BaseDirectory, "Sql", "V001__create_outbox.sql"));
        var quoted = Quote(schema);
        await ExecuteAsync(connectionString, $"CREATE SCHEMA {quoted}; SET search_path TO {quoted};\n{script}");
    }

    public async Task<string> CreateOutboxAsync()
    {
        var schema = NewSchema();
        await CreateOutboxSchemaAsync(schema);
        return schema;
    }

    public Task ExecuteAsync(string sql) => ExecuteAsync(ConnectionString, sql);

    private static async Task ExecuteAsync(string connectionString, string sql)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    public async Task<T?> ScalarAsync<T>(string sql)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        return (T?)await command.ExecuteScalarAsync();
    }

    public async Task<int?> FindLockHolderPidAsync(string qualifiedTable)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            """
            SELECT pid FROM pg_locks
            WHERE locktype = 'advisory' AND granted
              AND classid = hashtext('outbox-relay')::oid
              AND objid = hashtext(@table)::oid
              AND objsubid = 2
            """,
            connection);
        command.Parameters.AddWithValue("table", qualifiedTable);
        return await command.ExecuteScalarAsync() is int pid ? pid : null;
    }

    public async Task<long> CountLockHoldersAsync(string qualifiedTable)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            """
            SELECT count(*) FROM pg_locks
            WHERE locktype = 'advisory' AND granted
              AND classid = hashtext('outbox-relay')::oid
              AND objid = hashtext(@table)::oid
              AND objsubid = 2
            """,
            connection);
        command.Parameters.AddWithValue("table", qualifiedTable);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    public async Task TerminateAsync(int pid)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();

        // Timeout waits until the backend has exited.
        await using var command = new NpgsqlCommand("SELECT pg_terminate_backend(@pid, 5000)", connection);
        command.Parameters.AddWithValue("pid", pid);
        await command.ExecuteScalarAsync();
    }

    public async Task<bool> BackendExistsAsync(int pid)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand("SELECT count(*) FROM pg_stat_activity WHERE pid = @pid", connection);
        command.Parameters.AddWithValue("pid", pid);
        return (long)(await command.ExecuteScalarAsync())! > 0;
    }

    public static string NewSchema() => $"s_{Guid.NewGuid():N}";

    private static string Quote(string identifier) => $"\"{identifier.Replace("\"", "\"\"", StringComparison.Ordinal)}\"";
}
