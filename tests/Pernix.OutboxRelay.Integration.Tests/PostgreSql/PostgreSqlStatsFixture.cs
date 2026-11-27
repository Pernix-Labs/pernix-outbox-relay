using Npgsql;
using Testcontainers.PostgreSql;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.PostgreSql;

// Used by PostgreSqlOutboxStatsTests only: gate lag is cluster-wide. Autovacuum off so no worker xid shows as gate lag.
public sealed class PostgreSqlStatsFixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer container = TestContainer.PostgreSql("postgres:17.11")
        .WithCommand("-c", "max_prepared_transactions=10", "-c", "autovacuum=off")
        .Build();

    public string ConnectionString => container.GetConnectionString();

    public async ValueTask InitializeAsync() => await container.StartBoundedAsync();

    public async ValueTask DisposeAsync() => await container.DisposeAsync();

    public async Task<string> CreateOutboxAsync()
    {
        var schema = PostgreSqlFixture.NewSchema();
        await PostgreSqlFixture.CreateOutboxSchemaAsync(ConnectionString, schema);
        return schema;
    }

    public Task ExecuteAsync(string sql) => ExecuteAsync(ConnectionString, sql);

    public static async Task ExecuteAsync(string connectionString, string sql)
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

    // Flush before ANALYZE, else the insert counts land after it and double its live-tuple count.
    public async Task SeedAndAnalyzeAsync(string seedSql, string table)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        foreach (var sql in new[] { seedSql, "SELECT pg_catalog.pg_stat_force_next_flush()", $"ANALYZE {table}" })
        {
            await using var command = new NpgsqlCommand(sql, connection);
            await command.ExecuteNonQueryAsync();
        }
    }

    public NpgsqlConnectionStringBuilder Builder() => new(ConnectionString);
}
