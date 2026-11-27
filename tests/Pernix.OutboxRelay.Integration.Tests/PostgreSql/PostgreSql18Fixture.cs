using Npgsql;
using Testcontainers.PostgreSql;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.PostgreSql;

// Used by PostgreSql18OutboxStoreTests only: NOT NULL ... NOT VALID exists from PG18, the shared fixture stays on 17.11.
public sealed class PostgreSql18Fixture : IAsyncLifetime
{
    private readonly PostgreSqlContainer container = TestContainer.PostgreSql("postgres:18.6").Build();

    public string ConnectionString => container.GetConnectionString();

    public async ValueTask InitializeAsync() => await container.StartBoundedAsync();

    public async ValueTask DisposeAsync() => await container.DisposeAsync();

    public async Task ExecuteAsync(string sql)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
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
}
