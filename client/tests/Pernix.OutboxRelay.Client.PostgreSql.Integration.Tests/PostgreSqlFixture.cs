using Pernix.OutboxRelay.Client.PostgreSql.Integration.Tests;
using Npgsql;
using Testcontainers.PostgreSql;
using Xunit;

[assembly: AssemblyFixture(typeof(PostgreSqlFixture))]

namespace Pernix.OutboxRelay.Client.PostgreSql.Integration.Tests;

public sealed class PostgreSqlFixture : IAsyncLifetime
{
    public const string Image = "postgres:17.11";

    private readonly PostgreSqlContainer container = TestContainer.PostgreSql(Image).Build();

    public string ConnectionString => container.GetConnectionString();

    public async ValueTask InitializeAsync() => await container.StartBoundedAsync();

    public async ValueTask DisposeAsync() => await container.DisposeAsync();

    public static string NewSchema() => $"s_{Guid.NewGuid():N}";

    // New schema with one outbox table created from the package DDL only.
    public async Task<OutboxTable> CreateOutboxAsync(string name = "outbox", string? namePrefix = null)
    {
        var schema = NewSchema();
        await ExecuteAsync($"CREATE SCHEMA {schema}");
        var table = OutboxTable.Parse($"{schema}.{name}");
        await ApplyScriptsAsync(ConnectionString, table, namePrefix);
        return table;
    }

    public Task ExecuteAsync(string sql) => ExecuteAsync(ConnectionString, sql);

    public static async Task ApplyScriptsAsync(string connectionString, OutboxTable table, string? namePrefix = null)
    {
        foreach (var script in PostgreSqlOutboxSchema.Scripts(table, namePrefix))
        {
            await ExecuteAsync(connectionString, script.Sql);
        }
    }

    public static async Task ExecuteAsync(string connectionString, string sql)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    public static async Task<List<string>> QueryStringsAsync(string connectionString, string sql)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync();
        var values = new List<string>();
        while (await reader.ReadAsync())
        {
            values.Add(reader.GetString(0));
        }

        return values;
    }
}
