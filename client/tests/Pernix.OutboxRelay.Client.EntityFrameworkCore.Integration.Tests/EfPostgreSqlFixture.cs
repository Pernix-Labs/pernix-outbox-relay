using System.Data.Common;
using Pernix.OutboxRelay.Client.EntityFrameworkCore;
using Pernix.OutboxRelay.Client.EntityFrameworkCore.Integration.Tests;
using Pernix.OutboxRelay.Client.PostgreSql;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.Logging;
using Npgsql;
using Npgsql.EntityFrameworkCore.PostgreSQL.Infrastructure;
using Testcontainers.PostgreSql;
using Xunit;

[assembly: AssemblyFixture(typeof(EfPostgreSqlFixture))]

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore.Integration.Tests;

// One logger factory per fixture: it is part of EF's internal service provider key.
public sealed class EfPostgreSqlFixture : IAsyncLifetime
{
    public const string Image = "postgres:17.11";

    private readonly PostgreSqlContainer container = TestContainer.PostgreSql(Image).Build();

    public EfPostgreSqlFixture()
    {
        LoggerFactory = Microsoft.Extensions.Logging.LoggerFactory.Create(builder => builder.AddProvider(Logs));
    }

    public string ConnectionString => container.GetConnectionString();

    public CapturingLoggerProvider Logs { get; } = new();

    public ILoggerFactory LoggerFactory { get; }

    public async ValueTask InitializeAsync() => await container.StartBoundedAsync();

    public async ValueTask DisposeAsync()
    {
        LoggerFactory.Dispose();
        await container.DisposeAsync();
    }
    public async Task<EfTestSchema> CreateSchemaAsync(string outboxName = "outbox")
    {
        var schema = $"s_{Guid.NewGuid():N}";
        var table = OutboxTable.Parse($"{schema}.{outboxName}");

        await ExecuteAsync(ConnectionString, $"""
            CREATE SCHEMA {schema};
            CREATE SEQUENCE {schema}.write_seq;
            CREATE TABLE {schema}.items (
                id serial PRIMARY KEY,
                name text NOT NULL UNIQUE,
                tx xid8 NOT NULL DEFAULT pg_current_xact_id(),
                seq bigint NOT NULL DEFAULT nextval('{schema}.write_seq'));
            """);

        foreach (var script in PostgreSqlOutboxSchema.Scripts(table))
        {
            await ExecuteAsync(ConnectionString, script.Sql);
        }

        await ExecuteAsync(ConnectionString, $"""
            CREATE TABLE {schema}.outbox_seq (id bigint PRIMARY KEY, seq bigint NOT NULL);
            CREATE FUNCTION {schema}.outbox_seq_probe() RETURNS trigger LANGUAGE plpgsql AS $$
            BEGIN
                INSERT INTO {schema}.outbox_seq (id, seq) VALUES (NEW.id, nextval('{schema}.write_seq'));
                RETURN NULL;
            END $$;
            CREATE TRIGGER outbox_seq_probe AFTER INSERT ON {table} FOR EACH ROW EXECUTE FUNCTION {schema}.outbox_seq_probe();
            """);

        // A unique search path is a new Npgsql pool whose idle connections outlive the test, so they would exhaust max_connections.
        var connectionString = new NpgsqlConnectionStringBuilder(ConnectionString) { SearchPath = schema, Pooling = false }.ConnectionString;
        return new EfTestSchema(this, schema, connectionString, table);
    }

    public static async Task ExecuteAsync(string connectionString, string sql)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }

    public static async Task<List<T>> QueryAsync<T>(string connectionString, string sql)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection);
        await using var reader = await command.ExecuteReaderAsync();
        var values = new List<T>();
        while (await reader.ReadAsync())
        {
            values.Add(reader.GetFieldValue<T>(0));
        }

        return values;
    }
}

public sealed class EfTestSchema
{
    private readonly EfPostgreSqlFixture fixture;

    internal EfTestSchema(EfPostgreSqlFixture fixture, string name, string connectionString, OutboxTable outboxTable)
    {
        this.fixture = fixture;
        Name = name;
        ConnectionString = connectionString;
        OutboxTable = outboxTable;
        Publisher = new PostgreSqlOutboxPublisher(outboxTable);
    }

    public string Name { get; }

    public string ConnectionString { get; }

    public OutboxTable OutboxTable { get; }

    public PostgreSqlOutboxPublisher Publisher { get; }

    public CapturingLoggerProvider Logs => fixture.Logs;

    public DbContextOptionsBuilder Configure(
        DbContextOptionsBuilder builder,
        IOutboxPublisher? publisher = null,
        Action<NpgsqlDbContextOptionsBuilder>? npgsql = null) =>
        ConfigureWithoutOutbox(builder, npgsql).UseOutbox(publisher ?? Publisher);

    public DbContextOptionsBuilder Configure(DbContextOptionsBuilder builder, DbConnection connection, IOutboxPublisher? publisher = null) =>
        ConfigureWithoutOutbox(builder, connection).UseOutbox(publisher ?? Publisher);

    public DbContextOptionsBuilder ConfigureWithoutOutbox(DbContextOptionsBuilder builder, DbConnection connection) =>
        builder.UseNpgsql(connection).UseLoggerFactory(fixture.LoggerFactory);

    public DbContextOptionsBuilder ConfigureWithoutOutbox(
        DbContextOptionsBuilder builder,
        Action<NpgsqlDbContextOptionsBuilder>? npgsql = null) =>
        builder.UseNpgsql(ConnectionString, npgsql).UseLoggerFactory(fixture.LoggerFactory);

    public ItemsDbContext CreateContext(IOutboxPublisher? publisher = null) =>
        new((DbContextOptions<ItemsDbContext>)Configure(new DbContextOptionsBuilder<ItemsDbContext>(), publisher).Options);

    public ItemsDbContext CreateContextWithoutOutbox() =>
        new((DbContextOptions<ItemsDbContext>)ConfigureWithoutOutbox(new DbContextOptionsBuilder<ItemsDbContext>()).Options);

    public IReadOnlyList<CapturedLogEntry> OutboxLogs() => fixture.Logs.ForOutboxTable(OutboxTable);

    public Task ExecuteAsync(string sql) => EfPostgreSqlFixture.ExecuteAsync(ConnectionString, sql);

    public Task<List<T>> QueryAsync<T>(string sql) => EfPostgreSqlFixture.QueryAsync<T>(ConnectionString, sql);

    public async Task<List<string>> OutboxPayloadsAsync() =>
        await QueryAsync<string>($"SELECT convert_from(payload, 'UTF8') FROM {OutboxTable} ORDER BY id");

    public async Task<List<string>> ItemNamesAsync() =>
        await QueryAsync<string>($"SELECT name FROM {Name}.items ORDER BY id");
}
