using System.Text;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.PostgreSql;
using DotNet.Testcontainers.Builders;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Npgsql;
using Testcontainers.PostgreSql;

namespace Pernix.OutboxRelay.Benchmarks.Infrastructure;

// One PostgreSQL per process, created on first use. The connection string never leaves this class.
internal sealed class PgEnvironment : IAsyncDisposable
{
    public const string ConnectionVariable = "OUTBOX_BENCH_CONNECTION";
    public const string Image = "postgres:17.11";
    public const string DockerMissing = "PostgreSQL needed: set OUTBOX_BENCH_CONNECTION or start Docker for Testcontainers";

    private const string ConnectionName = "Bench";
    private static readonly Lock Gate = new();
    private static Task<PgEnvironment>? current;

    private readonly string connectionString;
    private readonly PostgreSqlContainer? container;
    private readonly PostgreSqlOutboxStoreProvider provider;
    private readonly Lazy<string> script = new(ReadMigrations);

    private PgEnvironment(string connectionString, PostgreSqlContainer? container)
    {
        this.connectionString = connectionString;
        this.container = container;
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { [$"ConnectionStrings:{ConnectionName}"] = connectionString })
            .Build();
        provider = new PostgreSqlOutboxStoreProvider(new RelayConfiguration(configuration));
    }

    public static string Describe() =>
        string.IsNullOrEmpty(Environment.GetEnvironmentVariable(ConnectionVariable)) ? $"testcontainers {Image}" : "env";

    public static Task<PgEnvironment> GetAsync()
    {
        lock (Gate)
        {
            return current ??= CreateAsync();
        }
    }

    public static async ValueTask DisposeCurrentAsync()
    {
        Task<PgEnvironment>? task;
        lock (Gate)
        {
            task = current;
            current = null;
        }

        if (task is { IsCompletedSuccessfully: true })
        {
            await task.Result.DisposeAsync();
        }
    }

    public static async Task<PostgreSqlContainer> StartContainerAsync()
    {
        PostgreSqlContainer container;
        try
        {
            container = new PostgreSqlBuilder(Image).WithLogger(NullLogger.Instance).Build();
        }
        catch (DockerUnavailableException)
        {
            throw new InvalidOperationException(DockerMissing);
        }

        try
        {
            await container.StartAsync();
        }
        catch (DockerUnavailableException)
        {
            await container.DisposeAsync();
            throw new InvalidOperationException(DockerMissing);
        }

        return container;
    }

    public async Task<string> CreateSchemaAsync()
    {
        var schema = $"bench_{Guid.NewGuid():N}";
        await ExecuteAsync($"CREATE SCHEMA {schema}; SET search_path TO {schema};\n{script.Value}");
        return schema;
    }

    public Task SeedAsync(string schema, long first, int count, int headerCount = 1)
    {
        var headers = headerCount switch
        {
            0 => "'{}'::jsonb",
            1 => "jsonb_build_object('idempotency-key', g::text)",
            3 => "jsonb_build_object('idempotency-key', g::text, 'traceparent', '00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01', 'content-type', 'application/json')",
            _ => throw new ArgumentOutOfRangeException(nameof(headerCount), headerCount, "seed header count must be 0, 1 or 3"),
        };

        return ExecuteAsync($"""
            INSERT INTO {schema}.outbox (message_key, payload, headers)
            SELECT convert_to('k' || (g % {Workload.Keys}), 'UTF8'), convert_to(g::text, 'UTF8'), {headers}
            FROM generate_series({first}, {first + count - 1}) g ORDER BY g
            """);
    }

    public async Task ExecuteAsync(string sql)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(sql, connection) { CommandTimeout = 300 };
        await command.ExecuteNonQueryAsync();
    }

    // Backends in this database under the application name the relay session connection uses.
    public async Task<long> CountRelayBackendsAsync()
    {
        var name = new NpgsqlConnectionStringBuilder(PostgreSqlConnectionStrings.ForSession(connectionString)).ApplicationName;
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            "SELECT count(*) FROM pg_stat_activity WHERE datname = current_database() AND application_name = @name", connection);
        command.Parameters.AddWithValue("name", name ?? string.Empty);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    public Task<IValidatedOutboxStore> ValidatedStoreAsync(string schema, OrderingMode mode)
    {
        var store = provider.CreateStore(new OutboxOptions
        {
            Name = $"bench-{Guid.NewGuid():N}",
            Database = new DatabaseOptions { Provider = PostgreSqlOutboxStoreProvider.ProviderName, Connection = ConnectionName, Table = $"{schema}.outbox" },
            Ordering = new OrderingOptions { Mode = mode },
        });
        return store.ValidateAsync(CancellationToken.None);
    }

    public async Task<IOutboxSession> OpenSessionAsync(string schema, OrderingMode mode)
    {
        var store = await ValidatedStoreAsync(schema, mode);
        var session = await store.OpenSessionAsync(CancellationToken.None);
        if (!await session.TryLockAsync(CancellationToken.None))
        {
            await session.DisposeAsync();
            throw new InvalidOperationException($"bench session did not get the lock on {schema}.outbox");
        }

        return session;
    }

    public async ValueTask DisposeAsync()
    {
        if (container is not null)
        {
            await container.DisposeAsync();
        }
    }

    // Every Sql/V*.sql in ordinal name order, as one script.
    private static string ReadMigrations()
    {
        var files = Directory.GetFiles(Path.Combine(AppContext.BaseDirectory, "Sql"), "V*.sql");
        Array.Sort(files, StringComparer.Ordinal);
        if (files.Length == 0)
        {
            throw new InvalidOperationException("no Sql/V*.sql migration next to the benchmark binary");
        }

        var builder = new StringBuilder();
        foreach (var file in files)
        {
            builder.Append(File.ReadAllText(file)).Append('\n');
        }

        return builder.ToString();
    }

    private static async Task<PgEnvironment> CreateAsync()
    {
        var configured = Environment.GetEnvironmentVariable(ConnectionVariable);
        if (!string.IsNullOrEmpty(configured))
        {
            return new PgEnvironment(configured, null);
        }

        var container = await StartContainerAsync();
        return new PgEnvironment(container.GetConnectionString(), container);
    }
}
