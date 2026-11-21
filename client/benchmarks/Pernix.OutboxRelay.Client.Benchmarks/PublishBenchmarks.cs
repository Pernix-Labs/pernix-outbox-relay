using System.Text;
using Pernix.OutboxRelay.Client.PostgreSql;
using BenchmarkDotNet.Attributes;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Testcontainers.PostgreSql;

namespace Pernix.OutboxRelay.Client.Benchmarks;

/// <summary>Publish allocation baseline: begin, publish, rollback, minus <see cref="TransactionOnly"/>.</summary>
[MemoryDiagnoser]
public class PublishBenchmarks
{
    public const string ConnectionVariable = "OUTBOX_BENCH_CONNECTION";

    public const string Image = "postgres:17.11";

    private const int BatchSize = 100;

    private PostgreSqlContainer? container;
    private NpgsqlConnection connection = null!;
    private PostgreSqlOutboxPublisher publisher = null!;
    private OutboxMessage noHeaders;
    private OutboxMessage withHeaders;
    private OutboxMessage[] batch = null!;
    private ServiceProvider provider = null!;
    private IOutboxPublisher scopedPublisher = null!;
    private OutboxMessage routed;
    private OutboxMessage[] routedBatch = null!;

    [GlobalSetup]
    public async Task SetupAsync()
    {
        var connectionString = Environment.GetEnvironmentVariable(ConnectionVariable);
        if (string.IsNullOrEmpty(connectionString))
        {
            container = new PostgreSqlBuilder(Image).Build();
            await container.StartAsync();
            connectionString = container.GetConnectionString();
        }

        connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();

        // Own schema per run: safe against a shared OUTBOX_BENCH_CONNECTION database.
        var schema = $"bench_{Guid.NewGuid():N}";
        await ExecuteAsync($"CREATE SCHEMA {schema}");
        var table = OutboxTable.Parse($"{schema}.outbox");
        foreach (var script in PostgreSqlOutboxSchema.Scripts(table))
        {
            await ExecuteAsync(script.Sql);
        }

        publisher = new PostgreSqlOutboxPublisher(table);

        var payload = Encoding.UTF8.GetBytes(/*lang=json*/ """{"orderId":"5f0c2a8e-1b7d-4c3a-9e6f-2d8b4a1c7e90","amount":1250,"currency":"EUR","status":"created"}""");
        var headers = OutboxHeaders.CreateBuilder()
            .Add("type", "order.created")
            .Add("source", "orders-service")
            .Add("content-type", "application/json")
            .Build();

        noHeaders = OutboxMessage.Create("order-1", payload);
        withHeaders = OutboxMessage.Create("order-1", payload, headers);
        batch = new OutboxMessage[BatchSize];
        for (var i = 0; i < BatchSize; i++)
        {
            batch[i] = OutboxMessage.Create($"order-{i}", payload, headers);
        }

        routed = withHeaders.WithRoute("orders");
        routedBatch = new OutboxMessage[BatchSize];
        for (var i = 0; i < BatchSize; i++)
        {
            routedBatch[i] = batch[i].WithRoute(i % 2 == 0 ? "orders" : "audit");
        }

        provider = new ServiceCollection().AddKeyedPostgreSqlOutboxPublisher("orders", table.ToString(), "orders").BuildServiceProvider();
        scopedPublisher = provider.GetRequiredKeyedService<IOutboxPublisher>("orders");
    }

    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        await provider.DisposeAsync();
        await connection.DisposeAsync();
        if (container is not null)
        {
            await container.DisposeAsync();
        }
    }

    [Benchmark(Baseline = true)]
    public async Task TransactionOnly()
    {
        await using var transaction = await connection.BeginTransactionAsync();
        await transaction.RollbackAsync();
    }

    [Benchmark]
    public async Task Single_NoHeaders()
    {
        await using var transaction = await connection.BeginTransactionAsync();
        await publisher.PublishAsync(transaction, noHeaders);
        await transaction.RollbackAsync();
    }

    [Benchmark]
    public async Task Single_WithHeaders()
    {
        await using var transaction = await connection.BeginTransactionAsync();
        await publisher.PublishAsync(transaction, withHeaders);
        await transaction.RollbackAsync();
    }

    [Benchmark]
    public async Task Batch_100()
    {
        await using var transaction = await connection.BeginTransactionAsync();
        await publisher.PublishAsync(transaction, batch);
        await transaction.RollbackAsync();
    }

    [Benchmark]
    public async Task Single_Routed()
    {
        await using var transaction = await connection.BeginTransactionAsync();
        await publisher.PublishAsync(transaction, routed);
        await transaction.RollbackAsync();
    }

    [Benchmark]
    public async Task Batch_100_Routed()
    {
        await using var transaction = await connection.BeginTransactionAsync();
        await publisher.PublishAsync(transaction, routedBatch);
        await transaction.RollbackAsync();
    }

    [Benchmark]
    public async Task Single_Scoped()
    {
        await using var transaction = await connection.BeginTransactionAsync();
        await scopedPublisher.PublishAsync(transaction, withHeaders);
        await transaction.RollbackAsync();
    }

    private async Task ExecuteAsync(string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync();
    }
}
