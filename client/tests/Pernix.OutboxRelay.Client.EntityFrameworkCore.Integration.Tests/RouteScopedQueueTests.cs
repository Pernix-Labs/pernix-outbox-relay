using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Xunit;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore.Integration.Tests;

public sealed class RouteScopedQueueTests(EfPostgreSqlFixture postgres)
{
    private const string Alerts = "alerts";
    private const string Audit = "audit";

    private const string E4 =
        "The context's transaction came from Database.UseTransaction. The outbox queue writes only in transactions the context begins with Database.BeginTransaction[Async]. Call IOutboxPublisher.PublishAsync(DbContext, ...) after the business writes instead.";

    private const string E8 =
        "The context's execution strategy retries on failure and the context has no current EF transaction, so a retry could commit the business writes without this message. The message was not queued. Enqueue inside the operation passed to Database.CreateExecutionStrategy().ExecuteAsync, after Database.BeginTransactionAsync.";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public enum Registration
    {
        AddDbContext,
        AddDbContextPool,
    }

    [Fact]
    public async Task T17_null_route_is_stamped_and_equal_route_is_queued_unchanged()
    {
        var schema = await postgres.CreateSchemaAsync();
        await using var provider = Provider(services => services.AddDbContext<ItemsDbContext>(options => schema.Configure(options)));

        await using (var scope = provider.CreateAsyncScope())
        {
            var keyed = scope.ServiceProvider.GetRequiredKeyedService<IOutboxQueue<ItemsDbContext>>(Alerts);
            var context = scope.ServiceProvider.GetRequiredService<ItemsDbContext>();
            var equal = OutboxMessage.Create("k", "equal").WithRoute(new string(Alerts.AsSpan()));

            keyed.Enqueue(OutboxMessage.Create("k", "null"));
            keyed.Enqueue(equal);

            var queued = Queue(context).Messages;
            Assert.Equal(Alerts, queued[0].Route);
            Assert.Same(equal.Route, queued[1].Route);

            context.Items.Add(new Item { Name = "t17" });
            await EfUnitOfWorkShape.CommitAsync(context, Ct);
        }

        Assert.Equal([("null", Alerts), ("equal", Alerts)], await OutboxRouteRows.ReadAsync(schema));
        Assert.Empty(schema.OutboxLogs());
    }

    [Fact]
    public async Task T17_different_route_throws_CE13_and_queues_nothing()
    {
        var schema = await postgres.CreateSchemaAsync();
        await using var provider = Provider(services => services.AddDbContext<ItemsDbContext>(options => schema.Configure(options)));

        await using (var scope = provider.CreateAsyncScope())
        {
            var keyed = scope.ServiceProvider.GetRequiredKeyedService<IOutboxQueue<ItemsDbContext>>(Alerts);
            var context = scope.ServiceProvider.GetRequiredService<ItemsDbContext>();
            keyed.Enqueue(OutboxMessage.Create("secret-key", "before"));

            var thrown = Assert.Throws<ArgumentException>(() => keyed.Enqueue(OutboxMessage.Create("secret-key", "secret-payload").WithRoute(Audit)));

            Assert.Equal("message", thrown.ParamName);
            Assert.StartsWith("Message route 'audit' differs from scoped route 'alerts'.", thrown.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("secret", thrown.Message, StringComparison.Ordinal);
            Assert.Equal(1, Queue(context).Count);

            context.Items.Add(new Item { Name = "t17" });
            await EfUnitOfWorkShape.CommitAsync(context, Ct);
        }

        Assert.Equal([("before", Alerts)], await OutboxRouteRows.ReadAsync(schema));
        Assert.Equal(["t17"], await schema.ItemNamesAsync());
    }

    [Fact]
    public async Task T17_two_keyed_queues_in_one_scope_flush_both_rows_in_enqueue_order_in_one_flush()
    {
        var schema = await postgres.CreateSchemaAsync();
        var publisher = new RecordingPublisher(schema.Publisher);
        await using var provider = Provider(services => services.AddDbContext<ItemsDbContext>(options => schema.Configure(options, publisher)));

        await using (var scope = provider.CreateAsyncScope())
        {
            var alerts = scope.ServiceProvider.GetRequiredKeyedService<IOutboxQueue<ItemsDbContext>>(Alerts);
            var audit = scope.ServiceProvider.GetRequiredKeyedService<IOutboxQueue<ItemsDbContext>>(Audit);
            var context = scope.ServiceProvider.GetRequiredService<ItemsDbContext>();

            alerts.Enqueue(OutboxMessage.Create("k", "a0"));
            audit.Enqueue(OutboxMessage.Create("k", "b0"));
            alerts.Enqueue(OutboxMessage.Create("k", "a1"));

            context.Items.Add(new Item { Name = "t17" });
            await EfUnitOfWorkShape.CommitAsync(context, Ct);
        }

        Assert.Equal([3], publisher.BatchSizes);
        Assert.Equal([("a0", Alerts), ("b0", Audit), ("a1", Alerts)], await OutboxRouteRows.ReadAsync(schema));
        var itemTx = Assert.Single(await schema.QueryAsync<string>($"SELECT tx::text FROM {schema.Name}.items"));
        Assert.All(await schema.QueryAsync<string>($"SELECT tx_id::text FROM {schema.OutboxTable}"), tx => Assert.Equal(itemTx, tx));
    }

    [Theory]
    [InlineData(Registration.AddDbContext)]
    [InlineData(Registration.AddDbContextPool)]
    public async Task T17_keyed_queue_targets_the_scope_context_instance(Registration registration)
    {
        var schema = await postgres.CreateSchemaAsync();
        await using var provider = Provider(services =>
        {
            if (registration == Registration.AddDbContext)
            {
                services.AddDbContext<ItemsDbContext>(options => schema.Configure(options));
            }
            else
            {
                services.AddDbContextPool<ItemsDbContext>(options => schema.Configure(options));
            }
        });

        await using (var scope = provider.CreateAsyncScope())
        {
            var keyed = scope.ServiceProvider.GetRequiredKeyedService<IOutboxQueue<ItemsDbContext>>(Alerts);
            keyed.Enqueue(OutboxMessage.Create("k", "keyed"));

            var context = scope.ServiceProvider.GetRequiredService<ItemsDbContext>();
            Assert.Equal(1, Queue(context).Count);
            Assert.Same(keyed, scope.ServiceProvider.GetRequiredKeyedService<IOutboxQueue<ItemsDbContext>>(Alerts));

            context.Items.Add(new Item { Name = "keyed" });
            await EfUnitOfWorkShape.CommitAsync(context, Ct);
        }

        await using (var scope = provider.CreateAsyncScope())
        {
            var keyed = scope.ServiceProvider.GetRequiredKeyedService<IOutboxQueue<ItemsDbContext>>(Alerts);
            var context = scope.ServiceProvider.GetRequiredService<ItemsDbContext>();
            Assert.Equal(0, Queue(context).Count);

            keyed.Enqueue(OutboxMessage.Create("k", "second-scope"));
            Assert.Equal(1, Queue(context).Count);

            context.Items.Add(new Item { Name = "second-scope" });
            await EfUnitOfWorkShape.CommitAsync(context, Ct);
        }

        Assert.Equal([("keyed", Alerts), ("second-scope", Alerts)], await OutboxRouteRows.ReadAsync(schema));
        Assert.Equal(["keyed", "second-scope"], await schema.ItemNamesAsync());
    }

    [Fact]
    public async Task T17_non_keyed_queue_works_next_to_keyed_queues()
    {
        var schema = await postgres.CreateSchemaAsync();
        await using var provider = Provider(services =>
        {
            services.AddDbContext<ItemsDbContext>(options => schema.Configure(options));
            services.AddOutboxQueue<ItemsDbContext>();
        });

        await using (var scope = provider.CreateAsyncScope())
        {
            var plain = scope.ServiceProvider.GetRequiredService<IOutboxQueue<ItemsDbContext>>();
            var keyed = scope.ServiceProvider.GetRequiredKeyedService<IOutboxQueue<ItemsDbContext>>(Alerts);
            var context = scope.ServiceProvider.GetRequiredService<ItemsDbContext>();
            Assert.NotSame(plain, keyed);

            plain.Enqueue(OutboxMessage.Create("k", "plain"));
            keyed.Enqueue(OutboxMessage.Create("k", "keyed"));
            plain.Enqueue(OutboxMessage.Create("k", "plain-routed").WithRoute(Audit));

            context.Items.Add(new Item { Name = "mixed" });
            await EfUnitOfWorkShape.CommitAsync(context, Ct);
        }

        Assert.Equal([("plain", null), ("keyed", Alerts), ("plain-routed", Audit)], await OutboxRouteRows.ReadAsync(schema));
    }

    [Fact]
    public async Task T17_keyed_queue_throws_E1_for_a_context_without_UseOutbox()
    {
        var schema = await postgres.CreateSchemaAsync();
        await using var provider = Provider(services => services.AddDbContext<ItemsDbContext>(options => schema.ConfigureWithoutOutbox(options)));
        await using var scope = provider.CreateAsyncScope();
        var keyed = scope.ServiceProvider.GetRequiredKeyedService<IOutboxQueue<ItemsDbContext>>(Alerts);

        var thrown = Assert.Throws<InvalidOperationException>(() => keyed.Enqueue(OutboxMessage.Create("k", "e1")));

        Assert.Equal(
            "DbContext type 'ItemsDbContext' has no outbox queue. Call UseOutbox(publisher) on its DbContextOptionsBuilder.",
            thrown.Message);
    }

    [Fact]
    public async Task T17_keyed_queue_throws_E4_under_UseTransaction_and_queues_nothing()
    {
        var schema = await postgres.CreateSchemaAsync();
        await using var connection = new NpgsqlConnection(schema.ConnectionString);
        await connection.OpenAsync(Ct);
        await using var dbTransaction = await connection.BeginTransactionAsync(Ct);
        await using var provider = Provider(services => services.AddDbContext<ItemsDbContext>(options => schema.Configure(options, connection)));
        await using var scope = provider.CreateAsyncScope();
        var keyed = scope.ServiceProvider.GetRequiredKeyedService<IOutboxQueue<ItemsDbContext>>(Alerts);
        var context = scope.ServiceProvider.GetRequiredService<ItemsDbContext>();
        await context.Database.UseTransactionAsync(dbTransaction, Ct);

        var thrown = Assert.Throws<InvalidOperationException>(() => keyed.Enqueue(OutboxMessage.Create("k", "e4")));

        Assert.Equal(E4, thrown.Message);
        Assert.Equal(0, Queue(context).Count);
    }

    [Fact]
    public async Task T17_keyed_queue_throws_E8_under_retrying_strategy_without_transaction_and_queues_nothing()
    {
        var schema = await postgres.CreateSchemaAsync();
        await using var provider = Provider(services =>
            services.AddDbContext<ItemsDbContext>(options => schema.Configure(options, npgsql: static npgsql => npgsql.EnableRetryOnFailure())));
        await using var scope = provider.CreateAsyncScope();
        var keyed = scope.ServiceProvider.GetRequiredKeyedService<IOutboxQueue<ItemsDbContext>>(Alerts);
        var context = scope.ServiceProvider.GetRequiredService<ItemsDbContext>();

        var thrown = Assert.Throws<InvalidOperationException>(() => keyed.Enqueue(OutboxMessage.Create("k", "e8")));

        Assert.Equal(E8, thrown.Message);
        Assert.Equal(0, Queue(context).Count);
    }

    private static ServiceProvider Provider(Action<IServiceCollection> registerContext)
    {
        var services = new ServiceCollection();
        registerContext(services);
        services.AddKeyedOutboxQueue<ItemsDbContext>(Alerts, Alerts);
        services.AddKeyedOutboxQueue<ItemsDbContext>(Audit, Audit);
        return services.BuildServiceProvider();
    }

    private static OutboxQueue Queue(DbContext context) => Assert.IsType<OutboxQueue>(OutboxQueue.Find(context));
}
