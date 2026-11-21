using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore.Integration.Tests;

public sealed class RegistrationTests(EfPostgreSqlFixture postgres)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public enum Registration
    {
        AddDbContext,
        AddDbContextPool,
        AddDbContextFactory,
        AddPooledDbContextFactory,
    }

    [Theory]
    [InlineData(Registration.AddDbContext)]
    [InlineData(Registration.AddDbContextPool)]
    [InlineData(Registration.AddDbContextFactory)]
    [InlineData(Registration.AddPooledDbContextFactory)]
    public async Task Every_registration_shape_flushes_with_one_UseOutbox_line(Registration registration)
    {
        var schema = await postgres.CreateSchemaAsync();
        var services = new ServiceCollection();
        Register(services, registration, options => schema.Configure(options));
        await using var provider = services.BuildServiceProvider();

        for (var round = 1; round <= 2; round++)
        {
            await using var scope = provider.CreateAsyncScope();
            var factoryShape = registration is Registration.AddDbContextFactory or Registration.AddPooledDbContextFactory;
            var context = factoryShape
                ? await scope.ServiceProvider.GetRequiredService<IDbContextFactory<ItemsDbContext>>().CreateDbContextAsync(Ct)
                : scope.ServiceProvider.GetRequiredService<ItemsDbContext>();

            context.EnqueueOutbox(OutboxMessage.Create("k", $"message-{round}"));
            context.Items.Add(new Item { Name = $"item-{round}" });
            await EfUnitOfWorkShape.CommitAsync(context, Ct);

            if (factoryShape)
            {
                await context.DisposeAsync();
            }
        }

        Assert.Equal(["item-1", "item-2"], await schema.ItemNamesAsync());
        Assert.Equal(["message-1", "message-2"], await schema.OutboxPayloadsAsync());
        var sameTransaction = await schema.QueryAsync<long>(
            $"SELECT count(*) FROM {schema.OutboxTable} o JOIN {schema.Name}.items i ON i.name = 'item-' || substr(convert_from(o.payload, 'UTF8'), 9) WHERE i.tx = o.tx_id");
        Assert.Equal([2L], sameTransaction);
        Assert.Empty(schema.OutboxLogs());
    }

    [Theory]
    [InlineData(Registration.AddDbContext)]
    [InlineData(Registration.AddDbContextPool)]
    public async Task Facade_targets_the_scope_context_instance(Registration registration)
    {
        var schema = await postgres.CreateSchemaAsync();
        var services = new ServiceCollection();
        Register(services, registration, options => schema.Configure(options));
        services.AddOutboxQueue<ItemsDbContext>();
        await using var provider = services.BuildServiceProvider();

        await using (var scope = provider.CreateAsyncScope())
        {
            var facade = scope.ServiceProvider.GetRequiredService<IOutboxQueue<ItemsDbContext>>();
            facade.Enqueue(OutboxMessage.Create("k", "facade"));

            var context = scope.ServiceProvider.GetRequiredService<ItemsDbContext>();
            Assert.Equal(1, Assert.IsType<OutboxQueue>(OutboxQueue.Find(context)).Count);
            Assert.Same(facade, scope.ServiceProvider.GetRequiredService<IOutboxQueue<ItemsDbContext>>());

            context.Items.Add(new Item { Name = "facade" });
            await EfUnitOfWorkShape.CommitAsync(context, Ct);
        }

        Assert.Equal(["facade"], await schema.ItemNamesAsync());
        Assert.Equal(["facade"], await schema.OutboxPayloadsAsync());
        var itemTx = Assert.Single(await schema.QueryAsync<string>($"SELECT tx::text FROM {schema.Name}.items"));
        var outboxTx = Assert.Single(await schema.QueryAsync<string>($"SELECT tx_id::text FROM {schema.OutboxTable}"));
        Assert.Equal(itemTx, outboxTx);
        Assert.Empty(schema.OutboxLogs());
    }

    private static void Register(IServiceCollection services, Registration registration, Action<DbContextOptionsBuilder> configure)
    {
        switch (registration)
        {
            case Registration.AddDbContext:
                services.AddDbContext<ItemsDbContext>(configure);
                break;
            case Registration.AddDbContextPool:
                services.AddDbContextPool<ItemsDbContext>(configure);
                break;
            case Registration.AddDbContextFactory:
                services.AddDbContextFactory<ItemsDbContext>(configure);
                break;
            case Registration.AddPooledDbContextFactory:
                services.AddPooledDbContextFactory<ItemsDbContext>(configure);
                break;
            default:
                throw new ArgumentOutOfRangeException(nameof(registration));
        }
    }
}
