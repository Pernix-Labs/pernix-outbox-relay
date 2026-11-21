using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore.Integration.Tests;

public sealed class PoolingTests(EfPostgreSqlFixture postgres)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task T1_pooled_lease_messages_never_reach_next_lease()
    {
        var schema = await postgres.CreateSchemaAsync();
        var services = new ServiceCollection();
        services.AddDbContextPool<ItemsDbContext>(options => schema.Configure(options));
        await using var provider = services.BuildServiceProvider();

        ItemsDbContext leaseA;
        await using (var scope = provider.CreateAsyncScope())
        {
            leaseA = scope.ServiceProvider.GetRequiredService<ItemsDbContext>();
            leaseA.EnqueueOutbox(OutboxMessage.Create("a", "lease-a"));
        }

        await using (var scope = provider.CreateAsyncScope())
        {
            var leaseB = scope.ServiceProvider.GetRequiredService<ItemsDbContext>();
            Assert.Same(leaseA, leaseB);
            leaseB.EnqueueOutbox(OutboxMessage.Create("b", "lease-b"));
            leaseB.Items.Add(new Item { Name = "b" });
            await EfUnitOfWorkShape.CommitAsync(leaseB, Ct);
        }

        await AssertOnlySecondLeaseWrittenAsync(schema, "lease-b", "b", discarded: 1);
    }

    [Fact]
    public async Task T2_pooled_factory_messages_never_reach_next_lease()
    {
        var schema = await postgres.CreateSchemaAsync();
        var services = new ServiceCollection();
        services.AddPooledDbContextFactory<ItemsDbContext>(options => schema.Configure(options));
        await using var provider = services.BuildServiceProvider();
        var factory = provider.GetRequiredService<IDbContextFactory<ItemsDbContext>>();

        var leaseA = factory.CreateDbContext();
        leaseA.EnqueueOutbox(OutboxMessage.Create("a", "lease-a"));
        await leaseA.DisposeAsync();

        await using (var leaseB = await factory.CreateDbContextAsync(Ct))
        {
            Assert.Same(leaseA, leaseB);
            leaseB.EnqueueOutbox(OutboxMessage.Create("b", "lease-b"));
            leaseB.Items.Add(new Item { Name = "b" });
            await EfUnitOfWorkShape.CommitAsync(leaseB, Ct);
        }

        await AssertOnlySecondLeaseWrittenAsync(schema, "lease-b", "b", discarded: 1);
    }

    [Fact]
    public async Task T2_factory_contexts_have_own_empty_queue()
    {
        var schema = await postgres.CreateSchemaAsync();
        var services = new ServiceCollection();
        services.AddDbContextFactory<ItemsDbContext>(options => schema.Configure(options));
        await using var provider = services.BuildServiceProvider();
        var factory = provider.GetRequiredService<IDbContextFactory<ItemsDbContext>>();

        var first = factory.CreateDbContext();
        first.EnqueueOutbox(OutboxMessage.Create("1", "first"));

        await using (var second = factory.CreateDbContext())
        {
            Assert.NotSame(first, second);
            var firstQueue = Assert.IsType<OutboxQueue>(OutboxQueue.Find(first));
            var secondQueue = Assert.IsType<OutboxQueue>(OutboxQueue.Find(second));
            Assert.NotSame(firstQueue, secondQueue);
            Assert.Equal(1, firstQueue.Count);
            Assert.Equal(0, secondQueue.Count);

            second.EnqueueOutbox(OutboxMessage.Create("2", "second"));
            second.Items.Add(new Item { Name = "second" });
            await EfUnitOfWorkShape.CommitAsync(second, Ct);
        }

        await first.DisposeAsync();

        await AssertOnlySecondLeaseWrittenAsync(schema, "second", "second", discarded: 1);
    }

    [Fact]
    public async Task Queue_capacity_is_kept_across_leases()
    {
        const int Messages = 10;
        var schema = await postgres.CreateSchemaAsync();
        var services = new ServiceCollection();
        services.AddDbContextPool<ItemsDbContext>(options => schema.Configure(options));
        await using var provider = services.BuildServiceProvider();

        OutboxQueue queue;
        int capacity;
        await using (var scope = provider.CreateAsyncScope())
        {
            var context = scope.ServiceProvider.GetRequiredService<ItemsDbContext>();
            for (var i = 0; i < Messages; i++)
            {
                context.EnqueueOutbox(OutboxMessage.Create("k", "p"));
            }

            queue = Assert.IsType<OutboxQueue>(OutboxQueue.Find(context));
            capacity = Assert.IsType<List<OutboxMessage>>(queue.Messages).Capacity;
            Assert.True(capacity >= Messages);
        }

        await using (var scope = provider.CreateAsyncScope())
        {
            var context = scope.ServiceProvider.GetRequiredService<ItemsDbContext>();
            Assert.Same(queue, OutboxQueue.Find(context));
            Assert.Equal(0, queue.Count);
            Assert.Equal(capacity, Assert.IsType<List<OutboxMessage>>(queue.Messages).Capacity);
        }

        var warning = Assert.Single(schema.OutboxLogs());
        Assert.Equal(Messages, warning.Value("Count"));
    }

    private static async Task AssertOnlySecondLeaseWrittenAsync(EfTestSchema schema, string payload, string item, int discarded)
    {
        Assert.Equal([payload], await schema.OutboxPayloadsAsync());
        Assert.Equal([item], await schema.ItemNamesAsync());

        var warning = Assert.Single(schema.OutboxLogs());
        Assert.Equal(LogLevel.Warning, warning.Level);
        Assert.Equal("OutboxMessagesDiscarded", warning.EventId.Name);
        Assert.Equal(discarded, warning.Value("Count"));
    }
}
