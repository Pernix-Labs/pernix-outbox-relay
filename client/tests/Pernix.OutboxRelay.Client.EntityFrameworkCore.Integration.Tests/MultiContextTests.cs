using Pernix.OutboxRelay.Client.PostgreSql;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore.Integration.Tests;

public sealed class MultiContextTests(EfPostgreSqlFixture postgres)
{
    private const string KeyA = "outbox-a";
    private const string KeyB = "outbox-b";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task T3_two_contexts_in_one_scope_flush_only_their_own_messages()
    {
        var schemaA = await postgres.CreateSchemaAsync();
        var schemaB = await postgres.CreateSchemaAsync("outbox_b");
        var services = new ServiceCollection();
        services.AddKeyedPostgreSqlOutboxPublisher(KeyA, schemaA.OutboxTable.ToString());
        services.AddKeyedPostgreSqlOutboxPublisher(KeyB, schemaB.OutboxTable.ToString());
        services.AddDbContext<ItemsDbContext>((sp, options) => schemaA.Configure(options, sp.GetRequiredKeyedService<IOutboxPublisher>(KeyA)));
        services.AddDbContext<OtherItemsDbContext>((sp, options) => schemaB.Configure(options, sp.GetRequiredKeyedService<IOutboxPublisher>(KeyB)));
        await using var provider = services.BuildServiceProvider();

        await using (var scope = provider.CreateAsyncScope())
        {
            var contextA = scope.ServiceProvider.GetRequiredService<ItemsDbContext>();
            var contextB = scope.ServiceProvider.GetRequiredService<OtherItemsDbContext>();

            contextA.EnqueueOutbox(OutboxMessage.Create("a", "a-1"));
            contextB.EnqueueOutbox(OutboxMessage.Create("b", "b-1"));
            contextB.EnqueueOutbox(OutboxMessage.Create("b", "b-2"));

            contextA.Items.Add(new Item { Name = "a" });
            await EfUnitOfWorkShape.CommitAsync(contextA, Ct);

            var queueB = Queue(contextB);
            Assert.Equal(2, queueB.Count);
            Assert.Equal(["b-1", "b-2"], [.. queueB.Messages.Select(m => System.Text.Encoding.UTF8.GetString(m.Payload.Span))]);
            Assert.Equal(0, Queue(contextA).Count);

            contextB.Items.Add(new Item { Name = "b" });
            await EfUnitOfWorkShape.CommitAsync(contextB, Ct);
        }

        Assert.Equal(["a-1"], await schemaA.OutboxPayloadsAsync());
        Assert.Equal(["a"], await schemaA.ItemNamesAsync());
        Assert.Equal(["b-1", "b-2"], await schemaB.OutboxPayloadsAsync());
        Assert.Equal(["b"], await schemaB.ItemNamesAsync());

        var txA = await AssertOneTransactionAsync(schemaA);
        var txB = await AssertOneTransactionAsync(schemaB);
        Assert.NotEqual(txA, txB);
        Assert.Empty(schemaA.OutboxLogs());
        Assert.Empty(schemaB.OutboxLogs());
    }

    [Fact]
    public async Task T3_context_without_UseOutbox_refuses_enqueue_and_writes_no_outbox_row()
    {
        var schema = await postgres.CreateSchemaAsync();
        var services = new ServiceCollection();
        services.AddDbContext<ItemsDbContext>(options => schema.Configure(options));
        services.AddDbContext<OtherItemsDbContext>(options => schema.ConfigureWithoutOutbox(options));
        await using var provider = services.BuildServiceProvider();

        await using (var scope = provider.CreateAsyncScope())
        {
            var outboxed = scope.ServiceProvider.GetRequiredService<ItemsDbContext>();
            var plain = scope.ServiceProvider.GetRequiredService<OtherItemsDbContext>();

            var thrown = Assert.Throws<InvalidOperationException>(() => plain.EnqueueOutbox(OutboxMessage.Create("k", "plain")));
            Assert.Equal(
                "DbContext type 'OtherItemsDbContext' has no outbox queue. Call UseOutbox(publisher) on its DbContextOptionsBuilder.",
                thrown.Message);
            Assert.Null(OutboxQueue.Find(plain));

            outboxed.EnqueueOutbox(OutboxMessage.Create("k", "outboxed"));
            outboxed.Items.Add(new Item { Name = "outboxed" });
            await EfUnitOfWorkShape.CommitAsync(outboxed, Ct);

            plain.Items.Add(new Item { Name = "plain" });
            await EfUnitOfWorkShape.CommitAsync(plain, Ct);
        }

        Assert.Equal(["outboxed", "plain"], await schema.ItemNamesAsync());
        Assert.Equal(["outboxed"], await schema.OutboxPayloadsAsync());
        var outboxTx = Assert.Single(await schema.QueryAsync<string>($"SELECT tx_id::text FROM {schema.OutboxTable}"));
        var outboxedTx = Assert.Single(await schema.QueryAsync<string>($"SELECT tx::text FROM {schema.Name}.items WHERE name = 'outboxed'"));
        Assert.Equal(outboxedTx, outboxTx);
        Assert.Empty(schema.OutboxLogs());
    }

    private static OutboxQueue Queue(DbContext context) => Assert.IsType<OutboxQueue>(OutboxQueue.Find(context));

    private static async Task<string> AssertOneTransactionAsync(EfTestSchema schema)
    {
        var itemTx = Assert.Single(await schema.QueryAsync<string>($"SELECT tx::text FROM {schema.Name}.items"));
        var outboxTx = await schema.QueryAsync<string>($"SELECT tx_id::text FROM {schema.OutboxTable}");
        Assert.All(outboxTx, tx => Assert.Equal(itemTx, tx));
        return itemTx;
    }

    public sealed class OtherItemsDbContext(DbContextOptions<OtherItemsDbContext> options) : DbContext(options)
    {
        public DbSet<Item> Items => Set<Item>();

        protected override void OnModelCreating(ModelBuilder modelBuilder)
        {
            modelBuilder.Entity<Item>(item =>
            {
                item.ToTable("items");
                item.Property(x => x.Id).HasColumnName("id");
                item.Property(x => x.Name).HasColumnName("name");
            });
        }
    }
}
