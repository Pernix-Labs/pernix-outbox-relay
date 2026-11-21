using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore.Integration.Tests;

// Through the EfUnitOfWork shape.
public sealed class UnitOfWorkTests(EfPostgreSqlFixture postgres)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(EnqueuePath.Facade)]
    [InlineData(EnqueuePath.Enqueue)]
    public async Task L1_unit_of_work_commits_business_and_outbox_rows_together(EnqueuePath path)
    {
        var schema = await postgres.CreateSchemaAsync();
        await using (var provider = BuildProvider(schema))
        await using (var scope = provider.CreateAsyncScope())
        {
            var (context, enqueue) = Resolve(scope, path);
            enqueue(OutboxMessage.Create("k", "l1-1"));
            enqueue(OutboxMessage.Create("k", "l1-2"));
            context.Items.Add(new Item { Name = "l1" });

            await EfUnitOfWorkShape.CommitAsync(context, Ct);
        }

        Assert.Equal(["l1"], await schema.ItemNamesAsync());
        Assert.Equal(["l1-1", "l1-2"], await schema.OutboxPayloadsAsync());
        var itemTx = Assert.Single(await schema.QueryAsync<string>($"SELECT tx::text FROM {schema.Name}.items"));
        Assert.Equal([itemTx], await schema.QueryAsync<string>($"SELECT DISTINCT tx_id::text FROM {schema.OutboxTable}"));
        Assert.Empty(schema.OutboxLogs());
    }

    [Theory]
    [InlineData(EnqueuePath.Facade)]
    [InlineData(EnqueuePath.Enqueue)]
    public async Task L2_unit_of_work_with_zero_changes_commits_outbox_rows(EnqueuePath path)
    {
        var schema = await postgres.CreateSchemaAsync();
        await using (var provider = BuildProvider(schema))
        await using (var scope = provider.CreateAsyncScope())
        {
            var (context, enqueue) = Resolve(scope, path);
            enqueue(OutboxMessage.Create("k", "l2"));

            await EfUnitOfWorkShape.CommitAsync(context, Ct);
        }

        Assert.Empty(await schema.ItemNamesAsync());
        Assert.Equal(["l2"], await schema.OutboxPayloadsAsync());
        Assert.Empty(schema.OutboxLogs());
    }

    [Fact]
    public async Task L3_message_enqueued_after_last_save_is_committed()
    {
        var schema = await postgres.CreateSchemaAsync();
        await using (var context = schema.CreateContext())
        {
            await using var transaction = await context.Database.BeginTransactionAsync(Ct);
            context.Items.Add(new Item { Name = "l3" });
            await context.SaveChangesAsync(Ct);

            context.EnqueueOutbox(OutboxMessage.Create("k", "l3"));
            await transaction.CommitAsync(Ct);
        }

        Assert.Equal(["l3"], await schema.ItemNamesAsync());
        Assert.Equal(["l3"], await schema.OutboxPayloadsAsync());
        await AssertOneTransactionAsync(schema);
    }

    [Fact]
    public async Task T4_outbox_rows_share_business_tx_id_and_are_last_writes()
    {
        var schema = await postgres.CreateSchemaAsync();
        await using (var context = schema.CreateContext())
        {
            context.EnqueueOutbox(OutboxMessage.Create("k", "t4-1"));
            context.Items.Add(new Item { Name = "t4-a" });
            context.EnqueueOutbox(OutboxMessage.Create("k", "t4-2"));
            context.Items.Add(new Item { Name = "t4-b" });

            await EfUnitOfWorkShape.CommitAsync(context, Ct);
        }

        var itemTx = Assert.Single(await schema.QueryAsync<string>($"SELECT DISTINCT tx::text FROM {schema.Name}.items"));
        Assert.Equal([itemTx], await schema.QueryAsync<string>($"SELECT DISTINCT tx_id::text FROM {schema.OutboxTable}"));

        var minOutboxSeq = Assert.Single(await schema.QueryAsync<long>($"SELECT min(seq) FROM {schema.Name}.outbox_seq"));
        var maxItemSeq = Assert.Single(await schema.QueryAsync<long>(
            $"SELECT max(seq) FROM {schema.Name}.items WHERE tx::text = '{itemTx}'"));
        Assert.Equal(2, Assert.Single(await schema.QueryAsync<long>($"SELECT count(*) FROM {schema.Name}.outbox_seq")));
        Assert.True(minOutboxSeq > maxItemSeq, $"min(outbox_seq.seq) {minOutboxSeq} <= max(items.seq) {maxItemSeq}");
    }

    [Fact]
    public async Task Messages_flush_in_enqueue_order()
    {
        const int N = 5;
        var schema = await postgres.CreateSchemaAsync();
        var expected = new string[N];
        await using (var context = schema.CreateContext())
        {
            for (var i = 0; i < N; i++)
            {
                expected[i] = $"order-{N - i}";
                context.EnqueueOutbox(OutboxMessage.Create("k", expected[i]));
            }

            await EfUnitOfWorkShape.CommitAsync(context, Ct);
        }

        // OutboxPayloadsAsync orders by id: ids ascend in enqueue order.
        Assert.Equal(expected, await schema.OutboxPayloadsAsync());
    }

    [Fact]
    public async Task Enqueue_runs_no_command()
    {
        var schema = await postgres.CreateSchemaAsync();
        var counter = new CommandCountingInterceptor();
        var options = (DbContextOptions<ItemsDbContext>)schema.Configure(new DbContextOptionsBuilder<ItemsDbContext>())
            .AddInterceptors(counter)
            .Options;

        await using var context = new ItemsDbContext(options);
        context.EnqueueOutbox(OutboxMessage.Create("k", "before-begin"));
        Assert.Equal(0, counter.Commands);
        Assert.Equal(0, counter.ConnectionOpens);

        await using var transaction = await context.Database.BeginTransactionAsync(Ct);
        var commands = counter.Commands;
        var opens = counter.ConnectionOpens;
        context.EnqueueOutbox(OutboxMessage.Create("k", "in-transaction-1"));
        context.EnqueueOutbox(OutboxMessage.Create("k", "in-transaction-2"));
        Assert.Equal(commands, counter.Commands);
        Assert.Equal(opens, counter.ConnectionOpens);

        await transaction.RollbackAsync(Ct);
    }

    [Fact]
    public async Task Empty_queue_commit_does_not_call_publisher()
    {
        var schema = await postgres.CreateSchemaAsync();
        var publisher = new RecordingPublisher(schema.Publisher);
        await using (var context = schema.CreateContext(publisher))
        {
            context.Items.Add(new Item { Name = "empty-queue" });
            await EfUnitOfWorkShape.CommitAsync(context, Ct);
        }

        Assert.Equal(0, publisher.Calls);
        Assert.Equal(["empty-queue"], await schema.ItemNamesAsync());
        Assert.Empty(await schema.OutboxPayloadsAsync());
    }

    [Fact]
    public async Task Messages_from_several_saves_flush_once_at_commit()
    {
        var schema = await postgres.CreateSchemaAsync();
        var publisher = new RecordingPublisher(schema.Publisher);
        await using (var context = schema.CreateContext(publisher))
        {
            await using var transaction = await context.Database.BeginTransactionAsync(Ct);

            context.EnqueueOutbox(OutboxMessage.Create("k", "save-1"));
            context.Items.Add(new Item { Name = "a" });
            await context.SaveChangesAsync(Ct);

            context.EnqueueOutbox(OutboxMessage.Create("k", "save-2"));
            context.Items.Add(new Item { Name = "b" });
            await context.SaveChangesAsync(Ct);

            context.EnqueueOutbox(OutboxMessage.Create("k", "after-saves"));
            Assert.Equal(0, publisher.Calls);
            Assert.Empty(await schema.OutboxPayloadsAsync());

            await transaction.CommitAsync(Ct);
        }

        Assert.Equal([3], publisher.BatchSizes);
        Assert.Equal(["save-1", "save-2", "after-saves"], await schema.OutboxPayloadsAsync());
        Assert.Equal(["a", "b"], await schema.ItemNamesAsync());
    }

    public enum EnqueuePath
    {
        Facade,
        Enqueue,
    }

    private static ServiceProvider BuildProvider(EfTestSchema schema)
    {
        var services = new ServiceCollection();
        services.AddDbContext<ItemsDbContext>(options => schema.Configure(options));
        services.AddOutboxQueue<ItemsDbContext>();
        return services.BuildServiceProvider();
    }

    private static (ItemsDbContext Context, Action<OutboxMessage> Enqueue) Resolve(AsyncServiceScope scope, EnqueuePath path)
    {
        var context = scope.ServiceProvider.GetRequiredService<ItemsDbContext>();
        Action<OutboxMessage> enqueue = path == EnqueuePath.Facade
            ? scope.ServiceProvider.GetRequiredService<IOutboxQueue<ItemsDbContext>>().Enqueue
            : context.EnqueueOutbox;
        return (context, enqueue);
    }

    private static async Task AssertOneTransactionAsync(EfTestSchema schema)
    {
        var itemTx = Assert.Single(await schema.QueryAsync<string>($"SELECT tx::text FROM {schema.Name}.items"));
        var outboxTx = Assert.Single(await schema.QueryAsync<string>($"SELECT tx_id::text FROM {schema.OutboxTable}"));
        Assert.Equal(itemTx, outboxTx);
    }
}
