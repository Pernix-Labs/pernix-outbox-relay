using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Xunit;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore.Integration.Tests;

// E3, E5.
public sealed class FailureTests(EfPostgreSqlFixture postgres)
{
    private const string SyncCommitMessage =
        "The EF transaction has queued outbox messages, which are written asynchronously at commit. Commit with CommitAsync. The transaction was not committed.";

    private const string PoisonedMessage =
        "Writing the queued outbox messages failed in an earlier commit of this EF transaction. Roll back or dispose the transaction and re-run the unit of work on a fresh context.";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task L8_failed_save_in_open_transaction_keeps_queue_until_rollback()
    {
        var schema = await SchemaWithItemAsync("dup");
        await using var context = schema.CreateContext();

        await using var transaction = await context.Database.BeginTransactionAsync(Ct);
        context.EnqueueOutbox(OutboxMessage.Create("k", "l8"));
        context.Items.Add(new Item { Name = "dup" });

        await Assert.ThrowsAsync<DbUpdateException>(() => context.SaveChangesAsync(Ct));

        Assert.Same(transaction, context.Database.CurrentTransaction);
        Assert.Equal(1, Queue(context).Count);

        await transaction.RollbackAsync(Ct);

        Assert.Equal(0, Queue(context).Count);
        Assert.Empty(await schema.OutboxPayloadsAsync());
        Assert.Equal(["dup"], await schema.ItemNamesAsync());
        Assert.Empty(schema.OutboxLogs());
    }

    [Fact]
    public async Task L9_publish_failure_propagates_commits_nothing_and_clears_queue()
    {
        var schema = await postgres.CreateSchemaAsync();
        var failure = new InvalidOperationException("publish failed");
        var publisher = new ThrowingPublisher(schema.Publisher, failure);
        await using var context = schema.CreateContext(publisher);

        context.EnqueueOutbox(OutboxMessage.Create("k", "l9"));
        context.Items.Add(new Item { Name = "l9" });

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() => EfUnitOfWorkShape.CommitAsync(context, Ct));

        Assert.Same(failure, thrown);
        Assert.Equal(1, publisher.Calls);
        Assert.Equal(0, Queue(context).Count);
        Assert.Empty(await schema.OutboxPayloadsAsync());
        Assert.Empty(await schema.ItemNamesAsync());
    }

    [Fact]
    public async Task L9_commit_retry_after_publish_failure_throws()
    {
        var schema = await postgres.CreateSchemaAsync();
        var publisher = new ThrowingPublisher(schema.Publisher, new InvalidOperationException("publish failed"), failCount: 1);
        await using (var context = schema.CreateContext(publisher))
        {
            await using var transaction = await context.Database.BeginTransactionAsync(Ct);
            context.EnqueueOutbox(OutboxMessage.Create("k", "l9-retry"));
            context.Items.Add(new Item { Name = "l9-retry" });
            await context.SaveChangesAsync(Ct);

            await Assert.ThrowsAsync<InvalidOperationException>(() => transaction.CommitAsync(Ct));
            var retry = await Assert.ThrowsAsync<InvalidOperationException>(() => transaction.CommitAsync(Ct));

            Assert.Equal(PoisonedMessage, retry.Message);
            Assert.Equal(1, publisher.Calls);
        }

        Assert.Empty(await schema.OutboxPayloadsAsync());
        Assert.Empty(await schema.ItemNamesAsync());
    }

    [Fact]
    public async Task L10_commit_failure_after_flush_commits_nothing_and_queue_is_empty()
    {
        var schema = await SchemaWithItemAsync("dup");
        await schema.ExecuteAsync($"""
            ALTER TABLE {schema.Name}.items DROP CONSTRAINT items_name_key;
            ALTER TABLE {schema.Name}.items ADD CONSTRAINT items_name_key UNIQUE (name) DEFERRABLE INITIALLY DEFERRED;
            """);
        var publisher = new RecordingPublisher(schema.Publisher);
        await using var context = schema.CreateContext(publisher);

        context.EnqueueOutbox(OutboxMessage.Create("k", "l10"));
        context.Items.Add(new Item { Name = "dup" });

        var thrown = await Assert.ThrowsAsync<PostgresException>(() => EfUnitOfWorkShape.CommitAsync(context, Ct));

        Assert.Equal(PostgresErrorCodes.UniqueViolation, thrown.SqlState);
        Assert.Equal([1], publisher.BatchSizes);
        Assert.Equal(0, Queue(context).Count);
        Assert.Empty(await schema.OutboxPayloadsAsync());
        Assert.Equal(["dup"], await schema.ItemNamesAsync());
    }

    [Fact]
    public async Task Sync_commit_with_queue_throws_naming_CommitAsync_and_commits_nothing()
    {
        var schema = await postgres.CreateSchemaAsync();
        await using (var context = schema.CreateContext())
        {
            using var transaction = context.Database.BeginTransaction();
            context.EnqueueOutbox(OutboxMessage.Create("k", "e3"));
            context.Items.Add(new Item { Name = "e3" });
            context.SaveChanges();

            var thrown = Assert.Throws<InvalidOperationException>(() => transaction.Commit());

            Assert.Equal(SyncCommitMessage, thrown.Message);
            Assert.Contains("CommitAsync", thrown.Message, StringComparison.Ordinal);
            Assert.Equal(1, Queue(context).Count);
        }

        Assert.Empty(await schema.OutboxPayloadsAsync());
        Assert.Empty(await schema.ItemNamesAsync());
        var warning = Assert.Single(schema.OutboxLogs());
        Assert.Equal(1, warning.Value("Count"));
    }

    [Fact]
    public async Task CommitAsync_after_sync_commit_refusal_commits_atomically()
    {
        var schema = await postgres.CreateSchemaAsync();
        await using (var context = schema.CreateContext())
        {
            await using var transaction = await context.Database.BeginTransactionAsync(Ct);
            context.EnqueueOutbox(OutboxMessage.Create("k", "rf28"));
            context.Items.Add(new Item { Name = "rf28" });
            await context.SaveChangesAsync(Ct);

            Assert.Throws<InvalidOperationException>(() => transaction.Commit());
            await transaction.CommitAsync(Ct);

            Assert.Equal(0, Queue(context).Count);
        }

        Assert.Equal(["rf28"], await schema.OutboxPayloadsAsync());
        Assert.Equal(["rf28"], await schema.ItemNamesAsync());
        var businessTx = Assert.Single(await schema.QueryAsync<string>($"SELECT tx::text FROM {schema.Name}.items"));
        var outboxTx = Assert.Single(await schema.QueryAsync<string>($"SELECT tx_id::text FROM {schema.OutboxTable}"));
        Assert.Equal(businessTx, outboxTx);
        Assert.Empty(schema.OutboxLogs());
    }

    [Theory]
    [InlineData(RollbackKind.RollbackAsync)]
    [InlineData(RollbackKind.Rollback)]
    [InlineData(RollbackKind.DatabaseRollbackTransactionAsync)]
    public async Task Rollback_clears_queue(RollbackKind kind)
    {
        var schema = await postgres.CreateSchemaAsync();
        await using (var context = schema.CreateContext())
        {
            await using (var transaction = await context.Database.BeginTransactionAsync(Ct))
            {
                context.EnqueueOutbox(OutboxMessage.Create("k", "rolled-back"));
                context.Items.Add(new Item { Name = "rolled-back" });
                await context.SaveChangesAsync(Ct);

                switch (kind)
                {
                    case RollbackKind.RollbackAsync:
                        await transaction.RollbackAsync(Ct);
                        break;
                    case RollbackKind.Rollback:
                        transaction.Rollback();
                        break;
                    default:
                        await context.Database.RollbackTransactionAsync(Ct);
                        break;
                }

                Assert.Equal(0, Queue(context).Count);
            }

            context.ChangeTracker.Clear();
            context.EnqueueOutbox(OutboxMessage.Create("k", "next"));
            context.Items.Add(new Item { Name = "next" });
            await EfUnitOfWorkShape.CommitAsync(context, Ct);
        }

        Assert.Equal(["next"], await schema.OutboxPayloadsAsync());
        Assert.Equal(["next"], await schema.ItemNamesAsync());
        Assert.Empty(schema.OutboxLogs());
    }

    [Theory]
    [InlineData(EnqueuePath.Facade)]
    [InlineData(EnqueuePath.Enqueue)]
    public async Task Failed_unit_of_work_messages_are_not_written_by_next_unit_of_work(EnqueuePath path)
    {
        var schema = await SchemaWithItemAsync("dup");
        var services = new ServiceCollection();
        services.AddDbContext<ItemsDbContext>(options => schema.Configure(options));
        services.AddOutboxQueue<ItemsDbContext>();
        await using var provider = services.BuildServiceProvider();

        await using (var scope = provider.CreateAsyncScope())
        {
            var context = scope.ServiceProvider.GetRequiredService<ItemsDbContext>();
            var queue = scope.ServiceProvider.GetRequiredService<IOutboxQueue<ItemsDbContext>>();
            void Enqueue(OutboxMessage message)
            {
                if (path == EnqueuePath.Facade)
                {
                    queue.Enqueue(message);
                }
                else
                {
                    context.EnqueueOutbox(message);
                }
            }

            Enqueue(OutboxMessage.Create("k", "failed-1"));
            context.Items.Add(new Item { Name = "dup" });
            await Assert.ThrowsAsync<DbUpdateException>(() => EfUnitOfWorkShape.CommitAsync(context, Ct));

            context.ChangeTracker.Clear();
            Enqueue(OutboxMessage.Create("k", "second"));
            context.Items.Add(new Item { Name = "second" });
            await EfUnitOfWorkShape.CommitAsync(context, Ct);
        }

        Assert.Equal(["second"], await schema.OutboxPayloadsAsync());
        Assert.Equal(["dup", "second"], await schema.ItemNamesAsync());
        var warning = Assert.Single(schema.OutboxLogs());
        Assert.Equal(1, warning.Value("Count"));
    }

    [Fact]
    public async Task Plain_save_after_failed_unit_of_work_succeeds()
    {
        var schema = await SchemaWithItemAsync("dup");
        await using (var context = schema.CreateContext())
        {
            context.EnqueueOutbox(OutboxMessage.Create("k", "failed"));
            context.Items.Add(new Item { Name = "dup" });
            await Assert.ThrowsAsync<DbUpdateException>(() => EfUnitOfWorkShape.CommitAsync(context, Ct));

            context.ChangeTracker.Clear();
            context.Items.Add(new Item { Name = "plain" });
            await context.SaveChangesAsync(Ct);

            Assert.Equal(0, Queue(context).Count);
        }

        Assert.Empty(await schema.OutboxPayloadsAsync());
        Assert.Equal(["dup", "plain"], await schema.ItemNamesAsync());
        var warning = Assert.Single(schema.OutboxLogs());
        Assert.Equal(1, warning.Value("Count"));
    }

    public enum RollbackKind
    {
        RollbackAsync,
        Rollback,
        DatabaseRollbackTransactionAsync,
    }

    public enum EnqueuePath
    {
        Facade,
        Enqueue,
    }

    private static OutboxQueue Queue(DbContext context) => Assert.IsType<OutboxQueue>(OutboxQueue.Find(context));

    private async Task<EfTestSchema> SchemaWithItemAsync(string name)
    {
        var schema = await postgres.CreateSchemaAsync();
        await schema.ExecuteAsync($"INSERT INTO {schema.Name}.items (name) VALUES ('{name}')");
        return schema;
    }
}
