using System.Transactions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging;
using Npgsql;
using Xunit;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore.Integration.Tests;

// E2, E4, E7.
public sealed class TransactionOutsideEfTests(EfPostgreSqlFixture postgres)
{
    private const string E2 =
        "SaveChanges was called with queued outbox messages and no EF transaction, so the messages could not be written atomically. Nothing was saved. Begin a transaction with Database.BeginTransactionAsync and commit it with CommitAsync, or publish inside your own transaction with IOutboxPublisher.PublishAsync(DbContext, ...).";

    private const string E4 =
        "The context's transaction came from Database.UseTransaction. The outbox queue writes only in transactions the context begins with Database.BeginTransaction[Async]. Call IOutboxPublisher.PublishAsync(DbContext, ...) after the business writes instead.";

    private const string E7 =
        "The context has no current EF transaction. Begin one with Database.BeginTransactionAsync before publishing. Inside a TransactionScope, use IOutboxPublisher.PublishEnlistedAsync(context.Database.GetDbConnection(), ...).";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private enum Commit
    {
        ThroughEf,
        OnDbTransaction,
    }

    [Fact]
    public Task R13a_use_transaction_ef_commit_refuses_queue_and_o15_commits() => UseTransactionCaseAsync(Commit.ThroughEf);

    [Fact]
    public Task R13b_use_transaction_external_commit_refuses_queue_and_o15_commits() => UseTransactionCaseAsync(Commit.OnDbTransaction);

    [Fact]
    public async Task R13c_ambient_scope_save_refuses_queue_and_o15_refuses()
    {
        var schema = await postgres.CreateSchemaAsync();

        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await using var context = schema.CreateContext();
            context.EnqueueOutbox(OutboxMessage.Create("k", "queued"));
            context.Items.Add(new Item { Name = "ambient" });

            var guard = await Assert.ThrowsAsync<InvalidOperationException>(() => context.SaveChangesAsync(Ct));
            Assert.Equal(E2, guard.Message);

            var o15 = await Assert.ThrowsAsync<InvalidOperationException>(
                () => schema.Publisher.PublishAsync(context, OutboxMessage.Create("k", "direct"), Ct));
            Assert.Equal(E7, o15.Message);

            Assert.Null(context.Database.CurrentTransaction);
            Assert.Equal(1, Queue(context).Count);
        }

        Assert.Empty(await schema.ItemNamesAsync());
        Assert.Empty(await schema.OutboxPayloadsAsync());
        AssertSingleWarning(schema, 1);
    }

    [Fact]
    public async Task R13c_ambient_scope_execute_update_drops_queue_with_discard_warning()
    {
        var schema = await postgres.CreateSchemaAsync();
        await schema.ExecuteAsync($"INSERT INTO {schema.Name}.items (name) VALUES ('before')");

        using (var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await using (var context = schema.CreateContext())
            {
                context.EnqueueOutbox(OutboxMessage.Create("k", "queued"));
                await context.Items
                    .Where(i => i.Name == "before")
                    .ExecuteUpdateAsync(s => s.SetProperty(i => i.Name, "after"), Ct);

                Assert.Null(context.Database.CurrentTransaction);
                Assert.Equal(1, Queue(context).Count);
                Assert.Empty(schema.OutboxLogs());
            }

            scope.Complete();
        }

        Assert.Equal(["after"], await schema.ItemNamesAsync());
        Assert.Empty(await schema.OutboxPayloadsAsync());
        AssertSingleWarning(schema, 1);
    }

    [Fact]
    public async Task R13d_shared_transaction_owner_flushes_and_joined_context_refuses()
    {
        var schema = await postgres.CreateSchemaAsync();
        await using var connection = new NpgsqlConnection(schema.ConnectionString);
        await connection.OpenAsync(Ct);

        await using (var owner = CreateContext(schema, connection))
        await using (var joined = CreateContext(schema, connection))
        await using (var lateJoined = CreateContext(schema, connection))
        {
            await using var transaction = await owner.Database.BeginTransactionAsync(Ct);
            owner.EnqueueOutbox(OutboxMessage.Create("k", "owner-queued"));
            var dbTransaction = transaction.GetDbTransaction();

            await joined.Database.UseTransactionAsync(dbTransaction, Ct);
            var atEnqueue = Assert.Throws<InvalidOperationException>(() => joined.EnqueueOutbox(OutboxMessage.Create("k", "joined-queued")));
            Assert.Equal(E4, atEnqueue.Message);
            Assert.Equal(0, Queue(joined).Count);

            lateJoined.EnqueueOutbox(OutboxMessage.Create("k", "late-queued"));
            var atUse = await Assert.ThrowsAsync<InvalidOperationException>(() => lateJoined.Database.UseTransactionAsync(dbTransaction, Ct));
            Assert.Equal(E4, atUse.Message);
            Assert.Null(lateJoined.Database.CurrentTransaction);
            Assert.Equal(1, Queue(lateJoined).Count);

            joined.Items.Add(new Item { Name = "joined" });
            await joined.SaveChangesAsync(Ct);
            await schema.Publisher.PublishAsync(joined, OutboxMessage.Create("k", "joined-o15"), Ct);

            owner.Items.Add(new Item { Name = "owner" });
            await owner.SaveChangesAsync(Ct);
            await schema.Publisher.PublishAsync(owner, OutboxMessage.Create("k", "owner-o15"), Ct);

            await transaction.CommitAsync(Ct);
            Assert.Equal(1, Queue(lateJoined).Count);
        }

        Assert.Equal(["joined", "owner"], await schema.ItemNamesAsync());
        Assert.Equal(["joined-o15", "owner-o15", "owner-queued"], await schema.OutboxPayloadsAsync());
        await AssertOneTransactionAsync(schema);
        AssertSingleWarning(schema, 1);
    }

    private async Task UseTransactionCaseAsync(Commit commit)
    {
        var schema = await postgres.CreateSchemaAsync();

        await using (var connection = await OpenAsync(schema))
        await using (var dbTransaction = await connection.BeginTransactionAsync(Ct))
        await using (var context = CreateContext(schema, connection))
        {
            context.EnqueueOutbox(OutboxMessage.Create("k", "queued-before-use"));

            var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() => context.Database.UseTransactionAsync(dbTransaction, Ct));

            Assert.Equal(E4, thrown.Message);
            Assert.Null(context.Database.CurrentTransaction);
            Assert.Equal(1, Queue(context).Count);
        }

        await using (var connection = await OpenAsync(schema))
        await using (var dbTransaction = await connection.BeginTransactionAsync(Ct))
        await using (var context = CreateContext(schema, connection))
        {
            await context.Database.UseTransactionAsync(dbTransaction, Ct);

            var thrown = Assert.Throws<InvalidOperationException>(() => context.EnqueueOutbox(OutboxMessage.Create("k", "queued-after-use")));

            Assert.Equal(E4, thrown.Message);
            Assert.Equal(0, Queue(context).Count);

            context.Items.Add(new Item { Name = "refused" });
            await context.SaveChangesAsync(Ct);
            await CommitAsync(context, dbTransaction, commit);
        }

        Assert.Equal(["refused"], await schema.ItemNamesAsync());
        Assert.Empty(await schema.OutboxPayloadsAsync());

        await using (var connection = await OpenAsync(schema))
        await using (var dbTransaction = await connection.BeginTransactionAsync(Ct))
        await using (var context = CreateContext(schema, connection))
        {
            await context.Database.UseTransactionAsync(dbTransaction, Ct);
            Assert.Same(dbTransaction, context.Database.CurrentTransaction!.GetDbTransaction());

            context.Items.Add(new Item { Name = "o15" });
            await context.SaveChangesAsync(Ct);
            await schema.Publisher.PublishAsync(context, OutboxMessage.Create("k", "o15"), Ct);
            await CommitAsync(context, dbTransaction, commit);
        }

        Assert.Equal(["refused", "o15"], await schema.ItemNamesAsync());
        Assert.Equal(["o15"], await schema.OutboxPayloadsAsync());
        var o15Tx = Assert.Single(await schema.QueryAsync<string>($"SELECT tx::text FROM {schema.Name}.items WHERE name = 'o15'"));
        var outboxTx = Assert.Single(await schema.QueryAsync<string>($"SELECT tx_id::text FROM {schema.OutboxTable}"));
        Assert.Equal(o15Tx, outboxTx);

        // The refused UseTransaction keeps the queue until dispose.
        AssertSingleWarning(schema, 1);
    }

    private static async Task CommitAsync(ItemsDbContext context, System.Data.Common.DbTransaction dbTransaction, Commit commit)
    {
        if (commit == Commit.ThroughEf)
        {
            await context.Database.CommitTransactionAsync(Ct);
        }
        else
        {
            await dbTransaction.CommitAsync(Ct);
        }
    }

    private static async Task<NpgsqlConnection> OpenAsync(EfTestSchema schema)
    {
        var connection = new NpgsqlConnection(schema.ConnectionString);
        await connection.OpenAsync(Ct);
        return connection;
    }

    private static ItemsDbContext CreateContext(EfTestSchema schema, NpgsqlConnection connection) =>
        new((DbContextOptions<ItemsDbContext>)schema.Configure(new DbContextOptionsBuilder<ItemsDbContext>(), connection).Options);

    private static OutboxQueue Queue(DbContext context) => Assert.IsType<OutboxQueue>(OutboxQueue.Find(context));

    private static async Task AssertOneTransactionAsync(EfTestSchema schema)
    {
        var itemTx = await schema.QueryAsync<string>($"SELECT tx::text FROM {schema.Name}.items");
        var outboxTx = await schema.QueryAsync<string>($"SELECT tx_id::text FROM {schema.OutboxTable}");
        var tx = Assert.Single(itemTx.Distinct());
        Assert.All(outboxTx, t => Assert.Equal(tx, t));
    }

    private static void AssertSingleWarning(EfTestSchema schema, int count)
    {
        var warning = Assert.Single(schema.OutboxLogs());
        Assert.Equal(LogLevel.Warning, warning.Level);
        Assert.Equal("OutboxMessagesDiscarded", warning.EventId.Name);
        Assert.Equal(count, warning.Value("Count"));
    }
}
