using Microsoft.EntityFrameworkCore;
using Xunit;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore.Integration.Tests;

public sealed class PublishExtensionTests(EfPostgreSqlFixture postgres)
{
    private const string E7 =
        "The context has no current EF transaction. Begin one with Database.BeginTransactionAsync before publishing. Inside a TransactionScope, use IOutboxPublisher.PublishEnlistedAsync(context.Database.GetDbConnection(), ...).";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public enum Shape
    {
        Single,
        Batch,
    }

    [Theory]
    [InlineData(Shape.Single)]
    [InlineData(Shape.Batch)]
    public async Task T5_publish_extension_refuses_without_current_transaction(Shape shape)
    {
        var schema = await postgres.CreateSchemaAsync();
        var publisher = new RecordingPublisher(schema.Publisher);
        await using var context = schema.CreateContext();
        context.Items.Add(new Item { Name = "t5" });

        var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() => PublishAsync(publisher, context, shape, "t5"));

        Assert.Equal(E7, thrown.Message);
        Assert.Equal(0, publisher.Calls);
        Assert.Null(context.Database.CurrentTransaction);
        Assert.Empty(await schema.OutboxPayloadsAsync());
        Assert.Empty(await schema.ItemNamesAsync());
    }

    [Fact]
    public async Task Publish_extension_commits_with_business_rows_and_rolls_back_with_them()
    {
        var schema = await postgres.CreateSchemaAsync();
        await using var context = schema.CreateContext();

        await using (var transaction = await context.Database.BeginTransactionAsync(Ct))
        {
            context.Items.Add(new Item { Name = "committed" });
            await context.SaveChangesAsync(Ct);
            await schema.Publisher.PublishAsync(context, OutboxMessage.Create("k", "committed"), Ct);
            await transaction.CommitAsync(Ct);
        }

        await using (var transaction = await context.Database.BeginTransactionAsync(Ct))
        {
            context.Items.Add(new Item { Name = "rolled-back" });
            await context.SaveChangesAsync(Ct);
            await schema.Publisher.PublishAsync(context, [OutboxMessage.Create("k", "rolled-back-1"), OutboxMessage.Create("k", "rolled-back-2")], Ct);
            await transaction.RollbackAsync(Ct);
        }

        Assert.Equal(["committed"], await schema.ItemNamesAsync());
        Assert.Equal(["committed"], await schema.OutboxPayloadsAsync());
        var itemTx = Assert.Single(await schema.QueryAsync<string>($"SELECT tx::text FROM {schema.Name}.items"));
        var outboxTx = Assert.Single(await schema.QueryAsync<string>($"SELECT tx_id::text FROM {schema.OutboxTable}"));
        Assert.Equal(itemTx, outboxTx);
    }

    [Fact]
    public async Task Publish_extension_works_without_UseOutbox_and_leaves_queue_untouched()
    {
        var schema = await postgres.CreateSchemaAsync();

        await using (var plain = schema.CreateContextWithoutOutbox())
        {
            await using var transaction = await plain.Database.BeginTransactionAsync(Ct);
            plain.Items.Add(new Item { Name = "plain" });
            await plain.SaveChangesAsync(Ct);
            await schema.Publisher.PublishAsync(plain, OutboxMessage.Create("k", "plain"), Ct);
            await transaction.CommitAsync(Ct);
        }

        await using (var context = schema.CreateContext())
        {
            var queued = OutboxMessage.Create("k", "queued");
            context.EnqueueOutbox(queued);
            var queue = Assert.IsType<OutboxQueue>(OutboxQueue.Find(context));

            await using var transaction = await context.Database.BeginTransactionAsync(Ct);
            context.Items.Add(new Item { Name = "outboxed" });
            await context.SaveChangesAsync(Ct);
            await schema.Publisher.PublishAsync(context, OutboxMessage.Create("k", "direct"), Ct);
            await schema.Publisher.PublishAsync(context, [OutboxMessage.Create("k", "direct-batch")], Ct);

            Assert.Equal(1, queue.Count);
            Assert.Equal(queued.Payload.ToArray(), queue.Messages[0].Payload.ToArray());

            await transaction.CommitAsync(Ct);
        }

        Assert.Equal(["plain", "outboxed"], await schema.ItemNamesAsync());
        Assert.Equal(["plain", "direct", "direct-batch", "queued"], await schema.OutboxPayloadsAsync());
        Assert.Empty(schema.OutboxLogs());
    }

    private static Task PublishAsync(IOutboxPublisher publisher, DbContext context, Shape shape, string payload) =>
        shape == Shape.Single
            ? publisher.PublishAsync(context, OutboxMessage.Create("k", payload), Ct)
            : publisher.PublishAsync(context, [OutboxMessage.Create("k", payload), OutboxMessage.Create("k", payload + "-2")], Ct);
}
