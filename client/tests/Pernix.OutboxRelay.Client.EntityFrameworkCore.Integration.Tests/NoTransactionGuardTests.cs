using System.Text;
using Microsoft.EntityFrameworkCore;
using Xunit;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore.Integration.Tests;

// E2.
public sealed class NoTransactionGuardTests(EfPostgreSqlFixture postgres)
{
    private const string GuardMessage =
        "SaveChanges was called with queued outbox messages and no EF transaction, so the messages could not be written atomically. Nothing was saved. Begin a transaction with Database.BeginTransactionAsync and commit it with CommitAsync, or publish inside your own transaction with IOutboxPublisher.PublishAsync(DbContext, ...).";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(SaveKind.SaveChanges)]
    [InlineData(SaveKind.SaveChangesAsync)]
    public async Task L4_plain_save_with_changes_and_queue_throws_before_any_write(SaveKind kind)
    {
        var schema = await postgres.CreateSchemaAsync();
        await using (var context = schema.CreateContext())
        {
            context.EnqueueOutbox(OutboxMessage.Create("k", "l4"));
            context.Items.Add(new Item { Name = "l4" });

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => SaveAsync(context, kind));

            Assert.Equal(GuardMessage, exception.Message);
            Assert.Equal(["l4"], Payloads(context));
            Assert.Null(context.Database.CurrentTransaction);
        }

        Assert.Empty(await schema.ItemNamesAsync());
        Assert.Empty(await schema.OutboxPayloadsAsync());
    }

    [Theory]
    [InlineData(SaveKind.SaveChanges)]
    [InlineData(SaveKind.SaveChangesAsync)]
    public async Task L5_plain_save_with_zero_changes_and_queue_throws(SaveKind kind)
    {
        var schema = await postgres.CreateSchemaAsync();
        await using (var context = schema.CreateContext())
        {
            context.EnqueueOutbox(OutboxMessage.Create("k", "l5"));

            var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => SaveAsync(context, kind));

            Assert.Equal(GuardMessage, exception.Message);
            Assert.Equal(["l5"], Payloads(context));
        }

        Assert.Empty(await schema.OutboxPayloadsAsync());
    }

    [Fact]
    public async Task Guard_message_has_no_message_values_and_names_both_remedies()
    {
        const string Key = "guard-key-3d9b";
        const string Payload = "guard-payload-81fa";
        const string HeaderName = "guard-header-name-c07e";
        const string HeaderValue = "guard-header-value-5a62";

        var schema = await postgres.CreateSchemaAsync();
        var headers = OutboxHeaders.CreateBuilder().Add(HeaderName, HeaderValue).Build();
        await using var context = schema.CreateContext();
        context.EnqueueOutbox(OutboxMessage.Create(Key, Payload, headers));
        context.Items.Add(new Item { Name = "guard" });

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() => context.SaveChangesAsync(Ct));

        Assert.Equal(GuardMessage, exception.Message);
        Assert.Contains("Database.BeginTransactionAsync", exception.Message, StringComparison.Ordinal);
        Assert.Contains("IOutboxPublisher.PublishAsync(DbContext, ...)", exception.Message, StringComparison.Ordinal);
        foreach (var secret in new[] { Key, Payload, HeaderName, HeaderValue })
        {
            Assert.DoesNotContain(secret, exception.ToString(), StringComparison.Ordinal);
        }
    }

    [Fact]
    public async Task Guard_leaves_queue_unchanged()
    {
        var schema = await postgres.CreateSchemaAsync();
        await using (var context = schema.CreateContext())
        {
            context.EnqueueOutbox(OutboxMessage.Create("k", "q-1"));
            context.EnqueueOutbox(OutboxMessage.Create("k", "q-2"));
            context.EnqueueOutbox(OutboxMessage.Create("k", "q-3"));
            context.Items.Add(new Item { Name = "guarded" });

            await Assert.ThrowsAsync<InvalidOperationException>(() => context.SaveChangesAsync(Ct));
            Assert.Throws<InvalidOperationException>(() => context.SaveChanges());

            Assert.Equal(["q-1", "q-2", "q-3"], Payloads(context));

            await EfUnitOfWorkShape.CommitAsync(context, Ct);
        }

        Assert.Equal(["q-1", "q-2", "q-3"], await schema.OutboxPayloadsAsync());
        Assert.Equal(["guarded"], await schema.ItemNamesAsync());
        Assert.Empty(schema.OutboxLogs());
    }

    [Fact]
    public async Task Plain_save_with_empty_queue_succeeds()
    {
        var schema = await postgres.CreateSchemaAsync();
        var publisher = new RecordingPublisher(schema.Publisher);
        await using (var context = schema.CreateContext(publisher))
        {
            context.Items.Add(new Item { Name = "plain-async" });
            Assert.Equal(1, await context.SaveChangesAsync(Ct));

            context.Items.Add(new Item { Name = "plain-sync" });
            Assert.Equal(1, context.SaveChanges());
        }

        Assert.Equal(["plain-async", "plain-sync"], await schema.ItemNamesAsync());
        Assert.Empty(await schema.OutboxPayloadsAsync());
        Assert.Equal(0, publisher.Calls);
        Assert.Empty(schema.OutboxLogs());
    }

    public enum SaveKind
    {
        SaveChanges,
        SaveChangesAsync,
    }

    private static Task SaveAsync(DbContext context, SaveKind kind) =>
        kind == SaveKind.SaveChanges ? Task.FromResult(context.SaveChanges()) : context.SaveChangesAsync(Ct);

    private static string[] Payloads(DbContext context)
    {
        var messages = Assert.IsType<OutboxQueue>(OutboxQueue.Find(context)).Messages;
        var payloads = new string[messages.Count];
        for (var i = 0; i < payloads.Length; i++)
        {
            payloads[i] = Encoding.UTF8.GetString(messages[i].Payload.Span);
        }

        return payloads;
    }
}
