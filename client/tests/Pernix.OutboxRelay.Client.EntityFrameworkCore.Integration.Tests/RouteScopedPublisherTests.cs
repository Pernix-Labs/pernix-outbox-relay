using Pernix.OutboxRelay.Client.PostgreSql;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore.Integration.Tests;

public sealed class RouteScopedPublisherTests(EfPostgreSqlFixture postgres)
{
    private const string Alerts = "alerts";

    private const string PoisonedMessage =
        "Writing the queued outbox messages failed in an earlier commit of this EF transaction. Roll back or dispose the transaction and re-run the unit of work on a fresh context.";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    public enum Shape
    {
        Single,
        Batch,
    }

    [Theory]
    [InlineData(Shape.Single)]
    [InlineData(Shape.Batch)]
    public async Task T18_publish_extension_writes_null_and_equal_routes_with_the_scoped_route(Shape shape)
    {
        var schema = await postgres.CreateSchemaAsync();
        await using var provider = ScopedPublisherProvider(schema);
        var publisher = provider.GetRequiredKeyedService<IOutboxPublisher>(Alerts);

        await using (var context = schema.CreateContextWithoutOutbox())
        {
            await using var transaction = await context.Database.BeginTransactionAsync(Ct);
            context.Items.Add(new Item { Name = "t18" });
            await context.SaveChangesAsync(Ct);

            if (shape == Shape.Single)
            {
                await publisher.PublishAsync(context, OutboxMessage.Create("k", "null"), Ct);
                await publisher.PublishAsync(context, OutboxMessage.Create("k", "equal").WithRoute(Alerts), Ct);
            }
            else
            {
                await publisher.PublishAsync(context, [OutboxMessage.Create("k", "null"), OutboxMessage.Create("k", "equal").WithRoute(Alerts)], Ct);
            }

            await transaction.CommitAsync(Ct);
        }

        Assert.Equal([("null", Alerts), ("equal", Alerts)], await OutboxRouteRows.ReadAsync(schema));
    }

    [Theory]
    [InlineData(Shape.Single)]
    [InlineData(Shape.Batch)]
    public async Task T18_publish_extension_refuses_a_different_route_and_writes_no_row(Shape shape)
    {
        var schema = await postgres.CreateSchemaAsync();
        await using var provider = ScopedPublisherProvider(schema);
        var publisher = provider.GetRequiredKeyedService<IOutboxPublisher>(Alerts);

        await using (var context = schema.CreateContextWithoutOutbox())
        {
            await using var transaction = await context.Database.BeginTransactionAsync(Ct);
            context.Items.Add(new Item { Name = "t18" });
            await context.SaveChangesAsync(Ct);

            var thrown = shape == Shape.Single
                ? await Assert.ThrowsAsync<ArgumentException>(
                    () => publisher.PublishAsync(context, OutboxMessage.Create("secret-key", "secret-payload").WithRoute("audit"), Ct))
                : await Assert.ThrowsAsync<ArgumentException>(
                    () => publisher.PublishAsync(
                        context,
                        [OutboxMessage.Create("k", "b0"), OutboxMessage.Create("secret-key", "secret-payload").WithRoute("audit"), OutboxMessage.Create("k", "b2")],
                        Ct));

            Assert.Equal(shape == Shape.Single ? "message" : "messages", thrown.ParamName);
            Assert.StartsWith(
                shape == Shape.Single
                    ? "Message route 'audit' differs from scoped route 'alerts'."
                    : "Message 1 route 'audit' differs from scoped route 'alerts'.",
                thrown.Message,
                StringComparison.Ordinal);
            Assert.DoesNotContain("secret", thrown.Message, StringComparison.Ordinal);

            await transaction.CommitAsync(Ct);
        }

        Assert.Equal(["t18"], await schema.ItemNamesAsync());
        Assert.Empty(await OutboxRouteRows.ReadAsync(schema));
    }

    [Fact]
    public async Task T18_UseOutbox_with_scoped_publisher_commits_null_route_messages_with_the_scoped_route()
    {
        var schema = await postgres.CreateSchemaAsync();
        await using var provider = ScopedPublisherProvider(schema);
        var publisher = provider.GetRequiredKeyedService<IOutboxPublisher>(Alerts);

        await using (var context = schema.CreateContext(publisher))
        {
            context.EnqueueOutbox(OutboxMessage.Create("k", "m0"));
            context.EnqueueOutbox(OutboxMessage.Create("k", "m1").WithRoute(Alerts));
            context.EnqueueOutbox(OutboxMessage.Create("k", "m2"));
            context.Items.Add(new Item { Name = "t18" });
            await EfUnitOfWorkShape.CommitAsync(context, Ct);
        }

        Assert.Equal([("m0", Alerts), ("m1", Alerts), ("m2", Alerts)], await OutboxRouteRows.ReadAsync(schema));
        Assert.Equal(["t18"], await schema.ItemNamesAsync());
        Assert.Empty(schema.OutboxLogs());
    }

    [Fact]
    public async Task T18_UseOutbox_with_scoped_publisher_refuses_a_different_route_at_commit_and_poisons_the_transaction()
    {
        var schema = await postgres.CreateSchemaAsync();
        await using var provider = ScopedPublisherProvider(schema);
        var publisher = provider.GetRequiredKeyedService<IOutboxPublisher>(Alerts);

        await using (var context = schema.CreateContext(publisher))
        {
            await using var transaction = await context.Database.BeginTransactionAsync(Ct);
            context.EnqueueOutbox(OutboxMessage.Create("k", "m0"));
            context.EnqueueOutbox(OutboxMessage.Create("secret-key", "secret-payload").WithRoute("audit"));
            context.Items.Add(new Item { Name = "t18" });
            await context.SaveChangesAsync(Ct);

            var thrown = await Assert.ThrowsAsync<ArgumentException>(() => transaction.CommitAsync(Ct));

            Assert.Equal("messages", thrown.ParamName);
            Assert.StartsWith("Message 1 route 'audit' differs from scoped route 'alerts'.", thrown.Message, StringComparison.Ordinal);
            Assert.DoesNotContain("secret", thrown.Message, StringComparison.Ordinal);
            Assert.Equal(0, Assert.IsType<OutboxQueue>(OutboxQueue.Find(context)).Count);

            var retry = await Assert.ThrowsAsync<InvalidOperationException>(() => transaction.CommitAsync(Ct));
            Assert.Equal(PoisonedMessage, retry.Message);
        }

        Assert.Empty(await OutboxRouteRows.ReadAsync(schema));
        Assert.Empty(await schema.ItemNamesAsync());
    }

    private static ServiceProvider ScopedPublisherProvider(EfTestSchema schema) =>
        new ServiceCollection().AddKeyedPostgreSqlOutboxPublisher(Alerts, schema.OutboxTable.ToString(), Alerts).BuildServiceProvider();
}
