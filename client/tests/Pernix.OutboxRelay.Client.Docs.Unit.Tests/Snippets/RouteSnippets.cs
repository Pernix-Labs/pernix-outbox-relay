using Pernix.OutboxRelay.Client.EntityFrameworkCore;
using Pernix.OutboxRelay.Client.PostgreSql;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;

namespace Pernix.OutboxRelay.Client.Docs.Unit.Tests.Snippets;

internal static class RouteSnippets
{
    public static async Task PublishRoutedAsync(
        NpgsqlTransaction transaction, IOutboxPublisher publisher, OrderPlaced placed, CancellationToken ct)
    {
        #region docs:route-publish
        var headers = OutboxHeaders.CreateBuilder(1).Add("idempotency-key", placed.EventId.ToString()).Build();
        var message = OutboxMessage.Json(placed.OrderId.ToString(), placed, AppJson.Default.OrderPlaced, headers)
            .WithRoute("billing");

        await publisher.PublishAsync(transaction, message, ct);
        await transaction.CommitAsync(ct);
        #endregion
    }

    public static async Task PublishBroadcastAsync(
        NpgsqlTransaction transaction, IOutboxPublisher publisher, OrderPlaced placed, CancellationToken ct)
    {
        #region docs:route-broadcast
        // One message per route, one batch: one round trip, all rows commit or none.
        var headers = OutboxHeaders.CreateBuilder(1).Add("idempotency-key", placed.EventId.ToString()).Build();
        var message = OutboxMessage.Json(placed.OrderId.ToString(), placed, AppJson.Default.OrderPlaced, headers);
        OutboxMessage[] messages = [message.WithRoute("billing"), message.WithRoute("shipping")];

        await publisher.PublishAsync(transaction, messages, ct);
        await transaction.CommitAsync(ct);
        #endregion
    }

    public static void EnqueueRouted(IOutboxQueue<AppDb> outbox, OrderPlaced placed, OutboxHeaders headers)
    {
        #region docs:route-ef-enqueue
        var message = OutboxMessage.Json(placed.OrderId.ToString(), placed, AppJson.Default.OrderPlaced, headers);
        outbox.Enqueue(message.WithRoute("billing"));
        #endregion
    }

    public static void RegisterScopedPublishers(IServiceCollection services)
    {
        #region docs:route-scoped-publishers
        // Validates each route and parses the table now: a bad text fails at startup.
        services.AddKeyedPostgreSqlOutboxPublisher("billing", "sales.outbox", "billing");
        services.AddKeyedPostgreSqlOutboxPublisher("shipping", "sales.outbox", "shipping");
        #endregion
    }

    public static void RegisterScopedQueues(IServiceCollection services)
    {
        #region docs:route-scoped-queues
        services.AddKeyedOutboxQueue<AppDb>("billing", "billing");
        services.AddKeyedOutboxQueue<AppDb>("shipping", "shipping");
        #endregion
    }
}

#region docs:route-scoped-publisher-inject
public sealed class BillingNotifier([FromKeyedServices("billing")] IOutboxPublisher publisher)
{
    // Every message this publisher writes gets route 'billing'.
    public Task NotifyAsync(NpgsqlTransaction transaction, OutboxMessage message, CancellationToken ct) =>
        publisher.PublishAsync(transaction, message, ct);
}
#endregion

#region docs:route-scoped-queue-inject
public sealed class ShipOrderHandler(AppDb db, [FromKeyedServices("shipping")] IOutboxQueue<AppDb> outbox)
{
    public async Task HandleAsync(Order order, Guid eventId, CancellationToken ct)
    {
        await using var transaction = await db.Database.BeginTransactionAsync(ct);

        db.Orders.Add(order);

        // No WithRoute: the keyed queue writes route 'shipping'.
        var headers = OutboxHeaders.CreateBuilder(1).Add("idempotency-key", eventId.ToString()).Build();
        var placed = new OrderPlaced(eventId, order.Id, order.AccountId, order.Total);
        outbox.Enqueue(OutboxMessage.Json(order.Id.ToString(), placed, AppJson.Default.OrderPlaced, headers));

        await db.SaveChangesAsync(ct);
        await transaction.CommitAsync(ct);
    }
}
#endregion
