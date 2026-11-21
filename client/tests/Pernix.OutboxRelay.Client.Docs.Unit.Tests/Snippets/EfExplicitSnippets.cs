using System.Transactions;
using Pernix.OutboxRelay.Client.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore;

namespace Pernix.OutboxRelay.Client.Docs.Unit.Tests.Snippets;

internal static class EfExplicitSnippets
{
    public static async Task SaveChangesAsync(AppDb db, IOutboxPublisher publisher, Order order, Guid eventId, CancellationToken ct)
    {
        #region docs:ef-o15-savechanges
        await using var transaction = await db.Database.BeginTransactionAsync(ct);

        db.Orders.Add(order);
        await db.SaveChangesAsync(ct);

        var headers = OutboxHeaders.CreateBuilder(1).Add("idempotency-key", eventId.ToString()).Build();
        var placed = new OrderPlaced(eventId, order.Id, order.AccountId, order.Total);
        var message = OutboxMessage.Json(order.Id.ToString(), placed, AppJson.Default.OrderPlaced, headers);

        // Needs no UseOutbox. Writes in db.Database.CurrentTransaction, refused without one.
        await publisher.PublishAsync(db, message, ct);
        await transaction.CommitAsync(ct);
        #endregion
    }

    public static async Task ExecuteUpdateAsync(
        AppDb db, IOutboxPublisher publisher, Guid eventId, Guid accountId, decimal amount, CancellationToken ct)
    {
        #region docs:ef-o15-executeupdate
        await using var transaction = await db.Database.BeginTransactionAsync(ct);

        await db.Accounts
            .Where(a => a.Id == accountId)
            .ExecuteUpdateAsync(set => set.SetProperty(a => a.Balance, a => a.Balance - amount), ct);

        var headers = OutboxHeaders.CreateBuilder(1).Add("idempotency-key", eventId.ToString()).Build();
        var debited = new AccountDebited(eventId, accountId, amount);
        var message = OutboxMessage.Json(accountId.ToString(), debited, AppJson.Default.AccountDebited, headers);

        await publisher.PublishAsync(db, message, ct);
        await transaction.CommitAsync(ct);
        #endregion
    }

    public static async Task ScopeAsync(AppDb db, IOutboxPublisher publisher, Order order, OutboxMessage message, CancellationToken ct)
    {
        #region docs:ef-o15-scope
        using var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);

        // Open inside the scope and keep it open: PublishEnlistedAsync needs the open, enlisted connection.
        await db.Database.OpenConnectionAsync(ct);

        db.Orders.Add(order);
        await db.SaveChangesAsync(ct);

        await publisher.PublishEnlistedAsync(db.Database.GetDbConnection(), message, ct);
        scope.Complete();
        #endregion
    }
}
