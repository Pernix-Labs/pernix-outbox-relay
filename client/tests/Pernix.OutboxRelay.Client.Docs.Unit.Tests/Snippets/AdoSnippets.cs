using Dapper;
using Npgsql;

namespace Pernix.OutboxRelay.Client.Docs.Unit.Tests.Snippets;

internal static class AdoSnippets
{
    public static async Task PublishAsync(
        NpgsqlDataSource dataSource, IOutboxPublisher publisher, Guid eventId, Guid accountId, decimal amount, CancellationToken ct)
    {
        #region docs:ado-publish
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);

        int updated;
        await using (var debit = new NpgsqlCommand("UPDATE sales.accounts SET balance = balance - $1 WHERE id = $2", connection, transaction))
        {
            debit.Parameters.Add(new NpgsqlParameter<decimal> { TypedValue = amount });
            debit.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = accountId });
            updated = await debit.ExecuteNonQueryAsync(ct);
        }

        // No row changed: publish no event.
        if (updated != 1)
        {
            await transaction.RollbackAsync(ct);
            return;
        }

        // idempotency-key = the business event id, set by the producer.
        var headers = OutboxHeaders.CreateBuilder(1).Add("idempotency-key", eventId.ToString()).Build();
        var debited = new AccountDebited(eventId, accountId, amount);
        var message = OutboxMessage.Json(accountId.ToString(), debited, AppJson.Default.AccountDebited, headers);

        // After the business writes: the outbox row is the transaction's last write.
        await publisher.PublishAsync(transaction, message, ct);
        await transaction.CommitAsync(ct);
        #endregion
    }

    public static async Task PublishBatchAsync(
        NpgsqlTransaction transaction, IOutboxPublisher publisher, IReadOnlyList<OrderPlaced> events, CancellationToken ct)
    {
        #region docs:ado-publish-batch
        var messages = new OutboxMessage[events.Count];
        for (var i = 0; i < events.Count; i++)
        {
            var placed = events[i];
            var headers = OutboxHeaders.CreateBuilder(1).Add("idempotency-key", placed.EventId.ToString()).Build();
            messages[i] = OutboxMessage.Json(placed.OrderId.ToString(), placed, AppJson.Default.OrderPlaced, headers);
        }

        await publisher.PublishAsync(transaction, messages, ct);
        await transaction.CommitAsync(ct);
        #endregion
    }

    public static async Task DapperPublishAsync(
        NpgsqlDataSource dataSource, IOutboxPublisher publisher, Order order, Guid eventId, CancellationToken ct)
    {
        #region docs:dapper-publish
        await using var connection = await dataSource.OpenConnectionAsync(ct);
        await using var transaction = await connection.BeginTransactionAsync(ct);

        await connection.ExecuteAsync(new CommandDefinition(
            "INSERT INTO sales.orders (id, account_id, total) VALUES (@Id, @AccountId, @Total)",
            order,
            transaction,
            cancellationToken: ct));

        var headers = OutboxHeaders.CreateBuilder(1).Add("idempotency-key", eventId.ToString()).Build();
        var placed = new OrderPlaced(eventId, order.Id, order.AccountId, order.Total);
        var message = OutboxMessage.Json(order.Id.ToString(), placed, AppJson.Default.OrderPlaced, headers);

        await publisher.PublishAsync(transaction, message, ct);
        await transaction.CommitAsync(ct);
        #endregion
    }

    public static async Task NoAutocommitAsync(
        NpgsqlConnection connection, IOutboxPublisher publisher, OutboxMessage message, CancellationToken ct)
    {
        #region docs:no-autocommit
        // There is no autocommit publish. Without a transaction, wrap the publish in one.
        await using var transaction = await connection.BeginTransactionAsync(ct);
        await publisher.PublishAsync(transaction, message, ct);
        await transaction.CommitAsync(ct);
        #endregion
    }
}
