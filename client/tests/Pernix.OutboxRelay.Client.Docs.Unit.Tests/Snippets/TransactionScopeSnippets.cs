using System.Transactions;
using Npgsql;

namespace Pernix.OutboxRelay.Client.Docs.Unit.Tests.Snippets;

internal static class TransactionScopeSnippets
{
    public static async Task PublishEnlistedAsync(
        NpgsqlDataSource dataSource, IOutboxPublisher publisher, Guid eventId, Guid accountId, decimal amount, CancellationToken ct)
    {
        #region docs:transaction-scope
        using var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);

        // Open the connection inside the scope (Enlist=true, the default). A connection opened before the scope is not
        // enlisted: its writes and the outbox row autocommit independently.
        await using var connection = await dataSource.OpenConnectionAsync(ct);

        int updated;
        await using (var debit = new NpgsqlCommand("UPDATE sales.accounts SET balance = balance - $1 WHERE id = $2", connection))
        {
            debit.Parameters.Add(new NpgsqlParameter<decimal> { TypedValue = amount });
            debit.Parameters.Add(new NpgsqlParameter<Guid> { TypedValue = accountId });
            updated = await debit.ExecuteNonQueryAsync(ct);
        }

        // No row changed: publish no event and do not complete the scope.
        if (updated != 1)
        {
            return;
        }

        var headers = OutboxHeaders.CreateBuilder(1).Add("idempotency-key", eventId.ToString()).Build();
        var debited = new AccountDebited(eventId, accountId, amount);
        var message = OutboxMessage.Json(accountId.ToString(), debited, AppJson.Default.AccountDebited, headers);

        await publisher.PublishEnlistedAsync(connection, message, ct);
        scope.Complete();
        #endregion
    }
}
