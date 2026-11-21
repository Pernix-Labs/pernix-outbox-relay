using System.Data.Common;

namespace Pernix.OutboxRelay.Client;

/// <summary>
/// Writes outbox messages to one outbox table inside the caller's transaction. The relay delivers a message only after
/// that transaction commits. Each member requires a <see cref="DbTransaction"/> or an ambient
/// <see cref="System.Transactions.TransactionScope"/>. A connection not enlisted in the scope follows its own autocommit
/// state.
/// </summary>
/// <remarks>
/// Write-order rule: call a publish method after the business writes of the same transaction. A message published
/// before the business writes can reach consumers ahead of the state it describes.
/// </remarks>
public interface IOutboxPublisher
{
    /// <summary>Gets the outbox table this publisher writes to.</summary>
    OutboxTable Table { get; }

    /// <summary>
    /// Inserts one outbox row in <paramref name="transaction"/>. The row commits or rolls back with the transaction.
    /// </summary>
    /// <remarks>
    /// Write-order rule: call this after the business writes of the same transaction. The insert runs now, on the
    /// transaction's connection. The method never commits, rolls back or disposes the transaction.
    /// </remarks>
    /// <param name="transaction">The open transaction that holds the business writes.</param>
    /// <param name="message">The message to write.</param>
    /// <param name="ct">Cancels the insert. After cancellation the caller decides what happens to the transaction.</param>
    /// <returns>A task that completes when the row is inserted.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="transaction"/> is null.</exception>
    /// <exception cref="InvalidOperationException">
    /// The transaction belongs to another database provider or is already completed.
    /// </exception>
    /// <exception cref="OperationCanceledException"><paramref name="ct"/> is cancelled.</exception>
    Task PublishAsync(DbTransaction transaction, OutboxMessage message, CancellationToken ct = default);

    /// <summary>
    /// Inserts one outbox row per message in <paramref name="transaction"/>, in one round trip. Row ids ascend in list
    /// order. An empty list returns without contacting the database.
    /// </summary>
    /// <remarks>
    /// Write-order rule: call this after the business writes of the same transaction. A large batch holds the caller's
    /// transaction open and its rows in memory until the round trip completes. The batch size is the caller's choice.
    /// </remarks>
    /// <param name="transaction">The open transaction that holds the business writes.</param>
    /// <param name="messages">The messages to write, in delivery order.</param>
    /// <param name="ct">Cancels the insert. After cancellation the caller decides what happens to the transaction.</param>
    /// <returns>A task that completes when the rows are inserted.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="transaction"/> or <paramref name="messages"/> is null.</exception>
    /// <exception cref="InvalidOperationException">
    /// The transaction belongs to another database provider or is already completed.
    /// </exception>
    /// <exception cref="OperationCanceledException"><paramref name="ct"/> is cancelled.</exception>
    Task PublishAsync(DbTransaction transaction, IReadOnlyList<OutboxMessage> messages, CancellationToken ct = default);

    /// <summary>
    /// Inserts one outbox row on <paramref name="connection"/>, inside the ambient
    /// <see cref="System.Transactions.TransactionScope"/>. Requires <see cref="System.Transactions.Transaction.Current"/>.
    /// </summary>
    /// <remarks>
    /// Open the connection inside the scope with enlistment on (<c>Enlist=true</c>, the provider default), and create
    /// the scope with <see cref="System.Transactions.TransactionScopeAsyncFlowOption.Enabled"/>. The insert runs in
    /// whatever transaction the connection is in. A connection opened before the scope is not enlisted, so its insert
    /// autocommits like its business writes. Without a scope, pass the <see cref="DbTransaction"/> to
    /// <see cref="PublishAsync(DbTransaction, OutboxMessage, CancellationToken)"/> instead.
    /// Write-order rule: call this after the business writes of the same scope.
    /// </remarks>
    /// <param name="connection">The open connection, opened inside the scope, that holds the business writes.</param>
    /// <param name="message">The message to write.</param>
    /// <param name="ct">Cancels the insert. After cancellation the caller decides what happens to the scope.</param>
    /// <returns>A task that completes when the row is inserted.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="connection"/> is null.</exception>
    /// <exception cref="InvalidOperationException">
    /// No ambient transaction exists, the connection is not open, or it belongs to another database provider.
    /// </exception>
    /// <exception cref="OperationCanceledException"><paramref name="ct"/> is cancelled.</exception>
    Task PublishEnlistedAsync(DbConnection connection, OutboxMessage message, CancellationToken ct = default);

    /// <summary>
    /// Inserts one outbox row per message on <paramref name="connection"/>, inside the ambient
    /// <see cref="System.Transactions.TransactionScope"/>, in one round trip. Row ids ascend in list order. An empty
    /// list returns without contacting the database.
    /// </summary>
    /// <remarks>
    /// Open the connection inside the scope with enlistment on (<c>Enlist=true</c>, the provider default), and create
    /// the scope with <see cref="System.Transactions.TransactionScopeAsyncFlowOption.Enabled"/>. Without a scope, pass
    /// the <see cref="DbTransaction"/> to
    /// <see cref="PublishAsync(DbTransaction, IReadOnlyList{OutboxMessage}, CancellationToken)"/> instead.
    /// Write-order rule: call this after the business writes of the same scope. A large batch holds the caller's
    /// transaction open and its rows in memory until the round trip completes.
    /// </remarks>
    /// <param name="connection">The open connection, opened inside the scope, that holds the business writes.</param>
    /// <param name="messages">The messages to write, in delivery order.</param>
    /// <param name="ct">Cancels the insert. After cancellation the caller decides what happens to the scope.</param>
    /// <returns>A task that completes when the rows are inserted.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="connection"/> or <paramref name="messages"/> is null.</exception>
    /// <exception cref="InvalidOperationException">
    /// No ambient transaction exists, the connection is not open, or it belongs to another database provider.
    /// </exception>
    /// <exception cref="OperationCanceledException"><paramref name="ct"/> is cancelled.</exception>
    Task PublishEnlistedAsync(DbConnection connection, IReadOnlyList<OutboxMessage> messages, CancellationToken ct = default);
}
