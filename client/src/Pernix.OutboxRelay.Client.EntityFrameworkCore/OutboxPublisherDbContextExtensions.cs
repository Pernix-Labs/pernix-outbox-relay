using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore;

/// <summary>Publishes outbox messages in the current EF transaction of a <see cref="DbContext"/>.</summary>
public static class OutboxPublisherDbContextExtensions
{
    /// <summary>Writes <paramref name="message"/> in <c>context.Database.CurrentTransaction</c>.</summary>
    /// <remarks>
    /// Does not need <c>UseOutbox</c> and does not touch the outbox queue.
    /// Call it after the business writes, so the outbox row is the transaction's last write. Use it for writes an EF
    /// commit does not see (<c>Database.UseTransaction</c>, a transaction committed on its <c>DbTransaction</c>).
    /// Inside a <c>TransactionScope</c>, use <c>IOutboxPublisher.PublishEnlistedAsync(context.Database.GetDbConnection(), ...)</c>.
    /// </remarks>
    /// <param name="publisher">The publisher that writes the message.</param>
    /// <param name="context">The context whose current transaction receives the row.</param>
    /// <param name="message">The message to write.</param>
    /// <param name="ct">Cancels the write.</param>
    /// <returns>A task that completes when the row is written.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="publisher"/> or <paramref name="context"/> is null.</exception>
    /// <exception cref="InvalidOperationException">The context has no current EF transaction. Nothing is written.</exception>
    public static Task PublishAsync(this IOutboxPublisher publisher, DbContext context, OutboxMessage message, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(publisher);
        ArgumentNullException.ThrowIfNull(context);

        var transaction = context.Database.CurrentTransaction ?? throw new InvalidOperationException(OutboxErrors.NoCurrentTransaction);
        return publisher.PublishAsync(transaction.GetDbTransaction(), message, ct);
    }

    /// <summary>Writes <paramref name="messages"/> in <c>context.Database.CurrentTransaction</c> with one batch.</summary>
    /// <remarks>
    /// Does not need <c>UseOutbox</c> and does not touch the outbox queue.
    /// Call it after the business writes, so the outbox rows are the transaction's last writes. Use it for writes an EF
    /// commit does not see (<c>Database.UseTransaction</c>, a transaction committed on its <c>DbTransaction</c>).
    /// Inside a <c>TransactionScope</c>, use <c>IOutboxPublisher.PublishEnlistedAsync(context.Database.GetDbConnection(), ...)</c>.
    /// </remarks>
    /// <param name="publisher">The publisher that writes the messages.</param>
    /// <param name="context">The context whose current transaction receives the rows.</param>
    /// <param name="messages">The messages to write, in order.</param>
    /// <param name="ct">Cancels the write.</param>
    /// <returns>A task that completes when the rows are written.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="publisher"/>, <paramref name="context"/> or <paramref name="messages"/> is null.</exception>
    /// <exception cref="InvalidOperationException">The context has no current EF transaction. Nothing is written.</exception>
    public static Task PublishAsync(this IOutboxPublisher publisher, DbContext context, IReadOnlyList<OutboxMessage> messages, CancellationToken ct = default)
    {
        ArgumentNullException.ThrowIfNull(publisher);
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(messages);

        var transaction = context.Database.CurrentTransaction ?? throw new InvalidOperationException(OutboxErrors.NoCurrentTransaction);
        return publisher.PublishAsync(transaction.GetDbTransaction(), messages, ct);
    }
}
