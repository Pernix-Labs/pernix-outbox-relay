using Microsoft.EntityFrameworkCore;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore;

/// <summary>Queues outbox messages on a <see cref="DbContext"/> configured with <c>UseOutbox</c>.</summary>
public static class OutboxDbContextExtensions
{
    /// <summary>Appends <paramref name="message"/> to this context instance's outbox queue. No database round trip.</summary>
    /// <remarks>
    /// <para>
    /// Commit with <c>CommitAsync</c> on a transaction the context began with <c>Database.BeginTransactionAsync</c>: the
    /// queue is written there, after the business writes. A rollback to a savepoint does not drop messages enqueued
    /// after the savepoint. After a failed transaction, re-run the unit of work on a fresh context.
    /// </para>
    /// <para>
    /// Under <c>Database.UseTransaction</c>, inside a <c>TransactionScope</c>, or for <c>ExecuteUpdate</c> or raw SQL
    /// committed outside an EF transaction, use <c>IOutboxPublisher.PublishAsync(DbContext, ...)</c> instead.
    /// </para>
    /// <para>
    /// Under a retrying execution strategy (<c>EnableRetryOnFailure</c> or a custom <c>IExecutionStrategy</c>), enqueue
    /// inside the operation passed to <c>strategy.ExecuteAsync</c>, after <c>BeginTransactionAsync</c>. An enqueue with
    /// no current EF transaction, before <c>BeginTransactionAsync</c> inside that operation or anywhere outside it, throws
    /// <see cref="InvalidOperationException"/> and queues nothing, because a retry could commit the business writes
    /// without the message or write it twice. When a retry runs, the messages of the failed attempt are discarded with
    /// Warning <c>OutboxMessagesDiscarded</c> and the retry writes the messages it enqueues.
    /// </para>
    /// <para>
    /// The queue keeps references to the message's key and payload buffers until the transaction commits or rolls back,
    /// or the queue discards the message. Do not change or reuse those buffers before then.
    /// </para>
    /// </remarks>
    /// <param name="context">The context whose queue receives the message.</param>
    /// <param name="message">The message to queue.</param>
    /// <exception cref="ArgumentNullException"><paramref name="context"/> is null.</exception>
    /// <exception cref="InvalidOperationException">
    /// The context was configured without <c>UseOutbox</c>, or its execution strategy retries on failure and it has no
    /// current EF transaction, or its current transaction came from <c>Database.UseTransaction</c>. Nothing is queued.
    /// </exception>
    public static void EnqueueOutbox(this DbContext context, OutboxMessage message)
    {
        ArgumentNullException.ThrowIfNull(context);

        var queue = OutboxQueue.Find(context) ?? throw new InvalidOperationException(OutboxErrors.NoQueue(context));
        queue.Enqueue(context.Database, message);
    }
}
