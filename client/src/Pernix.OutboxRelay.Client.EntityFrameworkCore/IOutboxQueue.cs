using Microsoft.EntityFrameworkCore;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore;

/// <summary>Queues outbox messages on the scope's <typeparamref name="TContext"/> instance. Register with <c>AddOutboxQueue</c>, or with <c>AddKeyedOutboxQueue</c> for a route-scoped queue.</summary>
/// <typeparam name="TContext">A context type configured with <c>UseOutbox</c> and registered scoped.</typeparam>
public interface IOutboxQueue<TContext>
    where TContext : DbContext
{
    /// <summary>Calls <c>EnqueueOutbox</c> on the scope's <typeparamref name="TContext"/> instance. No database round trip.</summary>
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
    /// <param name="message">The message to queue.</param>
    /// <exception cref="InvalidOperationException">
    /// The context was configured without <c>UseOutbox</c>, or its execution strategy retries on failure and it has no
    /// current EF transaction, or its current transaction came from <c>Database.UseTransaction</c>. Nothing is queued.
    /// </exception>
    /// <exception cref="ArgumentException">The queue is route-scoped and <paramref name="message"/> has a different route.</exception>
    void Enqueue(OutboxMessage message);
}
