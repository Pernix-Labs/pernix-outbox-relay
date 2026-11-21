using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore;

/// <summary>Configures the outbox queue on a <see cref="DbContextOptionsBuilder"/>.</summary>
public static class OutboxDbContextOptionsBuilderExtensions
{
    /// <summary>Adds a per-context outbox queue and an interceptor that writes it through <paramref name="publisher"/>.</summary>
    /// <remarks>
    /// <para>
    /// The same line works with <c>AddDbContext</c>, <c>AddDbContextPool</c>, <c>AddDbContextFactory</c> and
    /// <c>AddPooledDbContextFactory</c>. Each context instance has its own queue, cleared for every pooled lease.
    /// </para>
    /// <para>
    /// Queued messages are written with one batch at <c>CommitAsync</c> of a transaction the context began with
    /// <c>Database.BeginTransactionAsync</c>, as that transaction's last writes. <c>SaveChanges</c> without such a
    /// transaction throws <see cref="InvalidOperationException"/> while the queue is non-empty, and saves nothing.
    /// </para>
    /// <para>
    /// Messages dropped without a commit (context disposed or returned to the pool, transaction ended outside EF) are
    /// reported with Warning <c>OutboxMessagesDiscarded</c> through the context's <c>ILoggerFactory</c>
    /// (<c>UseLoggerFactory</c> or the application logger factory). <c>LogTo</c> output does not receive it.
    /// </para>
    /// <para>
    /// Under a retrying execution strategy (<c>EnableRetryOnFailure</c> or a custom <c>IExecutionStrategy</c>), enqueue
    /// inside the operation passed to <c>strategy.ExecuteAsync</c>, after <c>BeginTransactionAsync</c>. An enqueue with
    /// no current EF transaction, before <c>BeginTransactionAsync</c> inside that operation or anywhere outside it, throws
    /// <see cref="InvalidOperationException"/> and queues nothing, because a retry could commit the business writes
    /// without the message or write it twice. When a retry runs, the messages of the failed attempt are discarded with
    /// Warning <c>OutboxMessagesDiscarded</c> and the retry writes the messages it enqueues.
    /// </para>
    /// </remarks>
    /// <param name="builder">The options builder of the context.</param>
    /// <param name="publisher">The publisher that writes the queued messages.</param>
    /// <returns><paramref name="builder"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> or <paramref name="publisher"/> is null.</exception>
    /// <exception cref="InvalidOperationException"><c>UseOutbox</c> was already called on this builder.</exception>
    public static DbContextOptionsBuilder UseOutbox(this DbContextOptionsBuilder builder, IOutboxPublisher publisher)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(publisher);

        if (builder.Options.FindExtension<OutboxOptionsExtension>() is not null)
        {
            throw new InvalidOperationException(OutboxErrors.UseOutboxTwice);
        }

        ((IDbContextOptionsBuilderInfrastructure)builder).AddOrUpdateExtension(new OutboxOptionsExtension(publisher.Table));
        return builder.AddInterceptors(new OutboxInterceptor(publisher));
    }

    /// <summary>Same as <see cref="UseOutbox(DbContextOptionsBuilder, IOutboxPublisher)"/>, with a typed return.</summary>
    /// <remarks>
    /// Under a retrying execution strategy (<c>EnableRetryOnFailure</c> or a custom <c>IExecutionStrategy</c>), enqueue
    /// inside the operation passed to <c>strategy.ExecuteAsync</c>, after <c>BeginTransactionAsync</c>. An enqueue with
    /// no current EF transaction, before <c>BeginTransactionAsync</c> inside that operation or anywhere outside it, throws
    /// <see cref="InvalidOperationException"/> and queues nothing, because a retry could commit the business writes
    /// without the message or write it twice. When a retry runs, the messages of the failed attempt are discarded with
    /// Warning <c>OutboxMessagesDiscarded</c> and the retry writes the messages it enqueues.
    /// </remarks>
    /// <typeparam name="TContext">The context type.</typeparam>
    /// <param name="builder">The options builder of the context.</param>
    /// <param name="publisher">The publisher that writes the queued messages.</param>
    /// <returns><paramref name="builder"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="builder"/> or <paramref name="publisher"/> is null.</exception>
    /// <exception cref="InvalidOperationException"><c>UseOutbox</c> was already called on this builder.</exception>
    public static DbContextOptionsBuilder<TContext> UseOutbox<TContext>(this DbContextOptionsBuilder<TContext> builder, IOutboxPublisher publisher)
        where TContext : DbContext
    {
        UseOutbox((DbContextOptionsBuilder)builder, publisher);
        return builder;
    }
}
