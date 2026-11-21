using Microsoft.EntityFrameworkCore;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore;

internal static class OutboxErrors
{
    public const string NoTransactionAtSave =
        "SaveChanges was called with queued outbox messages and no EF transaction, so the messages could not be written atomically. Nothing was saved. Begin a transaction with Database.BeginTransactionAsync and commit it with CommitAsync, or publish inside your own transaction with IOutboxPublisher.PublishAsync(DbContext, ...).";

    public const string SyncCommit =
        "The EF transaction has queued outbox messages, which are written asynchronously at commit. Commit with CommitAsync. The transaction was not committed.";

    public const string UsedTransaction =
        "The context's transaction came from Database.UseTransaction. The outbox queue writes only in transactions the context begins with Database.BeginTransaction[Async]. Call IOutboxPublisher.PublishAsync(DbContext, ...) after the business writes instead.";

    public const string PoisonedTransaction =
        "Writing the queued outbox messages failed in an earlier commit of this EF transaction. Roll back or dispose the transaction and re-run the unit of work on a fresh context.";

    public const string UseOutboxTwice =
        "UseOutbox was already called on this DbContextOptionsBuilder. A context has one outbox queue and one publisher.";

    public const string NoCurrentTransaction =
        "The context has no current EF transaction. Begin one with Database.BeginTransactionAsync before publishing. Inside a TransactionScope, use IOutboxPublisher.PublishEnlistedAsync(context.Database.GetDbConnection(), ...).";

    public const string RetryingStrategyWithoutTransaction =
        "The context's execution strategy retries on failure and the context has no current EF transaction, so a retry could commit the business writes without this message. The message was not queued. Enqueue inside the operation passed to Database.CreateExecutionStrategy().ExecuteAsync, after Database.BeginTransactionAsync.";

    public static string NoQueue(DbContext context) =>
        $"DbContext type '{context.GetType().Name}' has no outbox queue. Call UseOutbox(publisher) on its DbContextOptionsBuilder.";
}
