using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore;

internal sealed class OutboxInterceptor : ISaveChangesInterceptor, IDbTransactionInterceptor
{
    private readonly IOutboxPublisher publisher;

    public OutboxInterceptor(IOutboxPublisher publisher)
    {
        this.publisher = publisher;
    }

    public InterceptionResult<int> SavingChanges(DbContextEventData eventData, InterceptionResult<int> result)
    {
        Guard(eventData.Context);
        return result;
    }

    public ValueTask<InterceptionResult<int>> SavingChangesAsync(
        DbContextEventData eventData,
        InterceptionResult<int> result,
        CancellationToken cancellationToken = default)
    {
        Guard(eventData.Context);
        return new ValueTask<InterceptionResult<int>>(result);
    }

    public DbTransaction TransactionStarted(DbConnection connection, TransactionEndEventData eventData, DbTransaction result)
    {
        Started(eventData);
        return result;
    }

    public ValueTask<DbTransaction> TransactionStartedAsync(
        DbConnection connection,
        TransactionEndEventData eventData,
        DbTransaction result,
        CancellationToken cancellationToken = default)
    {
        Started(eventData);
        return new ValueTask<DbTransaction>(result);
    }

    public DbTransaction TransactionUsed(DbConnection connection, TransactionEventData eventData, DbTransaction result)
    {
        Used(eventData);
        return result;
    }

    public ValueTask<DbTransaction> TransactionUsedAsync(
        DbConnection connection,
        TransactionEventData eventData,
        DbTransaction result,
        CancellationToken cancellationToken = default)
    {
        Used(eventData);
        return new ValueTask<DbTransaction>(result);
    }

    public InterceptionResult TransactionCommitting(DbTransaction transaction, TransactionEventData eventData, InterceptionResult result)
    {
        var queue = OutboxQueue.Find(eventData.Context);
        if (queue is null)
        {
            return result;
        }

        if (queue.IsPoisoned(eventData.TransactionId))
        {
            throw new InvalidOperationException(OutboxErrors.PoisonedTransaction);
        }

        if (queue.Count > 0)
        {
            throw new InvalidOperationException(OutboxErrors.SyncCommit);
        }

        return result;
    }

    public ValueTask<InterceptionResult> TransactionCommittingAsync(
        DbTransaction transaction,
        TransactionEventData eventData,
        InterceptionResult result,
        CancellationToken cancellationToken = default)
    {
        var queue = OutboxQueue.Find(eventData.Context);
        if (queue is null)
        {
            return new ValueTask<InterceptionResult>(result);
        }

        if (queue.IsPoisoned(eventData.TransactionId))
        {
            throw new InvalidOperationException(OutboxErrors.PoisonedTransaction);
        }

        if (queue.Count == 0)
        {
            return new ValueTask<InterceptionResult>(result);
        }

        return FlushAsync(queue, transaction, eventData.TransactionId, result, cancellationToken);
    }

    public void TransactionCommitted(DbTransaction transaction, TransactionEndEventData eventData) =>
        OutboxQueue.Find(eventData.Context)?.ResetPending();

    public Task TransactionCommittedAsync(
        DbTransaction transaction,
        TransactionEndEventData eventData,
        CancellationToken cancellationToken = default)
    {
        OutboxQueue.Find(eventData.Context)?.ResetPending();
        return Task.CompletedTask;
    }

    public InterceptionResult TransactionRollingBack(DbTransaction transaction, TransactionEventData eventData, InterceptionResult result)
    {
        OutboxQueue.Find(eventData.Context)?.RollBack();
        return result;
    }

    public ValueTask<InterceptionResult> TransactionRollingBackAsync(
        DbTransaction transaction,
        TransactionEventData eventData,
        InterceptionResult result,
        CancellationToken cancellationToken = default)
    {
        OutboxQueue.Find(eventData.Context)?.RollBack();
        return new ValueTask<InterceptionResult>(result);
    }

    private static void Guard(DbContext? context)
    {
        var queue = OutboxQueue.Find(context);
        if (queue is null)
        {
            return;
        }

        var current = context!.Database.CurrentTransaction;
        queue.Sync(current);
        if (queue.Count > 0 && current is null)
        {
            throw new InvalidOperationException(OutboxErrors.NoTransactionAtSave);
        }
    }

    // CurrentTransaction is not yet assigned here, and any bound transaction has ended.
    private static void Started(TransactionEndEventData eventData)
    {
        var queue = OutboxQueue.Find(eventData.Context);
        if (queue is null)
        {
            return;
        }

        queue.Sync(null);
        queue.Bind(eventData.TransactionId, used: false);
    }

    private static void Used(TransactionEventData eventData)
    {
        var queue = OutboxQueue.Find(eventData.Context);
        if (queue is null)
        {
            return;
        }

        queue.Sync(null);
        if (queue.Count > 0)
        {
            throw new InvalidOperationException(OutboxErrors.UsedTransaction);
        }

        queue.Bind(eventData.TransactionId, used: true);
    }

    private async ValueTask<InterceptionResult> FlushAsync(
        OutboxQueue queue,
        DbTransaction transaction,
        Guid transactionId,
        InterceptionResult result,
        CancellationToken cancellationToken)
    {
        var count = queue.Count;
        try
        {
            await publisher.PublishAsync(transaction, queue.Messages, cancellationToken).ConfigureAwait(false);
        }
        catch
        {
            queue.RecordPending(transactionId, count);
            queue.Poison(transactionId);
            throw;
        }

        queue.RecordPending(transactionId, count);
        queue.Clear();
        return result;
    }
}
