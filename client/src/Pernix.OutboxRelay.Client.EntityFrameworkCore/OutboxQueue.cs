using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.Logging;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore;

internal sealed class OutboxQueue : IResettableService, IDisposable
{
    private const string LoggerCategory = "Pernix.OutboxRelay.Client.EntityFrameworkCore.OutboxQueue";

    private readonly List<OutboxMessage> messages = [];
    private readonly IDbContextOptions options;
    private readonly ILoggerFactory loggerFactory;

    private Guid boundTxId;
    private bool boundIsUsed;
    private Guid poisonedTxId;
    private Guid pendingTxId;
    private int pendingCount;
    private IExecutionStrategy? executionStrategy;

    public OutboxQueue(IDbContextOptions options, ILoggerFactory loggerFactory)
    {
        this.options = options;
        this.loggerFactory = loggerFactory;
    }

    public IReadOnlyList<OutboxMessage> Messages => messages;

    public int Count => messages.Count;

    public static OutboxQueue? Find(DbContext? context) =>
        context is null
            ? null
            : ((IInfrastructure<IServiceProvider>)context).Instance.GetService(typeof(OutboxQueue)) as OutboxQueue;

    public void Enqueue(DatabaseFacade database, OutboxMessage message)
    {
        var current = database.CurrentTransaction;

        if (current is null && IsConfiguredToRetry(database))
        {
            throw new InvalidOperationException(OutboxErrors.RetryingStrategyWithoutTransaction);
        }

        Sync(current);

        if (current is not null && boundIsUsed)
        {
            throw new InvalidOperationException(OutboxErrors.UsedTransaction);
        }

        messages.Add(message);

        if (current is not null)
        {
            boundTxId = current.TransactionId;
        }
    }

    public void Sync(IDbContextTransaction? current)
    {
        if (boundTxId != Guid.Empty && (current is null || current.TransactionId != boundTxId))
        {
            Discard(messages.Count);
            messages.Clear();
            Unbind();
        }

        if (pendingTxId != Guid.Empty && (current is null || current.TransactionId != pendingTxId))
        {
            Discard(pendingCount);
            ResetPending();
        }
    }

    public void Bind(Guid transactionId, bool used)
    {
        boundTxId = transactionId;
        boundIsUsed = used;
        poisonedTxId = Guid.Empty;
    }

    public bool IsPoisoned(Guid transactionId) => poisonedTxId != Guid.Empty && poisonedTxId == transactionId;

    public void Poison(Guid transactionId)
    {
        poisonedTxId = transactionId;
        messages.Clear();
    }

    public void RecordPending(Guid transactionId, int count)
    {
        pendingTxId = transactionId;
        pendingCount = count;
    }

    public void ResetPending()
    {
        pendingTxId = Guid.Empty;
        pendingCount = 0;
    }

    public void RollBack()
    {
        messages.Clear();
        Unbind();
        ResetPending();
    }

    public void Clear() => messages.Clear();

    public void ResetState()
    {
        Discard(messages.Count);
        messages.Clear();
        Unbind();
        poisonedTxId = Guid.Empty;
        ResetPending();
    }

    public Task ResetStateAsync(CancellationToken cancellationToken = default)
    {
        ResetState();
        return Task.CompletedTask;
    }

    public void Dispose() => ResetState();

    // EF suspends ExecutionStrategy.RetriesOnFailure inside ExecuteAsync, so MaxRetryCount is read instead.
    private bool IsConfiguredToRetry(DatabaseFacade database)
    {
        executionStrategy ??= database.CreateExecutionStrategy();
        return executionStrategy is ExecutionStrategy strategy
            ? strategy.MaxRetryCount > 0
            : executionStrategy.RetriesOnFailure;
    }

    private void Unbind()
    {
        boundTxId = Guid.Empty;
        boundIsUsed = false;
    }

    private void Discard(int count)
    {
        if (count == 0)
        {
            return;
        }

        var table = options.FindExtension<OutboxOptionsExtension>()?.Table.ToString() ?? string.Empty;
        OutboxLog.Discarded(loggerFactory.CreateLogger(LoggerCategory), count, table, null);
    }
}
