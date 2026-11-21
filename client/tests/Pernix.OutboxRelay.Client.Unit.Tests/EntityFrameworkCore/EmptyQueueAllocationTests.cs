using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using Pernix.OutboxRelay.Client.EntityFrameworkCore;
using Pernix.OutboxRelay.Client.PostgreSql;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Pernix.OutboxRelay.Client.Unit.Tests.EntityFrameworkCore;

// Offline context, no connection opened.
public sealed class EmptyQueueAllocationTests
{
    private const int WarmUp = 3;
    private const int Iterations = 1000;

    private static readonly OutboxMessage Message = OutboxMessage.Create("k", "p");

    private readonly PostgreSqlOutboxPublisher publisher = new(OutboxTable.Parse("s.t"));

    [Fact]
    public async Task Empty_queue_callbacks_allocate_nothing()
    {
        await using var context = new OfflineContext(
            new DbContextOptionsBuilder<OfflineContext>().UseNpgsql("Host=unused").UseOutbox(publisher).Options);
        var interceptor = new OutboxInterceptor(publisher);
        var queue = OutboxQueue.Find(context)!;
        var definition = new Definition(context.GetService<ILoggingOptions>());
        var connection = context.Database.GetDbConnection();
        var transaction = new FakeTransaction();
        var transactionId = Guid.NewGuid();
        var contextData = new DbContextEventData(definition, static (_, _) => string.Empty, context);
        var transactionData = new TransactionEventData(
            definition, static (_, _) => string.Empty, transaction, context, transactionId, Guid.NewGuid(), true, DateTimeOffset.UtcNow);
        var endData = new TransactionEndEventData(
            definition, static (_, _) => string.Empty, transaction, context, transactionId, Guid.NewGuid(), true, DateTimeOffset.UtcNow, TimeSpan.Zero);
        var ct = TestContext.Current.CancellationToken;

        // Synchronous loop: every async callback must complete synchronously, counted instead of awaited.
        var pending = 0;

        void Run(int count)
        {
            for (var i = 0; i < count; i++)
            {
                interceptor.SavingChanges(contextData, default);
                pending += Pending(interceptor.SavingChangesAsync(contextData, default, ct));
                interceptor.TransactionStarted(connection, endData, transaction);
                pending += Pending(interceptor.TransactionStartedAsync(connection, endData, transaction, ct));
                interceptor.TransactionCommitting(transaction, transactionData, default);
                pending += Pending(interceptor.TransactionCommittingAsync(transaction, transactionData, default, ct));
                interceptor.TransactionCommitted(transaction, endData);
                pending += Pending(interceptor.TransactionCommittedAsync(transaction, endData, ct));
                interceptor.TransactionRollingBack(transaction, transactionData, default);
                pending += Pending(interceptor.TransactionRollingBackAsync(transaction, transactionData, default, ct));
                interceptor.TransactionUsed(connection, transactionData, transaction);
                pending += Pending(interceptor.TransactionUsedAsync(connection, transactionData, transaction, ct));
                queue.ResetState();
                pending += Pending(queue.ResetStateAsync(ct));
            }
        }

        Run(WarmUp);
        var before = GC.GetAllocatedBytesForCurrentThread();
        Run(Iterations);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        TestContext.Current.TestOutputHelper?.WriteLine($"Empty-queue callbacks: {allocated} B over {Iterations} iterations");
        Assert.Equal(0, pending);
        Assert.Equal(0, allocated);
        Assert.Equal(0, queue.Count);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Enqueue_allocates_nothing_after_first_call(bool zeroRetryOption)
    {
        using var context = new OfflineContext(
            new DbContextOptionsBuilder<OfflineContext>()
                .UseNpgsql("Host=unused", zeroRetryOption ? static n => n.EnableRetryOnFailure(maxRetryCount: 0) : null)
                .UseOutbox(publisher)
                .Options);
        var queue = OutboxQueue.Find(context)!;

        // An ExecutionStrategy that never retries: covers the MaxRetryCount read. A retrying one throws E8 here.
        Run(context, queue, WarmUp);
        var before = GC.GetAllocatedBytesForCurrentThread();
        Run(context, queue, Iterations);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        TestContext.Current.TestOutputHelper?.WriteLine($"Enqueue (zeroRetryOption={zeroRetryOption}): {allocated} B over {Iterations} iterations");
        Assert.Equal(0, allocated);
    }

    private static void Run(OfflineContext context, OutboxQueue queue, int count)
    {
        for (var i = 0; i < count; i++)
        {
            context.EnqueueOutbox(Message);
            queue.Clear();
        }
    }

    private static int Pending<T>(ValueTask<T> task) => task.IsCompletedSuccessfully ? 0 : 1;

    private static int Pending(Task task) => task.IsCompletedSuccessfully ? 0 : 1;

    public sealed class OfflineContext(DbContextOptions<OfflineContext> options) : DbContext(options);

    private sealed class Definition(ILoggingOptions loggingOptions)
        : EventDefinitionBase(loggingOptions, new EventId(1, "Test"), LogLevel.Debug, "Test.Event");

    [SuppressMessage("Usage", "CA2215:Dispose methods should call base class dispose", Justification = "Nothing to dispose.")]
    private sealed class FakeTransaction : DbTransaction
    {
        public override IsolationLevel IsolationLevel => IsolationLevel.ReadCommitted;

        protected override DbConnection? DbConnection => null;

        public override void Commit()
        {
        }

        public override void Rollback()
        {
        }
    }
}
