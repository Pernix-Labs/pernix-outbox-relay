using System.Data.Common;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using Npgsql.EntityFrameworkCore.PostgreSQL.Infrastructure;
using Xunit;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore.Integration.Tests;

public sealed class ExecutionStrategyTests(EfPostgreSqlFixture postgres)
{
    private const string E8 =
        "The context's execution strategy retries on failure and the context has no current EF transaction, so a retry could commit the business writes without this message. The message was not queued. Enqueue inside the operation passed to Database.CreateExecutionStrategy().ExecuteAsync, after Database.BeginTransactionAsync.";

    private const string E4 =
        "The context's transaction came from Database.UseTransaction. The outbox queue writes only in transactions the context begins with Database.BeginTransaction[Async]. Call IOutboxPublisher.PublishAsync(DbContext, ...) after the business writes instead.";

    private static readonly Action<NpgsqlDbContextOptionsBuilder> Retrying =
        static npgsql => npgsql.ExecutionStrategy(static dependencies => new OneRetryExecutionStrategy(dependencies));

    public enum Fault
    {
        BeforeBegin,
        BusinessCommand,
        Flush,
        AfterFlush,
    }

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Enqueue_outside_retrying_strategy_without_transaction_throws_and_leaves_queue_unchanged(bool facade)
    {
        var schema = await postgres.CreateSchemaAsync();
        var counter = new CommandCountingInterceptor();
        var services = new ServiceCollection();
        services.AddDbContext<ItemsDbContext>(options => schema.Configure(options, npgsql: Retrying).AddInterceptors(counter));
        services.AddOutboxQueue<ItemsDbContext>();
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<ItemsDbContext>();
        var outboxQueue = scope.ServiceProvider.GetRequiredService<IOutboxQueue<ItemsDbContext>>();
        Action<OutboxMessage> enqueue = facade ? outboxQueue.Enqueue : context.EnqueueOutbox;
        var queue = Assert.IsType<OutboxQueue>(OutboxQueue.Find(context));
        var strategy = context.Database.CreateExecutionStrategy();

        AssertRefused(enqueue, queue, counter, expectedCount: 0);

        // Inside-enqueue in a transaction that ends without commit: the message stays queued until the next sight.
        await strategy.ExecuteAsync(async () =>
        {
            await using var transaction = await context.Database.BeginTransactionAsync(Ct);
            enqueue(OutboxMessage.Create("k", "stale"));
        });

        AssertRefused(enqueue, queue, counter, expectedCount: 1);
        Assert.Empty(schema.OutboxLogs());

        await strategy.ExecuteAsync(async () =>
        {
            context.ChangeTracker.Clear();
            await using var transaction = await context.Database.BeginTransactionAsync(Ct);
            enqueue(OutboxMessage.Create("k", "own"));
            context.Items.Add(new Item { Name = "own" });
            await context.SaveChangesAsync(Ct);
            await transaction.CommitAsync(Ct);
        });

        Assert.Equal(["own"], await schema.OutboxPayloadsAsync());
        Assert.Equal(["own"], await schema.ItemNamesAsync());
        AssertSingleDiscardWarning(schema);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Enqueue_inside_retrying_strategy_before_BeginTransactionAsync_throws_and_leaves_queue_unchanged(bool facade)
    {
        var schema = await postgres.CreateSchemaAsync();
        var counter = new CommandCountingInterceptor();
        var services = new ServiceCollection();
        services.AddDbContext<ItemsDbContext>(options => schema.Configure(options, npgsql: Retrying).AddInterceptors(counter));
        services.AddOutboxQueue<ItemsDbContext>();
        await using var provider = services.BuildServiceProvider();
        await using var scope = provider.CreateAsyncScope();
        var context = scope.ServiceProvider.GetRequiredService<ItemsDbContext>();
        var outboxQueue = scope.ServiceProvider.GetRequiredService<IOutboxQueue<ItemsDbContext>>();
        Action<OutboxMessage> enqueue = facade ? outboxQueue.Enqueue : context.EnqueueOutbox;
        var queue = Assert.IsType<OutboxQueue>(OutboxQueue.Find(context));

        // First enqueue of the context runs inside the operation, where EF suspends RetriesOnFailure.
        await context.Database.CreateExecutionStrategy().ExecuteAsync(async () =>
        {
            AssertRefused(enqueue, queue, counter, expectedCount: 0);

            await using var transaction = await context.Database.BeginTransactionAsync(Ct);
            enqueue(OutboxMessage.Create("k", "message"));
            context.Items.Add(new Item { Name = "business" });
            await context.SaveChangesAsync(Ct);
            await transaction.CommitAsync(Ct);
        });

        Assert.Equal(["business"], await schema.ItemNamesAsync());
        Assert.Equal(["message"], await schema.OutboxPayloadsAsync());
        await AssertOneTransactionAsync(schema);
        Assert.Empty(schema.OutboxLogs());
    }

    [Theory]
    [InlineData(Fault.BeforeBegin)]
    [InlineData(Fault.BusinessCommand)]
    [InlineData(Fault.Flush)]
    [InlineData(Fault.AfterFlush)]
    public async Task Inside_enqueue_after_begin_with_one_transient_failure_commits_business_and_outbox_rows_once(Fault fault)
    {
        var schema = await postgres.CreateSchemaAsync();
        var publisher = fault == Fault.Flush
            ? new ThrowingPublisher(schema.Publisher, new TransientFaultException(), failCount: 1)
            : (IOutboxPublisher)schema.Publisher;
        var builder = schema.Configure(new DbContextOptionsBuilder<ItemsDbContext>(), publisher, Retrying);
        var faultOnOpen = new FaultOnFirstOpenInterceptor();
        var faultOnInsert = new FaultOnFirstInsertInterceptor();
        var faultOnCommit = new FaultOnFirstCommitInterceptor();
        if (fault == Fault.BeforeBegin)
        {
            // BeginTransactionAsync opens the connection: the fault fires before EF raises TransactionStarted.
            builder.AddInterceptors(faultOnOpen);
        }
        else if (fault == Fault.BusinessCommand)
        {
            builder.AddInterceptors(faultOnInsert);
        }
        else if (fault == Fault.AfterFlush)
        {
            // Registered after UseOutbox: runs after the flush and skips the database commit.
            builder.AddInterceptors(faultOnCommit);
        }

        await using var context = new ItemsDbContext((DbContextOptions<ItemsDbContext>)builder.Options);
        var strategy = context.Database.CreateExecutionStrategy();
        var attempts = 0;

        await strategy.ExecuteAsync(async () =>
        {
            attempts++;
            context.ChangeTracker.Clear();
            await using var transaction = await context.Database.BeginTransactionAsync(Ct);
            context.EnqueueOutbox(OutboxMessage.Create("k", "message"));
            context.Items.Add(new Item { Name = "business" });
            await context.SaveChangesAsync(Ct);
            await transaction.CommitAsync(Ct);
        });

        Assert.Equal(2, attempts);
        Assert.Equal(fault == Fault.BeforeBegin, faultOnOpen.Faulted);
        Assert.Equal(fault == Fault.BusinessCommand, faultOnInsert.Faulted);
        Assert.Equal(fault == Fault.AfterFlush, faultOnCommit.Faulted);
        if (publisher is ThrowingPublisher throwing)
        {
            Assert.Equal(2, throwing.Calls);
        }

        Assert.Equal(["business"], await schema.ItemNamesAsync());
        Assert.Equal(["message"], await schema.OutboxPayloadsAsync());
        await AssertOneTransactionAsync(schema);
        if (fault == Fault.BeforeBegin)
        {
            // Attempt 1 failed before its enqueue: nothing to discard.
            Assert.Empty(schema.OutboxLogs());
        }
        else
        {
            AssertSingleDiscardWarning(schema);
        }
    }

    [Fact]
    public async Task Non_retrying_strategy_enqueue_before_begin_commits_atomically()
    {
        var schema = await postgres.CreateSchemaAsync();
        await using var context = schema.CreateContext();
        Assert.False(context.Database.CreateExecutionStrategy().RetriesOnFailure);

        context.EnqueueOutbox(OutboxMessage.Create("k", "message"));
        context.Items.Add(new Item { Name = "business" });
        await EfUnitOfWorkShape.CommitAsync(context, Ct);

        Assert.Equal(["business"], await schema.ItemNamesAsync());
        Assert.Equal(["message"], await schema.OutboxPayloadsAsync());
        await AssertOneTransactionAsync(schema);
        Assert.Empty(schema.OutboxLogs());
    }

    [Fact]
    public async Task Enqueue_under_UseTransaction_with_retrying_strategy_throws_E4()
    {
        var schema = await postgres.CreateSchemaAsync();
        await using var connection = new NpgsqlConnection(schema.ConnectionString);
        await connection.OpenAsync(Ct);
        await using var dbTransaction = await connection.BeginTransactionAsync(Ct);
        var options = new DbContextOptionsBuilder<ItemsDbContext>()
            .UseNpgsql(connection, Retrying)
            .UseLoggerFactory(postgres.LoggerFactory)
            .UseOutbox(schema.Publisher);
        await using var context = new ItemsDbContext((DbContextOptions<ItemsDbContext>)options.Options);
        Assert.True(context.Database.CreateExecutionStrategy().RetriesOnFailure);
        await context.Database.UseTransactionAsync(dbTransaction, Ct);

        var thrown = Assert.Throws<InvalidOperationException>(() => context.EnqueueOutbox(OutboxMessage.Create("k", "message")));

        Assert.Equal(E4, thrown.Message);
        Assert.Equal(0, Assert.IsType<OutboxQueue>(OutboxQueue.Find(context)).Count);
    }

    private static void AssertRefused(Action<OutboxMessage> enqueue, OutboxQueue queue, CommandCountingInterceptor counter, int expectedCount)
    {
        var commands = counter.Commands;
        var opens = counter.ConnectionOpens;

        var thrown = Assert.Throws<InvalidOperationException>(() => enqueue(OutboxMessage.Create("k", "outside")));

        Assert.Equal(E8, thrown.Message);
        Assert.Equal(expectedCount, queue.Count);
        Assert.Equal(commands, counter.Commands);
        Assert.Equal(opens, counter.ConnectionOpens);
    }

    private static void AssertSingleDiscardWarning(EfTestSchema schema)
    {
        var warning = Assert.Single(schema.OutboxLogs());
        Assert.Equal(LogLevel.Warning, warning.Level);
        Assert.Equal("OutboxMessagesDiscarded", warning.EventId.Name);
        Assert.Equal(1, warning.Value("Count"));
    }

    private static async Task AssertOneTransactionAsync(EfTestSchema schema)
    {
        var itemTx = Assert.Single(await schema.QueryAsync<string>($"SELECT tx::text FROM {schema.Name}.items"));
        var outboxTx = Assert.Single(await schema.QueryAsync<string>($"SELECT tx_id::text FROM {schema.OutboxTable}"));
        Assert.Equal(itemTx, outboxTx);
    }

    private sealed class TransientFaultException() : Exception("Transient test fault.");

    private sealed class OneRetryExecutionStrategy(ExecutionStrategyDependencies dependencies)
        : ExecutionStrategy(dependencies, maxRetryCount: 1, maxRetryDelay: TimeSpan.Zero)
    {
        protected override bool ShouldRetryOn(Exception exception) => exception is TransientFaultException;
    }

    private sealed class FaultOnFirstOpenInterceptor : DbConnectionInterceptor
    {
        public bool Faulted { get; private set; }

        public override ValueTask<InterceptionResult> ConnectionOpeningAsync(
            DbConnection connection, ConnectionEventData eventData, InterceptionResult result, CancellationToken cancellationToken = default)
        {
            if (!Faulted)
            {
                Faulted = true;
                throw new TransientFaultException();
            }

            return new(result);
        }
    }

    private sealed class FaultOnFirstInsertInterceptor : DbCommandInterceptor
    {
        public bool Faulted { get; private set; }

        public override ValueTask<InterceptionResult<DbDataReader>> ReaderExecutingAsync(
            DbCommand command, CommandEventData eventData, InterceptionResult<DbDataReader> result, CancellationToken cancellationToken = default)
        {
            FaultOnce(command);
            return new(result);
        }

        public override ValueTask<InterceptionResult<int>> NonQueryExecutingAsync(
            DbCommand command, CommandEventData eventData, InterceptionResult<int> result, CancellationToken cancellationToken = default)
        {
            FaultOnce(command);
            return new(result);
        }

        private void FaultOnce(DbCommand command)
        {
            if (!Faulted && command.CommandText.Contains("INSERT", StringComparison.OrdinalIgnoreCase))
            {
                Faulted = true;
                throw new TransientFaultException();
            }
        }
    }

    private sealed class FaultOnFirstCommitInterceptor : DbTransactionInterceptor
    {
        public bool Faulted { get; private set; }

        public override ValueTask<InterceptionResult> TransactionCommittingAsync(
            DbTransaction transaction, TransactionEventData eventData, InterceptionResult result, CancellationToken cancellationToken = default)
        {
            if (!Faulted)
            {
                Faulted = true;
                throw new TransientFaultException();
            }

            return new(result);
        }
    }
}
