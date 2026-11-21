using System.Data.Common;
using System.Transactions;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Diagnostics;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Xunit;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore.Integration.Tests;
public sealed class EfBehaviourPinTests(EfPostgreSqlFixture postgres)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;
    [Fact]
    public async Task SavingChanges_fires_for_zero_changes_with_and_without_transaction()
    {
        var schema = await postgres.CreateSchemaAsync();
        var recorder = new EventRecorder();
        await using var context = CreateContext(schema, recorder);

        context.SaveChanges();
        await context.SaveChangesAsync(Ct);
        await using (var transaction = await context.Database.BeginTransactionAsync(Ct))
        {
            context.SaveChanges();
            await context.SaveChangesAsync(Ct);
            await transaction.CommitAsync(Ct);
        }

        var saves = recorder.Matching("SavingChanges");
        Assert.Equal(["SavingChanges", "SavingChangesAsync", "SavingChanges", "SavingChangesAsync"], Names(saves));
        Assert.Equal([false, false, true, true], saves.Select(e => e.HadCurrentTransaction));
    }
    [Fact]
    public async Task TransactionCommitting_fires_for_begun_transaction_without_writes()
    {
        var schema = await postgres.CreateSchemaAsync();
        var recorder = new EventRecorder();
        await using var context = CreateContext(schema, recorder);

        using (var transaction = context.Database.BeginTransaction())
        {
            transaction.Commit();
        }

        await using (var transaction = await context.Database.BeginTransactionAsync(Ct))
        {
            await transaction.CommitAsync(Ct);
        }

        Assert.Equal(["TransactionCommitting", "TransactionCommittingAsync"], Names(recorder.Matching("TransactionCommitting")));
    }
    [Fact]
    public async Task TransactionCommittingAsync_exception_prevents_commit_and_propagates()
    {
        var schema = await postgres.CreateSchemaAsync();
        var boom = new InvalidOperationException("boom");
        var recorder = new EventRecorder { ThrowOn = "TransactionCommittingAsync", Exception = boom };
        await using var context = CreateContext(schema, recorder);

        await using (var transaction = await context.Database.BeginTransactionAsync(Ct))
        {
            context.Items.Add(new Item { Name = "b3" });
            await context.SaveChangesAsync(Ct);

            var thrown = await Assert.ThrowsAsync<InvalidOperationException>(() => transaction.CommitAsync(Ct));

            Assert.Same(boom, thrown);
            Assert.Same(transaction, context.Database.CurrentTransaction);
            Assert.Empty(recorder.Matching("TransactionCommitted"));
        }

        Assert.Empty(await schema.ItemNamesAsync());
    }
    [Fact]
    public async Task CurrentTransaction_is_null_at_SavingChanges_for_plain_save_and_ambient_scope()
    {
        var schema = await postgres.CreateSchemaAsync();
        var recorder = new EventRecorder();

        await using (var context = CreateContext(schema, recorder))
        {
            context.Items.Add(new Item { Name = "plain-sync" });
            context.SaveChanges();
            context.Items.Add(new Item { Name = "plain-async" });
            await context.SaveChangesAsync(Ct);
        }

        using (new TransactionScope(TransactionScopeAsyncFlowOption.Enabled))
        {
            await using var context = CreateContext(schema, recorder);
            context.Items.Add(new Item { Name = "ambient" });
            await context.SaveChangesAsync(Ct);
        }

        var saves = recorder.Matching("SavingChanges");
        Assert.Equal(3, saves.Count);
        Assert.All(saves, e => Assert.False(e.HadCurrentTransaction));
        Assert.Equal(["plain-sync", "plain-async"], await schema.ItemNamesAsync());
    }
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Disposing_transaction_without_commit_raises_no_transaction_event(bool async)
    {
        var schema = await postgres.CreateSchemaAsync();
        var recorder = new EventRecorder();
        await using var context = CreateContext(schema, recorder);

        var transaction = async ? await context.Database.BeginTransactionAsync(Ct) : context.Database.BeginTransaction();
        context.Items.Add(new Item { Name = "b5" });
        await context.SaveChangesAsync(Ct);
        recorder.Clear();

        if (async)
        {
            await transaction.DisposeAsync();
        }
        else
        {
            transaction.Dispose();
        }

        Assert.Empty(recorder.Events);
        Assert.Null(context.Database.CurrentTransaction);
        Assert.Empty(await schema.ItemNamesAsync());
    }
    [Fact]
    public async Task ResetState_runs_for_extension_service_on_pool_return()
    {
        var schema = await postgres.CreateSchemaAsync();
        var services = new ServiceCollection();
        services.AddDbContextPool<ItemsDbContext>(options => AddProbe(schema.ConfigureWithoutOutbox(options)));
        await using var provider = services.BuildServiceProvider();

        ItemsDbContext first;
        ProbeService probe;
        using (var scope = provider.CreateScope())
        {
            first = scope.ServiceProvider.GetRequiredService<ItemsDbContext>();
            probe = Assert.IsType<ProbeService>(FindProbe(first));
            Assert.Equal(0, probe.ResetStateCalls);
        }

        Assert.Equal(1, probe.ResetStateCalls);

        await using (var scope = provider.CreateAsyncScope())
        {
            var second = scope.ServiceProvider.GetRequiredService<ItemsDbContext>();
            Assert.Same(first, second);
            Assert.Same(probe, FindProbe(second));
            Assert.Equal(0, probe.ResetStateAsyncCalls);
        }

        Assert.Equal(1, probe.ResetStateAsyncCalls);
        Assert.Equal(0, probe.DisposeCalls);
    }
    [Fact]
    public async Task TransactionStarted_id_equals_context_transaction_id()
    {
        var schema = await postgres.CreateSchemaAsync();
        var recorder = new EventRecorder();
        await using var context = CreateContext(schema, recorder);

        using (var transaction = context.Database.BeginTransaction())
        {
            var started = Assert.Single(recorder.Matching("TransactionStarted"));
            Assert.Equal("TransactionStarted", started.Name);
            Assert.NotEqual(Guid.Empty, started.TransactionId);
            Assert.Equal(transaction.TransactionId, started.TransactionId);
        }

        recorder.Clear();
        await using (var transaction = await context.Database.BeginTransactionAsync(Ct))
        {
            var started = Assert.Single(recorder.Matching("TransactionStarted"));
            Assert.Equal("TransactionStartedAsync", started.Name);
            Assert.Equal(transaction.TransactionId, started.TransactionId);
        }
    }
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task TransactionUsed_fires_and_its_exception_leaves_no_current_transaction(bool async)
    {
        var schema = await postgres.CreateSchemaAsync();
        var eventName = async ? "TransactionUsedAsync" : "TransactionUsed";
        await using var connection = new NpgsqlConnection(schema.ConnectionString);
        await connection.OpenAsync(Ct);
        await using var dbTransaction = await connection.BeginTransactionAsync(Ct);

        var recorder = new EventRecorder();
        await using (var context = CreateContext(schema, connection, recorder))
        {
            var used = async
                ? await context.Database.UseTransactionAsync(dbTransaction, Ct)
                : context.Database.UseTransaction(dbTransaction);

            var usedEvent = Assert.Single(recorder.Matching("TransactionUsed"));
            Assert.Equal(eventName, usedEvent.Name);
            Assert.NotNull(used);
            Assert.Same(used, context.Database.CurrentTransaction);
            Assert.Equal(used.TransactionId, usedEvent.TransactionId);
            Assert.Same(dbTransaction, used.GetDbTransaction());
        }

        var boom = new InvalidOperationException("boom");
        var throwing = new EventRecorder { ThrowOn = eventName, Exception = boom };
        await using (var context = CreateContext(schema, connection, throwing))
        {
            var thrown = async
                ? await Assert.ThrowsAsync<InvalidOperationException>(() => context.Database.UseTransactionAsync(dbTransaction, Ct))
                : Assert.Throws<InvalidOperationException>(() => context.Database.UseTransaction(dbTransaction));

            Assert.Same(boom, thrown);
            Assert.Null(context.Database.CurrentTransaction);
        }
    }
    [Fact]
    public async Task Failed_save_in_user_transaction_raises_savepoint_events_only()
    {
        var schema = await postgres.CreateSchemaAsync();
        var recorder = new EventRecorder();
        await using var context = CreateContext(schema, recorder);
        await using var transaction = await context.Database.BeginTransactionAsync(Ct);
        context.Items.Add(new Item { Name = "dup" });
        await context.SaveChangesAsync(Ct);
        context.ChangeTracker.Clear();
        recorder.Clear();

        context.Items.Add(new Item { Name = "dup" });
        await Assert.ThrowsAsync<DbUpdateException>(() => context.SaveChangesAsync(Ct));

        var names = Names(recorder.Events);
        Assert.Contains("CreatingSavepointAsync", names);
        Assert.Contains("RollingBackToSavepointAsync", names);
        Assert.DoesNotContain(names, n => n.StartsWith("TransactionRol", StringComparison.Ordinal));
        Assert.Same(transaction, context.Database.CurrentTransaction);
    }
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SavingChanges_exception_prevents_any_command(bool async)
    {
        var schema = await postgres.CreateSchemaAsync();
        var boom = new InvalidOperationException("boom");
        var counter = new CommandCountingInterceptor();
        var recorder = new EventRecorder { ThrowOn = async ? "SavingChangesAsync" : "SavingChanges", Exception = boom };
        await using var context = CreateContext(schema, counter, recorder);
        context.Items.Add(new Item { Name = "b10" });

        var thrown = async
            ? await Assert.ThrowsAsync<InvalidOperationException>(() => context.SaveChangesAsync(Ct))
            : Assert.Throws<InvalidOperationException>(() => context.SaveChanges());

        Assert.Same(boom, thrown);
        Assert.Equal(0, counter.Commands);
        Assert.Empty(await schema.ItemNamesAsync());
    }
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Internal_service_is_disposed_with_context(bool async)
    {
        var schema = await postgres.CreateSchemaAsync();
        var context = CreateContext(schema, probe: true);
        var probe = Assert.IsType<ProbeService>(FindProbe(context));
        Assert.Equal(0, probe.DisposeCalls);

        if (async)
        {
            await context.DisposeAsync();
        }
        else
        {
            context.Dispose();
        }

        Assert.Equal(1, probe.DisposeCalls);
    }
    [Theory]
    [InlineData("RollbackAsync", "TransactionRollingBackAsync")]
    [InlineData("Rollback", "TransactionRollingBack")]
    [InlineData("Database.RollbackTransactionAsync", "TransactionRollingBackAsync")]
    public async Task Rollback_variants_raise_TransactionRollingBack(string variant, string expectedEvent)
    {
        var schema = await postgres.CreateSchemaAsync();
        var recorder = new EventRecorder();
        await using var context = CreateContext(schema, recorder);
        await using var transaction = await context.Database.BeginTransactionAsync(Ct);
        recorder.Clear();

        switch (variant)
        {
            case "RollbackAsync":
                await transaction.RollbackAsync(Ct);
                break;
            case "Rollback":
                transaction.Rollback();
                break;
            default:
                await context.Database.RollbackTransactionAsync(Ct);
                break;
        }

        Assert.Equal([expectedEvent], Names(recorder.Matching("TransactionRollingBack")));
    }
    [Fact]
    public async Task Sync_commit_raises_sync_committing_and_exception_prevents_commit()
    {
        var schema = await postgres.CreateSchemaAsync();
        var boom = new InvalidOperationException("boom");
        var recorder = new EventRecorder { ThrowOn = "TransactionCommitting", Exception = boom };
        await using var context = CreateContext(schema, recorder);

        using (var transaction = context.Database.BeginTransaction())
        {
            context.Items.Add(new Item { Name = "b14" });
            context.SaveChanges();

            var thrown = Assert.Throws<InvalidOperationException>(() => transaction.Commit());

            Assert.Same(boom, thrown);
            Assert.Equal(["TransactionCommitting"], Names(recorder.Matching("TransactionCommitt")));
        }

        Assert.Empty(await schema.ItemNamesAsync());
    }
    [Fact]
    public async Task Internal_service_resolves_per_context_instance_and_null_without_extension()
    {
        var schema = await postgres.CreateSchemaAsync();
        var recorder = new EventRecorder();
        await using var first = CreateContext(schema, probe: true, recorder);
        await using var second = CreateContext(schema, probe: true, recorder);
        await using var without = CreateContext(schema);

        var firstProbe = Assert.IsType<ProbeService>(FindProbe(first));
        Assert.Same(firstProbe, FindProbe(first));
        Assert.NotSame(firstProbe, Assert.IsType<ProbeService>(FindProbe(second)));
        Assert.Null(FindProbe(without));

        foreach (var context in new[] { first, second })
        {
            recorder.Clear();
            await using (var transaction = await context.Database.BeginTransactionAsync(Ct))
            {
                await context.SaveChangesAsync(Ct);
                await transaction.CommitAsync(Ct);
            }

            Assert.Equal(["TransactionStartedAsync", "SavingChangesAsync", "TransactionCommittingAsync"], Names(recorder.Matching("TransactionStarted", "SavingChanges", "TransactionCommitting")));
            Assert.All(recorder.Events, e => Assert.Same(context, e.Context));
        }
    }
    [Theory]
    [InlineData("none", false)]
    [InlineData("EnableRetryOnFailure", true)]
    [InlineData("ExecutionStrategy subclass", true)]
    public async Task RetriesOnFailure_is_suspended_inside_execution_strategy_on_any_instance(string option, bool retriesOutside)
    {
        var options = new DbContextOptionsBuilder<ItemsDbContext>()
            .UseNpgsql("Host=unused", npgsql =>
            {
                if (option == "EnableRetryOnFailure")
                {
                    npgsql.EnableRetryOnFailure();
                }
                else if (option != "none")
                {
                    npgsql.ExecutionStrategy(static dependencies => new OneRetryExecutionStrategy(dependencies));
                }
            })
            .Options;
        await using var context = new ItemsDbContext(options);
        await using var other = new ItemsDbContext(options);

        var cached = context.Database.CreateExecutionStrategy();
        Assert.NotSame(cached, context.Database.CreateExecutionStrategy());
        Assert.Equal(retriesOutside, cached.RetriesOnFailure);

        // The cached instance is not the executing one. OutboxQueue reads MaxRetryCount, which is not suspended.
        await context.Database.CreateExecutionStrategy().ExecuteAsync(
            async ct =>
            {
                Assert.False(cached.RetriesOnFailure);
                Assert.Equal(retriesOutside, context.Database.CreateExecutionStrategy() is ExecutionStrategy { MaxRetryCount: > 0 });
                Assert.False(context.Database.CreateExecutionStrategy().RetriesOnFailure);
                await Task.Yield();
                Assert.False(cached.RetriesOnFailure);
                Assert.False(context.Database.CreateExecutionStrategy().RetriesOnFailure);
                await other.Database.CreateExecutionStrategy().ExecuteAsync(
                    async _ =>
                    {
                        await Task.Yield();
                        Assert.False(cached.RetriesOnFailure);
                    },
                    ct);
            },
            Ct);

        context.Database.CreateExecutionStrategy().Execute(() =>
        {
            Assert.False(cached.RetriesOnFailure);
            Assert.False(context.Database.CreateExecutionStrategy().RetriesOnFailure);
        });

        Assert.Equal(retriesOutside, cached.RetriesOnFailure);
        Assert.Equal(retriesOutside, context.Database.CreateExecutionStrategy().RetriesOnFailure);
    }

    private static ItemsDbContext CreateContext(EfTestSchema schema, params IInterceptor[] interceptors) =>
        CreateContext(schema, probe: false, interceptors);

    private static ItemsDbContext CreateContext(EfTestSchema schema, bool probe, params IInterceptor[] interceptors)
    {
        var builder = new DbContextOptionsBuilder<ItemsDbContext>();
        schema.ConfigureWithoutOutbox(builder).AddInterceptors(interceptors);
        if (probe)
        {
            AddProbe(builder);
        }

        return new ItemsDbContext(builder.Options);
    }

    private static ItemsDbContext CreateContext(EfTestSchema schema, DbConnection connection, params IInterceptor[] interceptors)
    {
        var builder = new DbContextOptionsBuilder<ItemsDbContext>();
        schema.ConfigureWithoutOutbox(builder, connection).AddInterceptors(interceptors);
        return new ItemsDbContext(builder.Options);
    }

    private static DbContextOptionsBuilder AddProbe(DbContextOptionsBuilder builder)
    {
        ((IDbContextOptionsBuilderInfrastructure)builder).AddOrUpdateExtension(new ProbeOptionsExtension());
        return builder;
    }

    private static object? FindProbe(DbContext context) =>
        ((IInfrastructure<IServiceProvider>)context).Instance.GetService(typeof(ProbeService));

    private static List<string> Names(IEnumerable<RecordedEvent> events) => events.Select(e => e.Name).ToList();

    private sealed record RecordedEvent(string Name, DbContext? Context, Guid TransactionId, bool HadCurrentTransaction);
    private sealed class EventRecorder : ISaveChangesInterceptor, IDbTransactionInterceptor
    {
        private readonly List<RecordedEvent> events = [];

        public string? ThrowOn { get; init; }

        public Exception? Exception { get; init; }

        public IReadOnlyList<RecordedEvent> Events => events;

        public List<RecordedEvent> Matching(params string[] prefixes) =>
            events.Where(e => prefixes.Any(p => e.Name.StartsWith(p, StringComparison.Ordinal))).ToList();

        public void Clear() => events.Clear();

        public InterceptionResult<int> SavingChanges(DbContextEventData eventData, InterceptionResult<int> result)
        {
            RecordSave(nameof(SavingChanges), eventData);
            return result;
        }

        public ValueTask<InterceptionResult<int>> SavingChangesAsync(
            DbContextEventData eventData, InterceptionResult<int> result, CancellationToken cancellationToken = default)
        {
            RecordSave(nameof(SavingChangesAsync), eventData);
            return new(result);
        }

        public InterceptionResult<DbTransaction> TransactionStarting(
            DbConnection connection, TransactionStartingEventData eventData, InterceptionResult<DbTransaction> result)
        {
            Record(nameof(TransactionStarting), eventData.Context, eventData.TransactionId);
            return result;
        }

        public ValueTask<InterceptionResult<DbTransaction>> TransactionStartingAsync(
            DbConnection connection, TransactionStartingEventData eventData, InterceptionResult<DbTransaction> result, CancellationToken cancellationToken = default)
        {
            Record(nameof(TransactionStartingAsync), eventData.Context, eventData.TransactionId);
            return new(result);
        }

        public DbTransaction TransactionStarted(DbConnection connection, TransactionEndEventData eventData, DbTransaction result)
        {
            Record(nameof(TransactionStarted), eventData);
            return result;
        }

        public ValueTask<DbTransaction> TransactionStartedAsync(
            DbConnection connection, TransactionEndEventData eventData, DbTransaction result, CancellationToken cancellationToken = default)
        {
            Record(nameof(TransactionStartedAsync), eventData);
            return new(result);
        }

        public DbTransaction TransactionUsed(DbConnection connection, TransactionEventData eventData, DbTransaction result)
        {
            Record(nameof(TransactionUsed), eventData);
            return result;
        }

        public ValueTask<DbTransaction> TransactionUsedAsync(
            DbConnection connection, TransactionEventData eventData, DbTransaction result, CancellationToken cancellationToken = default)
        {
            Record(nameof(TransactionUsedAsync), eventData);
            return new(result);
        }

        public InterceptionResult TransactionCommitting(DbTransaction transaction, TransactionEventData eventData, InterceptionResult result)
        {
            Record(nameof(TransactionCommitting), eventData);
            return result;
        }

        public ValueTask<InterceptionResult> TransactionCommittingAsync(
            DbTransaction transaction, TransactionEventData eventData, InterceptionResult result, CancellationToken cancellationToken = default)
        {
            Record(nameof(TransactionCommittingAsync), eventData);
            return new(result);
        }

        public void TransactionCommitted(DbTransaction transaction, TransactionEndEventData eventData) =>
            Record(nameof(TransactionCommitted), eventData);

        public Task TransactionCommittedAsync(DbTransaction transaction, TransactionEndEventData eventData, CancellationToken cancellationToken = default)
        {
            Record(nameof(TransactionCommittedAsync), eventData);
            return Task.CompletedTask;
        }

        public InterceptionResult TransactionRollingBack(DbTransaction transaction, TransactionEventData eventData, InterceptionResult result)
        {
            Record(nameof(TransactionRollingBack), eventData);
            return result;
        }

        public ValueTask<InterceptionResult> TransactionRollingBackAsync(
            DbTransaction transaction, TransactionEventData eventData, InterceptionResult result, CancellationToken cancellationToken = default)
        {
            Record(nameof(TransactionRollingBackAsync), eventData);
            return new(result);
        }

        public void TransactionRolledBack(DbTransaction transaction, TransactionEndEventData eventData) =>
            Record(nameof(TransactionRolledBack), eventData);

        public Task TransactionRolledBackAsync(DbTransaction transaction, TransactionEndEventData eventData, CancellationToken cancellationToken = default)
        {
            Record(nameof(TransactionRolledBackAsync), eventData);
            return Task.CompletedTask;
        }

        public InterceptionResult CreatingSavepoint(DbTransaction transaction, TransactionEventData eventData, InterceptionResult result)
        {
            Record(nameof(CreatingSavepoint), eventData);
            return result;
        }

        public ValueTask<InterceptionResult> CreatingSavepointAsync(
            DbTransaction transaction, TransactionEventData eventData, InterceptionResult result, CancellationToken cancellationToken = default)
        {
            Record(nameof(CreatingSavepointAsync), eventData);
            return new(result);
        }

        public void CreatedSavepoint(DbTransaction transaction, TransactionEventData eventData) =>
            Record(nameof(CreatedSavepoint), eventData);

        public Task CreatedSavepointAsync(DbTransaction transaction, TransactionEventData eventData, CancellationToken cancellationToken = default)
        {
            Record(nameof(CreatedSavepointAsync), eventData);
            return Task.CompletedTask;
        }

        public InterceptionResult RollingBackToSavepoint(DbTransaction transaction, TransactionEventData eventData, InterceptionResult result)
        {
            Record(nameof(RollingBackToSavepoint), eventData);
            return result;
        }

        public ValueTask<InterceptionResult> RollingBackToSavepointAsync(
            DbTransaction transaction, TransactionEventData eventData, InterceptionResult result, CancellationToken cancellationToken = default)
        {
            Record(nameof(RollingBackToSavepointAsync), eventData);
            return new(result);
        }

        public void RolledBackToSavepoint(DbTransaction transaction, TransactionEventData eventData) =>
            Record(nameof(RolledBackToSavepoint), eventData);

        public Task RolledBackToSavepointAsync(DbTransaction transaction, TransactionEventData eventData, CancellationToken cancellationToken = default)
        {
            Record(nameof(RolledBackToSavepointAsync), eventData);
            return Task.CompletedTask;
        }

        public InterceptionResult ReleasingSavepoint(DbTransaction transaction, TransactionEventData eventData, InterceptionResult result)
        {
            Record(nameof(ReleasingSavepoint), eventData);
            return result;
        }

        public ValueTask<InterceptionResult> ReleasingSavepointAsync(
            DbTransaction transaction, TransactionEventData eventData, InterceptionResult result, CancellationToken cancellationToken = default)
        {
            Record(nameof(ReleasingSavepointAsync), eventData);
            return new(result);
        }

        public void ReleasedSavepoint(DbTransaction transaction, TransactionEventData eventData) =>
            Record(nameof(ReleasedSavepoint), eventData);

        public Task ReleasedSavepointAsync(DbTransaction transaction, TransactionEventData eventData, CancellationToken cancellationToken = default)
        {
            Record(nameof(ReleasedSavepointAsync), eventData);
            return Task.CompletedTask;
        }

        public void TransactionFailed(DbTransaction transaction, TransactionErrorEventData eventData) =>
            Record(nameof(TransactionFailed), eventData);

        public Task TransactionFailedAsync(DbTransaction transaction, TransactionErrorEventData eventData, CancellationToken cancellationToken = default)
        {
            Record(nameof(TransactionFailedAsync), eventData);
            return Task.CompletedTask;
        }

        private void RecordSave(string name, DbContextEventData eventData)
        {
            events.Add(new RecordedEvent(name, eventData.Context, Guid.Empty, eventData.Context?.Database.CurrentTransaction is not null));
            ThrowIfConfigured(name);
        }

        private void Record(string name, TransactionEventData eventData) => Record(name, eventData.Context, eventData.TransactionId);

        private void Record(string name, DbContext? context, Guid transactionId)
        {
            events.Add(new RecordedEvent(name, context, transactionId, false));
            ThrowIfConfigured(name);
        }

        private void ThrowIfConfigured(string name)
        {
            if (name == ThrowOn)
            {
                throw Exception!;
            }
        }
    }

    private sealed class ProbeService : IResettableService, IDisposable
    {
        public int ResetStateCalls { get; private set; }

        public int ResetStateAsyncCalls { get; private set; }

        public int DisposeCalls { get; private set; }

        public void ResetState() => ResetStateCalls++;

        public Task ResetStateAsync(CancellationToken cancellationToken = default)
        {
            ResetStateAsyncCalls++;
            return Task.CompletedTask;
        }

        public void Dispose() => DisposeCalls++;
    }

    private sealed class ProbeOptionsExtension : IDbContextOptionsExtension
    {
        private DbContextOptionsExtensionInfo? info;

        public DbContextOptionsExtensionInfo Info => info ??= new ProbeInfo(this);

        public void ApplyServices(IServiceCollection services)
        {
            services.AddScoped<ProbeService>();
            services.AddScoped<IResettableService>(static sp => sp.GetRequiredService<ProbeService>());
        }

        public void Validate(IDbContextOptions options)
        {
        }

        private sealed class ProbeInfo(IDbContextOptionsExtension extension) : DbContextOptionsExtensionInfo(extension)
        {
            public override bool IsDatabaseProvider => false;

            public override string LogFragment => "probe ";

            public override int GetServiceProviderHashCode() => 0;

            public override bool ShouldUseSameServiceProvider(DbContextOptionsExtensionInfo other) => other is ProbeInfo;

            public override void PopulateDebugInfo(IDictionary<string, string> debugInfo) => debugInfo["Probe"] = "1";
        }
    }

    private sealed class OneRetryExecutionStrategy(ExecutionStrategyDependencies dependencies)
        : ExecutionStrategy(dependencies, 1, TimeSpan.Zero)
    {
        protected override bool ShouldRetryOn(Exception exception) => false;
    }
}
