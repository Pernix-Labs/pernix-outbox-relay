using Pernix.OutboxRelay.Client.EntityFrameworkCore;
using Pernix.OutboxRelay.Client.PostgreSql;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace Pernix.OutboxRelay.Client.Docs.Unit.Tests.Snippets;

internal static class EfQueueSnippets
{
    public static void Register(IServiceCollection services, string connectionString)
    {
        #region docs:ef-queue-register
        services.AddPostgreSqlOutboxPublisher("sales.outbox");
        services.AddDbContext<AppDb>((sp, options) => options
            .UseNpgsql(connectionString)
            .UseOutbox(sp.GetRequiredService<IOutboxPublisher>()));
        services.AddOutboxQueue<AppDb>();
        #endregion
    }

    public static void RegisterPool(IServiceCollection services, string connectionString)
    {
        #region docs:ef-queue-register-pool
        services.AddDbContextPool<AppDb>((sp, options) => options
            .UseNpgsql(connectionString)
            .UseOutbox(sp.GetRequiredService<IOutboxPublisher>()));
        services.AddOutboxQueue<AppDb>();
        #endregion
    }

    public static void RegisterFactory(IServiceCollection services, string connectionString)
    {
        #region docs:ef-queue-register-factory
        services.AddDbContextFactory<AppDb>((sp, options) => options
            .UseNpgsql(connectionString)
            .UseOutbox(sp.GetRequiredService<IOutboxPublisher>()));
        #endregion
    }

    public static void RegisterPooledFactory(IServiceCollection services, string connectionString)
    {
        #region docs:ef-queue-register-pooled-factory
        services.AddPooledDbContextFactory<AppDb>((sp, options) => options
            .UseNpgsql(connectionString)
            .UseOutbox(sp.GetRequiredService<IOutboxPublisher>()));
        #endregion
    }

    public static void RegisterKeyed(IServiceCollection services, string salesConnectionString, string appConnectionString)
    {
        #region docs:ef-queue-keyed
        services.AddKeyedPostgreSqlOutboxPublisher("sales", "sales.outbox");
        services.AddKeyedPostgreSqlOutboxPublisher("app", "app.outbox");

        services.AddDbContext<SalesDb>((sp, options) => options
            .UseNpgsql(salesConnectionString)
            .UseOutbox(sp.GetRequiredKeyedService<IOutboxPublisher>("sales")));
        services.AddDbContext<AppDb>((sp, options) => options
            .UseNpgsql(appConnectionString)
            .UseOutbox(sp.GetRequiredKeyedService<IOutboxPublisher>("app")));

        services.AddOutboxQueue<SalesDb>();
        services.AddOutboxQueue<AppDb>();
        #endregion
    }

    public static async Task RetryingStrategyAsync(AppDb db, Order order, OutboxMessage message, CancellationToken ct)
    {
        #region docs:ef-queue-retrying-strategy
        var strategy = db.Database.CreateExecutionStrategy();
        await strategy.ExecuteAsync(async () =>
        {
            db.ChangeTracker.Clear();
            await using var transaction = await db.Database.BeginTransactionAsync(ct);

            db.Orders.Add(order);
            // Enqueue after BeginTransactionAsync, inside the operation. Before it, Enqueue throws and queues nothing.
            db.EnqueueOutbox(message);

            await db.SaveChangesAsync(ct);
            await transaction.CommitAsync(ct);
        });
        #endregion
    }

    public static DbContextOptions<AppDb> LoggerFactory(string connectionString, ILoggerFactory loggerFactory, IOutboxPublisher publisher)
    {
        #region docs:ef-logger-factory
        // Outside DI, pass the application ILoggerFactory: LogTo output does not receive the discard Warning.
        var options = new DbContextOptionsBuilder<AppDb>()
            .UseNpgsql(connectionString)
            .UseLoggerFactory(loggerFactory)
            .UseOutbox(publisher)
            .Options;
        #endregion
        return options;
    }
}

#region docs:ef-queue-facade
public sealed class PlaceOrderHandler(AppDb db, IOutboxQueue<AppDb> outbox)
{
    public async Task HandleAsync(Order order, Guid eventId, CancellationToken ct)
    {
        await using var transaction = await db.Database.BeginTransactionAsync(ct);

        db.Orders.Add(order);

        var headers = OutboxHeaders.CreateBuilder(1).Add("idempotency-key", eventId.ToString()).Build();
        var placed = new OrderPlaced(eventId, order.Id, order.AccountId, order.Total);
        outbox.Enqueue(OutboxMessage.Json(order.Id.ToString(), placed, AppJson.Default.OrderPlaced, headers));

        await db.SaveChangesAsync(ct);
        await transaction.CommitAsync(ct);
    }
}
#endregion

#region docs:ef-queue-uow-handler
public sealed class DebitAccountHandler(IUnitOfWork unitOfWork, IAccountRepository accounts, IOutboxQueue<AppDb> outbox)
{
    public async Task HandleAsync(Guid accountId, decimal amount, Guid eventId, CancellationToken ct)
    {
        var account = await accounts.GetAsync(accountId, ct);
        account.Balance -= amount;

        var headers = OutboxHeaders.CreateBuilder(1).Add("idempotency-key", eventId.ToString()).Build();
        var debited = new AccountDebited(eventId, accountId, amount);
        outbox.Enqueue(OutboxMessage.Json(accountId.ToString(), debited, AppJson.Default.AccountDebited, headers));

        // Unchanged unit of work: the queue is written at its CommitAsync, after SaveChanges, in the same transaction.
        await unitOfWork.CommitAsync(cancellationToken: ct);
    }
}
#endregion

#region docs:ef-queue-factory
public sealed class OrderImporter(IDbContextFactory<AppDb> factory)
{
    public async Task ImportAsync(Order order, Guid eventId, CancellationToken ct)
    {
        await using var db = await factory.CreateDbContextAsync(ct);
        await using var transaction = await db.Database.BeginTransactionAsync(ct);

        db.Orders.Add(order);

        // Factory-created contexts have no IOutboxQueue<AppDb>: enqueue on the context.
        var headers = OutboxHeaders.CreateBuilder(1).Add("idempotency-key", eventId.ToString()).Build();
        var placed = new OrderPlaced(eventId, order.Id, order.AccountId, order.Total);
        db.EnqueueOutbox(OutboxMessage.Json(order.Id.ToString(), placed, AppJson.Default.OrderPlaced, headers));

        await db.SaveChangesAsync(ct);
        await transaction.CommitAsync(ct);
    }
}
#endregion
