using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore.Integration.Tests;

public sealed class DiscardTests(EfPostgreSqlFixture postgres)
{
    private const string Category = "Pernix.OutboxRelay.Client.EntityFrameworkCore.OutboxQueue";

    private const string Template =
        "Discarded {Count} queued outbox messages for outbox table {OutboxTable}. The context was disposed or returned to the pool, or its EF transaction ended without CommitAsync or a rollback through EF.";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(DisposeKind.Dispose)]
    [InlineData(DisposeKind.DisposeAsync)]
    public async Task L6_enqueue_then_dispose_without_commit_logs_discard_warning(DisposeKind kind)
    {
        var schema = await postgres.CreateSchemaAsync();
        var context = schema.CreateContext();

        context.EnqueueOutbox(OutboxMessage.Create("k1", "l6-1"));
        _ = await context.Database.BeginTransactionAsync(Ct);
        context.EnqueueOutbox(OutboxMessage.Create("k2", "l6-2"));
        context.Items.Add(new Item { Name = "l6" });
        await context.SaveChangesAsync(Ct);

        await DisposeAsync(context, kind);

        Assert.Empty(await schema.OutboxPayloadsAsync());
        Assert.Empty(await schema.ItemNamesAsync());
        AssertSingleWarning(schema, count: 2);
    }

    [Theory]
    [InlineData(OutsideCommit.DbTransactionCommit)]
    [InlineData(OutsideCommit.ExecuteUpdateWithoutTransaction)]
    public async Task L7_business_commit_outside_ef_writes_no_outbox_row_and_logs_discard(OutsideCommit path)
    {
        var schema = await postgres.CreateSchemaAsync();
        await schema.ExecuteAsync($"INSERT INTO {schema.Name}.items (name) VALUES ('before')");

        await using (var context = schema.CreateContext())
        {
            if (path == OutsideCommit.DbTransactionCommit)
            {
                await using var transaction = await context.Database.BeginTransactionAsync(Ct);
                context.EnqueueOutbox(OutboxMessage.Create("k", "l7"));
                context.Items.Add(new Item { Name = "after" });
                await context.SaveChangesAsync(Ct);

                await transaction.GetDbTransaction().CommitAsync(Ct);
            }
            else
            {
                context.EnqueueOutbox(OutboxMessage.Create("k", "l7"));
                await context.Items
                    .Where(i => i.Name == "before")
                    .ExecuteUpdateAsync(s => s.SetProperty(i => i.Name, "after"), Ct);
            }

            Assert.Equal(1, Queue(context).Count);
            Assert.Empty(schema.OutboxLogs());
        }

        Assert.Contains("after", await schema.ItemNamesAsync());
        Assert.Empty(await schema.OutboxPayloadsAsync());
        AssertSingleWarning(schema, count: 1);
    }

    [Theory]
    [InlineData(DisposeKind.Dispose)]
    [InlineData(DisposeKind.DisposeAsync)]
    [InlineData(DisposeKind.PoolReturn)]
    public async Task Dispose_and_pool_return_never_throw_for_queue(DisposeKind kind)
    {
        var schema = await postgres.CreateSchemaAsync();

        if (kind == DisposeKind.PoolReturn)
        {
            var services = new ServiceCollection();
            services.AddDbContextPool<ItemsDbContext>(options => schema.Configure(options));
            await using var provider = services.BuildServiceProvider();

            var scope = provider.CreateAsyncScope();
            var lease = scope.ServiceProvider.GetRequiredService<ItemsDbContext>();
            await EnqueueInOpenTransactionAsync(lease);

            Assert.Null(await Record.ExceptionAsync(async () => await scope.DisposeAsync()));

            await using var next = provider.CreateAsyncScope();
            var nextLease = next.ServiceProvider.GetRequiredService<ItemsDbContext>();
            Assert.Same(lease, nextLease);
            Assert.Equal(0, Queue(nextLease).Count);
        }
        else
        {
            var context = schema.CreateContext();
            await EnqueueInOpenTransactionAsync(context);

            Assert.Null(await Record.ExceptionAsync(() => DisposeAsync(context, kind)));
        }

        Assert.Empty(await schema.OutboxPayloadsAsync());
        AssertSingleWarning(schema, count: 2);
    }

    [Theory]
    [InlineData(DisposeKind.Dispose)]
    [InlineData(DisposeKind.DisposeAsync)]
    [InlineData(DisposeKind.PoolReturn)]
    public async Task Empty_queue_dispose_logs_nothing(DisposeKind kind)
    {
        var schema = await postgres.CreateSchemaAsync();

        if (kind == DisposeKind.PoolReturn)
        {
            var services = new ServiceCollection();
            services.AddDbContextPool<ItemsDbContext>(options => schema.Configure(options));
            await using var provider = services.BuildServiceProvider();

            await using (var scope = provider.CreateAsyncScope())
            {
                await CommitAndRollBackAsync(scope.ServiceProvider.GetRequiredService<ItemsDbContext>());
            }

            await using (var untouched = provider.CreateAsyncScope())
            {
                untouched.ServiceProvider.GetRequiredService<ItemsDbContext>();
            }
        }
        else
        {
            var context = schema.CreateContext();
            await CommitAndRollBackAsync(context);
            await DisposeAsync(context, kind);

            await DisposeAsync(schema.CreateContext(), kind);
        }

        Assert.Equal(["committed"], await schema.OutboxPayloadsAsync());
        Assert.Empty(schema.OutboxLogs());
    }

    [Fact]
    public async Task Discard_warning_has_count_and_table_only()
    {
        const string Key = "secret-key-7f3a";
        const string Payload = "secret-payload-9c1e";
        const string HeaderName = "secret-header-name-4b2d";
        const string HeaderValue = "secret-header-value-6e8f";

        var schema = await postgres.CreateSchemaAsync();
        var headers = OutboxHeaders.CreateBuilder().Add(HeaderName, HeaderValue).Build();

        await using (var context = schema.CreateContext())
        {
            for (var i = 0; i < 3; i++)
            {
                context.EnqueueOutbox(OutboxMessage.Create(Key, Payload, headers));
            }
        }

        var table = schema.OutboxTable.ToString();
        var warning = Assert.Single(schema.OutboxLogs());
        Assert.Equal(Category, warning.Category);
        Assert.Equal(LogLevel.Warning, warning.Level);
        Assert.Equal(1, warning.EventId.Id);
        Assert.Equal("OutboxMessagesDiscarded", warning.EventId.Name);
        Assert.Equal(
            $"Discarded 3 queued outbox messages for outbox table {table}. The context was disposed or returned to the pool, or its EF transaction ended without CommitAsync or a rollback through EF.",
            warning.Message);

        Assert.Equal(3, warning.Value("Count"));
        Assert.Equal(table, warning.Value("OutboxTable"));
        Assert.Equal(Template, warning.Value("{OriginalFormat}"));
        Assert.Equal(["Count", "OutboxTable", "{OriginalFormat}"], warning.State.Select(p => p.Key).Order(StringComparer.Ordinal));

        foreach (var secret in new[] { Key, Payload, HeaderName, HeaderValue })
        {
            Assert.DoesNotContain(secret, warning.Message, StringComparison.Ordinal);
            Assert.All(warning.State, p => Assert.DoesNotContain(secret, p.Value?.ToString() ?? string.Empty, StringComparison.Ordinal));
        }
    }

    public enum DisposeKind
    {
        Dispose,
        DisposeAsync,
        PoolReturn,
    }

    public enum OutsideCommit
    {
        DbTransactionCommit,
        ExecuteUpdateWithoutTransaction,
    }

    private static OutboxQueue Queue(DbContext context) => Assert.IsType<OutboxQueue>(OutboxQueue.Find(context));

    private static async Task DisposeAsync(DbContext context, DisposeKind kind)
    {
        if (kind == DisposeKind.Dispose)
        {
            context.Dispose();
        }
        else
        {
            await context.DisposeAsync();
        }
    }

    private static async Task EnqueueInOpenTransactionAsync(ItemsDbContext context)
    {
        context.EnqueueOutbox(OutboxMessage.Create("k1", "open-1"));
        await context.Database.BeginTransactionAsync(Ct);
        context.EnqueueOutbox(OutboxMessage.Create("k2", "open-2"));
        context.Items.Add(new Item { Name = "open" });
        await context.SaveChangesAsync(Ct);
    }

    private static async Task CommitAndRollBackAsync(ItemsDbContext context)
    {
        context.EnqueueOutbox(OutboxMessage.Create("k", "committed"));
        context.Items.Add(new Item { Name = "committed" });
        await EfUnitOfWorkShape.CommitAsync(context, Ct);

        await using var transaction = await context.Database.BeginTransactionAsync(Ct);
        context.EnqueueOutbox(OutboxMessage.Create("k", "rolled-back"));
        await transaction.RollbackAsync(Ct);
        Assert.Equal(0, Queue(context).Count);
    }

    private static void AssertSingleWarning(EfTestSchema schema, int count)
    {
        var warning = Assert.Single(schema.OutboxLogs());
        Assert.Equal(LogLevel.Warning, warning.Level);
        Assert.Equal("OutboxMessagesDiscarded", warning.EventId.Name);
        Assert.Equal(count, warning.Value("Count"));
    }
}
