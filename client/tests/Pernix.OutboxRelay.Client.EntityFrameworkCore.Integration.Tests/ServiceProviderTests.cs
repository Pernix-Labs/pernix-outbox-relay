using Pernix.OutboxRelay.Client.PostgreSql;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Infrastructure;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore.Integration.Tests;

// Asserts singleton identity, not EF's many-providers warning: EF's provider cache is process-wide.
public sealed class ServiceProviderTests(EfPostgreSqlFixture postgres)
{
    private const int Scopes = 30;

    [Fact]
    public async Task Separate_UseOutbox_calls_share_one_internal_service_provider()
    {
        var schema = await postgres.CreateSchemaAsync();
        IOutboxPublisher[] publishers = [schema.Publisher, new PostgreSqlOutboxPublisher(OutboxTable.Parse($"{schema.Name}.other_outbox"))];
        var useOutboxCalls = 0;
        var services = new ServiceCollection();
        services.AddDbContext<ItemsDbContext>(options => schema.Configure(options, publishers[useOutboxCalls++ % publishers.Length]));
        await using var provider = services.BuildServiceProvider();

        IModelSource? modelSource = null;
        var extensions = new HashSet<object>(ReferenceEqualityComparer.Instance);
        for (var i = 0; i < Scopes; i++)
        {
            await using var scope = provider.CreateAsyncScope();
            var context = scope.ServiceProvider.GetRequiredService<ItemsDbContext>();
            var current = context.GetService<IModelSource>();
            modelSource ??= current;
            Assert.Same(modelSource, current);
            Assert.True(extensions.Add(context.GetService<IDbContextOptions>().FindExtension<OutboxOptionsExtension>()!));
            Assert.Equal(publishers[i % publishers.Length].Table, OutboxTableOf(context));
        }

        Assert.Equal(Scopes, useOutboxCalls);
    }

    private static OutboxTable OutboxTableOf(DbContext context) =>
        context.GetService<IDbContextOptions>().FindExtension<OutboxOptionsExtension>()!.Table;
}
