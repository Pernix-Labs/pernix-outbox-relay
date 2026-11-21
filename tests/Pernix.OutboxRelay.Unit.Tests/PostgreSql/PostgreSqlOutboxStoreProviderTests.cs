using System.Reflection;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.PostgreSql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.PostgreSql;

public sealed class PostgreSqlOutboxStoreProviderTests
{
    [Theory]
    [InlineData("Host=db;Password=ab;cd=ef")]
    [InlineData("Host=db;Port=abc")]
    public void Malformed_connection_string_fails_without_any_part_of_it(string connectionString)
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { ["ConnectionStrings:Db"] = connectionString })
            .Build();
        var provider = new PostgreSqlOutboxStoreProvider(configuration);
        var outbox = new OutboxOptions
        {
            Name = "Orders",
            Database = new DatabaseOptions { Provider = PostgreSqlOutboxStoreProvider.ProviderName, Connection = "Db", Table = "public.outbox" },
        };

        var ex = Assert.Throws<OutboxValidationException>(() => provider.CreateStore(outbox));

        Assert.Equal("Outboxes[Orders].Database.Connection: connection string is not valid", ex.Message);
        Assert.Null(ex.InnerException);
        foreach (var part in new[] { connectionString, "Password", "Port", "ab;", "cd", "ef", "abc" })
        {
            Assert.DoesNotContain(part, ex.Message, StringComparison.OrdinalIgnoreCase);
        }
    }

    [Fact]
    public void Stores_from_one_provider_share_its_table_owner_map()
    {
        var configuration = new ConfigurationBuilder()
            .AddInMemoryCollection(new Dictionary<string, string?> { ["ConnectionStrings:Db"] = "Host=db" })
            .Build();
        var provider = new PostgreSqlOutboxStoreProvider(configuration);
        var first = provider.CreateStore(Outbox("Orders"));
        var second = provider.CreateStore(Outbox("Payments"));

        Assert.NotNull(OwnersOf(first));
        Assert.Same(OwnersOf(first), OwnersOf(second));
    }

    [Fact]
    public void Registration_adds_one_provider()
    {
        using var services = new ServiceCollection()
            .AddSingleton<IConfiguration>(new ConfigurationBuilder().Build())
            .AddPostgreSqlOutboxProvider()
            .AddPostgreSqlOutboxProvider()
            .BuildServiceProvider();

        Assert.Single(services.GetServices<IOutboxStoreProvider>());
    }

    private static OutboxOptions Outbox(string name) => new()
    {
        Name = name,
        Database = new DatabaseOptions { Provider = PostgreSqlOutboxStoreProvider.ProviderName, Connection = "Db", Table = "public.outbox" },
        Ordering = new OrderingOptions { Mode = OrderingMode.None },
    };

    private static object? OwnersOf(IOutboxStore store) =>
        typeof(PostgreSqlOutboxStore).GetField("tableOwners", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(store);
}
