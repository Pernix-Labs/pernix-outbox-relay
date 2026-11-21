using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Pernix.OutboxRelay.Client.PostgreSql.Integration.Tests;

// Route-scoped keyed publisher resolved from a ServiceProvider, all four methods.
public sealed class RouteScopedPublisherTests(PostgreSqlFixture postgres)
{
    [Theory]
    [MemberData(nameof(RoutePublish.Methods), MemberType = typeof(RoutePublish))]
    public async Task Route_less_and_equal_route_messages_are_written_with_the_scoped_route(RoutePublishMethod method)
    {
        var table = await postgres.CreateOutboxAsync();
        using var provider = new ServiceCollection().AddKeyedPostgreSqlOutboxPublisher("alerts", table.ToString(), "alerts").BuildServiceProvider();
        var publisher = provider.GetRequiredKeyedService<IOutboxPublisher>("alerts");
        var headers = OutboxHeaders.CreateBuilder().Add("h", "v").Build();

        Assert.Null(await RoutePublish.PublishThenCommitAsync(
            postgres.ConnectionString,
            publisher,
            method,
            OutboxMessage.Create("null", "p"),
            OutboxMessage.Create("equal", "p").WithRoute("alerts"),
            OutboxMessage.Create("null-headers", "p", headers),
            OutboxMessage.Create("equal-headers", "p", headers).WithRoute("alerts")));

        var rows = await RoutePublish.ReadRowsAsync(postgres.ConnectionString, table);
        Assert.Equal(
            [("null", "alerts"), ("equal", "alerts"), ("null-headers", "alerts"), ("equal-headers", "alerts")],
            rows.ConvertAll(row => (row.Key, row.Route)));
        Assert.Equal(["{}", "{}", "{\"h\": \"v\"}", "{\"h\": \"v\"}"], rows.ConvertAll(row => row.Headers));
    }

    [Theory]
    [MemberData(nameof(RoutePublish.Methods), MemberType = typeof(RoutePublish))]
    public async Task Different_route_throws_ArgumentException_and_writes_nothing(RoutePublishMethod method)
    {
        var table = await postgres.CreateOutboxAsync();
        using var provider = new ServiceCollection().AddKeyedPostgreSqlOutboxPublisher("alerts", table.ToString(), "alerts").BuildServiceProvider();
        var publisher = provider.GetRequiredKeyedService<IOutboxPublisher>("alerts");

        var exception = await RoutePublish.PublishThenCommitAsync(
            postgres.ConnectionString, publisher, method, OutboxMessage.Create("k", "p").WithRoute("audit"));

        var argumentException = Assert.IsType<ArgumentException>(exception);
        Assert.Equal(method is RoutePublishMethod.Transaction or RoutePublishMethod.Enlisted ? "message" : "messages", argumentException.ParamName);
        Assert.Empty(await RoutePublish.ReadRowsAsync(postgres.ConnectionString, table));
    }

    [Theory]
    [InlineData(RoutePublishMethod.TransactionBatch)]
    [InlineData(RoutePublishMethod.EnlistedBatch)]
    public async Task Batch_with_one_different_route_writes_no_row_of_the_batch(RoutePublishMethod method)
    {
        var table = await postgres.CreateOutboxAsync();
        using var provider = new ServiceCollection().AddKeyedPostgreSqlOutboxPublisher("alerts", table.ToString(), "alerts").BuildServiceProvider();
        var publisher = provider.GetRequiredKeyedService<IOutboxPublisher>("alerts");

        var exception = await RoutePublish.PublishThenCommitAsync(
            postgres.ConnectionString,
            publisher,
            method,
            OutboxMessage.Create("b0", "p"),
            OutboxMessage.Create("b1", "p").WithRoute("alerts"),
            OutboxMessage.Create("b2", "p").WithRoute("audit"),
            OutboxMessage.Create("b3", "p"));

        var argumentException = Assert.IsType<ArgumentException>(exception);
        Assert.Equal("messages", argumentException.ParamName);
        Assert.StartsWith("Message 2 route 'audit' differs from scoped route 'alerts'.", argumentException.Message, StringComparison.Ordinal);
        Assert.Empty(await RoutePublish.ReadRowsAsync(postgres.ConnectionString, table));
    }

    [Theory]
    [MemberData(nameof(RoutePublish.Methods), MemberType = typeof(RoutePublish))]
    public async Task Two_keys_on_one_table_each_write_only_their_route(RoutePublishMethod method)
    {
        var table = await postgres.CreateOutboxAsync();
        using var provider = new ServiceCollection()
            .AddKeyedPostgreSqlOutboxPublisher("alerts", table.ToString(), "alerts")
            .AddKeyedPostgreSqlOutboxPublisher("audit", table.ToString(), "audit")
            .BuildServiceProvider();
        var alerts = provider.GetRequiredKeyedService<IOutboxPublisher>("alerts");
        var audit = provider.GetRequiredKeyedService<IOutboxPublisher>("audit");

        Assert.Null(await RoutePublish.PublishThenCommitAsync(postgres.ConnectionString, alerts, method, OutboxMessage.Create("a1", "p"), OutboxMessage.Create("a2", "p")));
        Assert.Null(await RoutePublish.PublishThenCommitAsync(postgres.ConnectionString, audit, method, OutboxMessage.Create("u1", "p")));
        Assert.IsType<ArgumentException>(await RoutePublish.PublishThenCommitAsync(
            postgres.ConnectionString, audit, method, OutboxMessage.Create("x", "p").WithRoute("alerts")));

        var rows = await RoutePublish.ReadRowsAsync(postgres.ConnectionString, table);
        Assert.Equal([("a1", "alerts"), ("a2", "alerts"), ("u1", "audit")], rows.ConvertAll(row => (row.Key, row.Route)));
    }
}
