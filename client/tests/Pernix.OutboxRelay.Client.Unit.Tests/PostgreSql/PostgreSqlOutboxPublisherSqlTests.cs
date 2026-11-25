using Pernix.OutboxRelay.Client.PostgreSql;
using Xunit;

namespace Pernix.OutboxRelay.Client.Unit.Tests.PostgreSql;

// INSERT texts, selection per (route, headers) and the scoped conflict rule, through the internal SqlFor / RouteFor.
public sealed class PostgreSqlOutboxPublisherSqlTests
{
    // Route-less texts, byte for byte.
    private const string InsertPlain = "INSERT INTO risk.outbox (message_key, payload) VALUES ($1, $2)";
    private const string InsertWithHeaders = "INSERT INTO risk.outbox (message_key, payload, headers) VALUES ($1, $2, $3)";

    private const string InsertWithRoute = "INSERT INTO risk.outbox (message_key, payload, route) VALUES ($1, $2, $3)";
    private const string InsertWithHeadersAndRoute = "INSERT INTO risk.outbox (message_key, payload, headers, route) VALUES ($1, $2, $3, $4)";

    private static readonly OutboxTable Table = OutboxTable.Parse("risk.outbox");

    private static readonly OutboxHeaders Headers = OutboxHeaders.CreateBuilder().Add("hname", "hvalue-secret").Build();

    [Fact]
    public void Route_less_messages_select_the_route_less_texts()
    {
        var publisher = new PostgreSqlOutboxPublisher(Table);

        Assert.Equal(InsertPlain, publisher.SqlFor(null, OutboxMessage.Create("k", "p")));
        Assert.Equal(InsertWithHeaders, publisher.SqlFor(null, OutboxMessage.Create("k", "p", Headers)));
    }

    [Fact]
    public void Routed_messages_select_the_route_texts()
    {
        var publisher = new PostgreSqlOutboxPublisher(Table);

        Assert.Equal(InsertWithRoute, publisher.SqlFor("alerts", OutboxMessage.Create("k", "p")));
        Assert.Equal(InsertWithHeadersAndRoute, publisher.SqlFor("alerts", OutboxMessage.Create("k", "p", Headers)));
    }

    [Fact]
    public void Each_text_is_built_once_per_publisher()
    {
        var publisher = new PostgreSqlOutboxPublisher(Table);
        var plain = OutboxMessage.Create("k", "p");
        var withHeaders = OutboxMessage.Create("k", "p", Headers);

        Assert.Same(publisher.SqlFor(null, plain), publisher.SqlFor(null, plain));
        Assert.Same(publisher.SqlFor(null, withHeaders), publisher.SqlFor(null, withHeaders));
        Assert.Same(publisher.SqlFor("a", plain), publisher.SqlFor("b", plain));
        Assert.Same(publisher.SqlFor("a", withHeaders), publisher.SqlFor("b", withHeaders));
    }

    [Theory]
    [InlineData(false, false, InsertPlain)]
    [InlineData(false, true, InsertWithHeaders)]
    [InlineData(true, false, InsertWithRoute)]
    [InlineData(true, true, InsertWithHeadersAndRoute)]
    public void Plain_publisher_writes_the_message_route(bool routed, bool withHeaders, string expectedSql)
    {
        var publisher = new PostgreSqlOutboxPublisher(Table);
        var message = OutboxMessage.Create("k", "p", withHeaders ? Headers : null);
        if (routed)
        {
            message = message.WithRoute("alerts");
        }

        var route = publisher.RouteFor(message, -1);

        Assert.Same(message.Route, route);
        Assert.Equal(expectedSql, publisher.SqlFor(route, message));
    }

    [Theory]
    [InlineData(false, false, InsertWithRoute)]
    [InlineData(false, true, InsertWithHeadersAndRoute)]
    [InlineData(true, false, InsertWithRoute)]
    [InlineData(true, true, InsertWithHeadersAndRoute)]
    public void Scoped_publisher_writes_its_route_on_route_less_and_equal_messages(bool routed, bool withHeaders, string expectedSql)
    {
        var scoped = new string("alerts".AsSpan());
        var publisher = new PostgreSqlOutboxPublisher(Table, scoped);
        var message = OutboxMessage.Create("k", "p", withHeaders ? Headers : null);
        if (routed)
        {
            message = message.WithRoute("alerts");
        }

        var route = publisher.RouteFor(message, -1);

        Assert.Same(scoped, route);
        Assert.Same(scoped, publisher.RouteFor(message, 7));
        Assert.Equal(expectedSql, publisher.SqlFor(route, message));
    }

    [Fact]
    public void Scoped_publisher_conflict_on_a_single_message_names_both_routes_and_param_message()
    {
        var publisher = new PostgreSqlOutboxPublisher(Table, "alerts");
        var message = OutboxMessage.Create("ksecret", "psecret", Headers).WithRoute("audit");

        var exception = Assert.Throws<ArgumentException>(() => publisher.RouteFor(message, -1));

        Assert.Equal("message", exception.ParamName);
        Assert.StartsWith("Message route 'audit' differs from scoped route 'alerts'.", exception.Message, StringComparison.Ordinal);
        AssertNoMessageValues(exception.Message);
    }

    [Fact]
    public void Scoped_publisher_conflict_in_a_batch_names_the_position_both_routes_and_param_messages()
    {
        var publisher = new PostgreSqlOutboxPublisher(Table, "alerts");
        var message = OutboxMessage.Create("ksecret", "psecret", Headers).WithRoute("audit");

        var exception = Assert.Throws<ArgumentException>(() => publisher.RouteFor(message, 3));

        Assert.Equal("messages", exception.ParamName);
        Assert.StartsWith("Message 3 route 'audit' differs from scoped route 'alerts'.", exception.Message, StringComparison.Ordinal);
        AssertNoMessageValues(exception.Message);
    }

    [Fact]
    public void Scoped_publisher_compares_routes_ordinally()
    {
        var publisher = new PostgreSqlOutboxPublisher(Table, "alerts");

        Assert.Throws<ArgumentException>(() => publisher.RouteFor(OutboxMessage.Create("k", "p").WithRoute("alerts.eu"), -1));
    }

    [Fact]
    public void Scoped_constructor_validates_the_route_before_the_table()
    {
        Assert.Throws<ArgumentNullException>(() => new PostgreSqlOutboxPublisher(default, null!));

        var invalid = Assert.Throws<ArgumentException>(() => new PostgreSqlOutboxPublisher(default, "Alerts"));
        Assert.Equal("route", invalid.ParamName);

        var table = Assert.Throws<ArgumentException>(() => new PostgreSqlOutboxPublisher(default, "alerts"));
        Assert.Equal("table", table.ParamName);
    }

    [Fact]
    public void Scoped_publisher_keeps_the_table()
    {
        Assert.Equal("risk.outbox", new PostgreSqlOutboxPublisher(Table, "alerts").Table.ToString());
    }

    private static void AssertNoMessageValues(string text)
    {
        Assert.DoesNotContain("ksecret", text, StringComparison.Ordinal);
        Assert.DoesNotContain("psecret", text, StringComparison.Ordinal);
        Assert.DoesNotContain("hname", text, StringComparison.Ordinal);
        Assert.DoesNotContain("hvalue-secret", text, StringComparison.Ordinal);
    }
}
