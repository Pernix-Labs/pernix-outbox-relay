using System.Reflection;
using Pernix.OutboxRelay.Client.PostgreSql;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Pernix.OutboxRelay.Client.Unit.Tests.PostgreSql;

// Registrations proven by resolution.
public sealed class PostgreSqlOutboxServiceCollectionExtensionsTests
{
    [Fact]
    public void AddPostgreSqlOutboxPublisher_registers_a_singleton_for_the_table()
    {
        var services = new ServiceCollection();

        Assert.Same(services, services.AddPostgreSqlOutboxPublisher("risk.outbox"));

        var descriptor = Assert.Single(services);
        Assert.Equal(ServiceLifetime.Singleton, descriptor.Lifetime);
        Assert.Equal(typeof(IOutboxPublisher), descriptor.ServiceType);

        using var provider = services.BuildServiceProvider();
        var first = provider.GetRequiredService<IOutboxPublisher>();
        Assert.IsType<PostgreSqlOutboxPublisher>(first);
        Assert.Equal("risk.outbox", first.Table.ToString());
        Assert.Same(first, provider.GetRequiredService<IOutboxPublisher>());
    }

    [Fact]
    public void AddKeyedPostgreSqlOutboxPublisher_resolves_each_key_to_its_own_table()
    {
        var services = new ServiceCollection()
            .AddKeyedPostgreSqlOutboxPublisher("orders", "risk.orders_outbox")
            .AddKeyedPostgreSqlOutboxPublisher("payments", "payments_outbox");

        Assert.All(services, descriptor => Assert.Equal(ServiceLifetime.Singleton, descriptor.Lifetime));

        using var provider = services.BuildServiceProvider();
        var orders = provider.GetRequiredKeyedService<IOutboxPublisher>("orders");
        var payments = provider.GetRequiredKeyedService<IOutboxPublisher>("payments");

        Assert.IsType<PostgreSqlOutboxPublisher>(orders);
        Assert.Equal("risk.orders_outbox", orders.Table.ToString());
        Assert.Equal("payments_outbox", payments.Table.ToString());
        Assert.Same(orders, provider.GetRequiredKeyedService<IOutboxPublisher>("orders"));
        Assert.Same(payments, provider.GetRequiredKeyedService<IOutboxPublisher>("payments"));
        Assert.Null(provider.GetService<IOutboxPublisher>());
    }

    [Theory]
    [InlineData("")]
    [InlineData("a.b.c")]
    [InlineData("1outbox")]
    [InlineData("risk.out-box")]
    public void Invalid_table_throws_at_the_call_and_registers_nothing(string table)
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentException>(() => services.AddPostgreSqlOutboxPublisher(table));
        Assert.Throws<ArgumentException>(() => services.AddKeyedPostgreSqlOutboxPublisher("k", table));
        Assert.Empty(services);
    }

    [Fact]
    public void Null_table_throws_at_the_call()
    {
        var services = new ServiceCollection();

        Assert.Throws<ArgumentException>(() => services.AddPostgreSqlOutboxPublisher(null!));
        Assert.Throws<ArgumentException>(() => services.AddKeyedPostgreSqlOutboxPublisher("k", null!));
        Assert.Empty(services);
    }

    [Fact]
    public void Null_key_throws()
    {
        var services = new ServiceCollection();

        var exception = Assert.Throws<ArgumentNullException>(() => services.AddKeyedPostgreSqlOutboxPublisher(null!, "outbox"));

        Assert.Equal("key", exception.ParamName);
        Assert.Empty(services);
    }

    [Fact]
    public void Null_services_throws()
    {
        Assert.Throws<ArgumentNullException>(() => PostgreSqlOutboxServiceCollectionExtensions.AddPostgreSqlOutboxPublisher(null!, "outbox"));
        Assert.Throws<ArgumentNullException>(() => PostgreSqlOutboxServiceCollectionExtensions.AddKeyedPostgreSqlOutboxPublisher(null!, "k", "outbox"));
    }

    // Route-scoped keyed registration.
    [Fact]
    public void Route_scoped_registration_resolves_a_keyed_singleton_for_the_table_and_route()
    {
        var services = new ServiceCollection();

        Assert.Same(services, services.AddKeyedPostgreSqlOutboxPublisher("alerts", "risk.outbox", "alerts"));

        var descriptor = Assert.Single(services);
        Assert.Equal(ServiceLifetime.Singleton, descriptor.Lifetime);
        Assert.Equal(typeof(IOutboxPublisher), descriptor.ServiceType);

        using var provider = services.BuildServiceProvider();
        var publisher = Assert.IsType<PostgreSqlOutboxPublisher>(provider.GetRequiredKeyedService<IOutboxPublisher>("alerts"));
        Assert.Equal("risk.outbox", publisher.Table.ToString());
        Assert.Equal("alerts", publisher.RouteFor(OutboxMessage.Create("k", "p"), -1));
        Assert.Same(publisher, provider.GetRequiredKeyedService<IOutboxPublisher>("alerts"));
        Assert.Null(provider.GetService<IOutboxPublisher>());
    }

    [Fact]
    public void Two_keys_on_one_table_resolve_two_publishers_each_with_its_own_route()
    {
        var services = new ServiceCollection()
            .AddKeyedPostgreSqlOutboxPublisher("alerts", "risk.outbox", "alerts")
            .AddKeyedPostgreSqlOutboxPublisher("audit", "risk.outbox", "audit");

        using var provider = services.BuildServiceProvider();
        var alerts = Assert.IsType<PostgreSqlOutboxPublisher>(provider.GetRequiredKeyedService<IOutboxPublisher>("alerts"));
        var audit = Assert.IsType<PostgreSqlOutboxPublisher>(provider.GetRequiredKeyedService<IOutboxPublisher>("audit"));

        Assert.NotSame(alerts, audit);
        Assert.Equal(alerts.Table, audit.Table);
        Assert.Equal("alerts", alerts.RouteFor(OutboxMessage.Create("k", "p"), -1));
        Assert.Equal("audit", audit.RouteFor(OutboxMessage.Create("k", "p"), -1));
    }

    [Fact]
    public void Route_scoped_registration_null_arguments_throw_and_register_nothing()
    {
        var services = new ServiceCollection();

        Assert.Equal("services", Assert.Throws<ArgumentNullException>(
            () => PostgreSqlOutboxServiceCollectionExtensions.AddKeyedPostgreSqlOutboxPublisher(null!, "k", "outbox", "alerts")).ParamName);
        Assert.Equal("key", Assert.Throws<ArgumentNullException>(
            () => services.AddKeyedPostgreSqlOutboxPublisher(null!, "outbox", "alerts")).ParamName);
        Assert.Equal("route", Assert.Throws<ArgumentNullException>(
            () => services.AddKeyedPostgreSqlOutboxPublisher("k", "outbox", null!)).ParamName);
        Assert.Empty(services);
    }

    [Theory]
    [InlineData("")]
    [InlineData("Alerts")]
    [InlineData(".a")]
    [InlineData("a b")]
    [InlineData("a\n")]
    public void Route_scoped_registration_invalid_route_throws_the_route_rule_and_registers_nothing(string route)
    {
        var services = new ServiceCollection();

        var exception = Assert.Throws<ArgumentException>(() => services.AddKeyedPostgreSqlOutboxPublisher("k", "outbox", route));

        Assert.Equal("route", exception.ParamName);
        Assert.StartsWith(
            "Route must match ^[a-z0-9][a-z0-9._-]{0,62}$: 1 to 63 lowercase ASCII letters, digits, '.', '-' or '_', starting with a letter or digit.",
            exception.Message,
            StringComparison.Ordinal);
        Assert.Empty(services);
    }

    [Fact]
    public void Route_scoped_registration_checks_the_route_before_the_table()
    {
        var services = new ServiceCollection();

        Assert.Equal("route", Assert.Throws<ArgumentException>(
            () => services.AddKeyedPostgreSqlOutboxPublisher("k", "a.b.c", "Alerts")).ParamName);
        Assert.Equal("route", Assert.Throws<ArgumentNullException>(
            () => services.AddKeyedPostgreSqlOutboxPublisher("k", "a.b.c", null!)).ParamName);
        Assert.Empty(services);
    }

    [Theory]
    [InlineData("")]
    [InlineData("a.b.c")]
    [InlineData("1outbox")]
    [InlineData("risk.out-box")]
    [InlineData(null)]
    public void Route_scoped_registration_invalid_table_throws_as_Parse_and_registers_nothing(string? table)
    {
        var services = new ServiceCollection();

        var expected = Assert.ThrowsAny<ArgumentException>(() => OutboxTable.Parse(table!));
        var actual = Assert.ThrowsAny<ArgumentException>(() => services.AddKeyedPostgreSqlOutboxPublisher("k", table!, "alerts"));

        Assert.Equal(expected.GetType(), actual.GetType());
        Assert.Equal(expected.Message, actual.Message);
        Assert.Empty(services);
    }

    [Fact]
    public void Route_scoped_registration_replaces_on_key_reuse()
    {
        var services = new ServiceCollection()
            .AddKeyedPostgreSqlOutboxPublisher("k", "risk.outbox", "alerts")
            .AddKeyedPostgreSqlOutboxPublisher("k", "risk.outbox", "audit");

        using var provider = services.BuildServiceProvider();
        var publisher = Assert.IsType<PostgreSqlOutboxPublisher>(provider.GetRequiredKeyedService<IOutboxPublisher>("k"));
        Assert.Equal("audit", publisher.RouteFor(OutboxMessage.Create("k", "p"), -1));
    }

    // Existing overloads plus the one new overload.
    [Fact]
    public void Public_overload_set_is_the_existing_two_plus_the_route_scoped_one()
    {
        var signatures = typeof(PostgreSqlOutboxServiceCollectionExtensions)
            .GetMethods(BindingFlags.Public | BindingFlags.Static | BindingFlags.DeclaredOnly)
            .Select(method => $"{method.ReturnType.Name} {method.Name}({string.Join(", ", method.GetParameters().Select(parameter => $"{parameter.ParameterType.Name} {parameter.Name}"))})")
            .Order(StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(
            [
                "IServiceCollection AddKeyedPostgreSqlOutboxPublisher(IServiceCollection services, Object key, String table)",
                "IServiceCollection AddKeyedPostgreSqlOutboxPublisher(IServiceCollection services, Object key, String table, String route)",
                "IServiceCollection AddPostgreSqlOutboxPublisher(IServiceCollection services, String table)",
            ],
            signatures);
    }
}
