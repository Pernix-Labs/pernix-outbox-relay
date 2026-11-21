using Pernix.OutboxRelay.Client.EntityFrameworkCore;
using Pernix.OutboxRelay.Client.PostgreSql;
using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Pernix.OutboxRelay.Client.Unit.Tests.EntityFrameworkCore;

// Offline: UseNpgsql("Host=unused"), no connection opened.
public sealed class ArgumentTests
{
    private static readonly OutboxMessage Message = OutboxMessage.Create("k", "p");
    private static readonly OutboxMessage[] Messages = [Message];

    private readonly PostgreSqlOutboxPublisher publisher = new(OutboxTable.Parse("s.t"));

    [Fact]
    public void UseOutbox_rejects_null_builder()
    {
        var plain = Assert.Throws<ArgumentNullException>(() => ((DbContextOptionsBuilder)null!).UseOutbox(publisher));
        var typed = Assert.Throws<ArgumentNullException>(() => ((DbContextOptionsBuilder<OfflineContext>)null!).UseOutbox(publisher));

        Assert.Equal("builder", plain.ParamName);
        Assert.Equal("builder", typed.ParamName);
    }

    [Fact]
    public void UseOutbox_rejects_null_publisher()
    {
        var plain = Assert.Throws<ArgumentNullException>(() => new DbContextOptionsBuilder().UseOutbox(null!));
        var typed = Assert.Throws<ArgumentNullException>(() => new DbContextOptionsBuilder<OfflineContext>().UseOutbox(null!));

        Assert.Equal("publisher", plain.ParamName);
        Assert.Equal("publisher", typed.ParamName);
    }

    // E6
    [Fact]
    public void UseOutbox_twice_throws()
    {
        var builder = new DbContextOptionsBuilder<OfflineContext>().UseNpgsql("Host=unused").UseOutbox(publisher);

        var exception = Assert.Throws<InvalidOperationException>(() => builder.UseOutbox(publisher));

        Assert.Equal(
            "UseOutbox was already called on this DbContextOptionsBuilder. A context has one outbox queue and one publisher.",
            exception.Message);
    }

    [Fact]
    public void UseOutbox_returns_the_builder()
    {
        var plain = new DbContextOptionsBuilder();
        var typed = new DbContextOptionsBuilder<OfflineContext>();

        Assert.Same(plain, plain.UseOutbox(publisher));
        Assert.Same(typed, typed.UseOutbox(publisher));
    }

    [Fact]
    public void EnqueueOutbox_rejects_null_context()
    {
        var exception = Assert.Throws<ArgumentNullException>(() => ((DbContext)null!).EnqueueOutbox(Message));

        Assert.Equal("context", exception.ParamName);
    }

    // E1
    [Fact]
    public void Enqueue_without_UseOutbox_throws_naming_UseOutbox()
    {
        using var context = new OfflineContext(new DbContextOptionsBuilder<OfflineContext>().UseNpgsql("Host=unused").Options);

        var exception = Assert.Throws<InvalidOperationException>(() => context.EnqueueOutbox(Message));

        Assert.Equal(
            "DbContext type 'OfflineContext' has no outbox queue. Call UseOutbox(publisher) on its DbContextOptionsBuilder.",
            exception.Message);
    }

    [Fact]
    public void AddOutboxQueue_rejects_null_services()
    {
        var exception = Assert.Throws<ArgumentNullException>(() => ((IServiceCollection)null!).AddOutboxQueue<OfflineContext>());

        Assert.Equal("services", exception.ParamName);
    }

    [Fact]
    public void AddKeyedOutboxQueue_rejects_null_arguments_and_registers_nothing()
    {
        var services = new ServiceCollection();

        Assert.Equal("services", Assert.Throws<ArgumentNullException>(
            () => ((IServiceCollection)null!).AddKeyedOutboxQueue<OfflineContext>("k", "alerts")).ParamName);
        Assert.Equal("key", Assert.Throws<ArgumentNullException>(
            () => services.AddKeyedOutboxQueue<OfflineContext>(null!, "alerts")).ParamName);
        Assert.Equal("route", Assert.Throws<ArgumentNullException>(
            () => services.AddKeyedOutboxQueue<OfflineContext>("k", null!)).ParamName);
        Assert.Empty(services);
    }

    [Theory]
    [InlineData("")]
    [InlineData("Alerts")]
    [InlineData(".a")]
    [InlineData("a b")]
    [InlineData("a\n")]
    public void AddKeyedOutboxQueue_rejects_invalid_route_and_registers_nothing(string route)
    {
        var services = new ServiceCollection();

        var exception = Assert.Throws<ArgumentException>(() => services.AddKeyedOutboxQueue<OfflineContext>("k", route));

        Assert.Equal("route", exception.ParamName);
        Assert.StartsWith(
            "Route must match ^[a-z0-9][a-z0-9._-]{0,62}$: 1 to 63 lowercase ASCII letters, digits, '.', '-' or '_', starting with a letter or digit.",
            exception.Message,
            StringComparison.Ordinal);
        Assert.Empty(services);
    }

    [Fact]
    public void AddKeyedOutboxQueue_registers_one_keyed_scoped_queue_and_returns_services()
    {
        var services = new ServiceCollection();

        Assert.Same(services, services.AddKeyedOutboxQueue<OfflineContext>("k", "alerts"));

        var descriptor = Assert.Single(services);
        Assert.Equal(typeof(IOutboxQueue<OfflineContext>), descriptor.ServiceType);
        Assert.Equal("k", descriptor.ServiceKey);
        Assert.Equal(ServiceLifetime.Scoped, descriptor.Lifetime);
    }

    [Fact]
    public async Task PublishAsync_rejects_null_publisher()
    {
        using var context = NewContext();

        var single = await Assert.ThrowsAsync<ArgumentNullException>(() => ((IOutboxPublisher)null!).PublishAsync(context, Message, TestContext.Current.CancellationToken));
        var batch = await Assert.ThrowsAsync<ArgumentNullException>(() => ((IOutboxPublisher)null!).PublishAsync(context, Messages, TestContext.Current.CancellationToken));

        Assert.Equal("publisher", single.ParamName);
        Assert.Equal("publisher", batch.ParamName);
    }

    [Fact]
    public async Task PublishAsync_rejects_null_context()
    {
        var single = await Assert.ThrowsAsync<ArgumentNullException>(() => publisher.PublishAsync((DbContext)null!, Message, TestContext.Current.CancellationToken));
        var batch = await Assert.ThrowsAsync<ArgumentNullException>(() => publisher.PublishAsync((DbContext)null!, Messages, TestContext.Current.CancellationToken));

        Assert.Equal("context", single.ParamName);
        Assert.Equal("context", batch.ParamName);
    }

    // Null messages wins over a missing transaction.
    [Fact]
    public async Task PublishAsync_batch_rejects_null_messages()
    {
        using var context = NewContext();

        var exception = await Assert.ThrowsAsync<ArgumentNullException>(
            () => publisher.PublishAsync(context, (IReadOnlyList<OutboxMessage>)null!, TestContext.Current.CancellationToken));

        Assert.Equal("messages", exception.ParamName);
    }

    private OfflineContext NewContext() =>
        new(new DbContextOptionsBuilder<OfflineContext>().UseNpgsql("Host=unused").UseOutbox(publisher).Options);

    public sealed class OfflineContext(DbContextOptions<OfflineContext> options) : DbContext(options);
}
