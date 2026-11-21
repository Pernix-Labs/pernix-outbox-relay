using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Configuration;

public sealed class DestinationFactoryResolverTests
{
    private const string Path = "Destinations[Archive]";

    private readonly Dictionary<string, string?> settings = new()
    {
        ["Destinations:0:Name"] = "Archive",
        ["Destinations:0:Transport"] = "Kafka",
    };

    private readonly List<string> failures = [];

    [Fact]
    public void Transport_resolves_case_insensitively()
    {
        var kafka = new FakeDestinationFactory("Kafka");
        settings["Destinations:0:Transport"] = "KAFKA";

        Assert.Same(kafka, Resolve(kafka));
    }

    [Fact]
    public void Duplicate_plain_transport_throws()
    {
        var ex = Assert.Throws<InvalidOperationException>(() =>
            new DestinationFactoryResolver([new FakeDestinationFactory("Kafka"), new FakeDestinationFactory("kafka")]));

        Assert.Equal("Destination transport 'kafka' is registered more than once.", ex.Message);
    }

    [Fact]
    public void Plain_factory_resolves_unchanged()
    {
        var kafka = new FakeDestinationFactory("Kafka");
        settings["Destinations:0:Transport"] = "kafka";

        Assert.Same(kafka, Resolve(kafka));
        Assert.Empty(failures);
    }

    [Theory]
    [InlineData("Sqs")]
    [InlineData("Database")]
    public void Unknown_transport_is_not_implemented(string transport)
    {
        settings["Destinations:0:Transport"] = transport;

        Assert.Null(Resolve(new FakeDestinationFactory("Kafka")));

        Assert.Equal([$"{Path}.Transport: transport '{transport}' is not implemented in this build"], failures);
    }

    [Theory]
    [InlineData(null)]
    [InlineData(" ")]
    public void Missing_transport_resolves_nothing_and_adds_nothing(string? transport)
    {
        settings["Destinations:0:Transport"] = transport;

        Assert.Null(Resolve(new FakeDestinationFactory("Kafka")));

        Assert.Empty(failures);
    }

    private IDestinationFactory? Resolve(params IDestinationFactory[] factories)
    {
        var configuration = new ConfigurationBuilder().AddInMemoryCollection(settings).Build();
        var destination = RelayOptionsBinder.Bind(configuration).Destinations[0];
        return new DestinationFactoryResolver(factories).Resolve(destination, Path, failures);
    }
}
