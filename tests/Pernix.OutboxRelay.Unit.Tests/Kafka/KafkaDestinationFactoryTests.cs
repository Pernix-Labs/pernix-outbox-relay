using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Destination.Kafka;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Kafka;

public sealed class KafkaDestinationFactoryTests
{
    private readonly KafkaHarness harness = new();

    [Fact]
    public void Transport_is_kafka()
    {
        Assert.Equal("Kafka", harness.Factory.Transport);
    }

    [Fact]
    public void Valid_destination_passes()
    {
        Assert.Empty(harness.Validate());
    }

    [Fact]
    public void Destination_without_kafka_block_fails_with_both_required_messages()
    {
        harness.Remove("Destinations:0:Kafka");

        Assert.Equal(
            [
                "Destinations[OrdersTopic].Kafka.Cluster: required when Transport is Kafka",
                "Destinations[OrdersTopic].Kafka: Topic or Routes is required when Transport is Kafka",
            ],
            harness.Validate());
    }

    [Fact]
    public void Destination_without_topic_fails()
    {
        harness.Set("Destinations:0:Kafka:Topic", "");

        Assert.Equal(["Destinations[OrdersTopic].Kafka: Topic or Routes is required when Transport is Kafka"], harness.Validate());
    }

    [Fact]
    public void Routes_only_passes()
    {
        harness.UseRoutes(("alerts", "risk.alerts"), ("audit", "risk.audit"));

        Assert.Empty(harness.Validate());
    }

    [Fact]
    public void Topic_and_routes_both_set_fails()
    {
        harness.Set("Destinations:0:Kafka:Routes:alerts", "risk.alerts");

        Assert.Equal(["Destinations[OrdersTopic].Kafka: Topic and Routes are both set; set exactly one"], harness.Validate());
    }

    [Fact]
    public void Neither_topic_nor_routes_fails()
    {
        harness.Remove("Destinations:0:Kafka:Topic");

        Assert.Equal(["Destinations[OrdersTopic].Kafka: Topic or Routes is required when Transport is Kafka"], harness.Validate());
    }

    [Fact]
    public void Empty_routes_object_is_unset()
    {
        harness.Remove("Destinations:0:Kafka:Topic");
        harness.Set("Destinations:0:Kafka:Routes", "");

        Assert.Equal(["Destinations[OrdersTopic].Kafka: Topic or Routes is required when Transport is Kafka"], harness.Validate());
    }

    [Fact]
    public void Empty_routes_object_with_topic_passes()
    {
        harness.Set("Destinations:0:Kafka:Routes", "");

        Assert.Empty(harness.Validate());
    }

    [Fact]
    public void Blank_route_value_fails()
    {
        harness.UseRoutes(("alerts", "risk.alerts"), ("audit", " "));

        Assert.Equal(["Destinations[OrdersTopic].Kafka.Routes.audit: topic is required"], harness.Validate());
    }

    [Fact]
    public void Duplicate_route_values_pass()
    {
        harness.UseRoutes(("alerts", "risk.events"), ("audit", "risk.events"));

        Assert.Empty(harness.Validate());
    }

    [Fact]
    public async Task Instances_of_a_routes_destination_share_one_route_set()
    {
        harness.UseRoutes(("alerts", "risk.alerts"), ("audit", "risk.audit"));
        var (destination, _) = harness.Bind();
        Assert.Empty(harness.Validate());

        await using var probe = harness.Factory.Create(destination, DestinationContext.Probe);
        await using var term = harness.Factory.Create(destination, new DestinationContext(OrderingMode.PerKey));

        Assert.NotNull(probe.Routes);
        Assert.Same(probe.Routes, term.Routes);
        Assert.Equal(["alerts", "audit"], probe.Routes.Order(StringComparer.Ordinal));
    }

    [Fact]
    public async Task Topic_destination_declares_no_routes()
    {
        var (destination, _) = harness.Bind();
        Assert.Empty(harness.Validate());

        await using var created = harness.Factory.Create(destination, DestinationContext.Probe);

        Assert.Null(created.Routes);
    }

    [Fact]
    public void Undefined_cluster_fails()
    {
        harness.Set("Destinations:0:Kafka:Cluster", "Other");

        Assert.Equal(["Destinations[OrdersTopic].Kafka.Cluster: Kafka cluster 'Other' is not defined in KafkaClusters"], harness.Validate());
    }

    [Fact]
    public void Cluster_reference_is_case_insensitive()
    {
        harness.Set("Destinations:0:Kafka:Cluster", "MAIN");

        Assert.Empty(harness.Validate());
    }

    [Fact]
    public void Unknown_destination_setting_fails()
    {
        harness.Set("Destinations:0:Database:Provider", "PostgreSql");

        Assert.Equal(["Destinations[OrdersTopic]: unknown setting 'Database'"], harness.Validate());
    }

    [Fact]
    public void Unknown_kafka_setting_fails()
    {
        harness.Set("Destinations:0:Kafka:Partitions", "12");

        Assert.Equal(["Destinations[OrdersTopic].Kafka: unknown setting 'Partitions'"], harness.Validate());
    }

    [Fact]
    public void Create_before_validate_throws()
    {
        var (destination, _) = harness.Bind();

        Assert.Throws<InvalidOperationException>(() => harness.Factory.Create(destination, DestinationContext.Probe));
    }

    [Fact]
    public void Create_for_undefined_mode_throws()
    {
        var (destination, _) = harness.Bind();
        Assert.Empty(harness.Validate());

        Assert.Throws<ArgumentOutOfRangeException>(() => harness.Factory.Create(destination, new DestinationContext((OrderingMode)99)));
    }

    [Fact]
    public async Task Created_destinations_share_topic_limits()
    {
        await using var first = harness.Create(new TopicConfig(1024, Compacted: false));
        var (destination, _) = harness.Bind();
        await using var second = harness.Factory.Create(destination, new DestinationContext(OrderingMode.None));

        var result = await second.SendAsync(KafkaHarness.Message([1], new byte[2048]), CancellationToken.None);

        Assert.IsType<DeliveryResult.MessageFault>(result);
    }
}
