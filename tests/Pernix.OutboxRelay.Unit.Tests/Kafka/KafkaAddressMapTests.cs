using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Kafka;
using Confluent.Kafka;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Kafka;

public sealed class KafkaAddressMapTests
{
    private static readonly DeadLetterInfo DeadLetter = new("boom", 3, DateTimeOffset.UnixEpoch);

    [Fact]
    public void ForTopic_has_single_address_only()
    {
        var map = KafkaAddressMap.ForTopic("risk.alerts");

        Assert.NotNull(map.Single);
        Assert.Equal("risk.alerts", map.Single.Topic);
        Assert.Equal(new TopicPartition("risk.alerts", Partition.Any), map.Single.TopicPartition);
        Assert.Null(map.Single.Config);
        Assert.Null(map.ByRoute);
        Assert.Null(map.Routes);
        Assert.Same(map.Single, Assert.Single(map.Distinct));
    }

    [Fact]
    public void ForRoutes_two_routes_two_topics()
    {
        var map = Routed(("alerts", "risk.alerts"), ("audit", "risk.audit"));

        Assert.Null(map.Single);
        Assert.NotNull(map.ByRoute);
        Assert.NotNull(map.Routes);
        Assert.Equal(2, map.ByRoute.Count);
        Assert.Equal("risk.alerts", map.ByRoute["alerts"].Topic);
        Assert.Equal("risk.audit", map.ByRoute["audit"].Topic);
        Assert.Equal(new TopicPartition("risk.audit", Partition.Any), map.ByRoute["audit"].TopicPartition);
        Assert.Equal(["alerts", "audit"], map.Routes.Order(StringComparer.Ordinal));
        Assert.Equal(2, map.Distinct.Length);
        Assert.Same(map.ByRoute["alerts"], map.Distinct[0]);
        Assert.Same(map.ByRoute["audit"], map.Distinct[1]);
    }

    [Fact]
    public void ForRoutes_duplicate_topic_shares_one_address()
    {
        var map = Routed(("alerts", "risk.events"), ("audit", "risk.events"));

        Assert.NotNull(map.ByRoute);
        Assert.Same(map.ByRoute["alerts"], map.ByRoute["audit"]);
        Assert.Same(map.ByRoute["alerts"].TopicPartition, map.ByRoute["audit"].TopicPartition);
        Assert.Same(map.ByRoute["alerts"], Assert.Single(map.Distinct));
    }

    [Fact]
    public void ForRoutes_topic_compare_is_ordinal()
    {
        var map = Routed(("alerts", "risk.events"), ("audit", "Risk.Events"));

        Assert.NotNull(map.ByRoute);
        Assert.NotSame(map.ByRoute["alerts"], map.ByRoute["audit"]);
        Assert.Equal(2, map.Distinct.Length);
    }

    [Fact]
    public void Routes_are_ordinal()
    {
        var map = Routed(("alerts", "risk.alerts"));

        Assert.NotNull(map.Routes);
        Assert.NotNull(map.ByRoute);
        Assert.Contains("alerts", (IReadOnlySet<string>)map.Routes);
        Assert.DoesNotContain("Alerts", (IReadOnlySet<string>)map.Routes);
        Assert.False(map.ByRoute.ContainsKey("Alerts"));
        Assert.Null(map.Resolve(MainSend("Alerts")));
    }

    [Fact]
    public void Topic_map_main_without_route_resolves_single()
    {
        var map = KafkaAddressMap.ForTopic("risk.alerts");

        Assert.Same(map.Single, map.Resolve(MainSend(null)));
    }

    [Fact]
    public void Topic_map_main_with_route_resolves_null()
    {
        var map = KafkaAddressMap.ForTopic("risk.alerts");

        Assert.Null(map.Resolve(MainSend("alerts")));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("alerts")]
    public void Topic_map_dead_letter_resolves_single(string? route)
    {
        var map = KafkaAddressMap.ForTopic("risk.alerts");

        Assert.Same(map.Single, map.Resolve(Dead(route)));
    }

    [Fact]
    public void Routes_map_mapped_route_resolves_its_address()
    {
        var map = Routed(("alerts", "risk.alerts"), ("audit", "risk.audit"));

        Assert.NotNull(map.ByRoute);
        Assert.Same(map.ByRoute["alerts"], map.Resolve(MainSend("alerts")));
        Assert.Same(map.ByRoute["audit"], map.Resolve(MainSend("audit")));
    }

    [Fact]
    public void Routes_map_unmapped_route_resolves_null()
    {
        var map = Routed(("alerts", "risk.alerts"));

        Assert.Null(map.Resolve(MainSend("orders")));
    }

    [Fact]
    public void Routes_map_main_without_route_resolves_null()
    {
        var map = Routed(("alerts", "risk.alerts"));

        Assert.Null(map.Resolve(MainSend(null)));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("alerts")]
    public void Routes_map_dead_letter_resolves_null(string? route)
    {
        var map = Routed(("alerts", "risk.alerts"));

        Assert.Null(map.Resolve(Dead(route)));
    }

    [Fact]
    public void Resolve_on_topic_map_allocates_nothing()
    {
        var map = KafkaAddressMap.ForTopic("risk.alerts");

        Assert.Equal(0, ResolveBytes(map, [MainSend(null), MainSend("alerts"), Dead(null), Dead("alerts")]));
    }

    [Fact]
    public void Resolve_on_routes_map_allocates_nothing()
    {
        var map = Routed(("alerts", "risk.alerts"), ("audit", "risk.audit"));

        Assert.Equal(0, ResolveBytes(map, [MainSend("alerts"), MainSend("audit"), MainSend("orders"), MainSend(null), Dead("alerts")]));
    }

    private static long ResolveBytes(KafkaAddressMap map, OutgoingMessage[] messages)
    {
        const int Calls = 10_000;
        var hits = 0;
        for (var i = 0; i < Calls; i++)
        {
            hits += map.Resolve(messages[i % messages.Length]) is null ? 0 : 1;
        }

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < Calls; i++)
        {
            hits += map.Resolve(messages[i % messages.Length]) is null ? 0 : 1;
        }

        var bytes = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.True(hits >= 0);
        return bytes;
    }

    private static KafkaAddressMap Routed(params (string Route, string Topic)[] routes)
    {
        var map = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var (route, topic) in routes)
        {
            map.Add(route, topic);
        }

        return KafkaAddressMap.ForRoutes(map);
    }

    private static OutgoingMessage MainSend(string? route) => Message(route, null);

    private static OutgoingMessage Dead(string? route) => Message(route, DeadLetter);

    private static OutgoingMessage Message(string? route, DeadLetterInfo? deadLetter) => new()
    {
        Source = new MessageSource("orders", 1, "events"),
        Payload = [1],
        Headers = MessageHeaders.Empty,
        DeadLetter = deadLetter,
        Route = route,
    };
}
