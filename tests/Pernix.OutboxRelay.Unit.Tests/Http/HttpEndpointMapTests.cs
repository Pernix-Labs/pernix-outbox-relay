using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Destination.Http;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Http;

public sealed class HttpEndpointMapTests
{
    private const string Https = HttpPlanFixture.Https;

    private static readonly DeadLetterInfo DeadLetter = new("boom", 3, DateTimeOffset.UnixEpoch);

    [Fact]
    public void Url_map_resolves_main_sends_without_route()
    {
        var map = UrlMap();

        Assert.Null(map.Routes);
        Assert.Same(Assert.Single(map.Endpoints), map.Resolve(Send(null)));
    }

    [Fact]
    public void Url_map_refuses_a_main_send_with_a_route()
    {
        Assert.Null(UrlMap().Resolve(Send("alerts")));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("alerts")]
    public void Url_map_resolves_dead_letter_sends_with_any_route(string? route)
    {
        var map = UrlMap();

        Assert.Same(map.Endpoints[0], map.Resolve(Send(route, DeadLetter)));
    }

    [Fact]
    public void Static_url_shares_one_uri()
    {
        var map = UrlMap();

        var endpoint = map.Resolve(Send(null))!;
        Assert.NotNull(endpoint.StaticUri);
        Assert.Null(endpoint.UrlTemplate);
        Assert.Same(endpoint.StaticUri, map.Resolve(Send(null))!.StaticUri);
    }

    [Fact]
    public void Routes_map_resolves_by_route()
    {
        var map = RoutesMap();

        Assert.Equal(["alerts", "audit"], map.Routes!.Order(StringComparer.Ordinal));
        Assert.Equal("alerts", map.Resolve(Send("alerts"))!.RouteLabel);
        Assert.Equal("audit", map.Resolve(Send("audit"))!.RouteLabel);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("orders")]
    [InlineData("Alerts")]
    public void Routes_map_refuses_unknown_or_missing_route(string? route)
    {
        Assert.Null(RoutesMap().Resolve(Send(route)));
    }

    [Fact]
    public void Routes_map_refuses_dead_letter_sends()
    {
        Assert.Null(RoutesMap().Resolve(Send("alerts", DeadLetter)));
    }

    [Fact]
    public void Routes_with_the_same_static_url_share_one_uri()
    {
        var map = HttpPlanFixture.Plan(new()
        {
            ["Routes:alerts:Url"] = $"{Https}/events",
            ["Routes:audit:Url"] = $"{Https}/events",
        }).Endpoints;

        Assert.Same(map.Resolve(Send("alerts"))!.StaticUri, map.Resolve(Send("audit"))!.StaticUri);
    }

    [Fact]
    public void Several_outboxes_resolve_their_own_endpoint()
    {
        var map = HttpPlanFixture.Plan(new() { ["Url"] = $"{Https}/{{outbox}}" }, outboxes: ["Orders", "Billing"]).Endpoints;

        Assert.Equal($"{Https}/Orders", map.Resolve(Send(null, outbox: "Orders"))!.StaticUri!.AbsoluteUri);
        Assert.Equal($"{Https}/Billing", map.Resolve(Send(null, outbox: "billing"))!.StaticUri!.AbsoluteUri);
        Assert.Null(map.Resolve(Send(null, outbox: "Unknown")));
    }

    [Fact]
    public void Dead_letter_outbox_gets_an_endpoint()
    {
        var (failures, factory) = HttpPlanFixture.Validate(new()
        {
            ["Url"] = $"{Https}/{{outbox}}",
            ["!Outboxes:1:Name"] = "Billing",
            ["!Outboxes:1:Destination"] = "Other",
            ["!Outboxes:1:DeadLetter:Destination"] = "Sink",
        });

        Assert.Empty(failures);
        var map = factory.PlanOf("Sink").Endpoints;
        Assert.Equal($"{Https}/Billing", map.Resolve(Send(null, DeadLetter, "Billing"))!.StaticUri!.AbsoluteUri);
    }

    [Fact]
    public void Resolve_on_url_map_allocates_nothing()
    {
        var map = UrlMap();

        Assert.Equal(0, ResolveBytes(map, [Send(null), Send("alerts", DeadLetter)]));
    }

    [Fact]
    public void Resolve_on_routes_map_allocates_nothing()
    {
        var map = RoutesMap();

        Assert.Equal(0, ResolveBytes(map, [Send("alerts"), Send("audit"), Send("orders")]));
    }

    [Fact]
    public void Resolve_across_outboxes_allocates_nothing()
    {
        var map = HttpPlanFixture.Plan(new() { ["Url"] = $"{Https}/{{outbox}}" }, outboxes: ["Orders", "Billing"]).Endpoints;

        Assert.Equal(0, ResolveBytes(map, [Send(null, outbox: "Orders"), Send(null, outbox: "Billing")]));
    }

    [Theory]
    [InlineData(500, null, "HTTP 500")]
    [InlineData(503, "alerts", "route 'alerts': HTTP 503")]
    public void Paused_status_is_a_cached_destination_fault(int code, string? route, string text)
    {
        var endpoint = route is null ? UrlMap().Endpoints[0] : RoutesMap().Resolve(Send(route))!;

        var fault = Assert.IsType<DeliveryResult.DestinationFault>(endpoint.Classify(code));
        Assert.Equal(text, fault.Error);
        Assert.Same(fault, endpoint.Classify(code));
    }

    [Fact]
    public void Rejected_status_is_a_permanent_message_fault()
    {
        var endpoint = UrlMap().Endpoints[0];

        var fault = Assert.IsType<DeliveryResult.MessageFault>(endpoint.Classify(422));
        Assert.True(fault.Permanent);
        Assert.Equal("HTTP 422", fault.Error);
        Assert.Same(fault, endpoint.Classify(422));
    }

    [Fact]
    public void Retried_status_is_a_transient_message_fault()
    {
        var endpoint = HttpPlanFixture.Plan(new() { ["Url"] = $"{Https}/orders", ["StatusCodes:Retried:0"] = "423" }).Endpoints.Endpoints[0];

        var fault = Assert.IsType<DeliveryResult.MessageFault>(endpoint.Classify(423));
        Assert.False(fault.Permanent);
        Assert.Equal("HTTP 423", fault.Error);
    }

    [Fact]
    public void Delivered_status_is_the_shared_instance()
    {
        Assert.Same(DeliveryResult.Delivered.Instance, UrlMap().Endpoints[0].Classify(204));
    }

    [Fact]
    public void Status_beyond_the_cache_is_a_destination_fault()
    {
        var fault = Assert.IsType<DeliveryResult.DestinationFault>(UrlMap().Endpoints[0].Classify(1200));

        Assert.Equal("HTTP 1200", fault.Error);
    }

    [Fact]
    public void Cached_classification_allocates_nothing()
    {
        var endpoint = UrlMap().Endpoints[0];
        int[] codes = [204, 422, 500, 503];
        foreach (var code in codes)
        {
            endpoint.Classify(code);
        }

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++)
        {
            endpoint.Classify(codes[i % codes.Length]);
        }

        Assert.Equal(0, GC.GetAllocatedBytesForCurrentThread() - before);
    }

    private static HttpEndpointMap UrlMap() => HttpPlanFixture.Plan(new() { ["Url"] = $"{Https}/orders" }).Endpoints;

    private static HttpEndpointMap RoutesMap() => HttpPlanFixture.Plan(new()
    {
        ["Routes:alerts:Url"] = $"{Https}/alerts",
        ["Routes:audit:Url"] = $"{Https}/audit/{{id}}",
    }).Endpoints;

    private static OutgoingMessage Send(string? route, DeadLetterInfo? deadLetter = null, string outbox = "Orders") => new()
    {
        Source = new MessageSource(outbox, 1, "Sink"),
        Payload = [],
        Headers = MessageHeaders.Empty,
        Route = route,
        DeadLetter = deadLetter,
    };

    private static long ResolveBytes(HttpEndpointMap map, OutgoingMessage[] messages)
    {
        const int Calls = 10_000;
        var hits = 0;
        for (var i = 0; i < messages.Length; i++)
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
}
