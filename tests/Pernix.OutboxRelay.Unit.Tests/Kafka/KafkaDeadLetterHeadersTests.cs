using System.Text;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Destination.Kafka;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Kafka;

public sealed class KafkaDeadLetterHeadersTests
{
    private static readonly DeadLetterInfo Info = new("MsgSizeTooLarge: too big", 12345, new DateTimeOffset(2026, 11, 3, 23, 59, 58, 7, TimeSpan.FromHours(-5)));

    [Fact]
    public void Merge_keeps_original_headers_and_adds_six_x_headers()
    {
        var merged = KafkaDeadLetterHeaders.Merge(Message(new Dictionary<string, string> { ["traceparent"] = "00-abc", ["type"] = "Order" }));

        Assert.Equal(
            new Dictionary<string, string>
            {
                ["traceparent"] = "00-abc",
                ["type"] = "Order",
                ["x-dead-letter-reason"] = "MsgSizeTooLarge: too big",
                ["x-retry-attempts"] = "12345",
                ["x-original-destination"] = "OrdersDestination",
                ["x-original-outbox"] = "Orders",
                ["x-original-id"] = "9876543210",
                ["x-failed-at"] = "2026-11-04T04:59:58.007Z",
            },
            merged.ToStrings());
    }

    [Fact]
    public void Merge_replaces_spoofed_x_headers()
    {
        var original = new Dictionary<string, string> { ["type"] = "Order" };
        foreach (var name in KafkaDeadLetterHeaders.Names)
        {
            original[name] = "spoofed";
        }

        var merged = KafkaDeadLetterHeaders.Merge(Message(original)).ToStrings();

        Assert.Equal(7, merged.Count);
        Assert.Equal("Order", merged["type"]);
        Assert.Equal("9876543210", merged[KafkaDeadLetterHeaders.OriginalId]);
        Assert.DoesNotContain("spoofed", merged.Values);
    }

    [Fact]
    public void Merge_replaces_spoofed_x_headers_in_any_case()
    {
        var original = new Dictionary<string, string> { ["type"] = "Order" };
        foreach (var name in KafkaDeadLetterHeaders.Names)
        {
            original[name.ToUpperInvariant()] = "spoofed";
        }

        original["X-Original-Id"] = "spoofed";

        var merged = KafkaDeadLetterHeaders.Merge(Message(original)).ToStrings();

        Assert.Equal(7, merged.Count);
        Assert.Equal("9876543210", merged[KafkaDeadLetterHeaders.OriginalId]);
        Assert.DoesNotContain("spoofed", merged.Values);
    }

    [Fact]
    public void Merge_with_empty_original_headers_has_only_x_headers()
    {
        var merged = KafkaDeadLetterHeaders.Merge(Message(new Dictionary<string, string>()));

        Assert.Equal(
            KafkaDeadLetterHeaders.Names.Where(n => n != KafkaDeadLetterHeaders.OriginalRoute).Order(StringComparer.Ordinal),
            merged.Select(h => h.Name).Order(StringComparer.Ordinal));
    }

    [Fact]
    public void Header_names_match_design()
    {
        Assert.Equal(
            ["x-dead-letter-reason", "x-failed-at", "x-original-destination", "x-original-id", "x-original-outbox", "x-original-route", "x-retry-attempts"],
            KafkaDeadLetterHeaders.Names.Order(StringComparer.Ordinal));
    }

    [Fact]
    public void Merge_with_route_adds_original_route_header_last()
    {
        var merged = KafkaDeadLetterHeaders.Merge(Message(new Dictionary<string, string> { ["type"] = "Order" }, "orders.eu-ü"));

        var last = merged[merged.Count - 1];
        Assert.Equal(KafkaDeadLetterHeaders.OriginalRoute, last.Name);
        Assert.Equal(Encoding.UTF8.GetBytes("orders.eu-ü"), last.Value);
        Assert.Equal(KafkaDeadLetterHeaders.FailedAt, merged[merged.Count - 2].Name);
    }

    [Fact]
    public void Merge_without_route_has_no_original_route_header()
    {
        var merged = KafkaDeadLetterHeaders.Merge(Message(new Dictionary<string, string> { ["type"] = "Order" }));

        Assert.DoesNotContain(merged, h => string.Equals(h.Name, KafkaDeadLetterHeaders.OriginalRoute, StringComparison.OrdinalIgnoreCase));
        Assert.Equal(KafkaDeadLetterHeaders.FailedAt, merged[merged.Count - 1].Name);
    }

    [Theory]
    [InlineData("x-original-route", "orders")]
    [InlineData("X-Original-Route", "orders")]
    [InlineData("x-original-route", null)]
    [InlineData("X-Original-Route", null)]
    public void Merge_drops_producer_original_route_header(string name, string? route)
    {
        var merged = KafkaDeadLetterHeaders.Merge(Message(new Dictionary<string, string> { ["type"] = "Order", [name] = "spoofed" }, route));

        var routeHeaders = merged.Where(h => string.Equals(h.Name, KafkaDeadLetterHeaders.OriginalRoute, StringComparison.OrdinalIgnoreCase)).ToList();
        Assert.DoesNotContain("spoofed", merged.ToStrings().Values);
        if (route is null)
        {
            Assert.Empty(routeHeaders);
        }
        else
        {
            var header = Assert.Single(routeHeaders);
            Assert.Equal(KafkaDeadLetterHeaders.OriginalRoute, header.Name);
            Assert.Equal(Encoding.UTF8.GetBytes(route), header.Value);
        }
    }

    [Fact]
    public void Merge_reuses_cached_outbox_and_destination_bytes_for_the_same_names()
    {
        var first = KafkaDeadLetterHeaders.Merge(Message(new Dictionary<string, string>()));
        var second = KafkaDeadLetterHeaders.Merge(Message(new Dictionary<string, string> { ["type"] = "Order" }, "orders"));

        Assert.Same(Value(first, KafkaDeadLetterHeaders.OriginalOutbox), Value(second, KafkaDeadLetterHeaders.OriginalOutbox));
        Assert.Same(Value(first, KafkaDeadLetterHeaders.OriginalDestination), Value(second, KafkaDeadLetterHeaders.OriginalDestination));
    }

    [Fact]
    public void Merge_uses_different_cached_bytes_for_different_names()
    {
        var first = KafkaDeadLetterHeaders.Merge(Message(new Dictionary<string, string>()));
        var other = KafkaDeadLetterHeaders.Merge(Message(new Dictionary<string, string>()) with { Source = new MessageSource("Payments", 1, "PaymentsDestination") });

        Assert.NotSame(Value(first, KafkaDeadLetterHeaders.OriginalOutbox), Value(other, KafkaDeadLetterHeaders.OriginalOutbox));
        Assert.NotSame(Value(first, KafkaDeadLetterHeaders.OriginalDestination), Value(other, KafkaDeadLetterHeaders.OriginalDestination));
        Assert.Equal(Encoding.UTF8.GetBytes("Payments"), Value(other, KafkaDeadLetterHeaders.OriginalOutbox));
        Assert.Equal(Encoding.UTF8.GetBytes("PaymentsDestination"), Value(other, KafkaDeadLetterHeaders.OriginalDestination));
    }

    [Fact]
    public void Merge_never_caches_message_content()
    {
        var reason = "reason-" + Guid.NewGuid();
        var route = "route-" + Guid.NewGuid();
        var outbox = "outbox-" + Guid.NewGuid();
        var destination = "destination-" + Guid.NewGuid();

        KafkaDeadLetterHeaders.Merge(Message(new Dictionary<string, string>(), route) with
        {
            Source = new MessageSource(outbox, 1, destination),
            DeadLetter = Info with { Reason = reason },
        });

        Assert.True(KafkaDeadLetterHeaders.IsCached(outbox));
        Assert.True(KafkaDeadLetterHeaders.IsCached(destination));
        Assert.False(KafkaDeadLetterHeaders.IsCached(reason));
        Assert.False(KafkaDeadLetterHeaders.IsCached(route));
    }

    private static byte[] Value(MessageHeaders headers, string name) => headers.Single(h => h.Name == name).Value;

    private static OutgoingMessage Message(IReadOnlyDictionary<string, string> headers, string? route = null) => new()
    {
        Route = route,
        Source = new MessageSource("Orders", 9_876_543_210, "OrdersDestination"),
        Key = [1],
        Payload = [2],
        Headers = TestHeaders.Of(headers),
        DeadLetter = Info,
    };
}
