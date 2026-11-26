using System.Text;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Destination.Http;
using Pernix.OutboxRelay.Destination.Kafka;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Http;

public sealed class HttpDeadLetterHeadersTests
{
    private static readonly DeadLetterInfo Info = new("MsgSizeTooLarge: too big", 12345, new DateTimeOffset(2026, 11, 3, 23, 59, 58, 7, TimeSpan.FromHours(-5)));

    [Theory]
    [InlineData(null, 12345)]
    [InlineData("orders.eu-ü", 3)]
    [InlineData("orders", 0)]
    [InlineData(null, 100)]
    [InlineData(null, 101)]
    public void Values_equal_kafka_merge_for_the_same_message(string? route, int attempts)
    {
        var message = Message(route) with { DeadLetter = Info with { Attempts = attempts } };
        var request = new HttpRequestMessage();

        HttpDeadLetterHeaders.Apply(message, request.Headers);

        var kafka = KafkaDeadLetterHeaders.Merge(message)
            .Where(h => DeadLetterHeaderNames.All.Contains(h.Name))
            .ToDictionary(h => h.Name, h => Encoding.UTF8.GetString(h.Value));
        Assert.Equal(kafka.OrderBy(p => p.Key, StringComparer.Ordinal), Headers(request).OrderBy(p => p.Key, StringComparer.Ordinal));
    }

    [Fact]
    public void Formats_match_design()
    {
        var request = new HttpRequestMessage();

        HttpDeadLetterHeaders.Apply(Message("orders"), request.Headers);

        Assert.Equal(
            new Dictionary<string, string>
            {
                ["x-dead-letter-reason"] = "MsgSizeTooLarge: too big",
                ["x-retry-attempts"] = "12345",
                ["x-original-destination"] = "OrdersDestination",
                ["x-original-outbox"] = "Orders",
                ["x-original-id"] = "9876543210",
                ["x-failed-at"] = "2026-11-04T04:59:58.007Z",
                ["x-original-route"] = "orders",
            },
            Headers(request));
    }

    [Theory]
    [InlineData("x-original-id")]
    [InlineData("X-Original-Id")]
    public void Spoofed_producer_header_is_replaced(string name)
    {
        var request = new HttpRequestMessage();
        foreach (var dlq in DeadLetterHeaderNames.All)
        {
            request.Headers.TryAddWithoutValidation(dlq, "spoofed");
        }

        request.Headers.Remove(name);
        request.Headers.TryAddWithoutValidation(name, "spoofed");
        request.Headers.TryAddWithoutValidation("traceparent", "00-abc");

        HttpDeadLetterHeaders.Apply(Message(null), request.Headers);

        Assert.Equal("9876543210", Assert.Single(request.Headers.GetValues(DeadLetterHeaderNames.OriginalId)));
        Assert.Equal("00-abc", Assert.Single(request.Headers.GetValues("traceparent")));
        Assert.DoesNotContain(request.Headers, h => h.Value.Contains("spoofed"));
        Assert.False(request.Headers.Contains(DeadLetterHeaderNames.OriginalRoute));
    }

    [Fact]
    public void Original_route_only_when_a_route_exists()
    {
        var without = new HttpRequestMessage();
        var with = new HttpRequestMessage();

        HttpDeadLetterHeaders.Apply(Message(null), without.Headers);
        HttpDeadLetterHeaders.Apply(Message("orders"), with.Headers);

        Assert.False(without.Headers.Contains(DeadLetterHeaderNames.OriginalRoute));
        Assert.Equal(6, Headers(without).Count);
        Assert.Equal("orders", Assert.Single(with.Headers.GetValues(DeadLetterHeaderNames.OriginalRoute)));
    }

    [Fact]
    public void Cached_strings_are_reused()
    {
        var message = Message("orders");
        var request = new HttpRequestMessage();

        HttpDeadLetterHeaders.Apply(message, request.Headers);

        Assert.Same(HttpDeadLetterHeaders.AttemptsString(42), HttpDeadLetterHeaders.AttemptsString(42));
        Assert.Same(HttpDeadLetterHeaders.AttemptsString(0), HttpDeadLetterHeaders.AttemptsString(0));
        Assert.Same(message.Source.Outbox, Raw(request, DeadLetterHeaderNames.OriginalOutbox));
        Assert.Same(message.Source.Destination, Raw(request, DeadLetterHeaderNames.OriginalDestination));
        Assert.Same(message.DeadLetter!.Reason, Raw(request, DeadLetterHeaderNames.Reason));
        Assert.Same(message.Route, Raw(request, DeadLetterHeaderNames.OriginalRoute));
    }

    [Fact]
    public void Allocates_at_most_two_strings_per_send_after_warm_up()
    {
        var message = Message("orders") with { DeadLetter = Info with { Attempts = 7 } };
        var request = new HttpRequestMessage();
        const int Sends = 100;
        for (var i = 0; i < 3; i++)
        {
            request.Headers.Clear();
            HttpDeadLetterHeaders.Apply(message, request.Headers);
        }

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < Sends; i++)
        {
            request.Headers.Clear();
            HttpDeadLetterHeaders.Apply(message, request.Headers);
        }

        var perSend = (GC.GetAllocatedBytesForCurrentThread() - before) / Sends;
        Assert.Equal(StringBytes("9876543210".Length) + StringBytes("2026-11-04T04:59:58.007Z".Length), perSend);
    }

    private static object? Raw(HttpRequestMessage request, string name) =>
        request.Headers.NonValidated[name] is var values && values.Count == 1 ? values.ToString() : null;

    private static Dictionary<string, string> Headers(HttpRequestMessage request) =>
        request.Headers.NonValidated.ToDictionary(h => h.Key, h => h.Value.ToString());

    private static long StringBytes(int length)
    {
        var before = GC.GetAllocatedBytesForCurrentThread();
        GC.KeepAlive(new string('x', length));
        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    private static OutgoingMessage Message(string? route) => new()
    {
        Route = route,
        Source = new MessageSource("Orders", 9_876_543_210, "OrdersDestination"),
        Key = [1],
        Payload = [2],
        Headers = TestHeaders.Of(new Dictionary<string, string>()),
        DeadLetter = Info,
    };
}
