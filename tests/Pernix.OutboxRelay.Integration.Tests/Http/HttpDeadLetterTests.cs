using System.Globalization;
using System.Text;
using Pernix.OutboxRelay.Integration.Tests.Kafka;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.Http;

public sealed class HttpDeadLetterTests(PostgreSqlFixture postgres, KafkaFixture kafka)
{
    private const string MainPath = "/orders";
    private const string DeadLetterPath = "/dead-letters";

    private static readonly TimeSpan DeliveryTimeout = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan AbsenceWait = TimeSpan.FromSeconds(3);

    private static readonly HttpRow Row = new(
        "order-7",
        "{\"id\":7}",
        new Dictionary<string, string> { ["idempotency-key"] = "idem-7", ["type"] = "Ordér" });

    [Fact]
    public async Task Rejected_422_on_http_is_dead_lettered_to_http_with_relay_headers()
    {
        await using var endpoint = await HttpTestEndpoint.StartAsync();
        endpoint.Respond = r => r.Path == MainPath ? new EndpointResponse(422) : EndpointResponse.Ok;
        var setup = await HttpOutbox.CreateAsync(postgres, url: endpoint.Url(MainPath));
        setup.DeadLetterTo("Http", new Dictionary<string, string?> { ["Url"] = endpoint.Url(DeadLetterPath) });

        var startedAt = DateTimeOffset.UtcNow;
        var id = Assert.Single(await setup.InsertAsync(Row));
        await using var relay = setup.Relay();
        _ = relay.Services;
        Assert.True(await setup.DrainedAsync(DeliveryTimeout), "row not dead-lettered");
        await Task.Delay(AbsenceWait, TestContext.Current.CancellationToken);

        Assert.Single(endpoint.RequestsTo(MainPath));
        var dead = Assert.Single(endpoint.RequestsTo(DeadLetterPath));
        Assert.Equal(Row.PayloadBytes, dead.Body);
        HttpDeliveryTests.AssertHeaders(dead, new Dictionary<string, string>(DeadLetterHeaders(setup, id, dead.Header("x-failed-at")!))
        {
            ["Content-Type"] = "application/octet-stream",
            ["idempotency-key"] = "idem-7",
            ["type"] = "Ordér",
        });
        AssertFailedAt(dead.Header("x-failed-at")!, startedAt);
        Assert.Equal("HTTP 422", dead.Header("x-dead-letter-reason"));
    }

    [Fact]
    public async Task Rejected_422_on_http_is_dead_lettered_to_kafka_with_relay_headers()
    {
        await using var endpoint = await HttpTestEndpoint.StartAsync();
        endpoint.Respond = _ => new EndpointResponse(422);
        var topic = KafkaFixture.NewTopic();
        await kafka.CreateTopicAsync(topic, 1);
        var setup = await HttpOutbox.CreateAsync(postgres, url: endpoint.Url(MainPath));
        setup.Settings["KafkaClusters:Main:BootstrapServers"] = kafka.BootstrapServers;
        setup.DeadLetterTo("Kafka", new Dictionary<string, string?> { ["Cluster"] = "Main", ["Topic"] = topic });

        var startedAt = DateTimeOffset.UtcNow;
        var id = Assert.Single(await setup.InsertAsync(Row));
        await using var relay = setup.Relay();
        _ = relay.Services;
        Assert.True(await setup.DrainedAsync(DeliveryTimeout), "row not dead-lettered");

        Assert.Single(endpoint.RequestsTo(MainPath));
        Assert.Single(await kafka.ConsumeAllAsync(topic, 1, DeliveryTimeout));
        var record = Assert.Single(await kafka.ConsumeAllAsync(topic, 2, AbsenceWait)).Message;
        Assert.Equal(Encoding.UTF8.GetBytes(Row.Key!), record.Key);
        Assert.Equal(Row.PayloadBytes, record.Value);

        var headers = record.Headers.ToDictionary(h => h.Key, h => Encoding.UTF8.GetString(h.GetValueBytes()));
        Assert.Equal(
            new Dictionary<string, string>(DeadLetterHeaders(setup, id, headers["x-failed-at"])) { ["idempotency-key"] = "idem-7", ["type"] = "Ordér" }
                .OrderBy(h => h.Key, StringComparer.Ordinal),
            headers.OrderBy(h => h.Key, StringComparer.Ordinal));
        AssertFailedAt(headers["x-failed-at"], startedAt);
        Assert.Equal("HTTP 422", headers["x-dead-letter-reason"]);
    }

    [Fact]
    public async Task Oversized_kafka_row_is_dead_lettered_to_http_with_relay_headers()
    {
        await using var endpoint = await HttpTestEndpoint.StartAsync();
        var topic = KafkaFixture.NewTopic();
        await kafka.CreateTopicAsync(topic, 1, new Dictionary<string, string> { ["max.message.bytes"] = "1024" });
        var setup = await HttpOutbox.CreateAsync(postgres);
        setup.Settings["Destinations:0:Transport"] = "Kafka";
        setup.Settings["Destinations:0:Kafka:Cluster"] = "Main";
        setup.Settings["Destinations:0:Kafka:Topic"] = topic;
        setup.Settings["KafkaClusters:Main:BootstrapServers"] = kafka.BootstrapServers;
        setup.DeadLetterTo("Http", new Dictionary<string, string?> { ["Url"] = endpoint.Url(DeadLetterPath) });

        var big = Row with { Payload = new string('x', 2048) };
        var startedAt = DateTimeOffset.UtcNow;
        var id = Assert.Single(await setup.InsertAsync(big));
        await using var relay = setup.Relay();
        _ = relay.Services;
        Assert.True(await setup.DrainedAsync(DeliveryTimeout), "row not dead-lettered");
        await Task.Delay(AbsenceWait, TestContext.Current.CancellationToken);

        Assert.Empty(await kafka.ConsumeAllAsync(topic, 1, AbsenceWait));
        var dead = Assert.Single(endpoint.Requests);
        Assert.Equal(DeadLetterPath, dead.Target);
        Assert.Equal(big.PayloadBytes, dead.Body);
        var reason = dead.Header("x-dead-letter-reason");
        Assert.False(string.IsNullOrWhiteSpace(reason));
        HttpDeliveryTests.AssertHeaders(dead, new Dictionary<string, string>(DeadLetterHeaders(setup, id, dead.Header("x-failed-at")!, reason!))
        {
            ["Content-Type"] = "application/octet-stream",
            ["idempotency-key"] = "idem-7",
            ["type"] = "Ordér",
        });
        AssertFailedAt(dead.Header("x-failed-at")!, startedAt);
    }

    private static Dictionary<string, string> DeadLetterHeaders(HttpOutbox setup, long id, string failedAt, string reason = "HTTP 422") => new()
    {
        ["x-dead-letter-reason"] = reason,
        ["x-retry-attempts"] = "1",
        ["x-original-destination"] = HttpOutbox.DestinationName,
        ["x-original-outbox"] = setup.Outbox,
        ["x-original-id"] = id.ToString(CultureInfo.InvariantCulture),
        ["x-failed-at"] = failedAt,
    };

    private static void AssertFailedAt(string value, DateTimeOffset startedAt)
    {
        var failedAt = DateTimeOffset.ParseExact(value, "yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal);
        Assert.InRange(failedAt, startedAt.AddSeconds(-1), DateTimeOffset.UtcNow.AddSeconds(1));
    }
}
