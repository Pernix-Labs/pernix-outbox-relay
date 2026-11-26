using System.Globalization;
using Pernix.OutboxRelay.Integration.Tests.Leadership;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.Http;

public sealed class HttpDeliveryTests(PostgreSqlFixture postgres)
{
    private const int Keys = 3;
    private const int Rows = 30;

    private static readonly TimeSpan DeliveryTimeout = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan AbsenceWait = TimeSpan.FromSeconds(3);

    private static readonly HashSet<string> Framing = new(["Host", "Content-Length"], StringComparer.OrdinalIgnoreCase);

    [Theory]
    [InlineData("None")]
    [InlineData("PerKey")]
    [InlineData("Strict")]
    public async Task Url_destination_delivers_every_row_with_exact_body_and_headers_in_ordering_mode(string mode)
    {
        await using var endpoint = await HttpTestEndpoint.StartAsync();
        var setup = await HttpOutbox.CreateAsync(postgres, mode, endpoint.Url("/{outbox}/items/{key}?id={id}"));
        setup.Http("Headers:Idempotency-Key", "{outbox}-{id}");
        setup.Http("Headers:X-Static", "fixed");

        var rows = Enumerable.Range(1, Rows)
            .Select(i => new HttpRow(
                $"k{i % Keys}",
                $"{{\"n\":{i},\"text\":\"Ordér {i}\"}}",
                new Dictionary<string, string> { ["tenant-id"] = $"t-{i}", ["type"] = "Ordér" }))
            .ToArray();
        var ids = await setup.InsertAsync(rows);

        await using var relay = setup.Relay();
        _ = relay.Services;
        Assert.True(await setup.DrainedAsync(DeliveryTimeout), "table not empty");
        await Task.Delay(AbsenceWait, TestContext.Current.CancellationToken);

        var requests = endpoint.Requests;
        Assert.Equal(Rows, requests.Count);
        var byId = requests.ToDictionary(r => long.Parse(r.Header("Idempotency-Key")![(setup.Outbox.Length + 1)..], CultureInfo.InvariantCulture));
        Assert.Equal(ids.Order(), byId.Keys.Order());

        for (var i = 0; i < Rows; i++)
        {
            var request = byId[ids[i]];
            var row = rows[i];
            Assert.Equal("POST", request.Method);
            Assert.Equal("HTTP/1.1", request.Protocol);
            Assert.Equal($"/{setup.Outbox}/items/{row.Key}?id={ids[i]}", request.Target);
            Assert.Equal(row.PayloadBytes, request.Body);
            AssertHeaders(
                request,
                new Dictionary<string, string>
                {
                    ["Content-Type"] = "application/octet-stream",
                    ["Idempotency-Key"] = $"{setup.Outbox}-{ids[i]}",
                    ["X-Static"] = "fixed",
                    ["tenant-id"] = row.Headers!["tenant-id"],
                    ["type"] = "Ordér",
                });
        }

        var arrival = requests.Select(r => long.Parse(r.Header("Idempotency-Key")![(setup.Outbox.Length + 1)..], CultureInfo.InvariantCulture)).ToList();
        switch (mode)
        {
            case "Strict":
                Assert.Equal(ids, arrival);
                break;
            case "PerKey":
                var keyOf = ids.Select((id, i) => (id, rows[i].Key)).ToDictionary(x => x.id, x => x.Key);
                Assert.All(arrival.GroupBy(id => keyOf[id]), key => Assert.Equal(key.Order(), key));
                break;
        }
    }

    [Fact]
    public async Task Routes_destination_delivers_mapped_routes_with_per_route_settings_and_holds_null_and_unmapped_routes()
    {
        await using var endpoint = await HttpTestEndpoint.StartAsync();
        var setup = await HttpOutbox.CreateAsync(postgres);
        setup.Settings["Outboxes:0:Retry:InitialDelay"] = "00:01:00";
        setup.Settings["Outboxes:0:Retry:MaxDelay"] = "00:01:00";
        setup.Http("Headers:X-Common", "common");
        setup.Http("Routes:alerts:Url", endpoint.Url("/alerts/{route}/{id}"));
        setup.Http("Routes:audit:Url", endpoint.Url("/audit"));
        setup.Http("Routes:audit:Method", "PUT");
        setup.Http("Routes:audit:ContentType", "application/json");
        setup.Http("Routes:audit:Headers:X-Common", "audit-only");

        var ids = await setup.InsertAsync(
            new HttpRow("n", "none", Route: null),
            new HttpRow("a", "{\"alert\":1}", new Dictionary<string, string> { ["source"] = "a" }, "alerts"),
            new HttpRow("b", "{\"audit\":1}", new Dictionary<string, string> { ["source"] = "b" }, "audit"),
            new HttpRow("u", "unmapped", Route: "billing"));

        await using var relay = setup.Relay();
        _ = relay.Services;

        Assert.True(
            await LeadershipTests.EventuallyAsync(
                async () => await setup.FailureStatesAsync() is [var first, var second]
                    && first.Id == ids[0] && first.LastError == "route not mapped: (none)"
                    && second.Id == ids[3] && second.LastError == "route not mapped: billing",
                DeliveryTimeout),
            "mapped rows not delivered or null/unmapped rows not held");
        await Task.Delay(AbsenceWait, TestContext.Current.CancellationToken);

        Assert.Equal(2, (await setup.FailureStatesAsync()).Count);
        Assert.Equal(2, endpoint.Requests.Count);

        var alert = Assert.Single(endpoint.RequestsTo($"/alerts/alerts/{ids[1]}"));
        Assert.Equal("POST", alert.Method);
        Assert.Equal("{\"alert\":1}"u8.ToArray(), alert.Body);
        AssertHeaders(alert, new Dictionary<string, string>
        {
            ["Content-Type"] = "application/octet-stream",
            ["X-Common"] = "common",
            ["source"] = "a",
        });

        var audit = Assert.Single(endpoint.RequestsTo("/audit"));
        Assert.Equal("PUT", audit.Method);
        Assert.Equal("{\"audit\":1}"u8.ToArray(), audit.Body);
        AssertHeaders(audit, new Dictionary<string, string>
        {
            ["Content-Type"] = "application/json",
            ["X-Common"] = "audit-only",
            ["source"] = "b",
        });
    }

    [Fact]
    public async Task CloudEvents_row_headers_with_minimal_config_give_a_binary_mode_request()
    {
        await using var endpoint = await HttpTestEndpoint.StartAsync();
        var setup = await HttpOutbox.CreateAsync(postgres, url: endpoint.Url("/events"));
        var payload = "{\"orderId\":42}";
        var id = Assert.Single(await setup.InsertAsync(new HttpRow(
            "order-42",
            payload,
            new Dictionary<string, string>
            {
                ["ce-id"] = "evt-42",
                ["ce-type"] = "com.example.order.created",
                ["ce-source"] = "/orders",
                ["ce-specversion"] = "1.0",
                ["content-type"] = "application/json",
            })));

        await using var relay = setup.Relay();
        _ = relay.Services;
        Assert.True(await setup.DrainedAsync(DeliveryTimeout), $"row {id} not delivered");

        var request = Assert.Single(endpoint.Requests);
        Assert.Equal("POST", request.Method);
        Assert.Equal(System.Text.Encoding.UTF8.GetBytes(payload), request.Body);

        AssertHeaders(request, new Dictionary<string, string>
        {
            ["ce-id"] = "evt-42",
            ["ce-type"] = "com.example.order.created",
            ["ce-source"] = "/orders",
            ["ce-specversion"] = "1.0",
            ["Content-Type"] = "application/json",
        });
    }

    [Fact]
    public async Task Status_503_pauses_the_outbox_then_resumes_and_delivers()
    {
        await using var endpoint = await HttpTestEndpoint.StartAsync();
        endpoint.Respond = r => r.Sequence <= 2 ? new EndpointResponse(503) : EndpointResponse.Ok;
        var setup = await HttpOutbox.CreateAsync(postgres, url: endpoint.Url("/orders"));
        var row = new HttpRow("k", "{\"id\":1}");
        Assert.Single(await setup.InsertAsync(row));

        await using var relay = setup.Relay();
        _ = relay.Services;
        Assert.True(await setup.DrainedAsync(DeliveryTimeout), "row not delivered after 503");

        Assert.Equal(["HTTP 503", "HTTP 503"], setup.Logs.PauseErrors);
        Assert.Contains(setup.Logs.Entries, e => e.EventId == RelayLogCapture.DeliveryResumed);
        Assert.Equal(3, endpoint.Requests.Count);
        Assert.All(endpoint.Requests, r => Assert.Equal(row.PayloadBytes, r.Body));
    }

    [Fact]
    public async Task Send_timeout_pauses_the_outbox_then_resumes_and_delivers()
    {
        await using var endpoint = await HttpTestEndpoint.StartAsync();
        endpoint.Respond = r => r.Sequence == 1 ? new EndpointResponse(200, TimeSpan.FromSeconds(10)) : EndpointResponse.Ok;
        var setup = await HttpOutbox.CreateAsync(postgres, url: endpoint.Url("/orders"));
        setup.Http("SendTimeout", "00:00:01");
        setup.Http("ConnectTimeout", "00:00:01");
        var row = new HttpRow("k", "{\"id\":2}");
        Assert.Single(await setup.InsertAsync(row));

        await using var relay = setup.Relay();
        _ = relay.Services;
        Assert.True(await setup.DrainedAsync(DeliveryTimeout), "row not delivered after timeout");

        Assert.Equal(["timeout after 00:00:01"], setup.Logs.PauseErrors);
        Assert.Contains(setup.Logs.Entries, e => e.EventId == RelayLogCapture.DeliveryResumed);
        Assert.Equal(2, endpoint.Requests.Count);
        Assert.All(endpoint.Requests, r => Assert.Equal(row.PayloadBytes, r.Body));
    }

    [Fact]
    public async Task Connections_equal_peak_concurrency_and_do_not_grow_after_warm_up_with_the_response_body_unread()
    {
        const int maxRequestsInFlight = 4;
        const int wave = 200;
        await using var endpoint = await HttpTestEndpoint.StartAsync();
        endpoint.Respond = _ => new EndpointResponse(200, TimeSpan.FromMilliseconds(20));
        var setup = await HttpOutbox.CreateAsync(postgres, url: endpoint.Url("/orders"));
        setup.Http("MaxRequestsInFlight", maxRequestsInFlight.ToString(CultureInfo.InvariantCulture));

        await setup.InsertAsync([.. Enumerable.Range(1, wave).Select(i => new HttpRow($"k{i}", $"{{\"n\":{i}}}"))]);
        await using var relay = setup.Relay();
        _ = relay.Services;
        Assert.True(await setup.DrainedAsync(DeliveryTimeout), "first wave not delivered");

        var warm = endpoint.Connects;
        Assert.Equal(maxRequestsInFlight, endpoint.PeakInFlight);
        Assert.Equal(endpoint.PeakInFlight, warm);

        await setup.InsertAsync([.. Enumerable.Range(wave + 1, wave).Select(i => new HttpRow($"k{i}", $"{{\"n\":{i}}}"))]);
        Assert.True(await setup.DrainedAsync(DeliveryTimeout), "second wave not delivered");

        Assert.Equal(2 * wave, endpoint.Requests.Count);
        Assert.Equal(warm, endpoint.Connects);
        Assert.Equal(warm, endpoint.Requests.Select(r => r.ConnectionId).Distinct().Count());
    }

    internal static void AssertHeaders(RecordedRequest request, IReadOnlyDictionary<string, string> expected)
    {
        var actual = request.Headers.Where(h => !Framing.Contains(h.Key)).ToList();
        Assert.Equal(
            expected.Select(h => $"{h.Key.ToLowerInvariant()}: {h.Value}").Order(StringComparer.Ordinal),
            actual.Select(h => $"{h.Key.ToLowerInvariant()}: {h.Value}").Order(StringComparer.Ordinal));
        Assert.Equal(request.Body.Length.ToString(CultureInfo.InvariantCulture), request.Header("Content-Length"));
    }
}
