using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Text;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Destination.Http;
using Pernix.OutboxRelay.Unit.Tests.Leadership;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Http;

internal sealed class HttpDestinationFixture
{
    public const string Https = HttpPlanFixture.Https;

    public RecordingLogger<HttpDestination> Logger { get; } = new();

    public FakeTimeProvider Time { get; } = new(new DateTimeOffset(2026, 11, 26, 12, 0, 0, TimeSpan.Zero));

    public List<StubHandler> Handlers { get; } = [];

    // Keys are under Destinations:0:Http unless prefixed '!' (relay-section keys). Every handler built answers with respond.
    public HttpDestinationFactory Factory(Dictionary<string, string?> settings, Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> respond)
    {
        var all = new Dictionary<string, string?>
        {
            ["Destinations:0:Name"] = "Sink",
            ["Destinations:0:Transport"] = "Http",
            ["Outboxes:0:Name"] = "Orders",
            ["Outboxes:0:Destination"] = "Sink",
        };
        foreach (var (key, value) in settings.Where(static s => s.Value is not null))
        {
            all[key.StartsWith('!') ? key[1..] : $"Destinations:0:Http:{key}"] = value;
        }

        var config = new ConfigurationBuilder().AddInMemoryCollection(all).Build();
        var factory = new HttpDestinationFactory(
            new SingleLoggerFactory(Logger),
            new HttpCredentialSection(new RelayConfiguration(config)),
            HttpHookLookup.None,
            _ =>
            {
                var handler = new StubHandler(respond);
                Handlers.Add(handler);
                return handler;
            },
            Time);
        var relay = RelayOptionsBinder.Bind(config);
        Destination = Assert.Single(relay.Destinations);
        Assert.Empty(factory.Validate(Destination, HttpPlanFixture.Path, relay));
        return factory;
    }

    public DestinationOptions Destination { get; private set; } = null!;

    public HttpDestination Create(
        Dictionary<string, string?> settings,
        Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> respond,
        DestinationContext? context = null) =>
        (HttpDestination)Factory(settings, respond).Create(Destination, context ?? new DestinationContext(OrderingMode.None));

    public HttpDestination Create(Dictionary<string, string?> settings, HttpStatusCode status = HttpStatusCode.OK) =>
        Create(settings, (_, _) => Task.FromResult(new HttpResponseMessage(status)));

    public static Dictionary<string, string?> Url(string path = "/orders") => new() { ["Url"] = Https + path };

    public static OutgoingMessage Message(
        byte[]? payload = null,
        byte[]? key = null,
        MessageHeaders? headers = null,
        string? route = null,
        DeadLetterInfo? deadLetter = null,
        long id = 42) => new()
    {
        Source = new MessageSource("Orders", id, "Sink"),
        Key = key,
        Payload = payload ?? "{\"total\":10}"u8.ToArray(),
        Headers = headers ?? MessageHeaders.Empty,
        Route = route,
        DeadLetter = deadLetter,
    };

    public static MessageHeaders Headers(params (string Name, string Value)[] headers) =>
        TestHeaders.Of(headers.Select(static h => KeyValuePair.Create(h.Name, h.Value)));

    private sealed class SingleLoggerFactory(ILogger logger) : ILoggerFactory
    {
        public ILogger CreateLogger(string categoryName) => logger;

        public void AddProvider(ILoggerProvider provider)
        {
        }

        public void Dispose()
        {
        }
    }
}

internal sealed record SeenRequest(
    HttpMethod Method,
    string Uri,
    Version Version,
    Dictionary<string, string> Headers,
    Dictionary<string, string> ContentHeaders,
    long? ContentLength,
    byte[] Body);

internal sealed class StubHandler(Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> respond) : HttpMessageHandler
{
    private readonly List<SeenRequest> requests = [];

    public bool Disposed { get; private set; }

    public IReadOnlyList<SeenRequest> Requests
    {
        get
        {
            lock (requests)
            {
                return [.. requests];
            }
        }
    }

    protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        var body = request.Content is null ? [] : await request.Content.ReadAsByteArrayAsync(cancellationToken);
        lock (requests)
        {
            requests.Add(new SeenRequest(
                request.Method,
                request.RequestUri!.OriginalString,
                request.Version,
                request.Headers.NonValidated.ToDictionary(h => h.Key, h => string.Join(", ", h.Value), StringComparer.OrdinalIgnoreCase),
                request.Content?.Headers.NonValidated.ToDictionary(h => h.Key, h => string.Join(", ", h.Value), StringComparer.OrdinalIgnoreCase) ?? [],
                request.Content?.Headers.ContentLength,
                body));
        }

        return await respond(request, cancellationToken);
    }

    protected override void Dispose(bool disposing)
    {
        Disposed = true;
        base.Dispose(disposing);
    }
}

public sealed class HttpDestinationSendTests
{
    private const string Https = HttpDestinationFixture.Https;

    private readonly HttpDestinationFixture fixture = new();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Static_url_sends_the_payload_as_is_and_returns_delivered()
    {
        await using var destination = fixture.Create(HttpDestinationFixture.Url());
        var payload = "{\"total\":10}"u8.ToArray();

        var result = await destination.SendAsync(HttpDestinationFixture.Message(payload), Ct);

        Assert.Same(DeliveryResult.Delivered.Instance, result);
        var request = Assert.Single(fixture.Handlers.Single().Requests);
        Assert.Equal(HttpMethod.Post, request.Method);
        Assert.Equal($"{Https}/orders", request.Uri);
        Assert.Equal(HttpVersion.Version11, request.Version);
        Assert.Equal(payload, request.Body);
        Assert.Equal(payload.Length, request.ContentLength);
    }

    [Fact]
    public async Task Send_bound_is_the_send_timeout_and_routes_come_from_the_plan()
    {
        await using var single = fixture.Create(new() { ["Url"] = $"{Https}/orders", ["SendTimeout"] = "00:00:12" });
        Assert.Equal(TimeSpan.FromSeconds(12), single.SendBound);
        Assert.Null(single.Routes);

        await using var routed = fixture.Create(new()
        {
            ["Routes:alerts:Url"] = $"{Https}/alerts",
            ["Routes:audit:Url"] = $"{Https}/audit",
        });
        Assert.Equal(["alerts", "audit"], routed.Routes!.Order(StringComparer.Ordinal));
        Assert.Equal(HttpDestinationPlan.DefaultSendTimeout, routed.SendBound);
    }

    [Theory]
    [InlineData("application/cloudevents+json", "application/json", "application/cloudevents+json")]
    [InlineData(null, "application/json", "application/json")]
    [InlineData(null, null, "application/octet-stream")]
    public async Task Content_type_prefers_row_header_then_setting_then_octet_stream(string? row, string? setting, string expected)
    {
        var settings = HttpDestinationFixture.Url();
        settings["ContentType"] = setting;
        await using var destination = fixture.Create(settings);
        var headers = row is null ? MessageHeaders.Empty : HttpDestinationFixture.Headers(("content-type", row));

        Assert.Same(DeliveryResult.Delivered.Instance, await destination.SendAsync(HttpDestinationFixture.Message(headers: headers), Ct));

        var request = Assert.Single(fixture.Handlers.Single().Requests);
        Assert.Equal(expected, request.ContentHeaders["Content-Type"]);
        Assert.False(request.Headers.ContainsKey("content-type"));
    }

    [Fact]
    public async Task Content_length_equals_payload_length_for_an_empty_payload()
    {
        await using var destination = fixture.Create(HttpDestinationFixture.Url());

        await destination.SendAsync(HttpDestinationFixture.Message([]), Ct);

        Assert.Equal(0, Assert.Single(fixture.Handlers.Single().Requests).ContentLength);
    }

    [Fact]
    public async Task Content_headers_from_forwarding_and_the_headers_map_land_on_content_headers()
    {
        var settings = HttpDestinationFixture.Url();
        settings["Headers:Content-Language"] = "en";
        settings["Headers:X-Source"] = "{outbox}";
        await using var destination = fixture.Create(settings);
        var headers = HttpDestinationFixture.Headers(("Content-Encoding", "gzip"), ("x-tenant", "t1"));

        await destination.SendAsync(HttpDestinationFixture.Message(headers: headers), Ct);

        var request = Assert.Single(fixture.Handlers.Single().Requests);
        Assert.Equal("gzip", request.ContentHeaders["Content-Encoding"]);
        Assert.Equal("en", request.ContentHeaders["Content-Language"]);
        Assert.Equal("t1", request.Headers["x-tenant"]);
        Assert.Equal("Orders", request.Headers["X-Source"]);
    }

    [Fact]
    public async Task Traceparent_is_forwarded_unchanged()
    {
        const string traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        await using var destination = fixture.Create(HttpDestinationFixture.Url());

        await destination.SendAsync(HttpDestinationFixture.Message(headers: HttpDestinationFixture.Headers(("traceparent", traceparent))), Ct);

        Assert.Equal(traceparent, Assert.Single(fixture.Handlers.Single().Requests).Headers["traceparent"]);
    }

    [Theory]
    [InlineData(HttpStatusCode.OK, null, null)]
    [InlineData(HttpStatusCode.NoContent, null, null)]
    [InlineData(HttpStatusCode.BadRequest, "HTTP 400", true)]
    [InlineData(HttpStatusCode.UnprocessableEntity, "HTTP 422", true)]
    [InlineData(HttpStatusCode.InternalServerError, "HTTP 500", null)]
    [InlineData(HttpStatusCode.NotFound, "HTTP 404", null)]
    public async Task Status_is_classified_by_the_table(HttpStatusCode status, string? error, bool? permanent)
    {
        await using var destination = fixture.Create(HttpDestinationFixture.Url(), status);

        var result = await destination.SendAsync(HttpDestinationFixture.Message(), Ct);

        switch (result)
        {
            case DeliveryResult.Delivered:
                Assert.Null(error);
                break;
            case DeliveryResult.MessageFault fault:
                Assert.Equal(error, fault.Error);
                Assert.Equal(permanent, fault.Permanent);
                break;
            case DeliveryResult.DestinationFault fault:
                Assert.Null(permanent);
                Assert.Equal(error, fault.Error);
                break;
        }
    }

    [Fact]
    public async Task Retried_class_returns_a_transient_message_fault_and_route_label_prefixes_the_text()
    {
        await using var destination = fixture.Create(
            new()
            {
                ["Routes:alerts:Url"] = $"{Https}/alerts",
                ["StatusCodes:Retried:0"] = "500",
            },
            HttpStatusCode.InternalServerError);

        var result = await destination.SendAsync(HttpDestinationFixture.Message(route: "alerts"), Ct);

        Assert.Equal(new DeliveryResult.MessageFault("route 'alerts': HTTP 500", Permanent: false), result);
    }

    [Fact]
    public async Task Redirect_is_not_followed_and_is_classified_by_the_table()
    {
        await using var destination = fixture.Create(HttpDestinationFixture.Url(), (_, _) =>
        {
            var response = new HttpResponseMessage(HttpStatusCode.Found);
            response.Headers.Location = new Uri($"{Https}/elsewhere");
            return Task.FromResult(response);
        });

        var result = await destination.SendAsync(HttpDestinationFixture.Message(), Ct);

        Assert.Equal(new DeliveryResult.DestinationFault("HTTP 302"), result);
        Assert.Single(fixture.Handlers.Single().Requests);
    }

    [Fact]
    public async Task Retry_after_is_ignored()
    {
        var withHeader = true;
        await using var destination = fixture.Create(HttpDestinationFixture.Url(), (_, _) =>
        {
            var response = new HttpResponseMessage(HttpStatusCode.ServiceUnavailable);
            if (withHeader)
            {
                response.Headers.RetryAfter = new System.Net.Http.Headers.RetryConditionHeaderValue(TimeSpan.FromSeconds(120));
            }

            return Task.FromResult(response);
        });

        var first = await destination.SendAsync(HttpDestinationFixture.Message(), Ct);
        withHeader = false;
        var second = await destination.SendAsync(HttpDestinationFixture.Message(), Ct);

        Assert.Equal(new DeliveryResult.DestinationFault("HTTP 503"), first);
        Assert.Same(second, first);
    }

    [Fact]
    public async Task Response_is_disposed_unread()
    {
        var content = new TrackingContent();
        await using var destination = fixture.Create(HttpDestinationFixture.Url(), (_, _) =>
            Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = content }));

        await destination.SendAsync(HttpDestinationFixture.Message(), Ct);

        Assert.True(content.Disposed);
        Assert.False(content.Read);
    }

    [Fact]
    public async Task Timeout_returns_a_destination_fault()
    {
        await using var destination = fixture.Create(Timed(), Hang);

        var send = destination.SendAsync(HttpDestinationFixture.Message(), Ct).AsTask();
        fixture.Time.Advance(TimeSpan.FromSeconds(5));

        Assert.Equal(new DeliveryResult.DestinationFault("timeout after 00:00:05"), await send);
        AssertGateSettled(destination);
    }

    [Fact]
    public async Task Slot_wait_timeout_returns_the_slot_wait_fault()
    {
        var settings = Timed();
        settings["MaxRequestsInFlight"] = "1";
        // The holder ignores its deadline, so its slot is not handed to the waiter before the waiter's deadline fires.
        var release = new TaskCompletionSource<HttpResponseMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var destination = fixture.Create(settings, (_, _) => release.Task);

        var holder = destination.SendAsync(HttpDestinationFixture.Message(id: 1), Ct).AsTask();
        var waiter = destination.SendAsync(HttpDestinationFixture.Message(id: 2), Ct).AsTask();
        Assert.Equal(1, destination.Gate.QueuedEntries);
        fixture.Time.Advance(TimeSpan.FromSeconds(5));

        Assert.Same(HttpSendGate.SlotWaitTimeout, await waiter);
        Assert.Single(fixture.Handlers.Single().Requests);
        release.SetResult(new HttpResponseMessage(HttpStatusCode.OK));
        await holder;
        AssertGateSettled(destination);
    }

    [Fact]
    public async Task Term_cancellation_while_waiting_for_a_slot_rethrows()
    {
        var settings = HttpDestinationFixture.Url();
        settings["MaxRequestsInFlight"] = "1";
        var release = new TaskCompletionSource<HttpResponseMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var destination = fixture.Create(settings, (_, _) => release.Task);
        using var term = new CancellationTokenSource();

        var holder = destination.SendAsync(HttpDestinationFixture.Message(id: 1), Ct).AsTask();
        var waiter = destination.SendAsync(HttpDestinationFixture.Message(id: 2), term.Token).AsTask();
        await term.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiter);
        release.SetResult(new HttpResponseMessage(HttpStatusCode.OK));
        Assert.Same(DeliveryResult.Delivered.Instance, await holder);
        AssertGateSettled(destination);
    }

    [Fact]
    public async Task Term_cancellation_during_the_request_rethrows()
    {
        await using var destination = fixture.Create(HttpDestinationFixture.Url(), Hang);
        using var term = new CancellationTokenSource();

        var send = destination.SendAsync(HttpDestinationFixture.Message(), term.Token).AsTask();
        await term.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => send);
        AssertGateSettled(destination);
    }

    public static TheoryData<int, string> TransportFailures => new()
    {
        { 0, "HttpRequestException: ConnectionError" },
        { 1, "HttpRequestException: NameResolutionError" },
        { 2, "HttpRequestException: SecureConnectionError" },
        { 3, "IOException" },
        { 4, "SocketException" },
        { 5, "AuthenticationException" },
    };

    [Theory]
    [MemberData(nameof(TransportFailures))]
    public async Task Transport_failures_map_to_destination_faults_without_url_or_message(int failure, string expected)
    {
        const string secret = "https://ledger.example/orders?token=s3cr3t";
        Exception exception = failure switch
        {
            0 => new HttpRequestException(HttpRequestError.ConnectionError, secret),
            1 => new HttpRequestException(HttpRequestError.NameResolutionError, secret),
            2 => new HttpRequestException(HttpRequestError.SecureConnectionError, secret, new AuthenticationException(secret)),
            3 => new IOException(secret),
            4 => new SocketException((int)SocketError.ConnectionReset),
            _ => new AuthenticationException(secret),
        };
        await using var destination = fixture.Create(HttpDestinationFixture.Url(), (_, _) => Task.FromException<HttpResponseMessage>(exception));

        var result = await destination.SendAsync(HttpDestinationFixture.Message(), Ct);

        Assert.Equal(new DeliveryResult.DestinationFault(expected), result);
        AssertGateSettled(destination);
    }

    [Fact]
    public async Task Object_disposed_without_term_cancellation_is_a_destination_fault()
    {
        await using var destination = fixture.Create(HttpDestinationFixture.Url(), (_, _) =>
            Task.FromException<HttpResponseMessage>(new ObjectDisposedException("SocketsHttpHandler")));

        var result = await destination.SendAsync(HttpDestinationFixture.Message(), Ct);

        Assert.Equal(new DeliveryResult.DestinationFault("ObjectDisposedException"), result);
    }

    [Fact]
    public async Task Send_racing_dispose_faults_or_rethrows_as_cancellation_on_term_end()
    {
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var destination = fixture.Create(HttpDestinationFixture.Url(), async (_, _) =>
        {
            entered.TrySetResult();
            await release.Task;
            throw new ObjectDisposedException("SocketsHttpHandler");
        });
        using var term = new CancellationTokenSource();

        var faulted = destination.SendAsync(HttpDestinationFixture.Message(id: 1), Ct).AsTask();
        var cancelled = destination.SendAsync(HttpDestinationFixture.Message(id: 2), term.Token).AsTask();
        await entered.Task;
        await destination.DisposeAsync();
        await term.CancelAsync();
        release.SetResult();

        Assert.Equal(new DeliveryResult.DestinationFault("ObjectDisposedException"), await faulted);
        await Assert.ThrowsAsync<OperationCanceledException>(() => cancelled);
        Assert.Equal(destination.Gate.Rented, destination.Gate.Returned);
    }

    [Fact]
    public async Task Send_after_dispose_faults_or_rethrows_as_cancellation()
    {
        var destination = fixture.Create(HttpDestinationFixture.Url());
        await destination.DisposeAsync();
        using var term = new CancellationTokenSource();
        await term.CancelAsync();

        Assert.Equal(
            new DeliveryResult.DestinationFault("ObjectDisposedException"),
            await destination.SendAsync(HttpDestinationFixture.Message(), Ct));
        await Assert.ThrowsAsync<OperationCanceledException>(() => destination.SendAsync(HttpDestinationFixture.Message(), term.Token).AsTask());
        Assert.Empty(fixture.Handlers.Single().Requests);
    }

    [Fact]
    public async Task Unresolvable_endpoint_is_a_contract_violation()
    {
        await using var destination = fixture.Create(HttpDestinationFixture.Url());

        await Assert.ThrowsAsync<ArgumentException>(() => destination.SendAsync(HttpDestinationFixture.Message(route: "alerts"), Ct).AsTask());
        Assert.Empty(fixture.Handlers.Single().Requests);
    }

    [Fact]
    public async Task Dead_letter_sends_to_the_single_url_with_relay_owned_dead_letter_headers()
    {
        await using var destination = fixture.Create(HttpDestinationFixture.Url("/dlq"));
        var payload = "poison"u8.ToArray();
        var headers = HttpDestinationFixture.Headers(("x-dead-letter-reason", "forged"), ("x-tenant", "t1"));
        var message = HttpDestinationFixture.Message(
            payload,
            headers: headers,
            route: "alerts",
            deadLetter: new DeadLetterInfo("HTTP 422", 3, new DateTimeOffset(2026, 11, 26, 10, 30, 0, 125, TimeSpan.Zero)));

        Assert.Same(DeliveryResult.Delivered.Instance, await destination.SendAsync(message, Ct));

        var request = Assert.Single(fixture.Handlers.Single().Requests);
        Assert.Equal($"{Https}/dlq", request.Uri);
        Assert.Equal(payload, request.Body);
        Assert.Equal("HTTP 422", request.Headers[DeadLetterHeaderNames.Reason]);
        Assert.Equal("3", request.Headers[DeadLetterHeaderNames.RetryAttempts]);
        Assert.Equal("Sink", request.Headers[DeadLetterHeaderNames.OriginalDestination]);
        Assert.Equal("Orders", request.Headers[DeadLetterHeaderNames.OriginalOutbox]);
        Assert.Equal("42", request.Headers[DeadLetterHeaderNames.OriginalId]);
        Assert.Equal("2026-11-26T10:30:00.125Z", request.Headers[DeadLetterHeaderNames.FailedAt]);
        Assert.Equal("alerts", request.Headers[DeadLetterHeaderNames.OriginalRoute]);
        Assert.Equal("t1", request.Headers["x-tenant"]);
    }

    [Fact]
    public async Task Templated_url_renders_per_row_and_render_faults_name_no_value()
    {
        await using var destination = fixture.Create(HttpDestinationFixture.Url("/orders/{id}?key={key}"));

        Assert.Same(
            DeliveryResult.Delivered.Instance,
            await destination.SendAsync(HttpDestinationFixture.Message(key: "a b/c"u8.ToArray(), id: 7), Ct));
        var fault = Assert.IsType<DeliveryResult.MessageFault>(await destination.SendAsync(HttpDestinationFixture.Message(key: null), Ct));

        Assert.Equal($"{Https}/orders/7?key=a%20b%2Fc", Assert.Single(fixture.Handlers.Single().Requests).Uri);
        Assert.True(fault.Permanent);
        Assert.DoesNotContain("ledger.example", fault.Error, StringComparison.Ordinal);
        AssertGateSettled(destination);
    }

    [Fact]
    public async Task Forwarding_fault_names_the_header_not_the_value()
    {
        await using var destination = fixture.Create(HttpDestinationFixture.Url());
        var headers = HttpDestinationFixture.Headers(("x-secret", "s3cr3t\r\nInjected: yes"));

        var fault = Assert.IsType<DeliveryResult.MessageFault>(await destination.SendAsync(HttpDestinationFixture.Message(headers: headers), Ct));

        Assert.True(fault.Permanent);
        Assert.Contains("x-secret", fault.Error, StringComparison.Ordinal);
        Assert.DoesNotContain("s3cr3t", fault.Error, StringComparison.Ordinal);
        Assert.Empty(fixture.Handlers.Single().Requests);
    }

    [Fact]
    public async Task Static_authentication_header_is_relay_owned()
    {
        var settings = HttpDestinationFixture.Url();
        settings["Authentication:Scheme"] = "Bearer";
        settings["Authentication:Credential"] = "Ledger";
        settings["!HttpCredentials:Ledger:Token"] = "tok";
        await using var destination = fixture.Create(settings);

        await destination.SendAsync(HttpDestinationFixture.Message(headers: HttpDestinationFixture.Headers(("Authorization", "Bearer forged"))), Ct);

        Assert.Equal("Bearer tok", Assert.Single(fixture.Handlers.Single().Requests).Headers["Authorization"]);
    }

    [Fact]
    public async Task OAuth2_token_is_sent_and_a_401_invalidates_it()
    {
        var issued = 0;
        var status = HttpStatusCode.Unauthorized;
        await using var destination = fixture.Create(OAuth2(), (request, _) => Task.FromResult(
            request.RequestUri!.AbsolutePath == "/token"
                ? Token($"tok-{++issued}")
                : new HttpResponseMessage(status)));

        Assert.Equal(new DeliveryResult.DestinationFault("HTTP 401"), await destination.SendAsync(HttpDestinationFixture.Message(), Ct));
        status = HttpStatusCode.OK;
        Assert.Same(DeliveryResult.Delivered.Instance, await destination.SendAsync(HttpDestinationFixture.Message(), Ct));

        var sends = fixture.Handlers.SelectMany(static h => h.Requests).Where(static r => r.Uri.EndsWith("/orders", StringComparison.Ordinal)).ToArray();
        Assert.Equal(["Bearer tok-1", "Bearer tok-2"], sends.Select(static r => r.Headers["Authorization"]));
    }

    [Fact]
    public async Task OAuth2_token_failure_is_a_destination_fault_and_nothing_is_sent()
    {
        await using var destination = fixture.Create(OAuth2(), (request, _) => Task.FromResult(new HttpResponseMessage(
            request.RequestUri!.AbsolutePath == "/token" ? HttpStatusCode.BadRequest : HttpStatusCode.OK)));

        var result = await destination.SendAsync(HttpDestinationFixture.Message(), Ct);

        Assert.Equal(new DeliveryResult.DestinationFault("OAuth2 token: HTTP 400"), result);
        Assert.DoesNotContain(fixture.Handlers.SelectMany(static h => h.Requests), static r => r.Uri.EndsWith("/orders", StringComparison.Ordinal));
        AssertGateSettled(destination);
    }

    [Fact]
    public async Task Standard_webhooks_signing_uses_the_endpoint_webhook_id()
    {
        var settings = HttpDestinationFixture.Url();
        settings["Signing:Scheme"] = "StandardWebhooks";
        settings["Signing:Credential"] = "Signing";
        settings["!HttpCredentials:Signing:SigningSecret"] = "whsec_" + Convert.ToBase64String("0123456789abcdef0123456789abcdef"u8);
        await using var destination = fixture.Create(settings);

        await destination.SendAsync(HttpDestinationFixture.Message(headers: HttpDestinationFixture.Headers(("webhook-id", "forged"))), Ct);

        var request = Assert.Single(fixture.Handlers.Single().Requests);
        Assert.Equal("Orders-42", request.Headers[RequestSigner.WebhookIdHeader]);
        Assert.Equal(fixture.Time.GetUtcNow().ToUnixTimeSeconds().ToString(System.Globalization.CultureInfo.InvariantCulture), request.Headers[RequestSigner.WebhookTimestampHeader]);
        Assert.StartsWith("v1,", request.Headers[RequestSigner.WebhookSignatureHeader], StringComparison.Ordinal);
    }

    [Fact]
    public async Task Gate_context_and_slot_return_after_mixed_outcomes()
    {
        var settings = HttpDestinationFixture.Url("/orders/{key}");
        settings["MaxRequestsInFlight"] = "2";
        var calls = 0;
        await using var destination = fixture.Create(settings, (_, _) => (Interlocked.Increment(ref calls) % 3) switch
        {
            0 => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)),
            1 => Task.FromResult(new HttpResponseMessage(HttpStatusCode.ServiceUnavailable)),
            _ => Task.FromException<HttpResponseMessage>(new HttpRequestException(HttpRequestError.ConnectionError)),
        });

        var sends = Enumerable.Range(0, 30)
            .Select(i => destination.SendAsync(HttpDestinationFixture.Message(key: i % 5 == 0 ? null : "k"u8.ToArray(), id: i), Ct).AsTask());
        await Task.WhenAll(sends);

        AssertGateSettled(destination);
        Assert.Equal(30, destination.Gate.Rented);
    }

    [Fact]
    public async Task One_handler_per_instance_disposed_with_it_and_logged()
    {
        var factory = fixture.Factory(HttpDestinationFixture.Url(), (_, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)));
        var first = factory.Create(fixture.Destination, new DestinationContext(OrderingMode.None));
        var second = factory.Create(fixture.Destination, new DestinationContext(OrderingMode.PerKey));

        Assert.Equal(2, fixture.Handlers.Count);
        await first.DisposeAsync();
        Assert.True(fixture.Handlers[0].Disposed);
        Assert.False(fixture.Handlers[1].Disposed);
        await second.DisposeAsync();
        Assert.True(fixture.Handlers[1].Disposed);

        var created = fixture.Logger.WithEvent(300).ToArray();
        Assert.Equal(2, created.Length);
        Assert.All(created, static e => Assert.Equal(LogLevel.Debug, e.Level));
        Assert.All(created, static e => Assert.Contains("ledger.example", e.Message, StringComparison.Ordinal));
        Assert.All(created, static e => Assert.DoesNotContain("/orders", e.Message, StringComparison.Ordinal));
    }

    [Fact]
    public async Task Probe_instance_only_validates()
    {
        await using var destination = fixture.Create(
            HttpDestinationFixture.Url(), (_, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)), DestinationContext.Probe);

        await Assert.ThrowsAsync<InvalidOperationException>(() => destination.SendAsync(HttpDestinationFixture.Message(), Ct).AsTask());
        Assert.Empty(fixture.Handlers.Single().Requests);
    }

    [Fact]
    public void Create_without_validation_throws()
    {
        var factory = fixture.Factory(HttpDestinationFixture.Url(), (_, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK)));

        Assert.Throws<InvalidOperationException>(() =>
            factory.Create(new DestinationOptions { Name = "Other", Transport = "Http" }, new DestinationContext(OrderingMode.None)));
    }

    [Fact]
    public async Task Real_handler_sends_utf8_header_values_and_traceparent_unchanged()
    {
        const string traceparent = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01";
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        var server = ServeOnceAsync(listener);

        using var source = new ActivitySource("http-destination-test");
        using var activityListener = new ActivityListener
        {
            ShouldListenTo = static s => s.Name == "http-destination-test",
            Sample = static (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
        };
        ActivitySource.AddActivityListener(activityListener);
        using var activity = source.StartActivity("send");
        Assert.NotNull(Activity.Current);

        var factory = new HttpDestinationFactory(
            Microsoft.Extensions.Logging.Abstractions.NullLoggerFactory.Instance,
            new HttpCredentialSection(new RelayConfiguration(new ConfigurationBuilder().Build())),
            HttpHookLookup.None);
        var config = new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["Destinations:0:Name"] = "Sink",
            ["Destinations:0:Transport"] = "Http",
            ["Destinations:0:Http:Url"] = $"http://127.0.0.1:{port}/orders",
            ["Outboxes:0:Name"] = "Orders",
            ["Outboxes:0:Destination"] = "Sink",
        }).Build();
        var relay = RelayOptionsBinder.Bind(config);
        var options = Assert.Single(relay.Destinations);
        Assert.Empty(factory.Validate(options, HttpPlanFixture.Path, relay));
        await using var destination = factory.Create(options, new DestinationContext(OrderingMode.None));
        var headers = HttpDestinationFixture.Headers(("x-customer", "Zoë Ångström"), ("traceparent", traceparent));

        var result = await destination.SendAsync(HttpDestinationFixture.Message("abc"u8.ToArray(), headers: headers), Ct);

        Assert.Same(DeliveryResult.Delivered.Instance, result);
        var received = await server;
        Assert.True(received.AsSpan().IndexOf("x-customer: Zoë Ångström\r\n"u8) >= 0);
        Assert.True(received.AsSpan().IndexOf("Content-Length: 3\r\n"u8) >= 0);
        var text = Encoding.UTF8.GetString(received);
        Assert.Single(text.Split("\r\n").Where(static l => l.StartsWith("traceparent:", StringComparison.OrdinalIgnoreCase)), $"traceparent: {traceparent}");
        Assert.DoesNotContain(activity!.TraceId.ToHexString(), text, StringComparison.Ordinal);
    }

    private static async Task<byte[]> ServeOnceAsync(TcpListener listener)
    {
        using var client = await listener.AcceptTcpClientAsync(Ct);
        var stream = client.GetStream();
        var buffer = new List<byte>();
        var chunk = new byte[4096];
        int end;
        while ((end = IndexOf(buffer, "\r\n\r\n"u8)) < 0)
        {
            var read = await stream.ReadAsync(chunk, Ct);
            buffer.AddRange(chunk.AsSpan(0, read));
        }

        while (buffer.Count < end + 4 + 3)
        {
            var read = await stream.ReadAsync(chunk, Ct);
            buffer.AddRange(chunk.AsSpan(0, read));
        }

        await stream.WriteAsync("HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n"u8.ToArray(), Ct);
        return [.. buffer];
    }

    private static int IndexOf(List<byte> buffer, ReadOnlySpan<byte> value) =>
        System.Runtime.InteropServices.CollectionsMarshal.AsSpan(buffer).IndexOf(value);

    private static Dictionary<string, string?> Timed() => new()
    {
        ["Url"] = $"{Https}/orders",
        ["SendTimeout"] = "00:00:05",
        ["ConnectTimeout"] = "00:00:01",
    };

    private static Dictionary<string, string?> OAuth2() => new()
    {
        ["Url"] = $"{Https}/orders",
        ["Authentication:Scheme"] = "OAuth2ClientCredentials",
        ["Authentication:Credential"] = "Ledger",
        ["Authentication:TokenUrl"] = $"{Https}/token",
        ["!HttpCredentials:Ledger:ClientId"] = "id",
        ["!HttpCredentials:Ledger:ClientSecret"] = "secret",
    };

    private static HttpResponseMessage Token(string token) => new(HttpStatusCode.OK)
    {
        Content = new StringContent($"{{\"access_token\":\"{token}\",\"expires_in\":3600}}", Encoding.UTF8, "application/json"),
    };

    private static async Task<HttpResponseMessage> Hang(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
        throw new UnreachableException();
    }

    private static void AssertGateSettled(HttpDestination destination)
    {
        Assert.Equal(destination.Gate.Rented, destination.Gate.Returned);
        Assert.Equal(destination.Gate.MaxRequestsInFlight, destination.Gate.FreeCount);
        Assert.Equal(0, destination.Gate.HeldCount);
    }

    private sealed class TrackingContent : HttpContent
    {
        public bool Disposed { get; private set; }

        public bool Read { get; private set; }

        protected override Task SerializeToStreamAsync(Stream stream, TransportContext? context)
        {
            Read = true;
            return Task.CompletedTask;
        }

        protected override bool TryComputeLength(out long length)
        {
            length = 0;
            return true;
        }

        protected override void Dispose(bool disposing)
        {
            Disposed = true;
            base.Dispose(disposing);
        }
    }
}
