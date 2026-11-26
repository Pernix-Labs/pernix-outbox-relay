using System.Net;
using System.Security.Cryptography;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Destination.Http;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Http;

public sealed class HttpHooksTests
{
    private const string Https = HttpDestinationFixture.Https;

    private readonly List<StubHandler> handlers = [];

    private DestinationOptions destination = null!;

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public void AddHttpDestination_registers_hooks_as_keyed_singletons()
    {
        using var provider = new ServiceCollection()
            .AddHttpDestination(o => o.AddRequestMapper<EnvelopeMapper>("envelope").AddResponseClassifier<FixedClassifier>("ledger"))
            .BuildServiceProvider();

        var mapper = provider.GetRequiredKeyedService<IHttpRequestMapper>("envelope");
        Assert.IsType<EnvelopeMapper>(mapper);
        Assert.Same(mapper, provider.GetRequiredKeyedService<IHttpRequestMapper>("envelope"));
        Assert.IsType<FixedClassifier>(provider.GetRequiredKeyedService<IHttpResponseClassifier>("ledger"));
    }

    [Fact]
    public void Same_name_and_type_again_adds_no_descriptor()
    {
        var services = new ServiceCollection().AddHttpDestination(o => o.AddRequestMapper<EnvelopeMapper>("envelope"));
        var count = services.Count;

        services.AddHttpDestination(o => o.AddRequestMapper<EnvelopeMapper>("envelope"));

        Assert.Equal(count, services.Count);
    }

    [Fact]
    public void Same_name_with_another_type_throws()
    {
        var services = new ServiceCollection()
            .AddHttpDestination(o => o.AddRequestMapper<EnvelopeMapper>("envelope").AddResponseClassifier<FixedClassifier>("ledger"));

        Assert.Throws<InvalidOperationException>(() => services.AddHttpDestination(o => o.AddRequestMapper<ShortCircuitMapper>("envelope")));
        Assert.Throws<InvalidOperationException>(() => services.AddHttpDestination(o => o.AddResponseClassifier<ThrowingClassifier>("ledger")));
    }

    [Fact]
    public void Unregistered_names_fail_configuration()
    {
        var settings = HttpDestinationFixture.Url();
        settings["RequestMapper"] = "missing-mapper";
        settings["ResponseClassifier"] = "missing-classifier";
        var config = Config(settings);
        using var provider = new ServiceCollection()
            .AddSingleton(new RelayConfiguration(config))
            .AddLogging()
            .AddHttpDestination(o => o.AddRequestMapper<EnvelopeMapper>("envelope"))
            .BuildServiceProvider();
        var factory = provider.GetServices<IDestinationFactory>().OfType<HttpDestinationFactory>().Single();
        var relay = RelayOptionsBinder.Bind(config);

        var failures = factory.Validate(Assert.Single(relay.Destinations), HttpPlanFixture.Path, relay);

        Assert.Contains($"{HttpPlanFixture.Path}.Http.RequestMapper: no request mapper is registered under this name", failures);
        Assert.Contains($"{HttpPlanFixture.Path}.Http.ResponseClassifier: no response classifier is registered under this name", failures);
    }

    [Fact]
    public void Registered_names_pass_configuration()
    {
        var settings = HttpDestinationFixture.Url();
        settings["RequestMapper"] = "envelope";
        settings["ResponseClassifier"] = "ledger";
        var config = Config(settings);
        using var provider = new ServiceCollection()
            .AddSingleton(new RelayConfiguration(config))
            .AddLogging()
            .AddHttpDestination(o => o.AddRequestMapper<EnvelopeMapper>("envelope").AddResponseClassifier<FixedClassifier>("ledger"))
            .BuildServiceProvider();
        var factory = provider.GetServices<IDestinationFactory>().OfType<HttpDestinationFactory>().Single();
        var relay = RelayOptionsBinder.Bind(config);

        Assert.Empty(factory.Validate(Assert.Single(relay.Destinations), HttpPlanFixture.Path, relay));
    }

    [Fact]
    public async Task Mapper_result_short_circuits_without_sending()
    {
        var settings = HttpDestinationFixture.Url();
        settings["RequestMapper"] = "skip";
        await using var destination = Create(settings, o => o.AddRequestMapper<ShortCircuitMapper>("skip"));

        var result = await destination.SendAsync(HttpDestinationFixture.Message(), Ct);

        Assert.Equal(ShortCircuitMapper.Result, result);
        Assert.Empty(handlers.Single().Requests);
        AssertGateSettled(destination);
    }

    [Fact]
    public async Task Mapper_runs_after_config_headers_and_before_auth()
    {
        var settings = HttpDestinationFixture.Url();
        settings["RequestMapper"] = "envelope";
        settings["Headers:X-Tenant"] = "acme";
        settings["Authentication:Scheme"] = "Bearer";
        settings["Authentication:Credential"] = "Api";
        settings["!HttpCredentials:Api:Token"] = "t0ken";
        await using var destination = Create(settings, o => o.AddRequestMapper<EnvelopeMapper>("envelope"));

        Assert.Equal(Delivered, await destination.SendAsync(HttpDestinationFixture.Message(), Ct));

        var mapper = (EnvelopeMapper)Mapper("envelope");
        Assert.Equal("acme", mapper.SeenTenant);
        Assert.False(mapper.SawAuthorization);
        var request = Assert.Single(handlers.Single().Requests);
        Assert.Equal("Bearer t0ken", request.Headers["Authorization"]);
        Assert.Equal(EnvelopeMapper.Body, request.Body);
    }

    [Fact]
    public async Task Mapper_body_is_signed()
    {
        var settings = HttpDestinationFixture.Url();
        settings["RequestMapper"] = "envelope";
        settings["Signing:Scheme"] = "HmacSha256";
        settings["Signing:HeaderName"] = "X-Signature";
        settings["Signing:Credential"] = "Signing";
        settings["!HttpCredentials:Signing:SigningSecret"] = "secret";
        await using var destination = Create(settings, o => o.AddRequestMapper<EnvelopeMapper>("envelope"));

        await destination.SendAsync(HttpDestinationFixture.Message(), Ct);

        var request = Assert.Single(handlers.Single().Requests);
        Assert.Equal(EnvelopeMapper.Body, request.Body);
        var expected = Convert.ToHexStringLower(HMACSHA256.HashData("secret"u8, EnvelopeMapper.Body));
        Assert.Equal(expected, request.Headers["X-Signature"]);
    }

    [Fact]
    public async Task Classifier_replaces_the_status_table_and_gets_a_4_KB_body_prefix()
    {
        var settings = HttpDestinationFixture.Url();
        settings["ResponseClassifier"] = "ledger";
        var body = Enumerable.Range(0, 10_000).Select(static i => (byte)i).ToArray();
        await using var destination = Create(
            settings,
            o => o.AddResponseClassifier<FixedClassifier>("ledger"),
            (_, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.InternalServerError) { Content = new ByteArrayContent(body) }));

        var result = await destination.SendAsync(HttpDestinationFixture.Message(), Ct);

        Assert.Equal(Delivered, result);
        var classifier = (FixedClassifier)Classifier("ledger");
        Assert.Equal(500, classifier.SeenStatus);
        Assert.Equal(body.AsSpan(0, HttpDestination.BodyPrefixSize).ToArray(), classifier.SeenPrefix);
    }

    [Fact]
    public async Task Classifier_gets_a_short_body_whole()
    {
        var settings = HttpDestinationFixture.Url();
        settings["ResponseClassifier"] = "ledger";
        await using var destination = Create(
            settings,
            o => o.AddResponseClassifier<FixedClassifier>("ledger"),
            (_, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent("{\"ok\":false}") }));

        await destination.SendAsync(HttpDestinationFixture.Message(), Ct);

        Assert.Equal("{\"ok\":false}"u8.ToArray(), ((FixedClassifier)Classifier("ledger")).SeenPrefix);
    }

    [Fact]
    public async Task Without_a_classifier_the_body_is_not_read()
    {
        await using var destination = Create(
            HttpDestinationFixture.Url(),
            null,
            (_, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK) { Content = new UnreadableContent() }));

        Assert.Equal(Delivered, await destination.SendAsync(HttpDestinationFixture.Message(), Ct));
    }

    [Fact]
    public async Task Mapper_throw_is_a_destination_fault_and_returns_the_gate_context()
    {
        var settings = HttpDestinationFixture.Url();
        settings["RequestMapper"] = "broken";
        await using var destination = Create(settings, o => o.AddRequestMapper<ThrowingMapper>("broken"));

        var result = await destination.SendAsync(HttpDestinationFixture.Message(), Ct);

        Assert.Equal(new DeliveryResult.DestinationFault("FormatException in request mapper 'broken'"), result);
        Assert.Empty(handlers.Single().Requests);
        AssertGateSettled(destination);
    }

    [Fact]
    public async Task Classifier_throw_is_a_destination_fault_and_returns_the_gate_context()
    {
        var settings = HttpDestinationFixture.Url();
        settings["ResponseClassifier"] = "broken";
        await using var destination = Create(settings, o => o.AddResponseClassifier<ThrowingClassifier>("broken"));

        var result = await destination.SendAsync(HttpDestinationFixture.Message(), Ct);

        Assert.Equal(new DeliveryResult.DestinationFault("FormatException in response classifier 'broken'"), result);
        AssertGateSettled(destination);
    }

    [Fact]
    public async Task Mixed_hook_outcomes_return_every_gate_context()
    {
        var settings = HttpDestinationFixture.Url();
        settings["RequestMapper"] = "flaky";
        settings["ResponseClassifier"] = "flaky";
        settings["MaxRequestsInFlight"] = "2";
        await using var destination = Create(settings, o => o.AddRequestMapper<FlakyMapper>("flaky").AddResponseClassifier<FlakyClassifier>("flaky"));

        var results = await Task.WhenAll(Enumerable.Range(0, 30)
            .Select(i => destination.SendAsync(HttpDestinationFixture.Message(id: i), Ct).AsTask()));

        Assert.Contains(results, static r => r is DeliveryResult.Delivered);
        Assert.Contains(results, static r => r == ShortCircuitMapper.Result);
        Assert.Contains(results, static r => r is DeliveryResult.DestinationFault { Error: "FormatException in request mapper 'flaky'" });
        Assert.Contains(results, static r => r is DeliveryResult.DestinationFault { Error: "FormatException in response classifier 'flaky'" });
        Assert.Equal(30, destination.Gate.Rented);
        AssertGateSettled(destination);
    }

    private static readonly DeliveryResult Delivered = DeliveryResult.Delivered.Instance;

    private ServiceProvider? provider;

    private IHttpRequestMapper Mapper(string name) => provider!.GetRequiredKeyedService<IHttpRequestMapper>(name);

    private IHttpResponseClassifier Classifier(string name) => provider!.GetRequiredKeyedService<IHttpResponseClassifier>(name);

    private static void AssertGateSettled(HttpDestination destination)
    {
        Assert.Equal(destination.Gate.Rented, destination.Gate.Returned);
        Assert.Equal(destination.Gate.MaxRequestsInFlight, destination.Gate.FreeCount);
        Assert.Equal(0, destination.Gate.HeldCount);
    }

    // Keys are under Destinations:0:Http unless prefixed '!' (relay-section keys).
    private static IConfigurationRoot Config(Dictionary<string, string?> settings)
    {
        var all = new Dictionary<string, string?>
        {
            ["Destinations:0:Name"] = "Sink",
            ["Destinations:0:Transport"] = "Http",
            ["Outboxes:0:Name"] = "Orders",
            ["Outboxes:0:Destination"] = "Sink",
        };
        foreach (var (key, value) in settings)
        {
            all[key.StartsWith('!') ? key[1..] : $"Destinations:0:Http:{key}"] = value;
        }

        return new ConfigurationBuilder().AddInMemoryCollection(all).Build();
    }

    private HttpDestination Create(
        Dictionary<string, string?> settings,
        Action<HttpDestinationBuilder>? hooks,
        Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>>? respond = null)
    {
        respond ??= (_, _) => Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));
        var config = Config(settings);
        provider = new ServiceCollection().AddHttpDestination(hooks).BuildServiceProvider();
        var factory = new HttpDestinationFactory(
            NullLoggerFactory.Instance,
            new HttpCredentialSection(new RelayConfiguration(config)),
            HttpHooks.From(provider),
            _ =>
            {
                var handler = new StubHandler(respond);
                handlers.Add(handler);
                return handler;
            },
            null);
        var relay = RelayOptionsBinder.Bind(config);
        destination = Assert.Single(relay.Destinations);
        Assert.Empty(factory.Validate(destination, HttpPlanFixture.Path, relay));
        return (HttpDestination)factory.Create(destination, new DestinationContext(OrderingMode.None));
    }

    private sealed class EnvelopeMapper : IHttpRequestMapper
    {
        public static readonly byte[] Body = "{\"envelope\":true}"u8.ToArray();

        public string? SeenTenant { get; private set; }

        public bool SawAuthorization { get; private set; }

        public DeliveryResult? Map(OutgoingMessage message, HttpRequestMessage request)
        {
            SeenTenant = request.Headers.TryGetValues("X-Tenant", out var tenant) ? string.Join(",", tenant) : null;
            SawAuthorization = request.Headers.Contains("Authorization");
            request.Content = new ByteArrayContent(Body);
            return null;
        }
    }

    private sealed class ShortCircuitMapper : IHttpRequestMapper
    {
        public static readonly DeliveryResult Result = new DeliveryResult.MessageFault("not for this endpoint", Permanent: true);

        public DeliveryResult? Map(OutgoingMessage message, HttpRequestMessage request) => Result;
    }

    private sealed class ThrowingMapper : IHttpRequestMapper
    {
        public DeliveryResult? Map(OutgoingMessage message, HttpRequestMessage request) => throw new FormatException("row 42 secret");
    }

    private sealed class FlakyMapper : IHttpRequestMapper
    {
        public DeliveryResult? Map(OutgoingMessage message, HttpRequestMessage request) => (message.Source.Id % 4) switch
        {
            0 => throw new FormatException(),
            1 => ShortCircuitMapper.Result,
            _ => null,
        };
    }

    private sealed class FixedClassifier : IHttpResponseClassifier
    {
        public int SeenStatus { get; private set; }

        public byte[] SeenPrefix { get; private set; } = [];

        public DeliveryResult Classify(HttpResponseMessage response, ReadOnlySpan<byte> bodyPrefix)
        {
            SeenStatus = (int)response.StatusCode;
            SeenPrefix = bodyPrefix.ToArray();
            return DeliveryResult.Delivered.Instance;
        }
    }

    private sealed class ThrowingClassifier : IHttpResponseClassifier
    {
        public DeliveryResult Classify(HttpResponseMessage response, ReadOnlySpan<byte> bodyPrefix) => throw new FormatException();
    }

    private sealed class FlakyClassifier : IHttpResponseClassifier
    {
        private int calls;

        public DeliveryResult Classify(HttpResponseMessage response, ReadOnlySpan<byte> bodyPrefix) =>
            Interlocked.Increment(ref calls) % 2 == 0 ? throw new FormatException() : DeliveryResult.Delivered.Instance;
    }

    private sealed class UnreadableContent : HttpContent
    {
        protected override Task SerializeToStreamAsync(Stream stream, System.Net.TransportContext? context) =>
            throw new InvalidOperationException("body read");

        protected override bool TryComputeLength(out long length)
        {
            length = 0;
            return false;
        }
    }
}
