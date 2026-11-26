using System.Reflection;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Destination.Http;
using Pernix.OutboxRelay.Destination.Kafka;
using Pernix.OutboxRelay.Hosting;
using Pernix.OutboxRelay.Outbox.PostgreSql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Http;

public sealed class HttpRegistrationTests
{
    private const string Origin = "https:" + "//ledger.example";

    private static Dictionary<string, string?> KafkaSettings() => new()
    {
        ["HostOptions:ShutdownTimeout"] = "00:00:30",
        ["OutboxRelay:ConnectionStrings:OrdersDb"] = "Host=127.0.0.1;Port=1;Database=orders;Username=relay;Password=unused",
        ["OutboxRelay:KafkaClusters:Main:BootstrapServers"] = "127.0.0.1:1",
        ["OutboxRelay:Destinations:0:Name"] = "OrdersSink",
        ["OutboxRelay:Destinations:0:Transport"] = "Kafka",
        ["OutboxRelay:Destinations:0:Kafka:Cluster"] = "Main",
        ["OutboxRelay:Destinations:0:Kafka:Topic"] = "orders.events",
        ["OutboxRelay:Outboxes:0:Name"] = "Orders",
        ["OutboxRelay:Outboxes:0:Destination"] = "OrdersSink",
        ["OutboxRelay:Outboxes:0:Database:Provider"] = "PostgreSql",
        ["OutboxRelay:Outboxes:0:Database:Connection"] = "OrdersDb",
        ["OutboxRelay:Outboxes:0:Database:Table"] = "public.outbox",
        ["OutboxRelay:Outboxes:0:Ordering:Mode"] = "None",
    };

    [Fact]
    public void AddHttpDestination_registers_factory_and_its_credential_section_as_validator()
    {
        using var provider = WithRelayConfiguration().AddLogging().AddHttpDestination().BuildServiceProvider();

        var factory = Assert.IsType<HttpDestinationFactory>(Assert.Single(provider.GetServices<IDestinationFactory>()));
        var validator = Assert.Single(provider.GetServices<IRelaySectionValidator>());
        Assert.Equal("Http", factory.Transport);
        Assert.Same(CredentialsOf(factory), validator);
        Assert.Same(provider.GetRequiredService<HttpCredentialSection>(), validator);
    }

    [Fact]
    public void Repeat_registration_adds_no_descriptor()
    {
        var services = new ServiceCollection().AddHttpDestination();
        var count = services.Count;

        services.AddHttpDestination();

        Assert.Equal(count, services.Count);
    }

    [Fact]
    public void Repeat_registration_resolves_one_factory_and_one_validator()
    {
        using var provider = WithRelayConfiguration().AddLogging().AddHttpDestination().AddHttpDestination().BuildServiceProvider();

        Assert.Single(provider.GetServices<IDestinationFactory>());
        Assert.Single(provider.GetServices<IRelaySectionValidator>());
    }

    [Fact]
    public void Builder_overload_returns_the_same_builder()
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());

        Assert.Same(builder, builder.AddHttpDestination());
        Assert.Contains(builder.Services, d => d.ServiceType == typeof(IDestinationFactory));
        Assert.Contains(builder.Services, d => d.ServiceType == typeof(IRelaySectionValidator));
    }

    [Fact]
    public async Task Repeat_registration_with_valid_config_passes_configuration_stage()
    {
        var validator = BuildValidator(KafkaSettings(), b => b.AddHttpDestination().AddHttpDestination());

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => validator.StartingAsync(new CancellationToken(canceled: true)));
    }

    [Fact]
    public async Task Http_transport_without_registration_fails_with_the_unregistered_transport_message()
    {
        var settings = KafkaSettings();
        settings["OutboxRelay:Destinations:0:Transport"] = "Http";

        var validator = BuildValidator(settings, _ => { });

        var ex = await Assert.ThrowsAsync<RelayStartupValidationException>(() => validator.StartingAsync(new CancellationToken(canceled: true)));
        Assert.Contains("OutboxRelay.Destinations[OrdersSink].Transport: transport 'Http' is not registered", ex.Failures);
    }

    [Fact]
    public async Task Http_credentials_are_ignored_without_registration()
    {
        var settings = KafkaSettings();
        settings["OutboxRelay:HttpCredentials:Ledger:NotAField"] = "x";

        var validator = BuildValidator(settings, _ => { });

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => validator.StartingAsync(new CancellationToken(canceled: true)));
    }

    [Fact]
    public async Task Http_credentials_are_validated_with_registration()
    {
        var settings = KafkaSettings();
        settings["OutboxRelay:HttpCredentials:Ledger:NotAField"] = "sentinel";

        var validator = BuildValidator(settings, b => b.AddHttpDestination());

        var ex = await Assert.ThrowsAsync<RelayStartupValidationException>(() => validator.StartingAsync(new CancellationToken(canceled: true)));
        Assert.Contains(ex.Failures, f => f.EndsWith("HttpCredentials[Ledger]: unknown setting 'NotAField'", StringComparison.Ordinal));
        Assert.All(ex.Failures, f => Assert.DoesNotContain("sentinel", f, StringComparison.Ordinal));
    }

    [Fact]
    public void Validate_binds_the_full_http_block_without_failures()
    {
        var (factory, destination) = Destination(new()
        {
            ["Name"] = "LedgerSink",
            ["Transport"] = "Http",
            ["Http:Url"] = $"{Origin}/accounts/{{key:base64url}}/entries?source={{outbox}}",
            ["Http:Method"] = "PUT",
            ["Http:ContentType"] = "application/vnd.ledger.entry+json",
            ["Http:Headers:Idempotency-Key"] = "{outbox}-{id}",
            ["Http:ForwardHeaders:Mode"] = "listed",
            ["Http:ForwardHeaders:Names:0"] = "traceparent",
            ["Http:ForwardHeaders:Exclude:0"] = "cookie",
            ["Http:ForwardHeaders:NamePrefix"] = "x-outbox-",
            ["Http:StatusCodes:Delivered:0"] = "2xx",
            ["Http:StatusCodes:Delivered:1"] = "409",
            ["Http:StatusCodes:Rejected:0"] = "400",
            ["Http:StatusCodes:Retried:0"] = "423",
            ["Http:StatusCodes:Paused:0"] = "503",
            ["Http:SendTimeout"] = "00:00:10",
            ["Http:ConnectTimeout"] = "00:00:03",
            ["Http:MaxRequestsInFlight"] = "16",
            ["Http:HttpVersion"] = "2",
            ["Http:Authentication:Scheme"] = "OAuth2ClientCredentials",
            ["Http:Authentication:Credential"] = "Ledger",
            ["Http:Authentication:HeaderName"] = "Authorization",
            ["Http:Authentication:ValuePrefix"] = "Bearer ",
            ["Http:Authentication:TokenUrl"] = $"{Origin}/token",
            ["Http:Authentication:Scope"] = "ledger",
            ["Http:Authentication:Audience"] = "ledger",
            ["Http:Authentication:ClientCredentialsIn"] = "Body",
            ["Http:Signing:Scheme"] = "HmacSha256",
            ["Http:Signing:Credential"] = "LedgerSigning",
            ["Http:Signing:HeaderName"] = "X-Signature",
            ["Http:Signing:Encoding"] = "base64",
            ["Http:Signing:ValuePrefix"] = "sha256=",
            ["Http:Signing:WebhookId"] = "{outbox}-{id}",
            ["Http:Tls:CaCertificatePath"] = "/etc/relay/tls/internal-ca.pem",
            ["Http:Tls:ClientCertificate:CertificatePath"] = "/etc/relay/tls/client.crt",
            ["Http:Tls:ClientCertificate:KeyPath"] = "/etc/relay/tls/client.key",
            ["Http:Tls:ClientCertificate:PfxPath"] = "/etc/relay/tls/client.pfx",
            ["Http:Tls:ClientCertificate:Credential"] = "LedgerTls",
            ["Http:HealthProbe:Url"] = $"{Origin}/healthz",
            ["Http:HealthProbe:Method"] = "GET",
            ["Http:HealthProbe:Expected:0"] = "2xx",
            ["Http:RequestMapper"] = "ledger-envelope",
            ["Http:ResponseClassifier"] = "ledger-classifier",
        });

        Assert.Empty(factory.Validate(destination, "Destinations[LedgerSink]", new RelayOptions()));
    }

    [Fact]
    public void Validate_binds_routes()
    {
        var (factory, destination) = Destination(new()
        {
            ["Name"] = "LedgerSink",
            ["Transport"] = "Http",
            ["Http:Routes:orders:Url"] = $"{Origin}/orders",
            ["Http:Routes:orders:Method"] = "PATCH",
            ["Http:Routes:orders:ContentType"] = "application/json",
            ["Http:Routes:orders:Headers:X-Route"] = "{route}",
        });

        Assert.Empty(factory.Validate(destination, "Destinations[LedgerSink]", new RelayOptions()));
    }

    [Fact]
    public void Validate_fails_unknown_sibling_and_http_keys_without_values()
    {
        var (factory, destination) = Destination(new()
        {
            ["Name"] = "LedgerSink",
            ["Transport"] = "Http",
            ["Kafka:Topic"] = "sentinel-topic",
            ["Http:Url"] = $"{Origin}/",
            ["Http:Timeout"] = "sentinel-timeout",
            ["Http:Signing:Encoding"] = "sentinel-encoding",
            ["Http:SendTimeout"] = "30",
        });

        var failures = factory.Validate(destination, "Destinations[LedgerSink]", new RelayOptions());

        Assert.Equal(
            [
                "Destinations[LedgerSink].Http.SendTimeout: must be a duration in hh:mm:ss form",
                "Destinations[LedgerSink].Http.Signing.Encoding: must be one of Hex, Base64",
                "Destinations[LedgerSink].Http: unknown setting 'Timeout'",
                "Destinations[LedgerSink]: unknown setting 'Kafka'",
            ],
            failures.Order(StringComparer.Ordinal));
        Assert.All(failures, f => Assert.DoesNotContain("sentinel", f, StringComparison.Ordinal));
    }

    [Fact]
    public void Create_is_not_implemented_yet()
    {
        var (factory, destination) = Destination(new() { ["Name"] = "LedgerSink", ["Transport"] = "Http" });

        Assert.Throws<NotImplementedException>(() => factory.Create(destination, null!));
    }

    private static RelayStartupValidator BuildValidator(Dictionary<string, string?> settings, Action<HostApplicationBuilder> register)
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Configuration.AddInMemoryCollection(settings);
        builder.Services.Configure<HostOptions>(builder.Configuration.GetSection("HostOptions"));
        builder.AddOutboxRelay().AddPostgreSqlOutbox().AddKafkaDestination();
        register(builder);
        var host = builder.Build();
        return host.Services.GetServices<IHostedService>().OfType<RelayStartupValidator>().Single();
    }

    private static (HttpDestinationFactory Factory, DestinationOptions Destination) Destination(Dictionary<string, string?> settings)
    {
        var config = new ConfigurationBuilder()
            .AddInMemoryCollection(settings.Select(s => KeyValuePair.Create($"Destinations:0:{s.Key}", s.Value)))
            .Build();
        var factory = new HttpDestinationFactory(NullLoggerFactory.Instance, new HttpCredentialSection(new RelayConfiguration(config)));
        return (factory, Assert.Single(RelayOptionsBinder.Bind(config).Destinations));
    }

    private static IServiceCollection WithRelayConfiguration()
    {
        var services = new ServiceCollection();
        var section = new ConfigurationBuilder().AddInMemoryCollection(KafkaSettings()).Build().GetSection(RelayConfiguration.SectionName);
        services.TryAddSingleton(new RelayConfiguration(section));
        return services;
    }

    private static object? CredentialsOf(HttpDestinationFactory factory) =>
        typeof(HttpDestinationFactory).GetField("credentials", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(factory);
}
