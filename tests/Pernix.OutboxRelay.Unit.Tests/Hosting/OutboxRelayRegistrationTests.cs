using Pernix.OutboxRelay.Core;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Core.Diagnostics;
using Pernix.OutboxRelay.Core.Leadership;
using Pernix.OutboxRelay.Core.Stats;
using Pernix.OutboxRelay.Hosting;
using Pernix.OutboxRelay.Hosting.Health;
using System.Text.Json;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Hosting;

public sealed class OutboxRelayRegistrationTests
{
    [Fact]
    public void Registers_relay_hosted_services()
    {
        var services = new ServiceCollection().AddOutboxRelay();

        var hosted = services.Where(d => d.ServiceType == typeof(IHostedService)).Select(d => d.ImplementationType).ToArray();

        Assert.Contains(typeof(RelayStartupValidator), hosted);
        Assert.Contains(typeof(OutboxWorkerService), hosted);
        Assert.Contains(typeof(OutboxStatsService), hosted);
        Assert.Contains(typeof(LeaderGaugeRegistration), hosted);
    }

    [Fact]
    public void Registers_no_outbox_provider_or_destination_factory()
    {
        var services = new ServiceCollection().AddOutboxRelay();

        Assert.DoesNotContain(services, d => d.ServiceType == typeof(IOutboxStoreProvider));
        Assert.DoesNotContain(services, d => d.ServiceType == typeof(IDestinationFactory));
        Assert.DoesNotContain(services, d => d.ServiceType == typeof(IRelaySectionValidator));
    }

    [Fact]
    public async Task Startup_without_providers_fails_naming_provider_and_transport()
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Configuration.AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["HostOptions:ShutdownTimeout"] = "00:00:30",
            ["OutboxRelay:ConnectionStrings:OrdersDb"] = "Host=127.0.0.1;Port=1;Database=orders;Username=relay;Password=unused",
            ["OutboxRelay:KafkaClusters:Main:BootstrapServers"] = "127.0.0.1:1",
            ["OutboxRelay:Destinations:0:Name"] = "OrdersTopic",
            ["OutboxRelay:Destinations:0:Transport"] = "Kafka",
            ["OutboxRelay:Destinations:0:Kafka:Cluster"] = "Main",
            ["OutboxRelay:Destinations:0:Kafka:Topic"] = "orders.events",
            ["OutboxRelay:Outboxes:0:Name"] = "Orders",
            ["OutboxRelay:Outboxes:0:Destination"] = "OrdersTopic",
            ["OutboxRelay:Outboxes:0:Database:Provider"] = "PostgreSql",
            ["OutboxRelay:Outboxes:0:Database:Connection"] = "OrdersDb",
            ["OutboxRelay:Outboxes:0:Database:Table"] = "public.outbox",
            ["OutboxRelay:Outboxes:0:Ordering:Mode"] = "None",
        });
        builder.Services.Configure<HostOptions>(builder.Configuration.GetSection("HostOptions"));
        builder.AddOutboxRelay();
        using var host = builder.Build();

        var ex = await Assert.ThrowsAsync<RelayStartupValidationException>(() => host.StartAsync(TestContext.Current.CancellationToken));

        Assert.Equal(2, ex.Failures.Count);
        Assert.Contains(ex.Failures, f => f.Contains("provider 'PostgreSql' is not registered", StringComparison.Ordinal));
        Assert.Contains(ex.Failures, f => f.Contains("transport 'Kafka' is not registered", StringComparison.Ordinal));
        Assert.DoesNotContain(ex.Failures, f => f.Contains("KafkaClusters", StringComparison.Ordinal));
    }

    [Fact]
    public void Hosting_references_no_provider_assembly()
    {
        var references = typeof(OutboxRelayServiceCollectionExtensions).Assembly.GetReferencedAssemblies().Select(a => a.Name!);

        Assert.DoesNotContain(references, n => n.StartsWith("Pernix.OutboxRelay.Outbox.", StringComparison.Ordinal));
        Assert.DoesNotContain(references, n => n.StartsWith("Pernix.OutboxRelay.Destination.", StringComparison.Ordinal));
    }

    // Assembly metadata omits a project reference whose types are unused. deps.json keeps it.
    [Fact]
    public void Hosting_project_depends_on_core_only()
    {
        var depsPath = Path.Combine(AppContext.BaseDirectory, "Pernix.OutboxRelay.Unit.Tests.deps.json");
        using var deps = JsonDocument.Parse(File.ReadAllText(depsPath));

        var target = deps.RootElement.GetProperty("targets").EnumerateObject().First().Value;
        var hosting = Assert.Single(target.EnumerateObject(), p => p.Name.StartsWith("Pernix.OutboxRelay.Hosting/", StringComparison.Ordinal));
        var relayDependencies = hosting.Value.GetProperty("dependencies").EnumerateObject()
            .Select(p => p.Name)
            .Where(n => n.StartsWith("Pernix.", StringComparison.Ordinal))
            .ToArray();

        Assert.Equal(["Pernix.OutboxRelay.Core"], relayDependencies);
    }

    [Fact]
    public void Registers_health_checks_with_names_and_tags()
    {
        using var provider = new ServiceCollection().AddOutboxRelay().BuildServiceProvider();

        var registrations = provider.GetRequiredService<IOptions<HealthCheckServiceOptions>>().Value.Registrations
            .ToDictionary(r => r.Name, r => r.Tags.ToArray());

        Assert.Equal(3, registrations.Count);
        Assert.Equal([OutboxRelayHealthChecks.ReadyTag], registrations[OutboxRelayHealthChecks.HostLifetime]);
        Assert.Equal([OutboxRelayHealthChecks.ReadyTag], registrations[OutboxRelayHealthChecks.OutboxDatabase]);
        Assert.Equal([OutboxRelayHealthChecks.LiveTag], registrations[OutboxRelayHealthChecks.WorkerLoop]);
    }

    [Fact]
    public void Builder_overload_registers_on_builder_services()
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());

        var returned = builder.AddOutboxRelay();

        Assert.Same(builder, returned);
        Assert.Contains(builder.Services, d => d.ServiceType == typeof(ValidatedOutboxes));
    }

    [Fact]
    public void Second_call_throws_and_registers_nothing()
    {
        var services = new ServiceCollection().AddOutboxRelay();
        var count = services.Count;

        var ex = Assert.Throws<InvalidOperationException>(() => services.AddOutboxRelay());

        Assert.Equal("AddOutboxRelay was already called on this service collection.", ex.Message);
        Assert.Equal(count, services.Count);
    }

    [Fact]
    public void Call_after_core_registration_throws()
    {
        var services = new ServiceCollection().AddOutboxRelayCore();
        var count = services.Count;

        Assert.Throws<InvalidOperationException>(() => services.AddOutboxRelay());
        Assert.Equal(count, services.Count);
    }

    [Theory]
    [InlineData(typeof(ILoggerProvider))]
    [InlineData(typeof(TracerProvider))]
    [InlineData(typeof(MeterProvider))]
    [InlineData(typeof(IConfigureOptions<KestrelServerOptions>))]
    [InlineData(typeof(IConfigureOptions<LoggerFilterOptions>))]
    public void Does_not_register_host_owned_services(Type serviceType)
    {
        var services = new ServiceCollection().AddOutboxRelay();

        Assert.DoesNotContain(services, d => d.ServiceType == serviceType);
    }
}
