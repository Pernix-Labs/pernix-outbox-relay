using Pernix.OutboxRelay.Core;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Core.Diagnostics;
using Pernix.OutboxRelay.Core.Leadership;
using Pernix.OutboxRelay.Core.Stats;
using Pernix.OutboxRelay.Hosting;
using Pernix.OutboxRelay.Hosting.Health;
using Pernix.OutboxRelay.Kafka;
using Pernix.OutboxRelay.PostgreSql;
using Microsoft.AspNetCore.Server.Kestrel.Core;
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
    public void Registers_postgresql_provider_and_kafka_factory()
    {
        var services = new ServiceCollection().AddOutboxRelay();

        Assert.Contains(services, d => d.ServiceType == typeof(IOutboxStoreProvider) && d.ImplementationType == typeof(PostgreSqlOutboxStoreProvider));
        Assert.Contains(services, d => d.ServiceType == typeof(IDestinationFactory) && d.ImplementationType == typeof(KafkaDestinationFactory));
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
