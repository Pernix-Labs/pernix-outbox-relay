using System.Reflection;
using Pernix.OutboxRelay.Core;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Core.Leadership;
using Pernix.OutboxRelay.Destination.Kafka;
using Pernix.OutboxRelay.Hosting;
using Pernix.OutboxRelay.Outbox.PostgreSql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Registration;

public sealed class ProviderRegistrationTests
{
    private static readonly Dictionary<string, string?> ValidSettings = new()
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
    };

    // Graph of AddOutboxRelay() before Hosting dropped the providers, as (service type, lifetime).
    private static readonly (Type Service, ServiceLifetime Lifetime)[] PinnedGraph =
    [
        (typeof(RelayConfiguration), ServiceLifetime.Singleton),
        (typeof(ValidatedOutboxes), ServiceLifetime.Singleton),
        (typeof(OutboxRuntimeState), ServiceLifetime.Singleton),
        (typeof(ILeadershipJitter), ServiceLifetime.Singleton),
        (typeof(TimeProvider), ServiceLifetime.Singleton),
        (typeof(IHostedService), ServiceLifetime.Singleton),
        (typeof(IHostedService), ServiceLifetime.Singleton),
        (typeof(IHostedService), ServiceLifetime.Singleton),
        (typeof(IHostedService), ServiceLifetime.Singleton),
        (typeof(IHostedService), ServiceLifetime.Singleton),
        (typeof(IOutboxStoreProvider), ServiceLifetime.Singleton),
        (typeof(KafkaClusterSection), ServiceLifetime.Singleton),
        (typeof(IRelaySectionValidator), ServiceLifetime.Singleton),
        (typeof(IDestinationFactory), ServiceLifetime.Singleton),
        (typeof(HealthCheckService), ServiceLifetime.Singleton),
        (typeof(IOptions<>), ServiceLifetime.Singleton),
        (typeof(IOptionsSnapshot<>), ServiceLifetime.Scoped),
        (typeof(IOptionsMonitor<>), ServiceLifetime.Singleton),
        (typeof(IOptionsFactory<>), ServiceLifetime.Transient),
        (typeof(IOptionsMonitorCache<>), ServiceLifetime.Singleton),
        (typeof(IConfigureOptions<HealthCheckServiceOptions>), ServiceLifetime.Singleton),
        (typeof(IConfigureOptions<HealthCheckServiceOptions>), ServiceLifetime.Singleton),
        (typeof(IConfigureOptions<HealthCheckServiceOptions>), ServiceLifetime.Singleton),
    ];

    private static readonly string[] PinnedHostedServiceOrder =
    [
        "Pernix.OutboxRelay.Core.Configuration.RelayStartupValidator",
        "Pernix.OutboxRelay.Core.Leadership.OutboxWorkerService",
        "Pernix.OutboxRelay.Core.Stats.OutboxStatsService",
        "Pernix.OutboxRelay.Core.Diagnostics.LeaderGaugeRegistration",
        "Microsoft.Extensions.Diagnostics.HealthChecks.HealthCheckPublisherHostedService",
    ];

    [Fact]
    public void AddPostgreSqlOutbox_registers_postgresql_provider()
    {
        using var provider = WithRelayConfiguration().AddPostgreSqlOutbox().BuildServiceProvider();

        var store = Assert.IsType<PostgreSqlOutboxStoreProvider>(Assert.Single(provider.GetServices<IOutboxStoreProvider>()));
        Assert.Equal("PostgreSql", store.Name);
    }

    [Fact]
    public void AddKafkaDestination_registers_factory_and_its_cluster_section_as_validator()
    {
        using var provider = WithRelayConfiguration().AddLogging().AddKafkaDestination().BuildServiceProvider();

        var factory = Assert.IsType<KafkaDestinationFactory>(Assert.Single(provider.GetServices<IDestinationFactory>()));
        var validator = Assert.Single(provider.GetServices<IRelaySectionValidator>());
        Assert.Equal("Kafka", factory.Transport);
        Assert.Same(ClustersOf(factory), validator);
        Assert.Same(provider.GetRequiredService<KafkaClusterSection>(), validator);
    }

    [Fact]
    public void Repeat_registration_adds_no_descriptor()
    {
        var postgreSql = new ServiceCollection().AddPostgreSqlOutbox();
        var postgreSqlCount = postgreSql.Count;
        var kafka = new ServiceCollection().AddKafkaDestination();
        var kafkaCount = kafka.Count;

        postgreSql.AddPostgreSqlOutbox();
        kafka.AddKafkaDestination();

        Assert.Equal(postgreSqlCount, postgreSql.Count);
        Assert.Equal(kafkaCount, kafka.Count);
    }

    // A pre-cancelled token stops at the first store validation, which runs only after the configuration stage passes.
    [Fact]
    public async Task Repeat_registration_with_valid_config_passes_configuration_stage()
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());
        builder.Configuration.AddInMemoryCollection(ValidSettings);
        builder.Services.Configure<HostOptions>(builder.Configuration.GetSection("HostOptions"));
        builder.AddOutboxRelay().AddPostgreSqlOutbox().AddKafkaDestination().AddPostgreSqlOutbox().AddKafkaDestination();
        using var host = builder.Build();
        var validator = host.Services.GetServices<IHostedService>().OfType<RelayStartupValidator>().Single();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => validator.StartingAsync(new CancellationToken(canceled: true)));
    }

    [Fact]
    public void Registration_without_AddOutboxRelay_does_not_throw()
    {
        var ex = Record.Exception(() => new ServiceCollection().AddPostgreSqlOutbox().AddKafkaDestination());

        Assert.Null(ex);
    }

    [Fact]
    public void Builder_overloads_return_the_same_builder()
    {
        var builder = Microsoft.Extensions.Hosting.Host.CreateEmptyApplicationBuilder(new HostApplicationBuilderSettings());

        Assert.Same(builder, builder.AddPostgreSqlOutbox());
        Assert.Same(builder, builder.AddKafkaDestination());
        Assert.Contains(builder.Services, d => d.ServiceType == typeof(IOutboxStoreProvider));
        Assert.Contains(builder.Services, d => d.ServiceType == typeof(IDestinationFactory));
    }

    [Fact]
    public void Full_chain_matches_the_pinned_graph()
    {
        var services = new ServiceCollection().AddOutboxRelay().AddPostgreSqlOutbox().AddKafkaDestination();

        Assert.Equal(Keys(PinnedGraph), Keys(services.Select(d => (d.ServiceType, d.Lifetime))));
        Assert.Equal(
            PinnedHostedServiceOrder,
            services.Where(d => d.ServiceType == typeof(IHostedService)).Select(d => d.ImplementationType?.FullName));
    }

    private static IServiceCollection WithRelayConfiguration()
    {
        var services = new ServiceCollection();
        var section = new ConfigurationBuilder().AddInMemoryCollection(ValidSettings).Build().GetSection(RelayConfiguration.SectionName);
        services.TryAddSingleton(new RelayConfiguration(section));
        return services;
    }

    private static string[] Keys(IEnumerable<(Type Service, ServiceLifetime Lifetime)> graph) =>
        graph.Select(e => $"{e.Service.FullName}|{e.Lifetime}").Order(StringComparer.Ordinal).ToArray();

    private static object? ClustersOf(KafkaDestinationFactory factory) =>
        typeof(KafkaDestinationFactory).GetField("clusters", BindingFlags.Instance | BindingFlags.NonPublic)!.GetValue(factory);
}
