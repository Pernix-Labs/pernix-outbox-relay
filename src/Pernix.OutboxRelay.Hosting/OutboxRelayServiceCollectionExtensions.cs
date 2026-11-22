using Pernix.OutboxRelay.Core;
using Pernix.OutboxRelay.Core.Leadership;
using Pernix.OutboxRelay.Hosting.Health;
using Pernix.OutboxRelay.Destination.Kafka;
using Pernix.OutboxRelay.Outbox.PostgreSql;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace Pernix.OutboxRelay.Hosting;

public static class OutboxRelayServiceCollectionExtensions
{
    // Relay services and health checks only. Logging, OpenTelemetry, Kestrel, endpoints and HostOptions stay with the host.
    public static IServiceCollection AddOutboxRelay(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        // ValidatedOutboxes is registered only by AddOutboxRelayCore. Checked before registering anything.
        if (services.Any(d => d.ServiceType == typeof(ValidatedOutboxes)))
        {
            throw new InvalidOperationException("AddOutboxRelay was already called on this service collection.");
        }

        services.AddOutboxRelayCore().AddPostgreSqlOutboxProvider().AddKafkaTransport();

        services.AddHealthChecks()
            .AddCheck<HostLifetimeReadinessCheck>(OutboxRelayHealthChecks.HostLifetime, tags: [OutboxRelayHealthChecks.ReadyTag])
            .AddCheck<OutboxDatabaseReadinessCheck>(OutboxRelayHealthChecks.OutboxDatabase, tags: [OutboxRelayHealthChecks.ReadyTag])
            .AddCheck<WorkerLoopLivenessCheck>(OutboxRelayHealthChecks.WorkerLoop, tags: [OutboxRelayHealthChecks.LiveTag]);

        return services;
    }

    public static TBuilder AddOutboxRelay<TBuilder>(this TBuilder builder)
        where TBuilder : IHostApplicationBuilder
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Services.AddOutboxRelay();
        return builder;
    }
}
