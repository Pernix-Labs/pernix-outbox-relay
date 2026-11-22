using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Pernix.OutboxRelay.Destination.Kafka;

public static class KafkaDestinationServiceCollectionExtensions
{
    public static IServiceCollection AddKafkaDestination(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddSingleton(sp => new KafkaClusterSection(sp.GetRequiredService<RelayConfiguration>()));
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IRelaySectionValidator, KafkaClusterSection>(
            sp => sp.GetRequiredService<KafkaClusterSection>()));
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IDestinationFactory, KafkaDestinationFactory>(
            sp => new KafkaDestinationFactory(sp.GetRequiredService<ILoggerFactory>(), sp.GetRequiredService<KafkaClusterSection>())));
        return services;
    }

    public static TBuilder AddKafkaDestination<TBuilder>(this TBuilder builder)
        where TBuilder : IHostApplicationBuilder
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Services.AddKafkaDestination();
        return builder;
    }
}
