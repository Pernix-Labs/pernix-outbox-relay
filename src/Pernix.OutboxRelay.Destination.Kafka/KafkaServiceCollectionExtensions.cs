using Pernix.OutboxRelay.Core.Abstractions;
using Microsoft.Extensions.DependencyInjection;

namespace Pernix.OutboxRelay.Destination.Kafka;

public static class KafkaServiceCollectionExtensions
{
    public static IServiceCollection AddKafkaTransport(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddSingleton<IDestinationFactory, KafkaDestinationFactory>();
        return services;
    }
}
