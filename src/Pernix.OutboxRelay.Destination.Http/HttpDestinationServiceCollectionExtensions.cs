using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;

namespace Pernix.OutboxRelay.Destination.Http;

public static class HttpDestinationServiceCollectionExtensions
{
    public static IServiceCollection AddHttpDestination(this IServiceCollection services, Action<HttpDestinationBuilder>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddSingleton(sp => new HttpCredentialSection(sp.GetRequiredService<RelayConfiguration>()));
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IRelaySectionValidator, HttpCredentialSection>(
            sp => sp.GetRequiredService<HttpCredentialSection>()));
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IDestinationFactory, HttpDestinationFactory>(
            sp => new HttpDestinationFactory(
                sp.GetRequiredService<ILoggerFactory>(), sp.GetRequiredService<HttpCredentialSection>(), HttpHooks.From(sp))));
        configure?.Invoke(new HttpDestinationBuilder(services));
        return services;
    }

    public static TBuilder AddHttpDestination<TBuilder>(this TBuilder builder, Action<HttpDestinationBuilder>? configure = null)
        where TBuilder : IHostApplicationBuilder
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Services.AddHttpDestination(configure);
        return builder;
    }
}
