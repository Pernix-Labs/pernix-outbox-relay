using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Core.Leadership;
using Pernix.OutboxRelay.Core.Stats;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Pernix.OutboxRelay.Core;

public static class OutboxRelayCoreServiceCollectionExtensions
{
    public static IServiceCollection AddOutboxRelayCore(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.AddSingleton<ValidatedOutboxes>();
        services.AddSingleton<OutboxRuntimeState>();
        services.TryAddSingleton<ILeadershipJitter, RandomLeadershipJitter>();
        services.TryAddSingleton(TimeProvider.System);

        services.AddHostedService<RelayStartupValidator>();
        services.AddHostedService<OutboxWorkerService>();
        services.AddHostedService<OutboxStatsService>();
        return services;
    }
}
