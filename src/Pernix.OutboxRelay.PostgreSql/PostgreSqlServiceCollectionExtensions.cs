using Pernix.OutboxRelay.Core.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Pernix.OutboxRelay.PostgreSql;

public static class PostgreSqlServiceCollectionExtensions
{
    public static IServiceCollection AddPostgreSqlOutboxProvider(this IServiceCollection services)
    {
        services.TryAddEnumerable(ServiceDescriptor.Singleton<IOutboxStoreProvider, PostgreSqlOutboxStoreProvider>());
        return services;
    }
}
