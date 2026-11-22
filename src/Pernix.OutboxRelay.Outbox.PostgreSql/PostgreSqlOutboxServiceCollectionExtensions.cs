using Pernix.OutboxRelay.Core.Abstractions;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;

namespace Pernix.OutboxRelay.Outbox.PostgreSql;

public static class PostgreSqlOutboxServiceCollectionExtensions
{
    public static IServiceCollection AddPostgreSqlOutbox(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddEnumerable(ServiceDescriptor.Singleton<IOutboxStoreProvider, PostgreSqlOutboxStoreProvider>());
        return services;
    }

    public static TBuilder AddPostgreSqlOutbox<TBuilder>(this TBuilder builder)
        where TBuilder : IHostApplicationBuilder
    {
        ArgumentNullException.ThrowIfNull(builder);

        builder.Services.AddPostgreSqlOutbox();
        return builder;
    }
}
