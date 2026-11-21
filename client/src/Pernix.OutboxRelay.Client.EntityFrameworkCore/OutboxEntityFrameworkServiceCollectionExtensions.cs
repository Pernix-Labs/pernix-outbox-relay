using Microsoft.EntityFrameworkCore;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace Pernix.OutboxRelay.Client.EntityFrameworkCore;

/// <summary>Registers the <see cref="IOutboxQueue{TContext}"/> facade.</summary>
public static class OutboxEntityFrameworkServiceCollectionExtensions
{
    /// <summary>Registers a scoped <see cref="IOutboxQueue{TContext}"/> unless one is already registered.</summary>
    /// <remarks>
    /// <typeparamref name="TContext"/> must be registered scoped (<c>AddDbContext</c>, <c>AddDbContextPool</c>).
    /// Contexts created by <c>IDbContextFactory&lt;TContext&gt;</c> call <c>EnqueueOutbox</c> directly.
    /// </remarks>
    /// <typeparam name="TContext">A context type configured with <c>UseOutbox</c>.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <returns><paramref name="services"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is null.</exception>
    public static IServiceCollection AddOutboxQueue<TContext>(this IServiceCollection services)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(services);

        services.TryAddScoped<IOutboxQueue<TContext>, OutboxQueueFacade<TContext>>();
        return services;
    }

    /// <summary>Registers a keyed scoped <see cref="IOutboxQueue{TContext}"/> under <paramref name="key"/> that writes <paramref name="route"/> on every message.</summary>
    /// <remarks>
    /// A message without a route gets <paramref name="route"/>, a message with the same route is queued as is, a message with another route throws <see cref="ArgumentException"/> and nothing is queued.
    /// Targets the scope's <typeparamref name="TContext"/> instance and works next to <see cref="AddOutboxQueue{TContext}"/> and other keyed queues. A later registration under the same key replaces this one.
    /// </remarks>
    /// <typeparam name="TContext">A context type configured with <c>UseOutbox</c> and registered scoped.</typeparam>
    /// <param name="services">The service collection.</param>
    /// <param name="key">The service key.</param>
    /// <param name="route">The route written on every message queued through the keyed queue.</param>
    /// <returns><paramref name="services"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/>, <paramref name="key"/> or <paramref name="route"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="route"/> is outside <c>^[a-z0-9][a-z0-9._-]{0,62}$</c>.</exception>
    public static IServiceCollection AddKeyedOutboxQueue<TContext>(this IServiceCollection services, object key, string route)
        where TContext : DbContext
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(key);
        RouteRules.Validate(route, nameof(route));

        services.AddKeyedScoped<IOutboxQueue<TContext>>(
            key, (provider, _) => new RouteScopedOutboxQueue<TContext>(provider.GetRequiredService<TContext>(), route));
        return services;
    }
}
