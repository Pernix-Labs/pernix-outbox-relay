using Microsoft.Extensions.DependencyInjection;

namespace Pernix.OutboxRelay.Client.PostgreSql;

/// <summary>Registers <see cref="PostgreSqlOutboxPublisher"/> instances as <see cref="IOutboxPublisher"/>.</summary>
public static class PostgreSqlOutboxServiceCollectionExtensions
{
    /// <summary>
    /// Parses <paramref name="table"/> now and registers one <see cref="PostgreSqlOutboxPublisher"/> for it as the
    /// singleton <see cref="IOutboxPublisher"/>.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="table">The outbox table, <c>name</c> or <c>schema.name</c>, as given to the relay's <c>Database:Table</c> setting.</param>
    /// <returns><paramref name="services"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="table"/> is invalid (see <see cref="OutboxTable.Parse"/>).</exception>
    public static IServiceCollection AddPostgreSqlOutboxPublisher(this IServiceCollection services, string table)
    {
        ArgumentNullException.ThrowIfNull(services);
        var publisher = new PostgreSqlOutboxPublisher(OutboxTable.Parse(table));
        return services.AddSingleton<IOutboxPublisher>(publisher);
    }

    /// <summary>
    /// Parses <paramref name="table"/> now and registers one <see cref="PostgreSqlOutboxPublisher"/> for it as the
    /// keyed singleton <see cref="IOutboxPublisher"/> under <paramref name="key"/>. Use one key per outbox table.
    /// </summary>
    /// <param name="services">The service collection.</param>
    /// <param name="key">The service key. Must not be null: a null key would register a non-keyed service.</param>
    /// <param name="table">The outbox table, <c>name</c> or <c>schema.name</c>, as given to the relay's <c>Database:Table</c> setting.</param>
    /// <returns><paramref name="services"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/> or <paramref name="key"/> is null.</exception>
    /// <exception cref="ArgumentException"><paramref name="table"/> is invalid (see <see cref="OutboxTable.Parse"/>).</exception>
    public static IServiceCollection AddKeyedPostgreSqlOutboxPublisher(this IServiceCollection services, object key, string table)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(key);
        var publisher = new PostgreSqlOutboxPublisher(OutboxTable.Parse(table));
        return services.AddKeyedSingleton<IOutboxPublisher>(key, publisher);
    }

    /// <summary>
    /// Parses <paramref name="table"/> now, validates <paramref name="route"/> now, and registers one
    /// <see cref="PostgreSqlOutboxPublisher"/> for them as the keyed singleton <see cref="IOutboxPublisher"/> under
    /// <paramref name="key"/>. The resolved publisher writes <paramref name="route"/> on every message: a message
    /// without a route gets it, a message with the same route is written as is, a message with another route throws
    /// <see cref="ArgumentException"/> and nothing is written.
    /// </summary>
    /// <remarks>Registering the same key again replaces the earlier registration.</remarks>
    /// <param name="services">The service collection.</param>
    /// <param name="key">The service key. Must not be null: a null key would register a non-keyed service.</param>
    /// <param name="table">The outbox table, <c>name</c> or <c>schema.name</c>, as given to the relay's <c>Database:Table</c> setting.</param>
    /// <param name="route">The route written on every message, matching <c>^[a-z0-9][a-z0-9._-]{0,62}$</c>.</param>
    /// <returns><paramref name="services"/>.</returns>
    /// <exception cref="ArgumentNullException"><paramref name="services"/>, <paramref name="key"/> or <paramref name="route"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="route"/> is outside <c>^[a-z0-9][a-z0-9._-]{0,62}$</c>, or <paramref name="table"/> is invalid
    /// (see <see cref="OutboxTable.Parse"/>).
    /// </exception>
    public static IServiceCollection AddKeyedPostgreSqlOutboxPublisher(this IServiceCollection services, object key, string table, string route)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(key);
        RouteRules.Validate(route, nameof(route));
        var publisher = new PostgreSqlOutboxPublisher(OutboxTable.Parse(table), route);
        return services.AddKeyedSingleton<IOutboxPublisher>(key, publisher);
    }
}
