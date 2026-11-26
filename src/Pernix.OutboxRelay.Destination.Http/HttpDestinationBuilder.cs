using Microsoft.Extensions.DependencyInjection;

namespace Pernix.OutboxRelay.Destination.Http;

public sealed class HttpDestinationBuilder
{
    private readonly IServiceCollection services;

    internal HttpDestinationBuilder(IServiceCollection services) => this.services = services;

    public HttpDestinationBuilder AddRequestMapper<T>(string name)
        where T : class, IHttpRequestMapper
    {
        Add<IHttpRequestMapper, T>(name, "request mapper");
        return this;
    }

    public HttpDestinationBuilder AddResponseClassifier<T>(string name)
        where T : class, IHttpResponseClassifier
    {
        Add<IHttpResponseClassifier, T>(name, "response classifier");
        return this;
    }

    // Same name and type again is a no-op. Same name with another type throws: last-wins would hide a host bug.
    private void Add<TService, TImplementation>(string name, string kind)
        where TService : class
        where TImplementation : class, TService
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);

        foreach (var existing in services)
        {
            if (existing.ServiceType != typeof(TService) || !existing.IsKeyedService || !Equals(existing.ServiceKey, name))
            {
                continue;
            }

            if (existing.KeyedImplementationType == typeof(TImplementation))
            {
                return;
            }

            throw new InvalidOperationException(
                $"Http {kind} '{name}' is already registered as '{existing.KeyedImplementationType?.FullName ?? "a factory"}'");
        }

        services.AddKeyedSingleton<TService, TImplementation>(name);
    }
}

internal sealed class HttpHooks(
    HttpHookLookup lookup,
    Func<string, IHttpRequestMapper?> mapper,
    Func<string, IHttpResponseClassifier?> classifier)
{
    public static HttpHooks None { get; } = From(HttpHookLookup.None);

    public HttpHookLookup Lookup { get; } = lookup;

    public static HttpHooks From(HttpHookLookup lookup) => new(lookup, static _ => null, static _ => null);

    public static HttpHooks From(IServiceProvider services)
    {
        var keyed = services.GetService<IServiceProviderIsKeyedService>();
        return new HttpHooks(
            new HttpHookLookup(
                n => keyed?.IsKeyedService(typeof(IHttpRequestMapper), n) == true,
                n => keyed?.IsKeyedService(typeof(IHttpResponseClassifier), n) == true),
            n => services.GetKeyedService<IHttpRequestMapper>(n),
            n => services.GetKeyedService<IHttpResponseClassifier>(n));
    }

    public IHttpRequestMapper? Mapper(string name) => mapper(name);

    public IHttpResponseClassifier? Classifier(string name) => classifier(name);
}
