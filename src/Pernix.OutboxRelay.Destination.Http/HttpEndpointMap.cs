using System.Collections.Frozen;
using Pernix.OutboxRelay.Core.Abstractions;

namespace Pernix.OutboxRelay.Destination.Http;

// {outbox} folds at compile, so each outbox that sends to the destination has its own endpoints.
internal sealed class HttpEndpointMap
{
    private readonly Variant? only;
    private readonly FrozenDictionary<string, Variant>? byOutbox;

    private HttpEndpointMap(Dictionary<string, Variant> variants, FrozenSet<string>? routes)
    {
        if (variants.Count == 1)
        {
            only = variants.Values.Single();
        }
        else
        {
            byOutbox = variants.ToFrozenDictionary(StringComparer.OrdinalIgnoreCase);
        }

        Routes = routes;
        Endpoints = [.. variants.Values.SelectMany(static v => v.Single is { } s ? [s] : v.ByRoute!.Values.AsEnumerable())];
    }

    public FrozenSet<string>? Routes { get; }

    public IReadOnlyList<HttpEndpoint> Endpoints { get; }

    public static HttpEndpointMap ForUrl(IReadOnlyDictionary<string, HttpEndpoint> byOutbox)
    {
        ArgumentNullException.ThrowIfNull(byOutbox);
        if (byOutbox.Count == 0)
        {
            throw new ArgumentException("at least one outbox variant is required", nameof(byOutbox));
        }

        var variants = new Dictionary<string, Variant>(StringComparer.OrdinalIgnoreCase);
        foreach (var (outbox, endpoint) in byOutbox)
        {
            variants.Add(outbox, new Variant(endpoint, null));
        }

        return new HttpEndpointMap(variants, null);
    }

    public static HttpEndpointMap ForRoutes(IReadOnlyDictionary<string, IReadOnlyDictionary<string, HttpEndpoint>> byOutbox)
    {
        ArgumentNullException.ThrowIfNull(byOutbox);
        if (byOutbox.Count == 0)
        {
            throw new ArgumentException("at least one outbox variant is required", nameof(byOutbox));
        }

        var variants = new Dictionary<string, Variant>(StringComparer.OrdinalIgnoreCase);
        FrozenSet<string>? routes = null;
        foreach (var (outbox, endpoints) in byOutbox)
        {
            var byRoute = endpoints.ToFrozenDictionary(StringComparer.Ordinal);
            routes ??= byRoute.Keys.ToFrozenSet(StringComparer.Ordinal);
            if (!routes.SetEquals(byRoute.Keys))
            {
                throw new ArgumentException("every outbox variant must declare the same routes", nameof(byOutbox));
            }

            variants.Add(outbox, new Variant(null, byRoute));
        }

        return new HttpEndpointMap(variants, routes);
    }

    public HttpEndpoint? Resolve(OutgoingMessage message)
    {
        var variant = only;
        if (variant is null && !byOutbox!.TryGetValue(message.Source.Outbox, out variant))
        {
            return null;
        }

        return variant.ByRoute is null
            ? (message.DeadLetter is not null || message.Route is null ? variant.Single : null)
            : (message.DeadLetter is null && message.Route is { } route && variant.ByRoute.TryGetValue(route, out var e) ? e : null);
    }

    private sealed record Variant(HttpEndpoint? Single, FrozenDictionary<string, HttpEndpoint>? ByRoute);
}
