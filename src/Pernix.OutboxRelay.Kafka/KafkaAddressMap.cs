using System.Collections.Frozen;
using Pernix.OutboxRelay.Core.Abstractions;

namespace Pernix.OutboxRelay.Kafka;

internal sealed class KafkaAddressMap
{
    private KafkaAddressMap(
        KafkaAddress? single,
        FrozenDictionary<string, KafkaAddress>? byRoute,
        FrozenSet<string>? routes,
        KafkaAddress[] distinct)
    {
        Single = single;
        ByRoute = byRoute;
        Routes = routes;
        Distinct = distinct;
    }

    public KafkaAddress? Single { get; }

    public FrozenDictionary<string, KafkaAddress>? ByRoute { get; }

    public FrozenSet<string>? Routes { get; }

    // One per distinct topic (Ordinal), first-seen order.
    public KafkaAddress[] Distinct { get; }

    public static KafkaAddressMap ForTopic(string topic)
    {
        var address = new KafkaAddress(topic);
        return new KafkaAddressMap(address, null, null, [address]);
    }

    // Startup only. Callers validate keys and values.
    public static KafkaAddressMap ForRoutes(IReadOnlyDictionary<string, string> routes)
    {
        var byTopic = new Dictionary<string, KafkaAddress>(StringComparer.Ordinal);
        var distinct = new List<KafkaAddress>();
        var byRoute = new Dictionary<string, KafkaAddress>(routes.Count, StringComparer.Ordinal);
        foreach (var (route, topic) in routes)
        {
            if (!byTopic.TryGetValue(topic, out var address))
            {
                address = new KafkaAddress(topic);
                byTopic.Add(topic, address);
                distinct.Add(address);
            }

            byRoute.Add(route, address);
        }

        var frozen = byRoute.ToFrozenDictionary(StringComparer.Ordinal);
        return new KafkaAddressMap(null, frozen, frozen.Keys.ToFrozenSet(StringComparer.Ordinal), [.. distinct]);
    }

    // Null: the send breaks the IDestination contract. Allocates nothing.
    public KafkaAddress? Resolve(OutgoingMessage message) =>
        ByRoute is null
            ? (message.DeadLetter is not null || message.Route is null ? Single : null)
            : (message.DeadLetter is null && message.Route is { } route && ByRoute.TryGetValue(route, out var a) ? a : null);
}
