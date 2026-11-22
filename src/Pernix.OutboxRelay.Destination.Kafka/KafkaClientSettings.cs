using System.Globalization;
using Pernix.OutboxRelay.Core.Configuration;
using Confluent.Kafka;

namespace Pernix.OutboxRelay.Destination.Kafka;

internal sealed class KafkaClientSettings
{
    internal const int DefaultMessageMaxBytes = 1_000_000;

    private const string ProducerOnlyPrefix = "dotnet.producer.";

    internal static readonly IReadOnlyDictionary<OrderingMode, IReadOnlyDictionary<string, string>> Forced =
        new Dictionary<OrderingMode, IReadOnlyDictionary<string, string>>
        {
            [OrderingMode.None] = ForcedSet(lingerMs: "5", messageTimeoutMs: "30000"),
            [OrderingMode.PerKey] = ForcedSet(lingerMs: "5", messageTimeoutMs: "0"),
            [OrderingMode.Strict] = ForcedSet(lingerMs: "0", messageTimeoutMs: "0"),
        };

    // librdkafka aliases of forced keys, mapped to the canonical name so the forced Set wins in either order.
    internal static readonly IReadOnlyDictionary<string, string> ForcedAliases = new Dictionary<string, string>(StringComparer.Ordinal)
    {
        ["queue.buffering.max.ms"] = "linger.ms",
        ["delivery.timeout.ms"] = "message.timeout.ms",
        ["request.required.acks"] = "acks",
        ["metadata.broker.list"] = "bootstrap.servers",
    };

    private KafkaClientSettings(ProducerConfig producer, AdminClientConfig admin, int messageMaxBytes)
    {
        Producer = producer;
        Admin = admin;
        MessageMaxBytes = messageMaxBytes;
    }

    public ProducerConfig Producer { get; }

    public AdminClientConfig Admin { get; }

    public int MessageMaxBytes { get; }

    public static KafkaClientSettings Build(KafkaClusterOptions cluster, OrderingMode mode)
    {
        ArgumentNullException.ThrowIfNull(cluster);

        if (!Forced.TryGetValue(mode, out var forced))
        {
            throw new ArgumentOutOfRangeException(nameof(mode), mode, "Unknown Ordering.Mode.");
        }

        var producer = new ProducerConfig();
        var admin = new AdminClientConfig();
        foreach (var (entry, value) in cluster.Producer)
        {
            var key = ForcedAliases.TryGetValue(entry, out var canonical) ? canonical : entry;
            producer.Set(key, value);

            // AdminClient rejects producer-only properties.
            if (!key.StartsWith(ProducerOnlyPrefix, StringComparison.Ordinal))
            {
                admin.Set(key, value);
            }
        }

        producer.Set("bootstrap.servers", cluster.BootstrapServers);
        admin.Set("bootstrap.servers", cluster.BootstrapServers);
        foreach (var (key, value) in forced)
        {
            producer.Set(key, value);
        }

        var messageMaxBytes = int.TryParse(producer.Get("message.max.bytes"), NumberStyles.None, CultureInfo.InvariantCulture, out var parsed) && parsed > 0
            ? parsed
            : DefaultMessageMaxBytes;
        return new KafkaClientSettings(producer, admin, messageMaxBytes);
    }

    private static Dictionary<string, string> ForcedSet(string lingerMs, string messageTimeoutMs) => new(StringComparer.Ordinal)
    {
        ["enable.idempotence"] = "true",
        ["acks"] = "all",
        ["partitioner"] = "murmur2_random",
        ["linger.ms"] = lingerMs,
        ["message.timeout.ms"] = messageTimeoutMs,
        ["dotnet.producer.delivery.report.fields"] = "status",
        ["dotnet.producer.enable.delivery.reports"] = "true",
    };
}
