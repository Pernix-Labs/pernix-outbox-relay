using System.Diagnostics.CodeAnalysis;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Microsoft.Extensions.Configuration;

namespace Pernix.OutboxRelay.Destination.Kafka;

internal sealed class KafkaClusterSection : IRelaySectionValidator
{
    internal const string Key = "KafkaClusters";
    private readonly Dictionary<string, KafkaClusterOptions> clusters = new(StringComparer.OrdinalIgnoreCase);
    private readonly IReadOnlyList<string> failures;

    public KafkaClusterSection(RelayConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        // Elements are bound one by one: binding a whole dictionary silently drops elements that fail to convert.
        var bindFailures = new List<string>();
        foreach (var cluster in configuration.Section.GetSection(Key).GetChildren())
        {
            clusters[cluster.Key] = RelayOptionsBinder.BindSection<KafkaClusterOptions>(cluster, $"{Key}[{cluster.Key}]", bindFailures);
        }

        failures = bindFailures.Distinct(StringComparer.Ordinal).ToList();
    }

    // Bound once in the constructor from RelayConfiguration.Section, the same section RelayStartupValidator passes.
    public IReadOnlyList<string> Validate(IConfiguration section) => failures;

    internal bool TryGet(string name, [NotNullWhen(true)] out KafkaClusterOptions? cluster) => clusters.TryGetValue(name, out cluster);
}
