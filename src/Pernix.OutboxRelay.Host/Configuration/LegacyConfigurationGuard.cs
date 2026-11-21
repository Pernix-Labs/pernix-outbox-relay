using System.Runtime.CompilerServices;
using Pernix.OutboxRelay.Core.Configuration;

[assembly: InternalsVisibleTo("Pernix.OutboxRelay.Unit.Tests")]

namespace Pernix.OutboxRelay.Host.Configuration;

// Pre-migration root-level config would otherwise start the image with zero outboxes.
public sealed class LegacyConfigurationGuard(IConfiguration configuration) : IHostedLifecycleService
{
    private static readonly string[] LegacyKeys = ["Outboxes", "Destinations", "KafkaClusters"];

    public Task StartingAsync(CancellationToken cancellationToken)
    {
        var failures = Find(configuration);
        return failures.Count == 0 ? Task.CompletedTask : throw new RelayStartupValidationException(failures);
    }

    public Task StartAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StartedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StoppingAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public Task StoppedAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    internal static IReadOnlyList<string> Find(IConfiguration root)
    {
        if (root.GetSection(RelayConfiguration.SectionName).Exists())
        {
            return [];
        }

        var failures = new List<string>();
        foreach (var key in LegacyKeys.Where(k => root.GetSection(k).Exists()))
        {
            failures.Add($"{key}: relay settings moved under OutboxRelay; move to OutboxRelay:{key} (env OutboxRelay__{key}__...)");
        }

        var names = root.GetSection("Outboxes").GetChildren()
            .Select(outbox => outbox["Database:Connection"])
            .OfType<string>()
            .Where(name => name.Length > 0 && root.GetSection($"ConnectionStrings:{name}").Exists())
            .Distinct(StringComparer.OrdinalIgnoreCase);
        foreach (var name in names)
        {
            failures.Add($"ConnectionStrings.{name}: move to OutboxRelay:ConnectionStrings:{name} (env OutboxRelay__ConnectionStrings__{name})");
        }

        return failures;
    }
}
