using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Kafka;
using Pernix.OutboxRelay.PostgreSql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Configuration;

// docs/runbook.md § 2 embeds docs/runbook-appsettings.example.json verbatim.
public sealed class RunbookExampleTests
{
    private static readonly string DocsDirectory = Path.Combine(RepositoryRoot(), "docs");
    private static readonly string ExamplePath = Path.Combine(DocsDirectory, "runbook-appsettings.example.json");

    [Fact]
    public void Example_binds_and_validates_with_no_failures()
    {
        var configuration = new ConfigurationBuilder().AddJsonFile(ExamplePath, optional: false, reloadOnChange: false).Build();
        var provider = new PostgreSqlOutboxStoreProvider(new RelayConfiguration(configuration));

        // The connection string comes from SSM or an env var in a deployment, never from the example file.
        var failures = RelayConfigurationValidator.Validate(
            RelayOptionsBinder.Bind(configuration),
            name => name == "Main" ? "Host=outbox-db;Database=riskbackoffice;Username=outbox_relay" : null,
            new Dictionary<string, IOutboxStoreProvider>(StringComparer.OrdinalIgnoreCase) { [provider.Name] = provider },
            new DestinationFactoryResolver([new KafkaDestinationFactory(NullLoggerFactory.Instance)]));

        Assert.Empty(failures);
    }

    [Fact]
    public void Example_has_one_PerKey_outbox_dead_lettering_to_a_second_Kafka_destination()
    {
        var configuration = new ConfigurationBuilder().AddJsonFile(ExamplePath, optional: false, reloadOnChange: false).Build();
        var options = RelayOptionsBinder.Bind(configuration);

        var outbox = Assert.Single(options.Outboxes);
        Assert.Equal(OrderingMode.PerKey, outbox.Ordering!.Mode);
        Assert.Equal("Snapshots", outbox.Destination);
        Assert.Equal("SnapshotsDlq", outbox.DeadLetter!.Destination);
        Assert.Equal(["Snapshots", "SnapshotsDlq"], options.Destinations.Select(d => d.Name));
        Assert.All(options.Destinations, d => Assert.Equal("Kafka", d.Transport));
        Assert.Null(configuration.GetConnectionString("Main"));
    }

    [Fact]
    public void Runbook_embeds_the_example_verbatim()
    {
        var example = File.ReadAllText(ExamplePath).ReplaceLineEndings("\n").TrimEnd('\n');
        var runbook = File.ReadAllText(Path.Combine(DocsDirectory, "runbook.md")).ReplaceLineEndings("\n");

        Assert.Contains("```json\n" + example + "\n```", runbook, StringComparison.Ordinal);
    }

    private static string RepositoryRoot()
    {
        for (var directory = new DirectoryInfo(AppContext.BaseDirectory); directory is not null; directory = directory.Parent)
        {
            if (File.Exists(Path.Combine(directory.FullName, "OutboxRelay.slnx")))
            {
                return directory.FullName;
            }
        }

        throw new InvalidOperationException($"OutboxRelay.slnx not found above {AppContext.BaseDirectory}.");
    }
}
