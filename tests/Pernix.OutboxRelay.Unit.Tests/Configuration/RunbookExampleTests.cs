using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Destination.Kafka;
using Pernix.OutboxRelay.Outbox.PostgreSql;
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
        var relay = new RelayConfiguration(LoadExample().GetSection(RelayConfiguration.SectionName));
        var provider = new PostgreSqlOutboxStoreProvider(relay);
        var kafka = new KafkaClusterSection(relay);

        // The connection string comes from SSM or an env var in a deployment, never from the example file.
        var failures = RelayConfigurationValidator.Validate(
            RelayOptionsBinder.Bind(relay.Section),
            name => name == "Main" ? "Host=outbox-db;Database=riskbackoffice;Username=outbox_relay" : null,
            new Dictionary<string, IOutboxStoreProvider>(StringComparer.OrdinalIgnoreCase) { [provider.Name] = provider },
            new DestinationFactoryResolver([new KafkaDestinationFactory(NullLoggerFactory.Instance, kafka)]));

        Assert.Empty(kafka.Validate(relay.Section));
        Assert.Empty(failures);
    }

    [Fact]
    public void Example_has_one_PerKey_outbox_dead_lettering_to_a_second_Kafka_destination()
    {
        var section = LoadExample().GetSection(RelayConfiguration.SectionName);
        var options = RelayOptionsBinder.Bind(section);

        var outbox = Assert.Single(options.Outboxes);
        Assert.Equal(OrderingMode.PerKey, outbox.Ordering!.Mode);
        Assert.Equal("Snapshots", outbox.Destination);
        Assert.Equal("SnapshotsDlq", outbox.DeadLetter!.Destination);
        Assert.Equal(["Snapshots", "SnapshotsDlq"], options.Destinations.Select(d => d.Name));
        Assert.All(options.Destinations, d => Assert.Equal("Kafka", d.Transport));
        Assert.Null(section.GetConnectionString("Main"));
    }

    [Fact]
    public void Example_keeps_relay_settings_only_under_the_OutboxRelay_section()
    {
        var configuration = LoadExample();

        Assert.True(configuration.GetSection(RelayConfiguration.SectionName).Exists());
        Assert.DoesNotContain(RootLevelRelayKeys, key => configuration.GetSection(key).Exists());
    }

    [Fact]
    public void Runbook_embeds_the_example_verbatim()
    {
        var example = File.ReadAllText(ExamplePath).ReplaceLineEndings("\n").TrimEnd('\n');
        var runbook = File.ReadAllText(Path.Combine(DocsDirectory, "runbook.md")).ReplaceLineEndings("\n");

        Assert.Contains("```json\n" + example + "\n```", runbook, StringComparison.Ordinal);
    }

    private static readonly string[] RootLevelRelayKeys = ["Outboxes", "Destinations", "KafkaClusters", "ConnectionStrings"];

    private static IConfigurationRoot LoadExample() =>
        new ConfigurationBuilder().AddJsonFile(ExamplePath, optional: false, reloadOnChange: false).Build();

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
