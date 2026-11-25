using System.Text;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Destination.Kafka;
using Pernix.OutboxRelay.Outbox.PostgreSql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Configuration;

// README.md opens with a minimal appsettings block, the json fence directly after "<!-- config: readme-minimal -->".
public sealed class ReadmeConfigExampleTests
{
    private const string Marker = "<!-- config: readme-minimal -->\n```json\n";
    private const string ConnectionName = "RiskDb";

    private static readonly string ReadmePath = Path.Combine(RepositoryRoot(), "README.md");

    [Fact]
    public void Minimal_example_binds_and_validates_with_no_failures()
    {
        var relay = new RelayConfiguration(LoadExample().GetSection(RelayConfiguration.SectionName));
        var provider = new PostgreSqlOutboxStoreProvider(relay);
        var kafka = new KafkaClusterSection(relay);

        // The connection string comes from the environment variable in the README's docker run command.
        var failures = RelayConfigurationValidator.Validate(
            RelayOptionsBinder.Bind(relay.Section),
            name => name == ConnectionName ? "Host=risk-db;Database=risk;Username=outbox_relay" : null,
            new Dictionary<string, IOutboxStoreProvider>(StringComparer.OrdinalIgnoreCase) { [provider.Name] = provider },
            new DestinationFactoryResolver([new KafkaDestinationFactory(NullLoggerFactory.Instance, kafka)]));

        Assert.Empty(kafka.Validate(relay.Section));
        Assert.Empty(failures);
    }

    [Fact]
    public void Minimal_example_has_one_outbox_and_no_connection_string()
    {
        var section = LoadExample().GetSection(RelayConfiguration.SectionName);
        var options = RelayOptionsBinder.Bind(section);

        var outbox = Assert.Single(options.Outboxes);
        Assert.Equal(ConnectionName, outbox.Database!.Connection);
        Assert.Equal("Kafka", Assert.Single(options.Destinations).Transport);
        Assert.Null(section.GetConnectionString(ConnectionName));
    }

    [Fact]
    public void Readme_passes_the_connection_string_as_the_matching_environment_variable()
    {
        Assert.Contains("-e OutboxRelay__ConnectionStrings__" + ConnectionName + "=", ReadReadme(), StringComparison.Ordinal);
    }

    private static string ReadReadme() => File.ReadAllText(ReadmePath).ReplaceLineEndings("\n");

    private static IConfigurationRoot LoadExample()
    {
        var readme = ReadReadme();
        var start = readme.IndexOf(Marker, StringComparison.Ordinal);
        Assert.True(start >= 0, "README.md: marker '" + Marker.Split('\n')[0] + "' followed by a json fence not found.");
        start += Marker.Length;
        var end = readme.IndexOf("\n```", start, StringComparison.Ordinal);
        Assert.True(end > start, "README.md: unterminated json fence after the readme-minimal marker.");

        var json = new MemoryStream(Encoding.UTF8.GetBytes(readme[start..end]));
        return new ConfigurationBuilder().AddJsonStream(json).Build();
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
