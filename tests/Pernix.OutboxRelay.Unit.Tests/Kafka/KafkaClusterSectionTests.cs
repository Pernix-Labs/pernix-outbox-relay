using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Destination.Kafka;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Kafka;

public sealed class KafkaClusterSectionTests
{
    private static (KafkaClusterSection Section, IConfiguration Config) Bind(Dictionary<string, string?> settings)
    {
        var config = new ConfigurationBuilder().AddInMemoryCollection(settings).Build();
        return (new KafkaClusterSection(new RelayConfiguration(config)), config);
    }

    [Fact]
    public void Absent_section_binds_empty()
    {
        var (section, config) = Bind([]);

        Assert.Empty(section.Validate(config));
        Assert.False(section.TryGet("Main", out _));
    }

    [Fact]
    public void Kafka_clusters_bind_case_insensitively_with_producer_settings()
    {
        var (section, config) = Bind(new()
        {
            ["KafkaClusters:Main:BootstrapServers"] = "b:9092",
            ["KafkaClusters:Main:Producer:acks"] = "all",
        });

        Assert.Empty(section.Validate(config));
        Assert.True(section.TryGet("MAIN", out var cluster));
        Assert.Equal("b:9092", cluster.BootstrapServers);
        Assert.Equal("all", cluster.Producer["acks"]);
    }

    [Fact]
    public void Free_form_producer_keys_are_not_unknown_settings()
    {
        var (section, config) = Bind(new()
        {
            ["KafkaClusters:Main:BootstrapServers"] = "b:9092",
            ["KafkaClusters:Main:Producer:sasl.mechanism"] = "PLAIN",
            ["KafkaClusters:Main:Producer:any:nested"] = "x",
        });

        Assert.Empty(section.Validate(config));
        Assert.True(section.TryGet("Main", out var cluster));
        Assert.Equal("PLAIN", cluster.Producer["sasl.mechanism"]);
    }

    [Fact]
    public void Unknown_key_fails_naming_the_key_and_not_the_value()
    {
        var (section, config) = Bind(new()
        {
            ["KafkaClusters:Main:BootstrapServer"] = "sentinel-unknown",
        });

        var failures = section.Validate(config);

        Assert.Contains("KafkaClusters[Main]: unknown setting 'BootstrapServer'", failures);
        Assert.All(failures, f => Assert.DoesNotContain("sentinel-unknown", f));
        Assert.True(section.TryGet("Main", out var cluster));
        Assert.Null(cluster.BootstrapServers);
    }

    [Fact]
    public void Repeat_validate_returns_same_failures()
    {
        var (section, config) = Bind(new()
        {
            ["KafkaClusters:Main:BootstrapServer"] = "x",
            ["KafkaClusters:Other:Unknown"] = "y",
        });

        var first = section.Validate(config);

        Assert.Equal(["KafkaClusters[Main]: unknown setting 'BootstrapServer'", "KafkaClusters[Other]: unknown setting 'Unknown'"], first);
        Assert.Equal(first, section.Validate(config));
        Assert.Equal(first, section.Validate(new ConfigurationBuilder().Build()));
    }

    [Fact]
    public void Try_get_returns_bound_clusters_without_validate()
    {
        var (section, _) = Bind(new()
        {
            ["KafkaClusters:Main:BootstrapServers"] = "b:9092",
        });

        Assert.True(section.TryGet("main", out var cluster));
        Assert.Equal("b:9092", cluster.BootstrapServers);
    }
}
