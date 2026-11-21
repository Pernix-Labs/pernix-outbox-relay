using System.Net;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Integration.Tests.Kafka;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.Startup;

public sealed class LegacyConfigurationTests(PostgreSqlFixture postgres, KafkaFixture kafka)
{
    private const string SecretConnection = "Host=127.0.0.1;Port=1;Username=relay;Password=legacy-s3cr3t;Database=relay";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Root_level_relay_config_refuses_to_start_naming_each_key()
    {
        var settings = RelayFactory.OutboxSettings(SecretConnection, "127.0.0.1:1", (RelayFactory.NewOutboxName(), "public"));
        await using var factory = new RelayFactory().WithRootSettings(settings);

        var ex = Record.Exception(() => factory.CreateClient());

        Assert.NotNull(ex);
        var failures = Assert.Single(Flatten(ex).OfType<RelayStartupValidationException>().Distinct()).Failures;
        Assert.Equal(
            [
                "Outboxes: relay settings moved under OutboxRelay; move to OutboxRelay:Outboxes (env OutboxRelay__Outboxes__...)",
                "Destinations: relay settings moved under OutboxRelay; move to OutboxRelay:Destinations (env OutboxRelay__Destinations__...)",
                "KafkaClusters: relay settings moved under OutboxRelay; move to OutboxRelay:KafkaClusters (env OutboxRelay__KafkaClusters__...)",
                "ConnectionStrings.Main: move to OutboxRelay:ConnectionStrings:Main (env OutboxRelay__ConnectionStrings__Main)",
            ],
            failures);
        Assert.Contains("OutboxRelay", ex.Message, StringComparison.Ordinal);
        foreach (var key in new[] { "Outboxes", "Destinations", "KafkaClusters", "ConnectionStrings.Main" })
        {
            Assert.Contains(key, ex.Message, StringComparison.Ordinal);
        }

        Assert.DoesNotContain("legacy-s3cr3t", ex.ToString(), StringComparison.Ordinal);
    }

    // A ShutdownTimeout below the minimum fails RelayStartupValidator. Only guard lines prove the guard ran first.
    [Fact]
    public async Task Guard_fails_before_relay_startup_validator()
    {
        var settings = RelayFactory.OutboxSettings(SecretConnection, "127.0.0.1:1", (RelayFactory.NewOutboxName(), "public"));
        settings["HostOptions:ShutdownTimeout"] = "00:00:01";
        await using var factory = new RelayFactory().WithRootSettings(settings);

        var ex = Record.Exception(() => factory.CreateClient());

        Assert.NotNull(ex);
        var failures = Assert.Single(Flatten(ex).OfType<RelayStartupValidationException>().Distinct()).Failures;
        Assert.DoesNotContain(failures, f => f.StartsWith("HostOptions", StringComparison.Ordinal));
        Assert.Contains(failures, f => f.StartsWith("Outboxes: ", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Root_keys_alongside_OutboxRelay_section_refuse_to_start()
    {
        var relay = RelayFactory.OutboxSettings(postgres.ConnectionString, kafka.BootstrapServers, (RelayFactory.NewOutboxName(), await postgres.CreateOutboxAsync()));
        var legacy = RelayFactory.OutboxSettings(SecretConnection, "127.0.0.1:1", (RelayFactory.NewOutboxName(), "public"));
        await using var factory = new RelayFactory().WithRelaySettings(relay).WithRootSettings(legacy);

        var ex = Record.Exception(() => factory.CreateClient());

        Assert.NotNull(ex);
        var failures = Assert.Single(Flatten(ex).OfType<RelayStartupValidationException>().Distinct()).Failures;
        Assert.Equal(
            [
                "Outboxes: relay settings moved under OutboxRelay; move to OutboxRelay:Outboxes (env OutboxRelay__Outboxes__...)",
                "Destinations: relay settings moved under OutboxRelay; move to OutboxRelay:Destinations (env OutboxRelay__Destinations__...)",
                "KafkaClusters: relay settings moved under OutboxRelay; move to OutboxRelay:KafkaClusters (env OutboxRelay__KafkaClusters__...)",
                "ConnectionStrings.Main: move to OutboxRelay:ConnectionStrings:Main (env OutboxRelay__ConnectionStrings__Main)",
            ],
            failures);
        Assert.DoesNotContain("legacy-s3cr3t", ex.ToString(), StringComparison.Ordinal);
    }

    // Partial migration: only the connection string moved under OutboxRelay, relay definitions still at the root.
    [Fact]
    public async Task Partial_migration_with_only_relay_connection_string_refuses_to_start()
    {
        var legacy = RelayFactory.OutboxSettings(SecretConnection, "127.0.0.1:1", (RelayFactory.NewOutboxName(), "public"));
        legacy.Remove("ConnectionStrings:Main");
        await using var factory = new RelayFactory()
            .WithRelaySettings(new Dictionary<string, string?> { ["ConnectionStrings:Main"] = SecretConnection })
            .WithRootSettings(legacy);

        var ex = Record.Exception(() => factory.CreateClient());

        Assert.NotNull(ex);
        var failures = Assert.Single(Flatten(ex).OfType<RelayStartupValidationException>().Distinct()).Failures;
        Assert.Equal(
            [
                "Outboxes: relay settings moved under OutboxRelay; move to OutboxRelay:Outboxes (env OutboxRelay__Outboxes__...)",
                "Destinations: relay settings moved under OutboxRelay; move to OutboxRelay:Destinations (env OutboxRelay__Destinations__...)",
                "KafkaClusters: relay settings moved under OutboxRelay; move to OutboxRelay:KafkaClusters (env OutboxRelay__KafkaClusters__...)",
            ],
            failures);
        Assert.DoesNotContain("legacy-s3cr3t", ex.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task OutboxRelay_section_without_root_keys_starts_and_is_ready()
    {
        var relay = RelayFactory.OutboxSettings(postgres.ConnectionString, kafka.BootstrapServers, (RelayFactory.NewOutboxName(), await postgres.CreateOutboxAsync()));
        await using var factory = new RelayFactory().WithRelaySettings(relay);
        using var client = factory.CreateClient();

        using var response = await client.GetAsync("/readyz", Ct);

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    private static IEnumerable<Exception> Flatten(Exception ex) =>
        ex is AggregateException aggregate
            ? aggregate.InnerExceptions.SelectMany(Flatten)
            : ex.InnerException is null ? [ex] : [ex, .. Flatten(ex.InnerException)];
}
