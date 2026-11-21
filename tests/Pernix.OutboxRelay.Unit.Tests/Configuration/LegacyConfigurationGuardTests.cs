using Pernix.OutboxRelay.Host.Configuration;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Configuration;

public sealed class LegacyConfigurationGuardTests
{
    private const string Secret = "Host=db;Username=relay;Password=s3cr3t-value";

    [Theory]
    [InlineData("Outboxes:0:Name", "Outboxes")]
    [InlineData("Destinations:0:Name", "Destinations")]
    [InlineData("KafkaClusters:Main:BootstrapServers", "KafkaClusters")]
    public void Find_reports_each_legacy_key(string setting, string key)
    {
        var failures = LegacyConfigurationGuard.Find(Config((setting, "x")));

        Assert.Equal([$"{key}: relay settings moved under OutboxRelay; move to OutboxRelay:{key} (env OutboxRelay__{key}__...)"], failures);
    }

    [Fact]
    public void Find_reports_referenced_root_connection_string()
    {
        var failures = LegacyConfigurationGuard.Find(Config(
            ("Outboxes:0:Database:Connection", "Main"),
            ("Outboxes:1:Database:Connection", "Main"),
            ("ConnectionStrings:Main", Secret)));

        Assert.Equal(
            [
                "Outboxes: relay settings moved under OutboxRelay; move to OutboxRelay:Outboxes (env OutboxRelay__Outboxes__...)",
                "ConnectionStrings.Main: move to OutboxRelay:ConnectionStrings:Main (env OutboxRelay__ConnectionStrings__Main)",
            ],
            failures);
    }

    [Fact]
    public void Find_ignores_unreferenced_or_missing_connection_strings()
    {
        var failures = LegacyConfigurationGuard.Find(Config(
            ("Outboxes:0:Database:Connection", "Missing"),
            ("ConnectionStrings:Other", Secret)));

        Assert.DoesNotContain(failures, f => f.StartsWith("ConnectionStrings.", StringComparison.Ordinal));
    }

    [Fact]
    public void Find_reports_root_keys_when_OutboxRelay_section_exists()
    {
        var failures = LegacyConfigurationGuard.Find(Config(
            ("OutboxRelay:Outboxes:0:Name", "orders"),
            ("Outboxes:0:Database:Connection", "Main"),
            ("Destinations:0:Name", "Events"),
            ("KafkaClusters:Main:BootstrapServers", "kafka:9092"),
            ("ConnectionStrings:Main", Secret)));

        Assert.Equal(AllRootKeyFailures, failures);
        Assert.All(failures, f => Assert.DoesNotContain("s3cr3t", f, StringComparison.Ordinal));
    }

    // Partial migration: only the connection string moved, relay definitions still at the root.
    [Fact]
    public void Find_reports_root_keys_when_only_relay_connection_string_migrated()
    {
        var failures = LegacyConfigurationGuard.Find(Config(
            ("OutboxRelay:ConnectionStrings:Main", Secret),
            ("Outboxes:0:Database:Connection", "Main"),
            ("Destinations:0:Name", "Events"),
            ("KafkaClusters:Main:BootstrapServers", "kafka:9092"),
            ("ConnectionStrings:Main", Secret)));

        Assert.Equal(AllRootKeyFailures, failures);
        Assert.All(failures, f => Assert.DoesNotContain("s3cr3t", f, StringComparison.Ordinal));
    }

    [Fact]
    public void Find_is_empty_when_OutboxRelay_section_exists_without_root_keys()
    {
        var failures = LegacyConfigurationGuard.Find(Config(
            ("OutboxRelay:Outboxes:0:Database:Connection", "Main"),
            ("OutboxRelay:Destinations:0:Name", "Events"),
            ("OutboxRelay:KafkaClusters:Main:BootstrapServers", "kafka:9092"),
            ("OutboxRelay:ConnectionStrings:Main", Secret),
            ("ConnectionStrings:Main", Secret)));

        Assert.Empty(failures);
    }

    [Fact]
    public void Find_never_includes_connection_string_values()
    {
        var failures = LegacyConfigurationGuard.Find(Config(
            ("Outboxes:0:Database:Connection", "Main"),
            ("ConnectionStrings:Main", Secret)));

        Assert.NotEmpty(failures);
        Assert.All(failures, f => Assert.DoesNotContain("s3cr3t", f, StringComparison.Ordinal));
    }

    [Fact]
    public async Task StartingAsync_throws_validation_exception_with_failures()
    {
        var guard = new LegacyConfigurationGuard(Config(("KafkaClusters:Main:BootstrapServers", "kafka:9092")));

        var ex = await Assert.ThrowsAsync<Pernix.OutboxRelay.Core.Configuration.RelayStartupValidationException>(
            () => guard.StartingAsync(TestContext.Current.CancellationToken));

        Assert.Single(ex.Failures);
    }

    [Fact]
    public async Task StartingAsync_passes_without_legacy_keys()
    {
        var guard = new LegacyConfigurationGuard(Config(("OutboxRelay:Outboxes:0:Name", "orders")));

        await guard.StartingAsync(TestContext.Current.CancellationToken);
    }

    private static readonly string[] AllRootKeyFailures =
    [
        "Outboxes: relay settings moved under OutboxRelay; move to OutboxRelay:Outboxes (env OutboxRelay__Outboxes__...)",
        "Destinations: relay settings moved under OutboxRelay; move to OutboxRelay:Destinations (env OutboxRelay__Destinations__...)",
        "KafkaClusters: relay settings moved under OutboxRelay; move to OutboxRelay:KafkaClusters (env OutboxRelay__KafkaClusters__...)",
        "ConnectionStrings.Main: move to OutboxRelay:ConnectionStrings:Main (env OutboxRelay__ConnectionStrings__Main)",
    ];

    private static IConfiguration Config(params (string Key, string? Value)[] values) =>
        new ConfigurationBuilder()
            .AddInMemoryCollection(values.Select(v => new KeyValuePair<string, string?>(v.Key, v.Value)))
            .Build();
}
