using Pernix.OutboxRelay.Host.Configuration;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Configuration;

public sealed class RelayIdentityTests
{
    [Theory]
    [InlineData(null, "outbox-relay", "outbox-relay")]
    [InlineData("outbox-relay-riskbackoffice", "outbox-relay-riskbackoffice", "outbox-relay-riskbackoffice")]
    [InlineData("OutboxRelay", "OutboxRelay", "outbox-relay")]
    public void From_derives_component_name_from_service_name(string? serviceName, string expectedService, string expectedComponent)
    {
        var identity = RelayIdentity.From(Config(("SERVICE_NAME", serviceName)));

        Assert.Equal(expectedService, identity.ServiceName);
        Assert.Equal(expectedComponent, identity.ComponentName);
    }

    [Fact]
    public void From_uses_defaults_when_config_is_empty()
    {
        var identity = RelayIdentity.From(Config());

        Assert.False(string.IsNullOrWhiteSpace(identity.Version));
    }

    [Fact]
    public void From_reads_configured_version()
    {
        var identity = RelayIdentity.From(Config(("SERVICE_VERSION", "1.2.3")));

        Assert.Equal("1.2.3", identity.Version);
    }

    [Fact]
    public void From_reads_configured_environment_and_commit()
    {
        var identity = RelayIdentity.From(Config(
            ("EnvironmentName", "config-environment"),
            ("SERVICE_COMMIT", "config-commit")));

        Assert.Equal("config-environment", identity.EnvironmentName);
        Assert.Equal("config-commit", identity.Commit);
    }

    private static IConfiguration Config(params (string Key, string? Value)[] values) =>
        new ConfigurationBuilder()
            .AddInMemoryCollection(values.Select(v => KeyValuePair.Create(v.Key, v.Value)))
            .Build();
}
