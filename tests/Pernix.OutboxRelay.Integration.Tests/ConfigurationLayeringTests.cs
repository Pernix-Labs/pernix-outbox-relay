using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests;

public sealed class ConfigurationLayeringTests(OutboxRelayFactory factory) : IClassFixture<OutboxRelayFactory>
{
    [Fact]
    public void Test_settings_file_is_loaded()
    {
        var config = factory.Services.GetRequiredService<IConfiguration>();

        Assert.Equal(RelayFactory.SentinelValue, config[RelayFactory.SentinelKey]);
    }

    [Fact]
    public void Env_overrides_appsettings()
    {
        var config = factory.Services.GetRequiredService<IConfiguration>();

        Assert.Equal(RelayFactory.LayeringEnvValue, config[RelayFactory.LayeringKey]);
    }
}
