using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Destination.Http;
using Microsoft.Extensions.Configuration;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Http;

public sealed class HttpCredentialSectionTests
{
    private static (HttpCredentialSection Section, IConfiguration Config) Bind(Dictionary<string, string?> settings)
    {
        var config = new ConfigurationBuilder().AddInMemoryCollection(settings).Build();
        return (new HttpCredentialSection(new RelayConfiguration(config)), config);
    }

    [Fact]
    public void Absent_section_binds_empty()
    {
        var (section, config) = Bind([]);

        Assert.Empty(section.Validate(config));
        Assert.False(section.TryGet("Ledger", out _));
    }

    [Fact]
    public void Every_field_binds_case_insensitively()
    {
        var (section, config) = Bind(new()
        {
            ["HttpCredentials:Ledger:Username"] = "u",
            ["HttpCredentials:Ledger:Password"] = "p",
            ["HttpCredentials:Ledger:Token"] = "t",
            ["HttpCredentials:Ledger:ClientId"] = "ci",
            ["HttpCredentials:Ledger:ClientSecret"] = "cs",
            ["HttpCredentials:Ledger:ApiKey"] = "ak",
            ["HttpCredentials:Ledger:SigningSecret"] = "ss",
            ["HttpCredentials:Ledger:certificatepassword"] = "cp",
        });

        Assert.Empty(section.Validate(config));
        Assert.True(section.TryGet("LEDGER", out var credential));
        Assert.Equal(
            ["u", "p", "t", "ci", "cs", "ak", "ss", "cp"],
            [credential.Username, credential.Password, credential.Token, credential.ClientId, credential.ClientSecret, credential.ApiKey, credential.SigningSecret, credential.CertificatePassword]);
    }

    [Fact]
    public void Unknown_key_fails_with_section_relative_path_and_not_the_value()
    {
        var (section, config) = Bind(new()
        {
            ["HttpCredentials:Ledger:Key"] = "sentinel-secret",
            ["HttpCredentials:Ledger:ApiKey:Nested"] = "sentinel-nested",
            ["HttpCredentials:Other:Secret"] = "sentinel-other",
        });

        var failures = section.Validate(config);

        Assert.Equal(
            [
                "HttpCredentials[Ledger]: unknown setting 'Key'",
                "HttpCredentials[Ledger].ApiKey: unknown setting 'Nested'",
                "HttpCredentials[Other]: unknown setting 'Secret'",
            ],
            failures);
        Assert.All(failures, f => Assert.DoesNotContain("sentinel", f, StringComparison.Ordinal));
        Assert.Equal(failures, section.Validate(config));
    }

    [Fact]
    public void Try_get_returns_bound_credentials_without_validate()
    {
        var (section, _) = Bind(new()
        {
            ["HttpCredentials:Ledger:Token"] = "t",
        });

        Assert.True(section.TryGet("ledger", out var credential));
        Assert.Equal("t", credential.Token);
    }
}
