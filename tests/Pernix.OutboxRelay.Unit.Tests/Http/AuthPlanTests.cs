using Microsoft.Extensions.Logging.Abstractions;
using Pernix.OutboxRelay.Destination.Http;
using Pernix.OutboxRelay.Destination.Http.Options;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Http;

public sealed class AuthPlanTests
{
    [Fact]
    public void Missing_options_mean_no_authentication()
    {
        var plan = AuthPlan.Create(null, null);

        Assert.Same(AuthPlan.None, plan);
        Assert.Null(plan.HeaderName);
        Assert.Null(plan.HeaderValue);
        Assert.Null(plan.TokenCache);
    }

    [Fact]
    public void Basic_encodes_username_and_password()
    {
        var plan = AuthPlan.Create(
            new AuthenticationOptions { Scheme = AuthenticationScheme.Basic },
            new HttpCredential { Username = "Aladdin", Password = "open sesame" });

        Assert.Equal("Authorization", plan.HeaderName);
        Assert.Equal("Basic QWxhZGRpbjpvcGVuIHNlc2FtZQ==", plan.HeaderValue);
    }

    [Fact]
    public void Bearer_prefixes_the_token()
    {
        var plan = AuthPlan.Create(
            new AuthenticationOptions { Scheme = AuthenticationScheme.Bearer },
            new HttpCredential { Token = "abc.def" });

        Assert.Equal("Authorization", plan.HeaderName);
        Assert.Equal("Bearer abc.def", plan.HeaderValue);
    }

    [Theory]
    [InlineData(null, "k-123")]
    [InlineData("Key ", "Key k-123")]
    public void ApiKey_uses_header_name_and_value_prefix(string? prefix, string expected)
    {
        var plan = AuthPlan.Create(
            new AuthenticationOptions { Scheme = AuthenticationScheme.ApiKey, HeaderName = "X-Api-Key", ValuePrefix = prefix },
            new HttpCredential { ApiKey = "k-123" });

        Assert.Equal("X-Api-Key", plan.HeaderName);
        Assert.Equal(expected, plan.HeaderValue);
    }

    [Fact]
    public void Static_header_is_built_once()
    {
        var plan = AuthPlan.Create(new AuthenticationOptions { Scheme = AuthenticationScheme.Bearer }, new HttpCredential { Token = "t" });

        Assert.Same(plan.HeaderValue, plan.HeaderValue);
    }

    [Fact]
    public void OAuth2_carries_the_token_cache()
    {
        var options = new AuthenticationOptions { Scheme = AuthenticationScheme.OAuth2ClientCredentials, TokenUrl = "https://idp.test/token" };
        using var cache = new OAuth2TokenCache(
            "d", options, new HttpCredential { ClientId = "id", ClientSecret = "secret" }, new StubHandler(), NullLogger.Instance);

        var plan = AuthPlan.Create(options, null, cache);

        Assert.Equal("Authorization", plan.HeaderName);
        Assert.Null(plan.HeaderValue);
        Assert.Same(cache, plan.TokenCache);
    }

    [Theory]
    [InlineData("Basic", "Password")]
    [InlineData("Bearer", "Token")]
    [InlineData("ApiKey", "ApiKey")]
    public void Missing_credential_field_names_the_field_only(string scheme, string field)
    {
        var exception = Assert.Throws<ArgumentException>(() => AuthPlan.Create(
            new AuthenticationOptions { Scheme = Enum.Parse<AuthenticationScheme>(scheme), HeaderName = "X-Api-Key" },
            new HttpCredential { Username = "user-secret-value" }));

        Assert.Contains(field, exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("user-secret-value", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void ApiKey_without_header_name_is_refused()
    {
        Assert.Throws<ArgumentException>(() => AuthPlan.Create(
            new AuthenticationOptions { Scheme = AuthenticationScheme.ApiKey }, new HttpCredential { ApiKey = "k" }));
    }

    [Fact]
    public void OAuth2_without_token_cache_is_refused()
    {
        Assert.Throws<ArgumentNullException>(() => AuthPlan.Create(
            new AuthenticationOptions { Scheme = AuthenticationScheme.OAuth2ClientCredentials }, null));
    }

    private sealed class StubHandler : HttpMessageHandler
    {
        protected override Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken) =>
            throw new NotSupportedException();
    }
}
