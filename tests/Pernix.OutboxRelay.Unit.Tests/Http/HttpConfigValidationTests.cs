using System.Net;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Destination.Http;
using Pernix.OutboxRelay.Destination.Http.Options;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Http;

internal static class HttpPlanFixture
{
    public const string Https = "https:" + "//ledger.example";
    public const string Http = "http:" + "//ledger.example";
    public const string Path = "Destinations[Sink]";
    public const string P = Path + ".Http";

    // Keys are under Destinations:0:Http unless prefixed '!' (relay-section keys). Null values are left out.
    public static (IReadOnlyList<string> Failures, HttpDestinationFactory Factory) Validate(
        Dictionary<string, string?> settings, HttpHookLookup? hooks = null, string[]? outboxes = null)
    {
        var all = new Dictionary<string, string?>
        {
            ["Destinations:0:Name"] = "Sink",
            ["Destinations:0:Transport"] = "Http",
        };
        outboxes ??= ["Orders"];
        for (var i = 0; i < outboxes.Length; i++)
        {
            all[$"Outboxes:{i}:Name"] = outboxes[i];
            all[$"Outboxes:{i}:Destination"] = "Sink";
        }

        foreach (var (key, value) in settings.Where(static s => s.Value is not null))
        {
            all[key.StartsWith('!') ? key[1..] : $"Destinations:0:Http:{key}"] = value;
        }

        var config = new ConfigurationBuilder().AddInMemoryCollection(all).Build();
        var factory = new HttpDestinationFactory(
            NullLoggerFactory.Instance, new HttpCredentialSection(new RelayConfiguration(config)), hooks ?? HttpHookLookup.None);
        var relay = RelayOptionsBinder.Bind(config);
        return (factory.Validate(Assert.Single(relay.Destinations), Path, relay), factory);
    }

    public static IReadOnlyList<string> Failures(Dictionary<string, string?> settings, HttpHookLookup? hooks = null) =>
        Validate(settings, hooks).Failures;

    public static HttpDestinationPlan Plan(Dictionary<string, string?> settings, HttpHookLookup? hooks = null, string[]? outboxes = null)
    {
        var (failures, factory) = Validate(settings, hooks, outboxes);
        Assert.Empty(failures);
        return factory.PlanOf("Sink");
    }
}

public sealed class HttpConfigValidationTests : IDisposable
{
    private const string P = HttpPlanFixture.P;
    private const string Https = HttpPlanFixture.Https;
    private const string Http = HttpPlanFixture.Http;

    private readonly string directory = Directory.CreateTempSubdirectory("http-config-").FullName;

    public void Dispose() => Directory.Delete(directory, recursive: true);

    private static IReadOnlyList<string> Failures(Dictionary<string, string?> settings, HttpHookLookup? hooks = null) =>
        HttpPlanFixture.Failures(settings, hooks);

    private static HttpDestinationPlan Plan(Dictionary<string, string?> settings, HttpHookLookup? hooks = null) =>
        HttpPlanFixture.Plan(settings, hooks);

    private static void Fails(Dictionary<string, string?> settings, string expected) =>
        Assert.Contains(expected, Failures(settings));

    [Fact]
    public void Minimal_url_compiles_with_defaults()
    {
        var plan = Plan(new() { ["Url"] = $"{Https}/orders" });

        var endpoint = Assert.Single(plan.Endpoints.Endpoints);
        Assert.Same(HttpMethod.Post, endpoint.Method);
        Assert.Equal("application/octet-stream", endpoint.ContentType);
        Assert.Equal(new Uri($"{Https}/orders"), endpoint.StaticUri);
        Assert.Null(plan.Endpoints.Routes);
        Assert.Equal(TimeSpan.FromSeconds(30), plan.SendTimeout);
        Assert.Equal(TimeSpan.FromSeconds(10), plan.Handler.ConnectTimeout);
        Assert.Equal(64, plan.MaxRequestsInFlight);
        Assert.Equal(HttpVersion.Version11, plan.Handler.RequestVersion);
        Assert.Equal("ledger.example", plan.Host);
        Assert.Null(plan.Signing);
        Assert.Null(plan.Probe);
    }

    [Fact]
    public void Validate_caches_the_plan_by_name_ignoring_case()
    {
        var (failures, factory) = HttpPlanFixture.Validate(new() { ["Url"] = $"{Https}/orders" });

        Assert.Empty(failures);
        Assert.Same(factory.PlanOf("Sink"), factory.PlanOf("SINK"));
    }

    [Fact]
    public void Failed_validation_caches_no_plan()
    {
        var (failures, factory) = HttpPlanFixture.Validate(new() { ["Url"] = "/orders" });

        Assert.NotEmpty(failures);
        Assert.Throws<KeyNotFoundException>(() => factory.PlanOf("Sink"));
    }

    [Fact]
    public void Url_or_routes_is_required()
    {
        Fails(new() { ["Method"] = "PUT" }, $"{P}: Url or Routes is required");
    }

    [Fact]
    public void Url_and_routes_together_fail()
    {
        Fails(new() { ["Url"] = $"{Https}/a", ["Routes:orders:Url"] = $"{Https}/b" }, $"{P}: Url and Routes are both set; set exactly one");
    }

    [Fact]
    public void Route_key_must_match_the_route_rule()
    {
        Fails(new() { ["Routes:Orders:Url"] = $"{Https}/b" }, $"{P}.Routes.Orders: route key must match ^[a-z0-9][a-z0-9._-]{{0,62}}$");
    }

    [Fact]
    public void Route_entry_needs_a_url()
    {
        Fails(new() { ["Routes:orders:Method"] = "PUT" }, $"{P}.Routes.orders.Url: required");
    }

    [Fact]
    public void Routes_expose_the_key_set_and_merge_headers_by_name()
    {
        var plan = Plan(new()
        {
            ["Method"] = "PUT",
            ["Headers:X-Shared"] = "shared",
            ["Headers:X-Over"] = "shared",
            ["Routes:orders:Url"] = $"{Https}/orders",
            ["Routes:orders:Method"] = "patch",
            ["Routes:orders:ContentType"] = "application/json",
            ["Routes:orders:Headers:x-over"] = "{route}",
            ["Routes:audit:Url"] = $"{Https}/audit/{{route}}",
        });

        Assert.Equal(["audit", "orders"], plan.Endpoints.Routes!.Order(StringComparer.Ordinal));
        var orders = plan.Endpoints.Endpoints.Single(e => e.RouteLabel == "orders");
        var audit = plan.Endpoints.Endpoints.Single(e => e.RouteLabel == "audit");
        Assert.Same(HttpMethod.Patch, orders.Method);
        Assert.Same(HttpMethod.Put, audit.Method);
        Assert.Equal("application/json", orders.ContentType);
        Assert.Equal(new Uri($"{Https}/audit/audit"), audit.StaticUri);
        Assert.Equal(2, orders.Headers.Count);
        Assert.Contains("x-over", (IReadOnlySet<string>)orders.Headers.Overridden);
    }

    [Theory]
    [InlineData("/orders")]
    [InlineData("ledger.example/orders")]
    [InlineData("ftp://ledger.example/orders")]
    public void Url_must_be_absolute_http_or_https(string url)
    {
        Fails(new() { ["Url"] = url }, $"{P}.Url: must be an absolute http or https URL");
    }

    [Fact]
    public void Url_must_not_carry_user_info()
    {
        var failures = Failures(new() { ["Url"] = "https:" + "//relay:sentinel@ledger.example/orders" });

        Assert.Contains($"{P}.Url: must not carry user info", failures);
        Assert.All(failures, f => Assert.DoesNotContain("sentinel", f, StringComparison.Ordinal));
    }

    [Fact]
    public void Url_must_not_carry_a_fragment()
    {
        Fails(new() { ["Url"] = $"{Https}/orders#top" }, $"{P}.Url: must not carry a fragment");
    }

    [Theory]
    [InlineData("https:" + "//{outbox}.ledger.example/orders")]
    [InlineData("https:" + "//ledger.example:{id}/orders")]
    [InlineData("{outbox}:" + "//ledger.example/orders")]
    public void Placeholders_are_allowed_only_in_path_and_query(string url)
    {
        Fails(new() { ["Url"] = url }, $"{P}.Url: placeholders are allowed only in path and query");
    }

    [Fact]
    public void Placeholders_in_path_and_query_compile_as_a_template()
    {
        var plan = Plan(new() { ["Url"] = $"{Https}/orders/{{id}}?key={{key:hex}}&source={{outbox}}" });

        var endpoint = Assert.Single(plan.Endpoints.Endpoints);
        Assert.Null(endpoint.StaticUri);
        Assert.NotNull(endpoint.UrlTemplate);
    }

    [Fact]
    public void Unknown_placeholder_fails()
    {
        Assert.Contains(Failures(new() { ["Url"] = $"{Https}/orders/{{nope}}" }), f => f.StartsWith($"{P}.Url: unknown placeholder '{{nope}}'", StringComparison.Ordinal));
    }

    [Fact]
    public void Route_placeholder_fails_on_a_single_url()
    {
        Fails(new() { ["Url"] = $"{Https}/{{route}}" }, $"{P}.Url: placeholder '{{route}}' is allowed only on a Routes entry");
    }

    [Theory]
    [InlineData("/a/../b")]
    [InlineData("/a/./b")]
    [InlineData("/a b")]
    [InlineData("/café")]
    public void Literal_path_must_be_canonical(string suffix)
    {
        Fails(
            new() { ["Url"] = Https + suffix },
            $"{P}.Url: path and query must be canonical: percent-encode reserved and non-ASCII characters, no dot segments");
    }

    [Fact]
    public void Header_placeholder_in_url_must_name_a_token()
    {
        Fails(new() { ["Url"] = $"{Https}/{{header:a b}}" }, $"{P}.Url: a {{header:<name>}} placeholder must name an RFC 9110 token");
    }

    [Theory]
    [InlineData(null, "https")]
    [InlineData(null, "http")]
    [InlineData("1.1", "https")]
    [InlineData("1.1", "http")]
    [InlineData("2", "https")]
    [InlineData("2-cleartext", "http")]
    public void Http_version_accepts_matching_schemes(string? version, string scheme)
    {
        var settings = new Dictionary<string, string?> { ["Url"] = $"{scheme}://ledger.example/orders" };
        if (version is not null)
        {
            settings["HttpVersion"] = version;
        }

        var plan = Plan(settings);

        Assert.Equal(version is null or "1.1" ? HttpVersion.Version11 : HttpVersion.Version20, plan.Handler.RequestVersion);
        Assert.Equal(
            version is null or "1.1" ? HttpVersionPolicy.RequestVersionOrLower : HttpVersionPolicy.RequestVersionExact,
            plan.Handler.VersionPolicy);
    }

    [Theory]
    [InlineData("Url")]
    [InlineData("Routes:orders:Url")]
    [InlineData("HealthProbe:Url")]
    public void Http_version_2_requires_https_on_every_url(string key)
    {
        var settings = new Dictionary<string, string?> { ["HttpVersion"] = "2", ["HealthProbe:Url"] = $"{Https}/healthz" };
        settings[key.StartsWith("Routes", StringComparison.Ordinal) ? key : "Url"] = $"{Https}/orders";
        settings[key] = $"{Http}/x";
        if (key.StartsWith("Routes", StringComparison.Ordinal))
        {
            settings.Remove("Url");
        }

        Fails(settings, $"{P}.HttpVersion: \"2\" requires https URLs; use \"2-cleartext\" for prior-knowledge h2c");
    }

    [Theory]
    [InlineData("Url")]
    [InlineData("Routes:orders:Url")]
    [InlineData("HealthProbe:Url")]
    public void Http_version_2_cleartext_requires_http_on_every_url(string key)
    {
        var settings = new Dictionary<string, string?> { ["HttpVersion"] = "2-cleartext", ["HealthProbe:Url"] = $"{Http}/healthz" };
        settings[key.StartsWith("Routes", StringComparison.Ordinal) ? key : "Url"] = $"{Http}/orders";
        settings[key] = $"{Https}/x";
        if (key.StartsWith("Routes", StringComparison.Ordinal))
        {
            settings.Remove("Url");
        }

        Fails(settings, $"{P}.HttpVersion: \"2-cleartext\" requires http URLs; use \"2\" for HTTP/2 over TLS");
    }

    [Fact]
    public void Unknown_http_version_fails()
    {
        Fails(new() { ["Url"] = $"{Https}/orders", ["HttpVersion"] = "3" }, $"{P}.HttpVersion: must be one of \"1.1\", \"2\", \"2-cleartext\"");
    }

    [Theory]
    [InlineData("post", "POST")]
    [InlineData("PUT", "PUT")]
    [InlineData("Patch", "PATCH")]
    public void Method_accepts_post_put_patch_in_any_case(string value, string expected)
    {
        var plan = Plan(new() { ["Url"] = $"{Https}/orders", ["Method"] = value });

        Assert.Equal(expected, Assert.Single(plan.Endpoints.Endpoints).Method.Method);
    }

    [Theory]
    [InlineData("Method")]
    [InlineData("Routes:orders:Method")]
    public void Method_other_than_post_put_patch_fails(string key)
    {
        var settings = new Dictionary<string, string?> { ["Routes:orders:Url"] = $"{Https}/orders", [key] = "GET" };

        Fails(settings, $"{P}.{key.Replace(':', '.')}: must be one of POST, PUT, PATCH");
    }

    [Fact]
    public void Content_type_must_be_a_media_type()
    {
        Fails(new() { ["Url"] = $"{Https}/orders", ["ContentType"] = "not a media type" }, $"{P}.ContentType: must be a media type such as application/json");
    }

    [Fact]
    public void Header_name_must_be_a_token()
    {
        Fails(new() { ["Url"] = $"{Https}/orders", ["Headers:X Bad"] = "v" }, $"{P}.Headers.X Bad: header name must be an RFC 9110 token");
    }

    [Theory]
    [InlineData("Connection")]
    [InlineData("transfer-encoding")]
    [InlineData("Host")]
    [InlineData("Content-Length")]
    [InlineData("Cookie")]
    public void Reserved_header_name_fails(string header)
    {
        Fails(new() { ["Url"] = $"{Https}/orders", [$"Headers:{header}"] = "v" }, $"{P}.Headers.{header}: is a hop-by-hop or framing header and cannot be set");
    }

    [Fact]
    public void Authorization_header_fails()
    {
        Fails(
            new() { ["Url"] = $"{Https}/orders", ["Headers:authorization"] = "Bearer sentinel" },
            $"{P}.Headers.authorization: Authorization is set by Authentication; use Authentication");
    }

    [Theory]
    [InlineData("Content-Type")]
    [InlineData("content-type")]
    public void Content_type_header_fails(string header)
    {
        Fails(new() { ["Url"] = $"{Https}/orders", [$"Headers:{header}"] = "application/json" }, $"{P}.Headers.{header}: Content-Type is set by ContentType; use ContentType");
    }

    [Fact]
    public void Route_headers_follow_the_same_name_rules()
    {
        Fails(new() { ["Routes:orders:Url"] = $"{Https}/orders", ["Routes:orders:Headers:Upgrade"] = "h2c" }, $"{P}.Routes.orders.Headers.Upgrade: is a hop-by-hop or framing header and cannot be set");
    }

    [Fact]
    public void Header_value_with_control_characters_fails()
    {
        Fails(new() { ["Url"] = $"{Https}/orders", ["Headers:X-Bad"] = "a\nb" }, $"{P}.Headers.X-Bad: value contains CR, LF or NUL");
    }

    [Fact]
    public void Header_value_placeholder_must_be_known()
    {
        Assert.Contains(
            Failures(new() { ["Url"] = $"{Https}/orders", ["Headers:X-Bad"] = "{nope}" }),
            f => f.StartsWith($"{P}.Headers.X-Bad: unknown placeholder", StringComparison.Ordinal));
    }

    [Fact]
    public void Header_value_header_placeholder_must_name_a_token()
    {
        Fails(new() { ["Url"] = $"{Https}/orders", ["Headers:X-Tenant"] = "{header:ten ant}" }, $"{P}.Headers.X-Tenant: a {{header:<name>}} placeholder must name an RFC 9110 token");
    }

    [Fact]
    public void Header_named_like_a_dead_letter_header_fails()
    {
        Fails(
            new() { ["Url"] = $"{Https}/orders", ["Headers:X-Dead-Letter-Reason"] = "v" },
            $"{P}.Headers.X-Dead-Letter-Reason: conflicts with the relay-owned header set by dead-letter metadata");
    }

    [Fact]
    public void Header_named_like_the_api_key_header_fails()
    {
        Fails(
            new()
            {
                ["Url"] = $"{Https}/orders",
                ["Headers:x-api-key"] = "v",
                ["Authentication:Scheme"] = "ApiKey",
                ["Authentication:HeaderName"] = "X-Api-Key",
                ["Authentication:Credential"] = "Ledger",
                ["!HttpCredentials:Ledger:ApiKey"] = "k",
            },
            $"{P}.Headers.x-api-key: conflicts with the relay-owned header set by Authentication");
    }

    [Theory]
    [InlineData("webhook-id")]
    [InlineData("Webhook-Timestamp")]
    [InlineData("webhook-signature")]
    public void Header_named_like_a_standard_webhooks_header_fails(string header)
    {
        Fails(
            new()
            {
                ["Url"] = $"{Https}/orders",
                [$"Headers:{header}"] = "v",
                ["Signing:Scheme"] = "StandardWebhooks",
                ["Signing:Credential"] = "Signing",
                ["!HttpCredentials:Signing:SigningSecret"] = "whsec_" + Convert.ToBase64String(new byte[24]),
            },
            $"{P}.Headers.{header}: conflicts with the relay-owned header set by Signing");
    }

    [Fact]
    public void Header_named_like_the_hmac_signature_header_fails()
    {
        Fails(
            new()
            {
                ["Url"] = $"{Https}/orders",
                ["Headers:X-Signature"] = "v",
                ["Signing:Scheme"] = "HmacSha256",
                ["Signing:HeaderName"] = "x-signature",
                ["Signing:Credential"] = "Signing",
                ["!HttpCredentials:Signing:SigningSecret"] = "secret",
            },
            $"{P}.Headers.X-Signature: conflicts with the relay-owned header set by Signing");
    }

    [Fact]
    public void Relay_owned_names_join_the_overridden_set()
    {
        var plan = Plan(new()
        {
            ["Url"] = $"{Https}/orders",
            ["Headers:X-Tenant"] = "t",
            ["Authentication:Scheme"] = "Bearer",
            ["Authentication:Credential"] = "Ledger",
            ["Signing:Scheme"] = "StandardWebhooks",
            ["Signing:Credential"] = "Ledger",
            ["!HttpCredentials:Ledger:Token"] = "t",
            ["!HttpCredentials:Ledger:SigningSecret"] = "whsec_" + Convert.ToBase64String(new byte[24]),
        });

        IReadOnlySet<string> overridden = Assert.Single(plan.Endpoints.Endpoints).Headers.Overridden;
        Assert.Contains("X-Tenant", overridden);
        Assert.Contains("Authorization", overridden);
        Assert.Contains("webhook-id", overridden);
        Assert.Contains("webhook-timestamp", overridden);
        Assert.Contains("webhook-signature", overridden);
        Assert.Contains("x-original-id", overridden);
    }

    [Fact]
    public void Status_code_entry_that_does_not_parse_fails()
    {
        Fails(
            new() { ["Url"] = $"{Https}/orders", ["StatusCodes:Retried:0"] = "600" },
            $"{P}.StatusCodes.Retried[0]: must be a code (NNN), a class (Nxx) or a range (NNN-NNN) within 100-599");
    }

    [Fact]
    public void Status_code_in_two_classes_fails()
    {
        Fails(
            new() { ["Url"] = $"{Https}/orders", ["StatusCodes:Delivered:0"] = "2xx", ["StatusCodes:Rejected:0"] = "200-204" },
            $"{P}.StatusCodes.Rejected: shares a status code with Delivered; a code may be in one class only");
    }

    [Fact]
    public void Status_codes_build_the_endpoint_table()
    {
        var plan = Plan(new() { ["Url"] = $"{Https}/orders", ["StatusCodes:Retried:0"] = "500" });

        Assert.Equal(StatusClass.Retried, Assert.Single(plan.Endpoints.Endpoints).Status.Classify(500));
    }

    [Theory]
    [InlineData("SendTimeout", "00:00:00", "must be greater than 00:00:00")]
    [InlineData("ConnectTimeout", "00:00:00", "must be greater than 00:00:00")]
    [InlineData("ConnectTimeout", "00:01:00", "must not exceed SendTimeout")]
    [InlineData("MaxRequestsInFlight", "0", "must be at least 1")]
    public void Limits_out_of_range_fail(string key, string value, string rule)
    {
        Fails(new() { ["Url"] = $"{Https}/orders", [key] = value }, $"{P}.{key}: {rule}");
    }

    [Fact]
    public void Limits_set_the_plan()
    {
        var plan = Plan(new() { ["Url"] = $"{Https}/orders", ["SendTimeout"] = "00:00:05", ["ConnectTimeout"] = "00:00:05", ["MaxRequestsInFlight"] = "1" });

        Assert.Equal(TimeSpan.FromSeconds(5), plan.SendTimeout);
        Assert.Equal(TimeSpan.FromSeconds(5), plan.Handler.ConnectTimeout);
        Assert.Equal(1, plan.MaxRequestsInFlight);
    }

    [Theory]
    [InlineData("Basic")]
    [InlineData("Bearer")]
    [InlineData("ApiKey")]
    [InlineData("OAuth2ClientCredentials")]
    public void Authentication_credential_is_required(string scheme)
    {
        Fails(new() { ["Url"] = $"{Https}/orders", ["Authentication:Scheme"] = scheme }, $"{P}.Authentication.Credential: required");
    }

    [Fact]
    public void Authentication_credential_must_be_defined()
    {
        var failures = Failures(new() { ["Url"] = $"{Https}/orders", ["Authentication:Scheme"] = "Bearer", ["Authentication:Credential"] = "sentinel" });

        Assert.Contains($"{P}.Authentication.Credential: not defined in HttpCredentials", failures);
        Assert.All(failures, f => Assert.DoesNotContain("sentinel", f, StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("Basic", "Username", "Username and Password")]
    [InlineData("Bearer", "Username", "Token")]
    [InlineData("ApiKey", "Token", "ApiKey")]
    [InlineData("OAuth2ClientCredentials", "ClientId", "ClientId and ClientSecret")]
    public void Authentication_credential_needs_the_scheme_fields(string scheme, string present, string needed)
    {
        var failures = Failures(new()
        {
            ["Url"] = $"{Https}/orders",
            ["Authentication:Scheme"] = scheme,
            ["Authentication:Credential"] = "Ledger",
            ["Authentication:HeaderName"] = scheme == "ApiKey" ? "X-Api-Key" : null,
            ["Authentication:TokenUrl"] = scheme == "OAuth2ClientCredentials" ? $"{Https}/token" : null,
            [$"!HttpCredentials:Ledger:{present}"] = "sentinel",
        });

        Assert.Contains($"{P}.Authentication.Credential: credential needs {needed}", failures);
        Assert.All(failures, f => Assert.DoesNotContain("sentinel", f, StringComparison.Ordinal));
    }

    [Fact]
    public void Basic_and_bearer_build_the_authorization_value_once()
    {
        var plan = Plan(new()
        {
            ["Url"] = $"{Https}/orders",
            ["Authentication:Scheme"] = "Basic",
            ["Authentication:Credential"] = "Ledger",
            ["!HttpCredentials:Ledger:Username"] = "relay",
            ["!HttpCredentials:Ledger:Password"] = "pw",
        });

        Assert.Equal("Authorization", plan.Auth.HeaderName);
        Assert.Equal("Basic " + Convert.ToBase64String("relay:pw"u8.ToArray()), plan.Auth.HeaderValue);
    }

    [Fact]
    public void Api_key_needs_a_header_name()
    {
        Fails(
            new() { ["Url"] = $"{Https}/orders", ["Authentication:Scheme"] = "ApiKey", ["Authentication:Credential"] = "Ledger", ["!HttpCredentials:Ledger:ApiKey"] = "k" },
            $"{P}.Authentication.HeaderName: required for Scheme ApiKey");
    }

    [Theory]
    [InlineData("X Api Key", "must be an RFC 9110 token")]
    [InlineData("Cookie", "must not be a reserved or content header")]
    [InlineData("Content-Language", "must not be a reserved or content header")]
    public void Api_key_header_name_rules(string header, string rule)
    {
        Fails(
            new()
            {
                ["Url"] = $"{Https}/orders",
                ["Authentication:Scheme"] = "ApiKey",
                ["Authentication:HeaderName"] = header,
                ["Authentication:Credential"] = "Ledger",
                ["!HttpCredentials:Ledger:ApiKey"] = "k",
            },
            $"{P}.Authentication.HeaderName: {rule}");
    }

    [Fact]
    public void Api_key_builds_its_header_once()
    {
        var plan = Plan(new()
        {
            ["Url"] = $"{Https}/orders",
            ["Authentication:Scheme"] = "apikey",
            ["Authentication:HeaderName"] = "X-Api-Key",
            ["Authentication:ValuePrefix"] = "Key ",
            ["Authentication:Credential"] = "Ledger",
            ["!HttpCredentials:Ledger:ApiKey"] = "k",
        });

        Assert.Equal("X-Api-Key", plan.Auth.HeaderName);
        Assert.Equal("Key k", plan.Auth.HeaderValue);
    }

    [Fact]
    public void Authentication_field_for_another_scheme_fails()
    {
        Fails(
            new()
            {
                ["Url"] = $"{Https}/orders",
                ["Authentication:Scheme"] = "Bearer",
                ["Authentication:HeaderName"] = "X-Token",
                ["Authentication:Credential"] = "Ledger",
                ["!HttpCredentials:Ledger:Token"] = "t",
            },
            $"{P}.Authentication.HeaderName: does not apply to Scheme Bearer");
    }

    [Fact]
    public void OAuth2_needs_a_token_url()
    {
        Fails(OAuth2(null), $"{P}.Authentication.TokenUrl: required for Scheme OAuth2ClientCredentials");
    }

    [Fact]
    public void OAuth2_token_url_must_be_https_unless_loopback()
    {
        Fails(OAuth2($"{Http}/token"), $"{P}.Authentication.TokenUrl: must be https; http is allowed only for a loopback host");
    }

    [Fact]
    public void OAuth2_token_url_may_be_http_on_loopback()
    {
        var plan = Plan(OAuth2("http:" + "//127.0.0.1:8080/token"));

        Assert.NotNull(plan.Auth.TokenCache);
        Assert.Equal("127.0.0.1", plan.Auth.TokenCache.TokenHost);
        plan.Auth.TokenCache.Dispose();
    }

    [Fact]
    public void OAuth2_token_url_must_be_absolute()
    {
        Fails(OAuth2("/token"), $"{P}.Authentication.TokenUrl: must be an absolute http or https URL");
    }

    [Fact]
    public void Signing_credential_is_required()
    {
        Fails(new() { ["Url"] = $"{Https}/orders", ["Signing:Scheme"] = "HmacSha256", ["Signing:HeaderName"] = "X-Sig" }, $"{P}.Signing.Credential: required");
    }

    [Fact]
    public void Signing_credential_must_be_defined()
    {
        Fails(
            new() { ["Url"] = $"{Https}/orders", ["Signing:Scheme"] = "HmacSha256", ["Signing:HeaderName"] = "X-Sig", ["Signing:Credential"] = "Missing" },
            $"{P}.Signing.Credential: not defined in HttpCredentials");
    }

    [Fact]
    public void Signing_credential_needs_a_signing_secret()
    {
        Fails(
            new()
            {
                ["Url"] = $"{Https}/orders",
                ["Signing:Scheme"] = "HmacSha256",
                ["Signing:HeaderName"] = "X-Sig",
                ["Signing:Credential"] = "Ledger",
                ["!HttpCredentials:Ledger:Token"] = "t",
            },
            $"{P}.Signing.Credential: credential needs SigningSecret");
    }

    [Fact]
    public void Standard_webhooks_secret_must_decode()
    {
        var failures = Failures(new()
        {
            ["Url"] = $"{Https}/orders",
            ["Signing:Scheme"] = "StandardWebhooks",
            ["Signing:Credential"] = "Ledger",
            ["!HttpCredentials:Ledger:SigningSecret"] = "sentinel",
        });

        Assert.Contains($"{P}.Signing.Credential: SigningSecret must be 'whsec_' followed by base64", failures);
        Assert.All(failures, f => Assert.DoesNotContain("sentinel", f, StringComparison.Ordinal));
    }

    [Theory]
    [InlineData("Connection")]
    [InlineData("Content-MD5")]
    [InlineData("Content-Signature")]
    [InlineData("Expires")]
    [InlineData("Last-Modified")]
    [InlineData("Allow")]
    public void Hmac_signature_header_must_not_be_reserved_or_content(string header)
    {
        Fails(Hmac(header), $"{P}.Signing.HeaderName: must not be a reserved or content header");
    }

    [Fact]
    public void Hmac_signature_header_must_be_a_token()
    {
        Fails(Hmac("X Sig"), $"{P}.Signing.HeaderName: required for HmacSha256 and must be an RFC 9110 token");
    }

    [Fact]
    public void Hmac_signature_header_must_not_be_the_authentication_header()
    {
        var settings = Hmac("X-Api-Key");
        settings["Authentication:Scheme"] = "ApiKey";
        settings["Authentication:HeaderName"] = "x-api-key";
        settings["Authentication:Credential"] = "Ledger";
        settings["!HttpCredentials:Ledger:ApiKey"] = "k";

        Fails(settings, $"{P}.Signing.HeaderName: conflicts with the header set by Authentication");
    }

    [Fact]
    public void Webhook_id_header_placeholder_must_name_a_token()
    {
        Fails(
            new()
            {
                ["Url"] = $"{Https}/orders",
                ["Signing:Scheme"] = "StandardWebhooks",
                ["Signing:Credential"] = "Ledger",
                ["Signing:WebhookId"] = "{header:a b}",
                ["!HttpCredentials:Ledger:SigningSecret"] = "whsec_" + Convert.ToBase64String(new byte[24]),
            },
            $"{P}.Signing.WebhookId: a {{header:<name>}} placeholder must name an RFC 9110 token");
    }

    [Fact]
    public void Standard_webhooks_compiles_a_webhook_id_per_endpoint()
    {
        var plan = Plan(new()
        {
            ["Url"] = $"{Https}/orders",
            ["Signing:Scheme"] = "StandardWebhooks",
            ["Signing:Credential"] = "Ledger",
            ["!HttpCredentials:Ledger:SigningSecret"] = "whsec_" + Convert.ToBase64String(new byte[24]),
        });

        Assert.NotNull(plan.Signing);
        var webhookId = Assert.Single(plan.Endpoints.Endpoints).WebhookId;
        Assert.NotNull(webhookId);
        Assert.True(webhookId.TryRenderHeaderValue(new RenderContext(42, null, Pernix.OutboxRelay.Core.Abstractions.MessageHeaders.Empty), out var id, out _));
        Assert.Equal("Orders-42", id);
    }

    [Fact]
    public void Ca_certificate_file_must_exist()
    {
        Fails(Tls(new() { ["Tls:CaCertificatePath"] = System.IO.Path.Combine(directory, "missing.pem") }), $"{P}.Tls.CaCertificatePath: file is missing or holds no PEM certificate");
    }

    [Fact]
    public void Ca_certificate_file_must_parse()
    {
        var file = System.IO.Path.Combine(directory, "garbage.pem");
        File.WriteAllText(file, "not a certificate");

        Fails(Tls(new() { ["Tls:CaCertificatePath"] = file }), $"{P}.Tls.CaCertificatePath: file is missing or holds no PEM certificate");
    }

    [Fact]
    public void Ca_certificate_bundle_loads()
    {
        var (certificate, _) = WritePem("ca");

        var plan = Plan(Tls(new() { ["Tls:CaCertificatePath"] = certificate }));

        Assert.Single(plan.Handler.TrustedRoots!);
    }

    [Fact]
    public void Client_certificate_needs_pem_pair_or_pfx()
    {
        Fails(Tls(new() { ["Tls:ClientCertificate:Credential"] = "Ledger" }), $"{P}.Tls.ClientCertificate: set CertificatePath and KeyPath, or PfxPath");
    }

    [Fact]
    public void Client_certificate_pem_needs_both_paths()
    {
        Fails(Tls(new() { ["Tls:ClientCertificate:CertificatePath"] = "/x.crt" }), $"{P}.Tls.ClientCertificate: CertificatePath and KeyPath are both required");
    }

    [Fact]
    public void Client_certificate_pem_and_pfx_together_fail()
    {
        Fails(
            Tls(new() { ["Tls:ClientCertificate:CertificatePath"] = "/x.crt", ["Tls:ClientCertificate:KeyPath"] = "/x.key", ["Tls:ClientCertificate:PfxPath"] = "/x.pfx" }),
            $"{P}.Tls.ClientCertificate: set CertificatePath and KeyPath, or PfxPath");
    }

    [Fact]
    public void Client_certificate_files_must_exist()
    {
        Fails(
            Tls(new()
            {
                ["Tls:ClientCertificate:CertificatePath"] = System.IO.Path.Combine(directory, "missing.crt"),
                ["Tls:ClientCertificate:KeyPath"] = System.IO.Path.Combine(directory, "missing.key"),
            }),
            $"{P}.Tls.ClientCertificate: file is missing or does not parse");
    }

    [Fact]
    public void Client_certificate_pem_pair_loads()
    {
        var (certificate, key) = WritePem("client");

        var plan = Plan(Tls(new() { ["Tls:ClientCertificate:CertificatePath"] = certificate, ["Tls:ClientCertificate:KeyPath"] = key }));

        Assert.True(plan.Handler.ClientCertificate!.HasPrivateKey);
    }

    [Fact]
    public void Client_certificate_pfx_credential_needs_a_certificate_password()
    {
        Fails(
            Tls(new()
            {
                ["Tls:ClientCertificate:PfxPath"] = "/x.pfx",
                ["Tls:ClientCertificate:Credential"] = "Ledger",
                ["!HttpCredentials:Ledger:Token"] = "t",
            }),
            $"{P}.Tls.ClientCertificate.Credential: credential needs CertificatePassword");
    }

    [Fact]
    public void Client_certificate_pfx_loads_with_its_password()
    {
        using var rsa = RSA.Create(2048);
        using var certificate = new CertificateRequest("CN=client", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1)
            .CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        var pfx = System.IO.Path.Combine(directory, "client.pfx");
        File.WriteAllBytes(pfx, certificate.Export(X509ContentType.Pfx, "pw"));

        var plan = Plan(Tls(new()
        {
            ["Tls:ClientCertificate:PfxPath"] = pfx,
            ["Tls:ClientCertificate:Credential"] = "Ledger",
            ["!HttpCredentials:Ledger:CertificatePassword"] = "pw",
        }));

        Assert.True(plan.Handler.ClientCertificate!.HasPrivateKey);
    }

    [Fact]
    public void Client_certificate_credential_applies_only_to_pfx()
    {
        Fails(
            Tls(new() { ["Tls:ClientCertificate:CertificatePath"] = "/x.crt", ["Tls:ClientCertificate:KeyPath"] = "/x.key", ["Tls:ClientCertificate:Credential"] = "Ledger" }),
            $"{P}.Tls.ClientCertificate.Credential: applies only to PfxPath");
    }

    [Fact]
    public void Health_probe_needs_a_url()
    {
        Fails(new() { ["Url"] = $"{Https}/orders", ["HealthProbe:Method"] = "GET" }, $"{P}.HealthProbe.Url: required");
    }

    [Fact]
    public void Health_probe_url_must_not_contain_placeholders()
    {
        Fails(new() { ["Url"] = $"{Https}/orders", ["HealthProbe:Url"] = $"{Https}/{{outbox}}" }, $"{P}.HealthProbe.Url: must not contain placeholders");
    }

    [Fact]
    public void Health_probe_method_must_be_head_or_get()
    {
        Fails(new() { ["Url"] = $"{Https}/orders", ["HealthProbe:Url"] = $"{Https}/healthz", ["HealthProbe:Method"] = "POST" }, $"{P}.HealthProbe.Method: must be one of HEAD, GET");
    }

    [Fact]
    public void Health_probe_expected_entries_must_parse()
    {
        Fails(
            new() { ["Url"] = $"{Https}/orders", ["HealthProbe:Url"] = $"{Https}/healthz", ["HealthProbe:Expected:0"] = "9xx" },
            $"{P}.HealthProbe.Expected[0]: must be a code (NNN), a class (Nxx) or a range (NNN-NNN) within 100-599");
    }

    [Fact]
    public void Health_probe_defaults_to_head_and_2xx()
    {
        var plan = Plan(new() { ["Url"] = $"{Https}/orders", ["HealthProbe:Url"] = $"{Https}/healthz" });

        Assert.NotNull(plan.Probe);
        Assert.Same(HttpMethod.Head, plan.Probe.Method);
        Assert.True(plan.Probe.IsExpected(204));
        Assert.False(plan.Probe.IsExpected(301));
    }

    [Fact]
    public void Unregistered_request_mapper_fails()
    {
        Fails(new() { ["Url"] = $"{Https}/orders", ["RequestMapper"] = "envelope" }, $"{P}.RequestMapper: no request mapper is registered under this name");
    }

    [Fact]
    public void Unregistered_response_classifier_fails()
    {
        Fails(new() { ["Url"] = $"{Https}/orders", ["ResponseClassifier"] = "ledger" }, $"{P}.ResponseClassifier: no response classifier is registered under this name");
    }

    [Fact]
    public void Registered_hooks_pass_and_are_named_in_the_plan()
    {
        var hooks = new HttpHookLookup(n => n == "envelope", n => n == "ledger");

        var plan = Plan(new() { ["Url"] = $"{Https}/orders", ["RequestMapper"] = "envelope", ["ResponseClassifier"] = "ledger" }, hooks);

        Assert.Equal("envelope", plan.RequestMapper);
        Assert.Equal("ledger", plan.ResponseClassifier);
    }

    [Fact]
    public void Forward_headers_listed_needs_names()
    {
        Fails(new() { ["Url"] = $"{Https}/orders", ["ForwardHeaders:Mode"] = "Listed" }, $"{P}.ForwardHeaders.Names: required when Mode is Listed");
    }

    [Fact]
    public void Forward_header_names_must_be_tokens()
    {
        Fails(
            new() { ["Url"] = $"{Https}/orders", ["ForwardHeaders:Exclude:0"] = "bad name" },
            $"{P}.ForwardHeaders.Exclude[0]: must be an RFC 9110 token");
    }

    [Fact]
    public void All_failures_are_reported_together()
    {
        var failures = Failures(new()
        {
            ["Url"] = $"{Https}/{{route}}",
            ["Method"] = "GET",
            ["MaxRequestsInFlight"] = "0",
            ["Headers:Host"] = "x",
            ["StatusCodes:Paused:0"] = "abc",
            ["RequestMapper"] = "envelope",
        });

        Assert.Equal(6, failures.Count);
    }

    [Fact]
    public void Failures_never_carry_configured_values()
    {
        var failures = Failures(new()
        {
            ["Url"] = "https:" + "//relay:sentinel-pw@ledger.example/sentinel-path#sentinel",
            ["Method"] = "sentinel-method",
            ["ContentType"] = "sentinel content",
            ["HttpVersion"] = "sentinel-version",
            ["Headers:X-A"] = "sentinel\nvalue",
            ["StatusCodes:Delivered:0"] = "sentinel-code",
            ["Authentication:Scheme"] = "Bearer",
            ["Authentication:Credential"] = "sentinel-credential",
            ["Signing:Scheme"] = "HmacSha256",
            ["Signing:HeaderName"] = "X-Sig",
            ["Signing:Credential"] = "sentinel-signing",
            ["Tls:CaCertificatePath"] = "/sentinel/ca.pem",
            ["HealthProbe:Url"] = "https:" + "//ledger.example/sentinel-{id}",
            ["HealthProbe:Method"] = "sentinel",
            ["RequestMapper"] = "sentinel-mapper",
            ["ResponseClassifier"] = "sentinel-classifier",
        });

        Assert.True(failures.Count >= 12, string.Join(Environment.NewLine, failures));
        Assert.All(failures, f => Assert.DoesNotContain("sentinel", f, StringComparison.OrdinalIgnoreCase));
    }

    [Fact]
    public void Binding_failure_suppresses_only_rules_on_the_failed_key()
    {
        var failures = Failures(new()
        {
            ["Url"] = $"{Https}/orders",
            ["Method"] = "GET",
            ["SendTimeout"] = "30",
            ["Signing:Scheme"] = "HmacSha256",
            ["Signing:HeaderName"] = "X-Sig",
            ["Signing:Encoding"] = "sentinel",
        });

        Assert.Equal(
            [
                $"{P}.Method: must be one of POST, PUT, PATCH",
                $"{P}.SendTimeout: must be a duration in hh:mm:ss form",
                $"{P}.Signing.Credential: required",
                $"{P}.Signing.Encoding: must be one of Hex, Base64",
            ],
            failures.Order(StringComparer.Ordinal));
    }

    [Fact]
    public void Binding_failure_on_a_scheme_suppresses_its_scheme_dependent_rules()
    {
        var failures = Failures(new()
        {
            ["Url"] = $"{Https}/orders",
            ["Authentication:Scheme"] = "sentinel",
            ["Authentication:Credential"] = "Ledger",
            ["Authentication:HeaderName"] = "X-Api-Key",
            ["Signing:Scheme"] = "HmacSha256",
            ["Signing:HeaderName"] = "X-Sig",
            ["Signing:Credential"] = "Missing",
        });

        Assert.Equal(
            [
                $"{P}.Authentication.Scheme: must be one of {string.Join(", ", Enum.GetNames<AuthenticationScheme>())}",
                $"{P}.Signing.Credential: not defined in HttpCredentials",
            ],
            failures.Order(StringComparer.Ordinal));
    }

    [Fact]
    public void Each_outbox_folds_its_own_name()
    {
        var plan = HttpPlanFixture.Plan(new() { ["Url"] = $"{Https}/{{outbox}}/events" }, outboxes: ["Orders", "Billing"]);

        Assert.Equal(
            [$"{Https}/Billing/events", $"{Https}/Orders/events"],
            plan.Endpoints.Endpoints.Select(e => e.StaticUri!.AbsoluteUri).Order(StringComparer.Ordinal));
    }

    private static Dictionary<string, string?> OAuth2(string? tokenUrl) => new()
    {
        ["Url"] = $"{Https}/orders",
        ["Authentication:Scheme"] = "OAuth2ClientCredentials",
        ["Authentication:Credential"] = "Ledger",
        ["Authentication:TokenUrl"] = tokenUrl,
        ["!HttpCredentials:Ledger:ClientId"] = "id",
        ["!HttpCredentials:Ledger:ClientSecret"] = "secret",
    };

    private static Dictionary<string, string?> Hmac(string header) => new()
    {
        ["Url"] = $"{Https}/orders",
        ["Signing:Scheme"] = "HmacSha256",
        ["Signing:HeaderName"] = header,
        ["Signing:Credential"] = "Signing",
        ["!HttpCredentials:Signing:SigningSecret"] = "secret",
    };

    private static Dictionary<string, string?> Tls(Dictionary<string, string?> tls)
    {
        tls["Url"] = $"{Https}/orders";
        return tls;
    }

    private (string Certificate, string Key) WritePem(string name)
    {
        using var rsa = RSA.Create(2048);
        using var certificate = new CertificateRequest($"CN={name}", rsa, HashAlgorithmName.SHA256, RSASignaturePadding.Pkcs1)
            .CreateSelfSigned(DateTimeOffset.UtcNow.AddDays(-1), DateTimeOffset.UtcNow.AddDays(1));
        var certificatePath = System.IO.Path.Combine(directory, $"{name}.crt");
        var keyPath = System.IO.Path.Combine(directory, $"{name}.key");
        File.WriteAllText(certificatePath, certificate.ExportCertificatePem());
        File.WriteAllText(keyPath, rsa.ExportPkcs8PrivateKeyPem());
        return (certificatePath, keyPath);
    }
}
