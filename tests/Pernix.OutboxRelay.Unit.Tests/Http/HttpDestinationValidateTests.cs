using System.Net;
using System.Text;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Destination.Http;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Http;

public sealed class HttpDestinationValidateTests
{
    private const string Https = HttpDestinationFixture.Https;
    private const string MainPath = "/orders";

    private readonly HttpDestinationFixture fixture = new();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private IEnumerable<SeenRequest> Requests => fixture.Handlers.SelectMany(static h => h.Requests);

    [Fact]
    public async Task Without_token_or_probe_nothing_is_sent()
    {
        await using var destination = Probe(HttpDestinationFixture.Url(), Ok);

        await destination.ValidateAsync(Ct);

        Assert.Empty(Requests);
    }

    [Fact]
    public async Task OAuth2_fetches_one_token_and_never_calls_the_main_url()
    {
        await using var destination = Probe(OAuth2(), Respond(token: HttpStatusCode.OK));

        await destination.ValidateAsync(Ct);

        var request = Assert.Single(Requests);
        Assert.Equal($"{Https}/token", request.Uri);
    }

    [Fact]
    public async Task OAuth2_failure_names_destination_and_token_host_only()
    {
        await using var destination = Probe(OAuth2(), Respond(token: HttpStatusCode.Unauthorized));

        var exception = await Assert.ThrowsAsync<DestinationValidationException>(() => destination.ValidateAsync(Ct));

        Assert.Equal("Destinations[Sink]: OAuth2 token from host 'ledger.example' failed: OAuth2 token: HTTP 401", exception.Message);
        Assert.DoesNotContain(Requests, static r => r.Uri.EndsWith(MainPath, StringComparison.Ordinal));
    }

    [Fact]
    public async Task Probe_defaults_to_head_and_2xx()
    {
        await using var destination = Probe(WithProbe(), Respond(probe: HttpStatusCode.NoContent));

        await destination.ValidateAsync(Ct);

        var request = Assert.Single(Requests);
        Assert.Equal(HttpMethod.Head, request.Method);
        Assert.Equal($"{Https}/healthz?secret=s3cr3t", request.Uri);
        Assert.Null(request.ContentLength);
    }

    [Fact]
    public async Task Probe_uses_configured_method_and_expected_codes()
    {
        var settings = WithProbe();
        settings["HealthProbe:Method"] = "GET";
        settings["HealthProbe:Expected:0"] = "404";
        await using var destination = Probe(settings, Respond(probe: HttpStatusCode.NotFound));

        await destination.ValidateAsync(Ct);

        Assert.Equal(HttpMethod.Get, Assert.Single(Requests).Method);
    }

    [Fact]
    public async Task Unexpected_probe_status_fails_naming_destination_and_host_and_logs()
    {
        var settings = WithProbe();
        settings["HealthProbe:Url"] = "https:" + "//health.example/healthz?secret=s3cr3t";
        await using var failing = Probe(settings, Respond(probe: HttpStatusCode.ServiceUnavailable));

        var exception = await Assert.ThrowsAsync<DestinationValidationException>(() => failing.ValidateAsync(Ct));

        Assert.Equal("Destinations[Sink]: health probe HEAD to host 'health.example' failed: HTTP 503", exception.Message);
        var logged = Assert.Single(fixture.Logger.WithEvent(302));
        Assert.Contains("health.example", logged.Message, StringComparison.Ordinal);
        Assert.Contains("503", logged.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("healthz", logged.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("s3cr3t", logged.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Probe_transport_failure_names_the_error_not_the_url()
    {
        await using var destination = Probe(WithProbe(), (_, _) =>
            Task.FromException<HttpResponseMessage>(new HttpRequestException(HttpRequestError.ConnectionError, $"{Https}/healthz?secret=s3cr3t")));

        var exception = await Assert.ThrowsAsync<DestinationValidationException>(() => destination.ValidateAsync(Ct));

        Assert.Equal("Destinations[Sink]: health probe HEAD to host 'ledger.example' failed: HttpRequestException: ConnectionError", exception.Message);
    }

    [Fact]
    public async Task Probe_timeout_fails_validation()
    {
        var settings = WithProbe();
        settings["SendTimeout"] = "00:00:00.2";
        settings["ConnectTimeout"] = "00:00:00.1";
        await using var destination = Probe(settings, static async (_, ct) =>
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            return new HttpResponseMessage(HttpStatusCode.OK);
        });

        var exception = await Assert.ThrowsAsync<DestinationValidationException>(() => destination.ValidateAsync(Ct));

        Assert.Equal("Destinations[Sink]: health probe HEAD to host 'ledger.example' failed: timeout after 00:00:00.2000000", exception.Message);
    }

    [Fact]
    public async Task Caller_cancellation_during_the_probe_propagates()
    {
        await using var destination = Probe(WithProbe(), static async (_, ct) =>
        {
            await Task.Delay(Timeout.InfiniteTimeSpan, ct);
            return new HttpResponseMessage(HttpStatusCode.OK);
        });
        using var cancel = new CancellationTokenSource();

        var validate = destination.ValidateAsync(cancel.Token);
        await cancel.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => validate);
    }

    [Fact]
    public async Task Token_then_probe_and_never_the_main_url()
    {
        var settings = OAuth2();
        settings["HealthProbe:Url"] = $"{Https}/healthz";
        await using var destination = Probe(settings, Respond(token: HttpStatusCode.OK, probe: HttpStatusCode.OK));

        await destination.ValidateAsync(Ct);

        Assert.Equal([$"{Https}/token", $"{Https}/healthz"], Requests.Select(static r => r.Uri));
    }

    [Fact]
    public async Task Routes_destination_validation_never_calls_a_route_url()
    {
        await using var destination = Probe(
            new()
            {
                ["Routes:alerts:Url"] = $"{Https}/alerts",
                ["HealthProbe:Url"] = $"{Https}/healthz",
            },
            Ok);

        await destination.ValidateAsync(Ct);

        Assert.Equal([$"{Https}/healthz"], Requests.Select(static r => r.Uri));
    }

    private static Task<HttpResponseMessage> Ok(HttpRequestMessage request, CancellationToken ct) =>
        Task.FromResult(new HttpResponseMessage(HttpStatusCode.OK));

    private static Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> Respond(
        HttpStatusCode token = HttpStatusCode.OK, HttpStatusCode probe = HttpStatusCode.OK) =>
        (request, _) =>
        {
            var path = request.RequestUri!.AbsolutePath;
            if (path == "/token")
            {
                return Task.FromResult(token == HttpStatusCode.OK
                    ? new HttpResponseMessage(HttpStatusCode.OK)
                    {
                        Content = new StringContent("{\"access_token\":\"tok\",\"expires_in\":3600}", Encoding.UTF8, "application/json"),
                    }
                    : new HttpResponseMessage(token));
            }

            return Task.FromResult(new HttpResponseMessage(path == "/healthz" ? probe : HttpStatusCode.InternalServerError));
        };

    private static Dictionary<string, string?> WithProbe()
    {
        var settings = HttpDestinationFixture.Url(MainPath);
        settings["HealthProbe:Url"] = $"{Https}/healthz?secret=s3cr3t";
        return settings;
    }

    private static Dictionary<string, string?> OAuth2() => new()
    {
        ["Url"] = Https + MainPath,
        ["Authentication:Scheme"] = "OAuth2ClientCredentials",
        ["Authentication:Credential"] = "Ledger",
        ["Authentication:TokenUrl"] = $"{Https}/token",
        ["!HttpCredentials:Ledger:ClientId"] = "id",
        ["!HttpCredentials:Ledger:ClientSecret"] = "secret",
    };

    private HttpDestination Probe(
        Dictionary<string, string?> settings, Func<HttpRequestMessage, CancellationToken, Task<HttpResponseMessage>> respond) =>
        fixture.Create(settings, respond, DestinationContext.Probe);
}
