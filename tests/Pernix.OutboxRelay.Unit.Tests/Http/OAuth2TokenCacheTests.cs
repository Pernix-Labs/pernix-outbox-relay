using System.Net;
using System.Net.Sockets;
using System.Text;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Time.Testing;
using Pernix.OutboxRelay.Destination.Http;
using Pernix.OutboxRelay.Destination.Http.Options;
using Pernix.OutboxRelay.Unit.Tests.Leadership;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Http;

public sealed class OAuth2TokenCacheTests
{
    private const string Secret = "s3cr3t+/=";

    private readonly FakeTimeProvider time = new(new DateTimeOffset(2026, 11, 26, 12, 0, 0, TimeSpan.Zero));
    private readonly RecordingLogger<OAuth2TokenCache> logger = new();

    [Fact]
    public async Task Header_credentials_post_the_client_credentials_form()
    {
        var handler = new StubHandler(_ => Token("tok-1", 3600));
        using var cache = Cache(handler, ClientCredentialsLocation.Header, scope: "read write", audience: "https://api.test");

        var result = await cache.GetAsync(TestContext.Current.CancellationToken);

        Assert.Equal("Bearer tok-1", result.HeaderValue);
        var request = Assert.Single(handler.Requests);
        Assert.Equal(HttpMethod.Post, request.Method);
        Assert.Equal("https://idp.test:8443/oauth/token", request.Uri);
        Assert.Equal(HttpVersion.Version11, request.Version);
        Assert.Equal("application/x-www-form-urlencoded", request.ContentType);
        Assert.Equal("grant_type=client_credentials&scope=read%20write&audience=https%3A%2F%2Fapi.test", request.Body);
        var expected = Convert.ToBase64String(Encoding.UTF8.GetBytes("client%20one:" + Uri.EscapeDataString(Secret)));
        Assert.Equal("Basic " + expected, request.Authorization);
    }

    [Fact]
    public async Task Body_credentials_go_in_the_form()
    {
        var handler = new StubHandler(_ => Token("tok-1", 3600));
        using var cache = Cache(handler, ClientCredentialsLocation.Body);

        await cache.GetAsync(TestContext.Current.CancellationToken);

        var request = Assert.Single(handler.Requests);
        Assert.Null(request.Authorization);
        Assert.Equal("grant_type=client_credentials&client_id=client%20one&client_secret=" + Uri.EscapeDataString(Secret), request.Body);
    }

    [Fact]
    public async Task Token_is_cached_until_expires_in_minus_60_seconds()
    {
        var issued = 0;
        var handler = new StubHandler(_ => Token($"tok-{++issued}", 120));
        using var cache = Cache(handler);
        var ct = TestContext.Current.CancellationToken;

        var first = await cache.GetAsync(ct);
        Assert.Equal(time.GetUtcNow().AddSeconds(60), first.ExpiresAt);

        time.Advance(TimeSpan.FromSeconds(59));
        var cached = await cache.GetAsync(ct);
        Assert.Same(first.HeaderValue, cached.HeaderValue);

        time.Advance(TimeSpan.FromSeconds(1));
        var refreshed = await cache.GetAsync(ct);
        Assert.Equal("Bearer tok-2", refreshed.HeaderValue);
        Assert.Equal(2, handler.Count);
    }

    [Fact]
    public async Task Lifetime_below_the_skew_is_not_cached()
    {
        var handler = new StubHandler(_ => Token("tok", 30));
        using var cache = Cache(handler);
        var ct = TestContext.Current.CancellationToken;

        var first = await cache.GetAsync(ct);
        await cache.GetAsync(ct);

        Assert.Equal(time.GetUtcNow(), first.ExpiresAt);
        Assert.Equal(2, handler.Count);
    }

    [Theory]
    [InlineData("86400")]
    [InlineData("1000000000000")]
    [InlineData("9223372036854775807")]
    public async Task Huge_expires_in_is_clamped_to_the_max_lifetime(string expiresIn)
    {
        var handler = new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent($"{{\"access_token\":\"tok\",\"expires_in\":{expiresIn}}}", Encoding.UTF8, "application/json"),
        });
        using var cache = Cache(handler);

        var result = await cache.GetAsync(TestContext.Current.CancellationToken);

        Assert.Equal("Bearer tok", result.HeaderValue);
        Assert.Null(result.Fault);
        Assert.Equal(time.GetUtcNow() + OAuth2TokenCache.MaxLifetime - OAuth2TokenCache.ExpirySkew, result.ExpiresAt);
    }

    [Fact]
    public async Task Concurrent_callers_share_one_fetch()
    {
        var release = new TaskCompletionSource();
        var handler = new StubHandler(async _ =>
        {
            await release.Task;
            return Token("tok-1", 3600);
        });
        using var cache = Cache(handler);

        var callers = Enumerable.Range(0, 32).Select(_ => cache.GetAsync(TestContext.Current.CancellationToken).AsTask()).ToArray();
        release.SetResult();
        var results = await Task.WhenAll(callers);

        Assert.Equal(1, handler.Count);
        Assert.All(results, r => Assert.Equal("Bearer tok-1", r.HeaderValue));
    }

    [Fact]
    public async Task Invalidate_clears_only_the_same_token_without_a_stampede()
    {
        var issued = 0;
        var handler = new StubHandler(_ => Token($"tok-{++issued}", 3600));
        using var cache = Cache(handler);
        var ct = TestContext.Current.CancellationToken;

        var rejected = (await cache.GetAsync(ct)).HeaderValue!;
        cache.Invalidate(rejected);
        var replacement = await cache.GetAsync(ct);

        foreach (var _ in Enumerable.Range(0, 16))
        {
            cache.Invalidate(rejected);
        }

        var results = await Task.WhenAll(Enumerable.Range(0, 16).Select(_ => cache.GetAsync(ct).AsTask()));

        Assert.Equal("Bearer tok-2", replacement.HeaderValue);
        Assert.All(results, r => Assert.Same(replacement.HeaderValue, r.HeaderValue));
        Assert.Equal(2, handler.Count);
    }

    [Fact]
    public async Task Invalidate_by_equal_text_from_another_fetch_is_ignored()
    {
        var handler = new StubHandler(_ => Token("tok", 3600));
        using var cache = Cache(handler);
        var ct = TestContext.Current.CancellationToken;

        var cached = (await cache.GetAsync(ct)).HeaderValue!;
        cache.Invalidate(new string(cached.AsSpan()));
        await cache.GetAsync(ct);

        Assert.Equal(1, handler.Count);
    }

    [Fact]
    public async Task Error_status_returns_a_fault_and_logs_destination_and_host_only()
    {
        var handler = new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.BadRequest)
        {
            Content = new StringContent("{\"error\":\"invalid_client\",\"secret\":\"" + Secret + "\"}"),
        });
        using var cache = Cache(handler);

        var result = await cache.GetAsync(TestContext.Current.CancellationToken);

        Assert.Null(result.HeaderValue);
        Assert.Equal("OAuth2 token: HTTP 400", result.Fault?.Error);
        var entry = Assert.Single(logger.WithEvent(301));
        Assert.Equal(LogLevel.Warning, entry.Level);
        Assert.Contains("orders-webhook", entry.Message, StringComparison.Ordinal);
        Assert.Contains("idp.test", entry.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("/oauth/token", entry.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(Secret, entry.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Connection_failure_names_exception_and_socket_error()
    {
        var handler = new StubHandler(HttpResponseMessage (_) => throw new HttpRequestException(
            HttpRequestError.ConnectionError, "refused https://idp.test:8443/oauth/token", new SocketException((int)SocketError.ConnectionRefused)));
        using var cache = Cache(handler);

        var result = await cache.GetAsync(TestContext.Current.CancellationToken);

        Assert.Equal("OAuth2 token: HttpRequestException: ConnectionRefused", result.Fault?.Error);
        Assert.Single(logger.WithEvent(301));
    }

    [Theory]
    [InlineData("not json")]
    [InlineData("[]")]
    [InlineData("{\"expires_in\":3600}")]
    [InlineData("{\"access_token\":42,\"expires_in\":3600}")]
    public async Task Invalid_body_returns_a_fault(string body)
    {
        var handler = new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(body) });
        using var cache = Cache(handler);

        var result = await cache.GetAsync(TestContext.Current.CancellationToken);

        Assert.Equal("OAuth2 token: invalid response", result.Fault?.Error);
    }

    [Fact]
    public async Task Missing_expires_in_caches_until_invalidated()
    {
        var issued = 0;
        var handler = new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.OK)
        {
            Content = new StringContent($"{{\"access_token\":\"tok-{++issued}\",\"token_type\":\"Bearer\"}}", Encoding.UTF8, "application/json"),
        });
        using var cache = Cache(handler);
        var ct = TestContext.Current.CancellationToken;

        var first = await cache.GetAsync(ct);
        time.Advance(TimeSpan.FromDays(3650));
        var cached = await cache.GetAsync(ct);

        Assert.Equal("Bearer tok-1", first.HeaderValue);
        Assert.Same(first.HeaderValue, cached.HeaderValue);
        Assert.Equal(1, handler.Count);

        cache.Invalidate(first.HeaderValue!);
        var refreshed = await cache.GetAsync(ct);

        Assert.Equal("Bearer tok-2", refreshed.HeaderValue);
        Assert.Equal(2, handler.Count);
    }

    [Fact]
    public async Task Oversized_body_returns_a_fault()
    {
        var body = "{\"access_token\":\"" + new string('a', OAuth2TokenCache.MaxResponseSize) + "\",\"expires_in\":3600}";
        var handler = new StubHandler(_ => new HttpResponseMessage(HttpStatusCode.OK) { Content = new StringContent(body) });
        using var cache = Cache(handler);

        var result = await cache.GetAsync(TestContext.Current.CancellationToken);

        Assert.Equal("OAuth2 token: response too large", result.Fault?.Error);
    }

    [Fact]
    public void Parser_accepts_string_expires_in_and_skips_unknown_fields()
    {
        var body = "{\"token_type\":\"Bearer\",\"ext\":{\"a\":[1,2]},\"expires_in\":\"300\",\"access_token\":\"t-1\"}"u8;

        Assert.True(OAuth2TokenCache.TryParse(body, out var token, out var expiresIn));
        Assert.Equal("t-1", token);
        Assert.Equal(300, expiresIn);
    }

    [Fact]
    public async Task Failure_is_not_cached()
    {
        var calls = 0;
        var handler = new StubHandler(_ => ++calls == 1 ? new HttpResponseMessage(HttpStatusCode.ServiceUnavailable) : Token("tok", 3600));
        using var cache = Cache(handler);
        var ct = TestContext.Current.CancellationToken;

        Assert.NotNull((await cache.GetAsync(ct)).Fault);
        Assert.Equal("Bearer tok", (await cache.GetAsync(ct)).HeaderValue);
    }

    [Fact]
    public async Task Caller_cancellation_propagates()
    {
        var handler = new StubHandler(async ct =>
        {
            await Task.Delay(Timeout.Infinite, ct);
            return Token("tok", 3600);
        });
        using var cache = Cache(handler);
        using var cts = new CancellationTokenSource();

        var pending = cache.GetAsync(cts.Token).AsTask();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
        Assert.Empty(logger.WithEvent(301));
    }

    [Fact]
    public async Task Cached_fast_path_allocates_nothing()
    {
        var handler = new StubHandler(_ => Token("tok", 3600));
        using var cache = Cache(handler);
        var ct = TestContext.Current.CancellationToken;
        await cache.GetAsync(ct);
        Measure(cache, ct);

        var bytes = Measure(cache, ct);

        Assert.Equal(0, bytes);
    }

    [Fact]
    public void Settings_constructor_builds_its_own_handler()
    {
        var options = new AuthenticationOptions { TokenUrl = "https://idp.test/token" };
        using var cache = new OAuth2TokenCache(
            "d", options, new HttpCredential { ClientId = "id", ClientSecret = "x" }, new HandlerSettings(TimeSpan.FromSeconds(2), 64, "2"), logger);

        Assert.Equal("idp.test", cache.TokenHost);
    }

    private static long Measure(OAuth2TokenCache cache, CancellationToken ct)
    {
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++)
        {
            var pending = cache.GetAsync(ct);
            if (!pending.IsCompletedSuccessfully || pending.Result.HeaderValue is null)
            {
                throw new InvalidOperationException("fast path missed");
            }
        }

        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    private OAuth2TokenCache Cache(
        StubHandler handler, ClientCredentialsLocation location = ClientCredentialsLocation.Header, string? scope = null, string? audience = null) =>
        new(
            "orders-webhook",
            new AuthenticationOptions
            {
                Scheme = AuthenticationScheme.OAuth2ClientCredentials,
                TokenUrl = "https://idp.test:8443/oauth/token",
                Scope = scope,
                Audience = audience,
                ClientCredentialsIn = location,
            },
            new HttpCredential { ClientId = "client one", ClientSecret = Secret },
            handler,
            logger,
            time);

    private static HttpResponseMessage Token(string token, int expiresIn) => new(HttpStatusCode.OK)
    {
        Content = new StringContent($"{{\"access_token\":\"{token}\",\"token_type\":\"Bearer\",\"expires_in\":{expiresIn}}}", Encoding.UTF8, "application/json"),
    };

    private sealed record SeenRequest(HttpMethod Method, string Uri, Version Version, string? ContentType, string Body, string? Authorization);

    private sealed class StubHandler : HttpMessageHandler
    {
        private readonly Func<CancellationToken, Task<HttpResponseMessage>> respond;
        private readonly List<SeenRequest> requests = [];
        private int count;

        public StubHandler(Func<CancellationToken, HttpResponseMessage> respond) => this.respond = ct => Task.FromResult(respond(ct));

        public StubHandler(Func<CancellationToken, Task<HttpResponseMessage>> respond) => this.respond = respond;

        public int Count => Volatile.Read(ref count);

        public IReadOnlyList<SeenRequest> Requests
        {
            get
            {
                lock (requests)
                {
                    return [.. requests];
                }
            }
        }

        protected override async Task<HttpResponseMessage> SendAsync(HttpRequestMessage request, CancellationToken cancellationToken)
        {
            Interlocked.Increment(ref count);
            var body = request.Content is null ? "" : await request.Content.ReadAsStringAsync(cancellationToken);
            lock (requests)
            {
                requests.Add(new SeenRequest(
                    request.Method,
                    request.RequestUri!.ToString(),
                    request.Version,
                    request.Content?.Headers.ContentType?.MediaType,
                    body,
                    request.Headers.Authorization?.ToString()));
            }

            return await respond(cancellationToken);
        }
    }
}
