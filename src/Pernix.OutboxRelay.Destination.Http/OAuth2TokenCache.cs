using System.Net;
using System.Net.Http.Headers;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using Microsoft.Extensions.Logging;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Destination.Http.Options;

namespace Pernix.OutboxRelay.Destination.Http;

internal readonly record struct OAuth2TokenResult(string? HeaderValue, DateTimeOffset ExpiresAt, DeliveryResult.DestinationFault? Fault);

internal sealed class OAuth2TokenCache : IDisposable
{
    internal static readonly TimeSpan ExpirySkew = TimeSpan.FromSeconds(60);
    internal static readonly TimeSpan MaxLifetime = TimeSpan.FromHours(24);
    internal const int MaxResponseSize = 64 * 1024;
    private const string FaultPrefix = "OAuth2 token: ";

    private static readonly MediaTypeHeaderValue FormContentType = new("application/x-www-form-urlencoded");
    private static readonly MediaTypeWithQualityHeaderValue JsonAccept = new("application/json");

    private readonly string destination;
    private readonly Uri tokenUrl;
    private readonly byte[] form;
    private readonly AuthenticationHeaderValue? clientAuthorization;
    private readonly HttpMessageInvoker invoker;
    private readonly ILogger logger;
    private readonly TimeProvider time;
    private readonly SemaphoreSlim refresh = new(1, 1);

    private CachedToken? current;

    public OAuth2TokenCache(
        string destination,
        AuthenticationOptions options,
        HttpCredential credential,
        HandlerSettings settings,
        ILogger logger,
        TimeProvider? time = null)
        : this(destination, options, credential, HttpHandlerFactory.Create(TokenSettings(settings)), logger, time)
    {
    }

    internal OAuth2TokenCache(
        string destination,
        AuthenticationOptions options,
        HttpCredential credential,
        HttpMessageHandler handler,
        ILogger logger,
        TimeProvider? time = null)
    {
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentNullException.ThrowIfNull(credential);
        ArgumentNullException.ThrowIfNull(handler);
        ArgumentNullException.ThrowIfNull(logger);

        if (!Uri.TryCreate(options.TokenUrl, UriKind.Absolute, out var url))
        {
            throw new ArgumentException("authentication needs an absolute TokenUrl", nameof(options));
        }

        var clientId = Required(credential.ClientId, nameof(HttpCredential.ClientId));
        var clientSecret = Required(credential.ClientSecret, nameof(HttpCredential.ClientSecret));

        this.destination = destination;
        this.logger = logger;
        this.time = time ?? TimeProvider.System;
        tokenUrl = url;

        var fields = new List<KeyValuePair<string, string>> { new("grant_type", "client_credentials") };
        if (!string.IsNullOrEmpty(options.Scope))
        {
            fields.Add(new("scope", options.Scope));
        }

        if (!string.IsNullOrEmpty(options.Audience))
        {
            fields.Add(new("audience", options.Audience));
        }

        if ((options.ClientCredentialsIn ?? ClientCredentialsLocation.Header) == ClientCredentialsLocation.Body)
        {
            fields.Add(new("client_id", clientId));
            fields.Add(new("client_secret", clientSecret));
        }
        else
        {
            // RFC 6749 2.3.1: the id and secret are form-encoded before Basic encoding.
            var pair = Uri.EscapeDataString(clientId) + ":" + Uri.EscapeDataString(clientSecret);
            clientAuthorization = new AuthenticationHeaderValue("Basic", Convert.ToBase64String(Encoding.UTF8.GetBytes(pair)));
        }

        form = Encoding.ASCII.GetBytes(string.Join('&', fields.Select(f => Uri.EscapeDataString(f.Key) + "=" + Uri.EscapeDataString(f.Value))));
        invoker = new HttpMessageInvoker(handler, disposeHandler: true);
    }

    public string TokenHost => tokenUrl.Host;

    public ValueTask<OAuth2TokenResult> GetAsync(CancellationToken cancellationToken)
    {
        var cached = Volatile.Read(ref current);
        if (cached is not null && cached.ExpiresAt > time.GetUtcNow())
        {
            return new(new OAuth2TokenResult(cached.HeaderValue, cached.ExpiresAt, null));
        }

        return RefreshAsync(cancellationToken);
    }

    public void Invalidate(string headerValue)
    {
        ArgumentNullException.ThrowIfNull(headerValue);

        var cached = Volatile.Read(ref current);
        if (cached is not null && ReferenceEquals(cached.HeaderValue, headerValue))
        {
            Interlocked.CompareExchange(ref current, null, cached);
        }
    }

    public void Dispose()
    {
        invoker.Dispose();
        refresh.Dispose();
    }

    private async ValueTask<OAuth2TokenResult> RefreshAsync(CancellationToken cancellationToken)
    {
        await refresh.WaitAsync(cancellationToken).ConfigureAwait(false);
        try
        {
            var cached = Volatile.Read(ref current);
            if (cached is not null && cached.ExpiresAt > time.GetUtcNow())
            {
                return new(cached.HeaderValue, cached.ExpiresAt, null);
            }

            var (fetched, failure) = await FetchAsync(cancellationToken).ConfigureAwait(false);
            if (fetched is null)
            {
                logger.HttpTokenRefreshFailed(destination, TokenHost, failure!);
                return new(null, default, new DeliveryResult.DestinationFault(FaultPrefix + failure));
            }

            Volatile.Write(ref current, fetched);
            return new(fetched.HeaderValue, fetched.ExpiresAt, null);
        }
        finally
        {
            refresh.Release();
        }
    }

    private async Task<(CachedToken? Token, string? Failure)> FetchAsync(CancellationToken cancellationToken)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, tokenUrl)
        {
            Version = HttpVersion.Version11,
            VersionPolicy = HttpVersionPolicy.RequestVersionOrLower,
            Content = new ByteArrayContent(form),
        };
        request.Content.Headers.ContentType = FormContentType;
        request.Headers.Accept.Add(JsonAccept);
        request.Headers.Authorization = clientAuthorization;

        var issuedAt = time.GetUtcNow();
        try
        {
            using var response = await invoker.SendAsync(request, cancellationToken).ConfigureAwait(false);
            if (response.StatusCode != HttpStatusCode.OK)
            {
                return (null, $"HTTP {(int)response.StatusCode}");
            }

            var body = await ReadBoundedAsync(response.Content, cancellationToken).ConfigureAwait(false);
            if (body is null)
            {
                return (null, "response too large");
            }

            if (!TryParse(body, out var accessToken, out var expiresIn))
            {
                return (null, "invalid response");
            }

            // No expires_in (optional per RFC 6749 5.1): cache until a 401 Invalidate clears it.
            if (expiresIn is not { } seconds)
            {
                return (new CachedToken("Bearer " + accessToken, DateTimeOffset.MaxValue), null);
            }

            return (new CachedToken("Bearer " + accessToken, issuedAt + Lifetime(seconds)), null);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            return (null, Describe(exception));
        }
    }

    // expiresIn is null when expires_in is absent, a token FetchAsync caches until Invalidate.
    internal static bool TryParse(ReadOnlySpan<byte> body, out string? accessToken, out long? expiresIn)
    {
        accessToken = null;
        expiresIn = null;
        var expiresValid = true;
        try
        {
            var reader = new Utf8JsonReader(body);
            if (!reader.Read() || reader.TokenType != JsonTokenType.StartObject)
            {
                return false;
            }

            while (reader.Read() && reader.TokenType == JsonTokenType.PropertyName)
            {
                if (reader.ValueTextEquals("access_token"u8))
                {
                    reader.Read();
                    accessToken = reader.TokenType == JsonTokenType.String ? reader.GetString() : null;
                }
                else if (reader.ValueTextEquals("expires_in"u8))
                {
                    reader.Read();
                    if (reader.TokenType == JsonTokenType.Number && reader.TryGetInt64(out var number))
                    {
                        expiresIn = number;
                    }
                    else if (reader.TokenType == JsonTokenType.String && long.TryParse(reader.GetString(), out var text))
                    {
                        expiresIn = text;
                    }
                    else
                    {
                        expiresIn = null;
                        expiresValid = false;
                        reader.Skip();
                    }
                }
                else
                {
                    reader.Read();
                    reader.Skip();
                }
            }
        }
        catch (JsonException)
        {
            return false;
        }

        return !string.IsNullOrEmpty(accessToken) && expiresValid && expiresIn is null or >= 0;
    }

    // Clamped before TimeSpan arithmetic: a huge expires_in would overflow TimeSpan or DateTimeOffset.
    internal static TimeSpan Lifetime(long expiresIn)
    {
        var lifetime = expiresIn >= (long)MaxLifetime.TotalSeconds ? MaxLifetime : TimeSpan.FromSeconds(expiresIn);
        lifetime -= ExpirySkew;
        return lifetime > TimeSpan.Zero ? lifetime : TimeSpan.Zero;
    }

    internal static string Describe(Exception exception)
    {
        var name = exception.GetType().Name;
        for (var inner = exception.InnerException; inner is not null; inner = inner.InnerException)
        {
            if (inner is SocketException socket)
            {
                return $"{name}: {socket.SocketErrorCode}";
            }
        }

        return exception is HttpRequestException { HttpRequestError: not HttpRequestError.Unknown } http
            ? $"{name}: {http.HttpRequestError}"
            : name;
    }

    private static HandlerSettings TokenSettings(HandlerSettings settings)
    {
        ArgumentNullException.ThrowIfNull(settings);
        return new HandlerSettings(settings.ConnectTimeout, 1, HandlerSettings.Http11, settings.TrustedRoots, settings.ClientCertificate);
    }

    private static async Task<byte[]?> ReadBoundedAsync(HttpContent content, CancellationToken cancellationToken)
    {
        if (content.Headers.ContentLength > MaxResponseSize)
        {
            return null;
        }

        var stream = await content.ReadAsStreamAsync(cancellationToken).ConfigureAwait(false);
        await using (stream.ConfigureAwait(false))
        {
            var buffer = new byte[MaxResponseSize + 1];
            var length = 0;
            int read;
            while (length < buffer.Length && (read = await stream.ReadAsync(buffer.AsMemory(length), cancellationToken).ConfigureAwait(false)) > 0)
            {
                length += read;
            }

            return length > MaxResponseSize ? null : buffer[..length];
        }
    }

    private static string Required(string? value, string field) =>
        string.IsNullOrEmpty(value) ? throw new ArgumentException($"authentication needs {field}", field) : value;

    private sealed record CachedToken(string HeaderValue, DateTimeOffset ExpiresAt);
}
