using System.Buffers;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Security.Cryptography;
using System.Text;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Destination.Http.Options;

namespace Pernix.OutboxRelay.Destination.Http;

internal sealed class SigningPlan
{
    internal const string DefaultWebhookId = "{outbox}-{id}";

    private const string SecretPrefix = "whsec_";

    private static readonly SearchValues<char> TokenChars =
        SearchValues.Create("!#$%&'*+-.^_`|~0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz");

    private static readonly SearchValues<char> ControlChars = SearchValues.Create("\r\n\0");

    private SigningPlan(SigningScheme scheme, byte[] secret, string? headerName, SignatureEncoding encoding, string valuePrefix, ValueTemplate? webhookId)
    {
        Scheme = scheme;
        Secret = secret;
        HeaderName = headerName;
        Encoding = encoding;
        ValuePrefix = valuePrefix;
        WebhookId = webhookId;
    }

    public SigningScheme Scheme { get; }

    public string? HeaderName { get; }

    public SignatureEncoding Encoding { get; }

    public string ValuePrefix { get; }

    public ValueTemplate? WebhookId { get; }

    internal byte[] Secret { get; }

    public static bool TryCreate(
        SigningOptions options,
        HttpCredential? credential,
        string outbox,
        string destination,
        [NotNullWhen(true)] out SigningPlan? plan,
        [NotNullWhen(false)] out string? error)
    {
        ArgumentNullException.ThrowIfNull(options);
        plan = null;
        if (options.Scheme is not { } scheme)
        {
            error = "Signing.Scheme: required; expected StandardWebhooks or HmacSha256";
            return false;
        }

        if (string.IsNullOrEmpty(credential?.SigningSecret))
        {
            error = "Signing.Credential: credential needs SigningSecret";
            return false;
        }

        if (scheme == SigningScheme.StandardWebhooks)
        {
            if (options.HeaderName is not null || options.Encoding is not null || options.ValuePrefix is not null)
            {
                error = "Signing: HeaderName, Encoding and ValuePrefix apply only to HmacSha256";
                return false;
            }

            if (!TryDecodeWebhookSecret(credential.SigningSecret, out var secret))
            {
                error = "Signing.Credential: SigningSecret must be 'whsec_' followed by base64";
                return false;
            }

            if (!ValueTemplate.TryCompile(
                    options.WebhookId ?? DefaultWebhookId, TemplateTarget.HeaderValue, outbox, destination, route: null, out var webhookId, out var templateError))
            {
                error = "Signing.WebhookId: " + templateError;
                return false;
            }

            plan = new SigningPlan(scheme, secret, null, SignatureEncoding.Base64, string.Empty, webhookId);
            error = null;
            return true;
        }

        if (options.WebhookId is not null)
        {
            error = "Signing.WebhookId: applies only to StandardWebhooks";
            return false;
        }

        if (string.IsNullOrEmpty(options.HeaderName) || options.HeaderName.AsSpan().ContainsAnyExcept(TokenChars))
        {
            error = "Signing.HeaderName: required for HmacSha256 and must be an RFC 9110 token";
            return false;
        }

        if (options.ValuePrefix is { } prefix && prefix.AsSpan().ContainsAny(ControlChars))
        {
            error = "Signing.ValuePrefix: contains CR, LF or NUL";
            return false;
        }

        plan = new SigningPlan(
            scheme,
            System.Text.Encoding.UTF8.GetBytes(credential.SigningSecret),
            options.HeaderName,
            options.Encoding ?? SignatureEncoding.Hex,
            options.ValuePrefix ?? string.Empty,
            null);
        error = null;
        return true;
    }

    private static bool TryDecodeWebhookSecret(string value, [NotNullWhen(true)] out byte[]? secret)
    {
        secret = null;
        if (!value.StartsWith(SecretPrefix, StringComparison.Ordinal))
        {
            return false;
        }

        var encoded = value.AsSpan(SecretPrefix.Length);
        var buffer = new byte[(encoded.Length * 3 / 4) + 3];
        if (!Convert.TryFromBase64Chars(encoded, buffer, out var written) || written == 0)
        {
            return false;
        }

        secret = buffer.AsSpan(0, written).ToArray();
        return true;
    }
}

// One per send slot: IncrementalHash is not thread-safe.
internal sealed class RequestSigner : IDisposable
{
    internal const string WebhookIdHeader = "webhook-id";
    internal const string WebhookTimestampHeader = "webhook-timestamp";
    internal const string WebhookSignatureHeader = "webhook-signature";

    private const int HashLength = 32;
    private const string WebhookSignaturePrefix = "v1,";
    private const int StackIdBytes = 256;

    private readonly SigningPlan plan;
    private readonly TimeProvider time;
    private readonly IncrementalHash hmac;
    private string? lastTimestamp;
    private long lastTimestampSecond = long.MinValue;

    public RequestSigner(SigningPlan plan, TimeProvider? time = null)
    {
        ArgumentNullException.ThrowIfNull(plan);
        this.plan = plan;
        this.time = time ?? TimeProvider.System;
        hmac = IncrementalHash.CreateHMAC(HashAlgorithmName.SHA256, plan.Secret);
    }

    public static async ValueTask<byte[]> ReadReplacedBodyAsync(HttpRequestMessage request, CancellationToken cancellationToken)
    {
        ArgumentNullException.ThrowIfNull(request);
        return request.Content is null ? [] : await request.Content.ReadAsByteArrayAsync(cancellationToken).ConfigureAwait(false);
    }

    public bool TrySign(
        HttpRequestMessage request,
        ReadOnlySpan<byte> body,
        in RenderContext row,
        [NotNullWhen(false)] out DeliveryResult? fault,
        string? sharedWebhookId = null)
    {
        ArgumentNullException.ThrowIfNull(request);
        if (plan.Scheme == SigningScheme.HmacSha256)
        {
            request.Headers.TryAddWithoutValidation(plan.HeaderName!, SignBody(body));
            fault = null;
            return true;
        }

        var id = sharedWebhookId;
        if (id is null && !plan.WebhookId!.TryRenderHeaderValue(row, out id, out fault))
        {
            return false;
        }

        var timestamp = Timestamp(out var second);
        request.Headers.TryAddWithoutValidation(WebhookIdHeader, id);
        request.Headers.TryAddWithoutValidation(WebhookTimestampHeader, timestamp);
        request.Headers.TryAddWithoutValidation(WebhookSignatureHeader, SignWebhook(id, second, body));
        fault = null;
        return true;
    }

    public void Dispose() => hmac.Dispose();

    internal string SignBody(ReadOnlySpan<byte> body)
    {
        Span<byte> hash = stackalloc byte[HashLength];
        hmac.AppendData(body);
        Finish(hash);
        return Format(plan.ValuePrefix, hash, plan.Encoding);
    }

    internal string SignWebhook(string id, long timestampSecond, ReadOnlySpan<byte> body)
    {
        byte[]? rented = null;
        var maxIdBytes = System.Text.Encoding.UTF8.GetMaxByteCount(id.Length);
        Span<byte> idBytes = maxIdBytes <= StackIdBytes
            ? stackalloc byte[StackIdBytes]
            : (rented = ArrayPool<byte>.Shared.Rent(maxIdBytes));
        try
        {
            var idLength = System.Text.Encoding.UTF8.GetBytes(id, idBytes);
            Span<byte> timestamp = stackalloc byte[20];
            timestampSecond.TryFormat(timestamp, out var timestampLength, provider: CultureInfo.InvariantCulture);

            hmac.AppendData(idBytes[..idLength]);
            hmac.AppendData("."u8);
            hmac.AppendData(timestamp[..timestampLength]);
            hmac.AppendData("."u8);
            hmac.AppendData(body);
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }

        Span<byte> hash = stackalloc byte[HashLength];
        Finish(hash);
        return Format(WebhookSignaturePrefix, hash, SignatureEncoding.Base64);
    }

    internal string Timestamp(out long second)
    {
        second = time.GetUtcNow().ToUnixTimeSeconds();
        if (second != lastTimestampSecond || lastTimestamp is null)
        {
            lastTimestamp = second.ToString(CultureInfo.InvariantCulture);
            lastTimestampSecond = second;
        }

        return lastTimestamp;
    }

    private static string Format(string prefix, ReadOnlySpan<byte> hash, SignatureEncoding encoding)
    {
        var length = prefix.Length + (encoding == SignatureEncoding.Hex ? HashLength * 2 : ((HashLength + 2) / 3) * 4);
        return string.Create(length, new FormatState(prefix, hash, encoding), static (chars, state) =>
        {
            state.Prefix.CopyTo(chars);
            var target = chars[state.Prefix.Length..];
            var ok = state.Encoding == SignatureEncoding.Hex
                ? Convert.TryToHexStringLower(state.Hash, target, out _)
                : Convert.TryToBase64Chars(state.Hash, target, out _);
            if (!ok)
            {
                throw new InvalidOperationException("signature buffer too small");
            }
        });
    }

    private void Finish(Span<byte> hash)
    {
        if (!hmac.TryGetHashAndReset(hash, out var written) || written != HashLength)
        {
            throw new CryptographicException("HMAC-SHA256 produced an unexpected hash length");
        }
    }

    private readonly ref struct FormatState(string prefix, ReadOnlySpan<byte> hash, SignatureEncoding encoding)
    {
        public string Prefix { get; } = prefix;

        public ReadOnlySpan<byte> Hash { get; } = hash;

        public SignatureEncoding Encoding { get; } = encoding;
    }
}
