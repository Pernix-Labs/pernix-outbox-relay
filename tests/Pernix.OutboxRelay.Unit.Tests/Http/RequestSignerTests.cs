using System.Security.Cryptography;
using System.Text;
using Microsoft.Extensions.Time.Testing;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Destination.Http;
using Pernix.OutboxRelay.Destination.Http.Options;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Http;

public sealed class RequestSignerTests
{
    // Published Standard Webhooks reference vector.
    private const string VectorSecret = "whsec_MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw";
    private const string VectorId = "msg_p5jXN8AQM9LWM0D4loKWxJek";
    private const long VectorTimestamp = 1614265330;
    private const string VectorPayload = "{\"test\": 2432232314}";
    private const string VectorSignature = "v1,g0hM9SsE+OTPJTGt/tmIKtSyZlE3uFJELVlNIOLJ1OE=";

    private static readonly byte[] Body = Encoding.UTF8.GetBytes("{\"order\":42}");

    private readonly FakeTimeProvider time = new(DateTimeOffset.FromUnixTimeSeconds(VectorTimestamp));

    [Fact]
    public void StandardWebhooks_matches_the_published_vector()
    {
        using var signer = WebhookSigner(VectorSecret, "msg_p5jXN8AQM9LWM0D4loKWxJek");
        using var request = new HttpRequestMessage();

        Assert.True(signer.TrySign(request, Encoding.UTF8.GetBytes(VectorPayload), Row(1), out var fault), fault?.ToString());

        Assert.Equal(VectorId, Single(request, "webhook-id"));
        Assert.Equal("1614265330", Single(request, "webhook-timestamp"));
        Assert.Equal(VectorSignature, Single(request, "webhook-signature"));
    }

    [Fact]
    public void StandardWebhooks_default_id_is_outbox_dash_id()
    {
        using var signer = WebhookSigner(VectorSecret, webhookId: null);
        using var request = new HttpRequestMessage();

        Assert.True(signer.TrySign(request, Body, Row(42), out _));

        Assert.Equal("orders-42", Single(request, "webhook-id"));
        Assert.Equal(ExpectedWebhook(VectorSecret, "orders-42", VectorTimestamp, Body), Single(request, "webhook-signature"));
    }

    [Fact]
    public void StandardWebhooks_uses_a_shared_id_string()
    {
        using var signer = WebhookSigner(VectorSecret, webhookId: null);
        using var request = new HttpRequestMessage();
        var shared = "orders-42";

        Assert.True(signer.TrySign(request, Body, Row(42), out _, shared));

        Assert.Same(shared, Single(request, "webhook-id"));
    }

    [Fact]
    public void StandardWebhooks_long_id_is_signed()
    {
        var id = new string('x', 300);
        using var signer = WebhookSigner(VectorSecret, "{header:id}");
        using var request = new HttpRequestMessage();

        Assert.True(signer.TrySign(request, Body, Row(1, ("id", id)), out _));

        Assert.Equal(ExpectedWebhook(VectorSecret, id, VectorTimestamp, Body), Single(request, "webhook-signature"));
    }

    [Fact]
    public void StandardWebhooks_id_render_fault_is_returned()
    {
        using var signer = WebhookSigner(VectorSecret, "{header:missing}");
        using var request = new HttpRequestMessage();

        Assert.False(signer.TrySign(request, Body, Row(1), out var fault));

        Assert.IsType<DeliveryResult.MessageFault>(fault);
        Assert.Empty(request.Headers);
    }

    [Fact]
    public void Timestamp_is_cached_per_second()
    {
        using var signer = WebhookSigner(VectorSecret, webhookId: null);

        var first = signer.Timestamp(out _);
        time.Advance(TimeSpan.FromMilliseconds(999));
        var same = signer.Timestamp(out _);
        time.Advance(TimeSpan.FromMilliseconds(1));
        var next = signer.Timestamp(out var second);

        Assert.Same(first, same);
        Assert.Equal("1614265331", next);
        Assert.Equal(VectorTimestamp + 1, second);
    }

    [Theory]
    [InlineData(true, null)]
    [InlineData(true, "sha256=")]
    [InlineData(false, null)]
    [InlineData(false, "sha256=")]
    public void HmacSha256_matches_HashData(bool hex, string? prefix)
    {
        using var signer = HmacSigner(hex ? SignatureEncoding.Hex : SignatureEncoding.Base64, prefix);
        using var request = new HttpRequestMessage();

        Assert.True(signer.TrySign(request, Body, Row(1), out _));

        var hash = HMACSHA256.HashData(Encoding.UTF8.GetBytes("raw-secret"), Body);
        var expected = (prefix ?? string.Empty) + (hex ? Convert.ToHexStringLower(hash) : Convert.ToBase64String(hash));
        Assert.Equal(expected, Single(request, "X-Signature"));
    }

    [Fact]
    public void HmacSha256_defaults_to_hex()
    {
        using var signer = HmacSigner(encoding: null, prefix: null);

        Assert.Equal(Convert.ToHexStringLower(HMACSHA256.HashData(Encoding.UTF8.GetBytes("raw-secret"), Body)), signer.SignBody(Body));
    }

    [Fact]
    public void Hash_resets_between_sends()
    {
        using var signer = HmacSigner(SignatureEncoding.Hex, null);

        Assert.Equal(signer.SignBody(Body), signer.SignBody(Body));
        Assert.Equal(signer.SignWebhook("a", 1, Body), signer.SignWebhook("a", 1, Body));
    }

    [Fact]
    public async Task Replaced_content_is_signed()
    {
        using var signer = HmacSigner(SignatureEncoding.Hex, null);
        using var request = new HttpRequestMessage { Content = new ReadOnlyMemoryContent(Body) };
        request.Content = new StringContent("mapped");

        var bytes = await RequestSigner.ReadReplacedBodyAsync(request, CancellationToken.None);
        Assert.True(signer.TrySign(request, bytes, Row(1), out _));

        Assert.Equal(Convert.ToHexStringLower(HMACSHA256.HashData(Encoding.UTF8.GetBytes("raw-secret"), "mapped"u8)), Single(request, "X-Signature"));
    }

    [Fact]
    public async Task Removed_content_reads_as_empty()
    {
        using var request = new HttpRequestMessage();

        Assert.Empty(await RequestSigner.ReadReplacedBodyAsync(request, CancellationToken.None));
    }

    [Fact]
    public void HmacSha256_allocates_one_string_per_send()
    {
        using var signer = HmacSigner(SignatureEncoding.Hex, "sha256=");
        signer.SignBody(Body);

        Assert.Equal(StringBytes("sha256=".Length + 64), Allocated(() => signer.SignBody(Body)));
    }

    [Fact]
    public void StandardWebhooks_allocates_one_string_per_send()
    {
        using var signer = WebhookSigner(VectorSecret, webhookId: null);
        signer.Timestamp(out var second);
        signer.SignWebhook("orders-42", second, Body);

        Assert.Equal(0, Allocated(() => signer.Timestamp(out _)));
        Assert.Equal(StringBytes(VectorSignature.Length), Allocated(() => signer.SignWebhook("orders-42", second, Body)));
    }

    [Fact]
    public void StandardWebhooks_secret_must_be_whsec_base64()
    {
        AssertCreateFails(new SigningOptions { Scheme = SigningScheme.StandardWebhooks }, "MfKQ9r8GKYqrTwjUPD8ILPZIo2LaLaSw", "Signing.Credential");
        AssertCreateFails(new SigningOptions { Scheme = SigningScheme.StandardWebhooks }, "whsec_not*base64", "Signing.Credential");
        AssertCreateFails(new SigningOptions { Scheme = SigningScheme.StandardWebhooks }, "whsec_", "Signing.Credential");
    }

    [Fact]
    public void Missing_scheme_or_secret_fails()
    {
        AssertCreateFails(new SigningOptions(), "raw-secret", "Signing.Scheme");
        AssertCreateFails(new SigningOptions { Scheme = SigningScheme.HmacSha256, HeaderName = "X-Signature" }, null, "Signing.Credential");
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("X Signature")]
    public void HmacSha256_needs_a_token_header_name(string? name) =>
        AssertCreateFails(new SigningOptions { Scheme = SigningScheme.HmacSha256, HeaderName = name }, "raw-secret", "Signing.HeaderName");

    [Fact]
    public void HmacSha256_prefix_rejects_control_characters() =>
        AssertCreateFails(new SigningOptions { Scheme = SigningScheme.HmacSha256, HeaderName = "X-Signature", ValuePrefix = "a\r\n" }, "raw-secret", "Signing.ValuePrefix");

    [Fact]
    public void Scheme_specific_keys_are_rejected_on_the_other_scheme()
    {
        AssertCreateFails(new SigningOptions { Scheme = SigningScheme.StandardWebhooks, HeaderName = "X" }, VectorSecret, "Signing:");
        AssertCreateFails(new SigningOptions { Scheme = SigningScheme.StandardWebhooks, Encoding = SignatureEncoding.Hex }, VectorSecret, "Signing:");
        AssertCreateFails(new SigningOptions { Scheme = SigningScheme.HmacSha256, HeaderName = "X", WebhookId = "{id}" }, "raw-secret", "Signing.WebhookId");
    }

    [Fact]
    public void Invalid_webhook_id_template_fails()
    {
        AssertCreateFails(new SigningOptions { Scheme = SigningScheme.StandardWebhooks, WebhookId = "{nope}" }, VectorSecret, "Signing.WebhookId");
    }

    private static void AssertCreateFails(SigningOptions options, string? secret, string path)
    {
        var credential = secret is null ? new HttpCredential() : new HttpCredential { SigningSecret = secret };

        Assert.False(SigningPlan.TryCreate(options, credential, "orders", "hooks", out var plan, out var error));
        Assert.Null(plan);
        Assert.StartsWith(path, error, StringComparison.Ordinal);
        if (secret is { Length: > 8 })
        {
            Assert.DoesNotContain(secret, error, StringComparison.Ordinal);
        }
    }

    private RequestSigner WebhookSigner(string secret, string? webhookId) =>
        Signer(new SigningOptions { Scheme = SigningScheme.StandardWebhooks, WebhookId = webhookId }, secret);

    private RequestSigner HmacSigner(SignatureEncoding? encoding, string? prefix) =>
        Signer(new SigningOptions { Scheme = SigningScheme.HmacSha256, HeaderName = "X-Signature", Encoding = encoding, ValuePrefix = prefix }, "raw-secret");

    private RequestSigner Signer(SigningOptions options, string secret)
    {
        Assert.True(SigningPlan.TryCreate(options, new HttpCredential { SigningSecret = secret }, "orders", "hooks", out var plan, out var error), error);
        return new RequestSigner(plan, time);
    }

    private static string ExpectedWebhook(string secret, string id, long timestamp, byte[] body)
    {
        var key = Convert.FromBase64String(secret["whsec_".Length..]);
        var input = Encoding.UTF8.GetBytes($"{id}.{timestamp}.").Concat(body).ToArray();
        return "v1," + Convert.ToBase64String(HMACSHA256.HashData(key, input));
    }

    private static string Single(HttpRequestMessage request, string name) => Assert.Single(request.Headers.GetValues(name));

    private static RenderContext Row(long id, params (string Name, string Value)[] headers) =>
        new(id, null, TestHeaders.Of(headers.Select(h => new KeyValuePair<string, string>(h.Name, h.Value))));

    private static long StringBytes(int length)
    {
        var before = GC.GetAllocatedBytesForCurrentThread();
        GC.KeepAlive(new string('x', length));
        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    private static long Allocated(Action send)
    {
        const int Sends = 100;
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < Sends; i++)
        {
            send();
        }

        return (GC.GetAllocatedBytesForCurrentThread() - before) / Sends;
    }
}
