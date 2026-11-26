using System.Collections.Frozen;
using System.Text;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Destination.Http;
using Pernix.OutboxRelay.Destination.Http.Options;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Http;

public sealed class HeaderForwardingTests
{
    private static readonly FrozenSet<string> NoOverrides = FrozenSet<string>.Empty;

    private static readonly string[] Reserved =
    [
        "Connection", "Keep-Alive", "Proxy-Connection", "Transfer-Encoding", "TE", "Trailer", "Upgrade",
        "Host", "Content-Length", "Expect", "Authorization", "Proxy-Authorization", "Cookie",
    ];

    public static TheoryData<string> ReservedNames => new(Reserved);

    [Fact]
    public void Default_mode_is_all_and_forwards_every_row_header()
    {
        var forwarding = new HeaderForwarding(null);

        var request = Forward(forwarding, Row(("traceparent", "00-abc"), ("tenant-id", "acme")));

        Assert.Equal(ForwardHeadersMode.All, forwarding.Mode);
        Assert.Equal("00-abc", Value(request, "traceparent"));
        Assert.Equal("acme", Value(request, "tenant-id"));
    }

    [Fact]
    public void None_forwards_nothing()
    {
        var request = Forward(Forwarding(ForwardHeadersMode.None), Row(("traceparent", "00-abc"), ("content-type", "text/plain")), out var contentType);

        Assert.Empty(request.Headers);
        Assert.Null(contentType);
    }

    [Fact]
    public void Listed_forwards_only_listed_names_case_insensitively()
    {
        var forwarding = Forwarding(ForwardHeadersMode.Listed, names: ["TraceParent"]);

        var request = Forward(forwarding, Row(("traceparent", "00-abc"), ("tenant-id", "acme")));

        Assert.Equal("00-abc", Value(request, "traceparent"));
        Assert.False(request.Headers.NonValidated.Contains("tenant-id"));
    }

    [Fact]
    public void Exclude_drops_named_headers_in_all_and_listed()
    {
        var all = Forward(Forwarding(ForwardHeadersMode.All, exclude: ["Tenant-Id"]), Row(("traceparent", "t"), ("tenant-id", "acme")));
        var listed = Forward(
            Forwarding(ForwardHeadersMode.Listed, names: ["traceparent", "tenant-id"], exclude: ["tenant-id"]),
            Row(("traceparent", "t"), ("tenant-id", "acme")));

        Assert.False(all.Headers.NonValidated.Contains("tenant-id"));
        Assert.True(all.Headers.NonValidated.Contains("traceparent"));
        Assert.False(listed.Headers.NonValidated.Contains("tenant-id"));
        Assert.True(listed.Headers.NonValidated.Contains("traceparent"));
    }

    [Theory]
    [MemberData(nameof(ReservedNames))]
    public void Reserved_name_is_never_forwarded(string name)
    {
        var row = Row((name.ToLowerInvariant(), "v"), ("traceparent", "t"));

        var all = Forward(new HeaderForwarding(null), row);
        var listed = Forward(Forwarding(ForwardHeadersMode.Listed, names: [name, "traceparent"]), row);

        Assert.False(all.Headers.NonValidated.Contains(name));
        Assert.False(listed.Headers.NonValidated.Contains(name));
        Assert.True(all.Headers.NonValidated.Contains("traceparent"));
    }

    [Fact]
    public void Reserved_set_is_the_spec_list()
    {
        Assert.Equal(13, HeaderForwarding.Reserved.Count);
        Assert.All(Reserved, name => Assert.True(HeaderForwarding.Reserved.Contains(name), name));
    }

    [Fact]
    public void Content_type_moves_to_the_content_headers()
    {
        var request = Forward(new HeaderForwarding(null), Row(("Content-Type", "application/cloudevents+json")), out var contentType);

        Assert.Equal("application/cloudevents+json", contentType);
        Assert.Empty(request.Headers);
    }

    [Fact]
    public void Content_type_wins_over_relay_owned_content_type_and_is_not_prefixed()
    {
        var forwarding = Forwarding(ForwardHeadersMode.All, prefix: "x-outbox-");
        var overridden = new[] { "Content-Type" }.ToFrozenSet(StringComparer.OrdinalIgnoreCase);

        var request = Forward(forwarding, Row(("content-type", "text/plain")), overridden, out var contentType);

        Assert.Equal("text/plain", contentType);
        Assert.Empty(request.Headers);
    }

    [Fact]
    public void Excluded_content_type_is_not_moved()
    {
        Forward(Forwarding(ForwardHeadersMode.All, exclude: ["content-type"]), Row(("content-type", "text/plain")), out var contentType);

        Assert.Null(contentType);
    }

    [Fact]
    public void Name_prefix_is_prepended_in_all()
    {
        var forwarding = Forwarding(ForwardHeadersMode.All, prefix: "x-outbox-");

        var request = Forward(forwarding, Row(("traceparent", "t")));

        Assert.Equal("t", Value(request, "x-outbox-traceparent"));
        Assert.False(request.Headers.NonValidated.Contains("traceparent"));
    }

    [Fact]
    public void Name_prefix_is_prepended_in_listed_from_a_precomputed_name()
    {
        var forwarding = Forwarding(ForwardHeadersMode.Listed, names: ["traceparent"], prefix: "x-outbox-");

        var first = Forward(forwarding, Row(("traceparent", "t")));
        var second = Forward(forwarding, Row(("traceparent", "u")));

        Assert.Null(forwarding.CreateNameCache());
        Assert.Equal("t", Value(first, "x-outbox-traceparent"));
        Assert.Same(NameOf(first), NameOf(second));
    }

    [Fact]
    public void Plan_header_wins_over_forwarded_header()
    {
        var plan = Plan([("Tenant-Id", "config")], []);
        var row = Row(("tenant-id", "row"), ("traceparent", "t"));
        var request = Request();
        HashSet<string>? seen = null;

        Assert.True(new HeaderForwarding(null).TryForward(row, request.Headers, request.Content!.Headers, plan.Overridden, new HeaderValueCache(), null, ref seen, out _, out var fault), Error(fault));
        Assert.True(plan.TryApply(Context(row), request.Headers, request.Content!.Headers, out fault), Error(fault));

        Assert.Equal("config", Value(request, "tenant-id"));
        Assert.Equal("t", Value(request, "traceparent"));
    }

    [Fact]
    public void Relay_owned_header_skips_forwarded_header()
    {
        var plan = Plan([], ["webhook-signature"]);

        var request = Forward(new HeaderForwarding(null), Row(("Webhook-Signature", "forged")), plan.Overridden, out _);

        Assert.Empty(request.Headers);
    }

    [Fact]
    public void Override_matches_the_prefixed_name()
    {
        var plan = Plan([("x-outbox-tenant-id", "config")], []);
        var forwarding = Forwarding(ForwardHeadersMode.All, prefix: "x-outbox-");

        var request = Forward(forwarding, Row(("tenant-id", "row")), plan.Overridden, out _);

        Assert.Empty(request.Headers);
    }

    [Fact]
    public void Plan_dedupes_names_last_wins()
    {
        var plan = Plan([("a", "1"), ("A", "2")], []);
        var request = Request();

        Assert.True(plan.TryApply(Context(Row()), request.Headers, request.Content!.Headers, out var fault), Error(fault));

        Assert.Equal(1, plan.Count);
        Assert.Equal("2", Value(request, "a"));
    }

    [Theory]
    [InlineData("bad name")]
    [InlineData("bad:name")]
    [InlineData("badé")]
    [InlineData("")]
    public void Non_token_name_is_a_permanent_fault_naming_the_header(string name)
    {
        var fault = ForwardFault(new HeaderForwarding(null), Row((name, "secret-value")));

        Assert.True(fault.Permanent);
        Assert.Contains($"'{name}'", fault.Error, StringComparison.Ordinal);
        Assert.DoesNotContain("secret-value", fault.Error, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("a\rb")]
    [InlineData("a\nb")]
    [InlineData("a\0b")]
    public void Control_byte_in_value_is_a_permanent_fault_naming_the_header(string value)
    {
        var fault = ForwardFault(new HeaderForwarding(null), Row(("tenant-id", value)));

        Assert.True(fault.Permanent);
        Assert.Contains("'tenant-id'", fault.Error, StringComparison.Ordinal);
        Assert.DoesNotContain(value, fault.Error, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(12)]
    public void Case_duplicate_is_a_permanent_fault(int count)
    {
        var headers = Enumerable.Range(0, count - 2).Select(i => ($"h{i}", "v")).Append(("Tenant-Id", "a")).Append(("tenant-id", "b")).ToArray();
        HashSet<string>? seen = null;

        var fault = ForwardFault(new HeaderForwarding(null), Row(headers), ref seen);

        Assert.True(fault.Permanent);
        Assert.Contains("'Tenant-Id'", fault.Error, StringComparison.Ordinal);
        Assert.Contains("'tenant-id'", fault.Error, StringComparison.Ordinal);
    }

    [Fact]
    public void Case_duplicate_set_is_reused_and_cleared()
    {
        var headers = Enumerable.Range(0, 10).Select(i => ($"h{i}", "v")).ToArray();
        HashSet<string>? seen = null;
        var forwarding = new HeaderForwarding(null);

        Assert.True(forwarding.TryForward(Row(headers), new HttpRequestMessage().Headers, new ByteArrayContent([]).Headers, NoOverrides, new HeaderValueCache(), null, ref seen, out _, out var fault), Error(fault));
        var first = seen;
        Assert.True(forwarding.TryForward(Row(headers), new HttpRequestMessage().Headers, new ByteArrayContent([]).Headers, NoOverrides, new HeaderValueCache(), null, ref seen, out _, out fault), Error(fault));

        Assert.NotNull(first);
        Assert.Same(first, seen);
    }

    [Fact]
    public void Skipped_header_is_not_validated()
    {
        var reserved = Forward(new HeaderForwarding(null), Row(("Cookie", "a\r\nb")));
        var unlisted = Forward(Forwarding(ForwardHeadersMode.Listed, names: ["traceparent"]), Row(("bad name", "a\r\nb")));

        Assert.Empty(reserved.Headers);
        Assert.Empty(unlisted.Headers);
    }

    [Fact]
    public void Content_header_set_is_the_routed_list()
    {
        Assert.Equal(
            ["Allow", "Content-Disposition", "Content-Encoding", "Content-Language", "Content-Location", "Content-MD5", "Content-Range", "Expires", "Last-Modified"],
            HeaderForwarding.ContentHeaders.Order(StringComparer.Ordinal));
    }

    [Theory]
    [InlineData("content-encoding", "gzip")]
    [InlineData("Allow", "GET")]
    [InlineData("Content-Disposition", "attachment")]
    [InlineData("Content-Language", "en")]
    [InlineData("Content-Location", "/a")]
    [InlineData("Content-MD5", "Q2hlY2s=")]
    [InlineData("Content-Range", "bytes 0-1/2")]
    [InlineData("Expires", "0")]
    [InlineData("Last-Modified", "Tue, 01 Dec 2026 00:00:00 GMT")]
    public void Forwarded_content_header_lands_in_content_headers(string name, string value)
    {
        var request = Forward(new HeaderForwarding(null), Row((name, value)));

        Assert.Empty(request.Headers);
        Assert.Equal(value, ContentValue(request, name));
    }

    [Fact]
    public void Prefixed_content_header_stays_on_request_headers()
    {
        var request = Forward(Forwarding(ForwardHeadersMode.All, prefix: "x-outbox-"), Row(("content-encoding", "gzip")));

        Assert.Equal("gzip", Value(request, "x-outbox-content-encoding"));
        Assert.Null(ContentValue(request, "x-outbox-content-encoding"));
    }

    [Fact]
    public void Plan_content_header_lands_in_content_headers()
    {
        var plan = Plan([("Content-Encoding", "gzip")], []);
        var request = Request();

        Assert.True(plan.TryApply(Context(Row()), request.Headers, request.Content!.Headers, out var fault), Error(fault));

        Assert.Empty(request.Headers);
        Assert.Equal("gzip", ContentValue(request, "content-encoding"));
    }

    [Fact]
    public void Plan_content_header_wins_over_forwarded_content_header()
    {
        var plan = Plan([("Content-Encoding", "br")], []);
        var row = Row(("content-encoding", "gzip"));
        var request = Request();
        HashSet<string>? seen = null;

        Assert.True(new HeaderForwarding(null).TryForward(row, request.Headers, request.Content!.Headers, plan.Overridden, new HeaderValueCache(), null, ref seen, out _, out var fault), Error(fault));
        Assert.True(plan.TryApply(Context(row), request.Headers, request.Content.Headers, out fault), Error(fault));

        Assert.Equal("br", ContentValue(request, "content-encoding"));
    }

    [Fact]
    public void Relay_owned_content_header_skips_forwarded_content_header()
    {
        var plan = Plan([], ["Content-Language"]);

        var request = Forward(new HeaderForwarding(null), Row(("content-language", "fr")), plan.Overridden, out _);

        Assert.Null(ContentValue(request, "content-language"));
    }

    [Fact]
    public void Forwarded_name_is_the_row_name_instance()
    {
        var name = new string("traceparent".AsSpan());
        var request = Forward(new HeaderForwarding(null), Row((name, "t")));

        Assert.Same(name, NameOf(request));
    }

    [Fact]
    public void Value_cache_hits_by_reference_and_by_bytes()
    {
        var cache = new HeaderValueCache();
        var bytes = Encoding.UTF8.GetBytes("acme");

        var first = cache.GetString("tenant-id", bytes);
        var byReference = cache.GetString("tenant-id", bytes);
        var byBytes = cache.GetString(new string("tenant-id".AsSpan()), Encoding.UTF8.GetBytes("acme"));
        var changed = cache.GetString("tenant-id", Encoding.UTF8.GetBytes("acmf"));
        var afterChange = cache.GetString("tenant-id", Encoding.UTF8.GetBytes("acmf"));

        Assert.Same(first, byReference);
        Assert.Same(first, byBytes);
        Assert.Equal("acmf", changed);
        Assert.Same(changed, afterChange);
        Assert.Equal(1, cache.Count);
    }

    [Fact]
    public void Value_cache_is_bounded_and_overflow_decodes()
    {
        var cache = new HeaderValueCache();
        for (var i = 0; i < HeaderValueCache.Capacity; i++)
        {
            cache.GetString($"h{i}", [(byte)'v']);
        }

        var bytes = Encoding.UTF8.GetBytes("x");
        var first = cache.GetString("overflow", bytes);
        var second = cache.GetString("overflow", bytes);

        Assert.Equal(HeaderValueCache.Capacity, cache.Count);
        Assert.Equal("x", second);
        Assert.NotSame(first, second);
    }

    [Fact]
    public void Prefixed_name_cache_is_bounded_and_overflow_concats()
    {
        var cache = new PrefixedNameCache("p-");
        for (var i = 0; i < PrefixedNameCache.Capacity; i++)
        {
            cache.Get($"h{i}");
        }

        var cached = cache.Get("h0");
        var first = cache.Get("overflow");
        var second = cache.Get("overflow");

        Assert.Same(cached, cache.Get(new string("h0".AsSpan())));
        Assert.Equal(PrefixedNameCache.Capacity, cache.Count);
        Assert.Equal("p-overflow", second);
        Assert.NotSame(first, second);
    }

    [Fact]
    public void Cache_hits_allocate_nothing()
    {
        var values = new HeaderValueCache();
        var names = new PrefixedNameCache("x-outbox-");
        var bytes = Encoding.UTF8.GetBytes("acme");
        var copy = Encoding.UTF8.GetBytes("acme");
        values.GetString("tenant-id", bytes);
        names.Get("tenant-id");

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 100; i++)
        {
            values.GetString("tenant-id", bytes);
            values.GetString("tenant-id", copy);
            names.Get("tenant-id");
        }

        Assert.Equal(0, GC.GetAllocatedBytesForCurrentThread() - before);
    }

    [Fact]
    public void Forwarding_with_warm_caches_reuses_name_and_value_strings()
    {
        var forwarding = Forwarding(ForwardHeadersMode.All, prefix: "x-outbox-");
        var values = new HeaderValueCache();
        var names = forwarding.CreateNameCache();
        HashSet<string>? seen = null;

        var first = Request();
        var second = Request();
        Assert.True(forwarding.TryForward(Row(("tenant-id", "acme")), first.Headers, first.Content!.Headers, NoOverrides, values, names, ref seen, out _, out var fault), Error(fault));
        Assert.True(forwarding.TryForward(Row(("tenant-id", "acme")), second.Headers, second.Content!.Headers, NoOverrides, values, names, ref seen, out _, out fault), Error(fault));

        Assert.NotNull(names);
        Assert.Same(NameOf(first), NameOf(second));
        Assert.Same(first.Headers.NonValidated["x-outbox-tenant-id"].ToString(), second.Headers.NonValidated["x-outbox-tenant-id"].ToString());
    }

    private static HeaderForwarding Forwarding(ForwardHeadersMode mode, string[]? names = null, string[]? exclude = null, string? prefix = null) =>
        new(new ForwardHeadersOptions { Mode = mode, Names = names, Exclude = exclude, NamePrefix = prefix });

    private static HeaderPlan Plan((string Name, string Value)[] headers, string[] relayOwned) =>
        new(headers.Select(h => (h.Name, Template(h.Value))), relayOwned);

    private static ValueTemplate Template(string text)
    {
        Assert.True(ValueTemplate.TryCompile(text, TemplateTarget.HeaderValue, "orders", "hooks", null, out var template, out var error), error);
        return template;
    }

    private static MessageHeaders Row(params (string Name, string Value)[] headers) =>
        new([.. headers.Select(h => new MessageHeader(h.Name, Encoding.UTF8.GetBytes(h.Value)))]);

    private static RenderContext Context(MessageHeaders headers) => new(1, null, headers);

    private static HttpRequestMessage Forward(HeaderForwarding forwarding, MessageHeaders row) =>
        Forward(forwarding, row, NoOverrides, out _);

    private static HttpRequestMessage Forward(HeaderForwarding forwarding, MessageHeaders row, out string? contentType) =>
        Forward(forwarding, row, NoOverrides, out contentType);

    private static HttpRequestMessage Forward(HeaderForwarding forwarding, MessageHeaders row, FrozenSet<string> overridden, out string? contentType)
    {
        var request = Request();
        HashSet<string>? seen = null;
        Assert.True(
            forwarding.TryForward(row, request.Headers, request.Content!.Headers, overridden, new HeaderValueCache(), forwarding.CreateNameCache(), ref seen, out contentType, out var fault),
            Error(fault));
        return request;
    }

    private static DeliveryResult.MessageFault ForwardFault(HeaderForwarding forwarding, MessageHeaders row)
    {
        HashSet<string>? seen = null;
        return ForwardFault(forwarding, row, ref seen);
    }

    private static DeliveryResult.MessageFault ForwardFault(HeaderForwarding forwarding, MessageHeaders row, ref HashSet<string>? seen)
    {
        Assert.False(forwarding.TryForward(row, new HttpRequestMessage().Headers, new ByteArrayContent([]).Headers, NoOverrides, new HeaderValueCache(), null, ref seen, out _, out var fault));
        return Assert.IsType<DeliveryResult.MessageFault>(fault);
    }

    private static string? Value(HttpRequestMessage request, string name) =>
        request.Headers.NonValidated.TryGetValues(name, out var values) ? values.ToString() : null;

    private static string? ContentValue(HttpRequestMessage request, string name) =>
        request.Content!.Headers.NonValidated.TryGetValues(name, out var values) ? values.ToString() : null;

    private static HttpRequestMessage Request() => new() { Content = new ByteArrayContent([]) };

    private static string NameOf(HttpRequestMessage request) => Assert.Single(request.Headers.NonValidated).Key;

    private static string? Error(DeliveryResult? fault) => (fault as DeliveryResult.MessageFault)?.Error;
}
