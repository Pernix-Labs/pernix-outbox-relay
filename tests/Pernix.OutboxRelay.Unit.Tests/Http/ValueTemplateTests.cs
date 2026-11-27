using System.Text;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Destination.Http;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Http;

public sealed class ValueTemplateTests
{
    private const string Base = "https://example.test";

    private static readonly RenderContext Row = Context(id: 42, key: "order-1", ("tenant", "acme"));

    [Fact]
    public void Outbox_folds_encoded()
    {
        var template = Compile(Base + "/{outbox}", outbox: "orders out");

        Assert.True(template.IsConstant);
        Assert.Equal(Base + "/orders%20out", template.Constant);
    }

    [Fact]
    public void Destination_folds_encoded()
    {
        var template = Compile(Base + "/{destination}", destination: "hooks/a");

        Assert.True(template.IsConstant);
        Assert.Equal(Base + "/hooks%2Fa", template.Constant);
    }

    [Fact]
    public void Route_folds_on_a_routes_entry()
    {
        var template = Compile(Base + "/{route}", route: "alerts");

        Assert.True(template.IsConstant);
        Assert.Equal(Base + "/alerts", template.Constant);
    }

    [Fact]
    public void Route_without_a_routes_entry_fails_compile()
    {
        Assert.False(ValueTemplate.TryCompile(Base + "/{route}", TemplateTarget.Url, "o", "d", route: null, out _, out var error));
        Assert.Contains("{route}", error, StringComparison.Ordinal);
    }

    [Fact]
    public void Folded_header_value_is_not_url_escaped()
    {
        var template = Compile("{outbox}/{destination} {route}", TemplateTarget.HeaderValue, outbox: "a b", destination: "c/d", route: "r");

        Assert.Equal("a b/c/d r", template.Constant);
    }

    [Fact]
    public void Id_renders_digits()
    {
        Assert.Equal(Base + "/items/42", RenderUrl(Base + "/items/{id}", Row));
        Assert.Equal(Base + "/items/-7", RenderUrl(Base + "/items/{id}", Context(id: -7, key: null)));
    }

    [Fact]
    public void Key_renders_utf8_percent_encoded()
    {
        Assert.Equal(Base + "/k/caf%C3%A9%2F1", RenderUrl(Base + "/k/{key}", Context(id: 1, key: "café/1")));
    }

    [Fact]
    public void Key_hex_renders_lowercase_hex()
    {
        var row = new RenderContext(1, [0x00, 0xAB, 0xFF], MessageHeaders.Empty);

        Assert.Equal(Base + "/k/00abff", RenderUrl(Base + "/k/{key:hex}", row));
    }

    [Fact]
    public void Key_base64url_renders_unpadded()
    {
        var row = new RenderContext(1, [0xFB, 0xFF], MessageHeaders.Empty);

        Assert.Equal(Base + "/k/-_8", RenderUrl(Base + "/k/{key:base64url}", row));
    }

    [Fact]
    public void Header_renders_percent_encoded()
    {
        var row = Context(id: 1, key: null, ("tenant", "a&b=c"));

        Assert.Equal(Base + "/t?tenant=a%26b%3Dc", RenderUrl(Base + "/t?tenant={header:tenant}", row));
    }

    [Fact]
    public void Header_value_target_renders_without_url_escaping()
    {
        var template = Compile("{outbox}-{id}-{key}-{header:tenant}", TemplateTarget.HeaderValue, outbox: "o");
        var row = Context(id: 9, key: "a b/é", ("tenant", "x y"));

        Assert.False(template.IsConstant);
        Assert.True(template.TryRenderHeaderValue(row, out var value, out var fault));
        Assert.Null(fault);
        Assert.Equal("o-9-a b/é-x y", value);
    }

    [Theory]
    [InlineData("AZaz09-._~", "AZaz09-._~")]
    [InlineData(" ", "%20")]
    [InlineData("/", "%2F")]
    [InlineData("?", "%3F")]
    [InlineData("#", "%23")]
    [InlineData("%", "%25")]
    [InlineData("&", "%26")]
    [InlineData("=", "%3D")]
    [InlineData("+", "%2B")]
    [InlineData(":", "%3A")]
    [InlineData("@", "%40")]
    [InlineData("!*'()", "%21%2A%27%28%29")]
    [InlineData("{}", "%7B%7D")]
    [InlineData("\u007F", "%7F")]
    [InlineData("é", "%C3%A9")]
    [InlineData("€", "%E2%82%AC")]
    [InlineData("😀", "%F0%9F%98%80")]
    public void Url_encoding_keeps_only_unreserved(string raw, string encoded)
    {
        Assert.Equal(Base + "/" + encoded, RenderUrl(Base + "/{key}", Context(id: 1, key: raw)));
    }

    [Fact]
    public void Constant_when_no_placeholder()
    {
        var template = Compile(Base + "/hooks");

        Assert.True(template.IsConstant);
        Assert.Equal(Base + "/hooks", template.Constant);
    }

    [Fact]
    public void Startup_placeholders_only_is_constant()
    {
        var template = Compile(Base + "/{outbox}/{destination}/{route}", outbox: "o", destination: "d", route: "r");

        Assert.True(template.IsConstant);
        Assert.Equal(Base + "/o/d/r", template.Constant);
        var buffer = new char[4];
        Assert.True(template.TryRenderUrl(Row, ref buffer, out var value, out _));
        Assert.Same(template.Constant, value);
    }

    [Fact]
    public void Per_row_placeholder_is_not_constant()
    {
        var template = Compile(Base + "/{outbox}/{id}", outbox: "o");

        Assert.False(template.IsConstant);
        Assert.Null(template.Constant);
    }

    [Theory]
    [InlineData("{nope}")]
    [InlineData("{key:raw}")]
    [InlineData("{header:}")]
    [InlineData("{Key}")]
    [InlineData("{}")]
    public void Unknown_placeholder_fails_compile(string placeholder)
    {
        Assert.False(ValueTemplate.TryCompile(Base + "/" + placeholder, TemplateTarget.Url, "o", "d", "r", out _, out var error));
        Assert.Contains(placeholder, error, StringComparison.Ordinal);
    }

    [Fact]
    public void Unterminated_placeholder_fails_compile()
    {
        Assert.False(ValueTemplate.TryCompile(Base + "/{id", TemplateTarget.Url, "o", "d", "r", out _, out var error));
        Assert.Contains("unterminated", error, StringComparison.Ordinal);
    }

    [Fact]
    public void Header_value_literal_with_control_char_fails_compile()
    {
        Assert.False(ValueTemplate.TryCompile("a\r\nb", TemplateTarget.HeaderValue, "o", "d", null, out _, out var error));
        Assert.Contains("CR, LF or NUL", error, StringComparison.Ordinal);
    }

    [Fact]
    public void Null_key_faults()
    {
        AssertUrlFault(Base + "/{key}", Context(id: 1, key: null), "placeholder '{key}' needs a non-null UTF-8 key");
        AssertUrlFault(Base + "/{key:hex}", Context(id: 1, key: null), "placeholder '{key:hex}' needs a non-null key");
        AssertUrlFault(Base + "/{key:base64url}", Context(id: 1, key: null), "placeholder '{key:base64url}' needs a non-null key");
    }

    [Fact]
    public void Invalid_utf8_key_faults_for_key_only()
    {
        var row = new RenderContext(1, [0xC3, 0x28], MessageHeaders.Empty);

        AssertUrlFault(Base + "/{key}", row, "placeholder '{key}' needs a non-null UTF-8 key");
        Assert.Equal(Base + "/c328", RenderUrl(Base + "/{key:hex}", row));
    }

    [Fact]
    public void Missing_header_faults()
    {
        AssertUrlFault(Base + "/{header:trace}", Row, "placeholder '{header:trace}' names a header the row does not have");
    }

    [Fact]
    public void Header_name_lookup_is_ordinal()
    {
        AssertUrlFault(Base + "/{header:Tenant}", Row, "placeholder '{header:Tenant}' names a header the row does not have");
    }

    [Theory]
    [InlineData("a\rb")]
    [InlineData("a\nb")]
    [InlineData("a\0b")]
    public void Control_char_in_header_value_target_faults(string raw)
    {
        var keyTemplate = Compile("k-{key}", TemplateTarget.HeaderValue);
        var headerTemplate = Compile("h-{header:x}", TemplateTarget.HeaderValue);
        var row = Context(id: 1, key: raw, ("x", raw));

        Assert.False(keyTemplate.TryRenderHeaderValue(row, out _, out var keyFault));
        Assert.Equal(new DeliveryResult.MessageFault("placeholder '{key}' renders CR, LF or NUL", Permanent: true), keyFault);
        Assert.False(headerTemplate.TryRenderHeaderValue(row, out _, out var headerFault));
        Assert.Equal(new DeliveryResult.MessageFault("placeholder '{header:x}' renders CR, LF or NUL", Permanent: true), headerFault);
    }

    [Fact]
    public void Control_char_in_url_is_percent_encoded()
    {
        Assert.Equal(Base + "/a%0D%0A%00", RenderUrl(Base + "/{key}", Context(id: 1, key: "a\r\n\0")));
    }

    [Theory]
    [InlineData("/{key}", ".")]
    [InlineData("/{key}", "..")]
    [InlineData("/a/{key}/b", ".")]
    [InlineData("/a/{key}/b", "..")]
    [InlineData("/a/{key}?q=1", "..")]
    [InlineData("/a/{key}{key}", ".")]
    [InlineData("/a/.{key}", ".")]
    public void Dot_segment_faults(string path, string key)
    {
        AssertUrlFault(Base + path, Context(id: 1, key: key), "placeholder '{key}' renders a dot segment");
    }

    [Fact]
    public void Dot_segment_from_header_names_header()
    {
        AssertUrlFault(Base + "/{header:x}/", Context(id: 1, key: null, ("x", "..")), "placeholder '{header:x}' renders a dot segment");
    }

    [Theory]
    [InlineData("/{key}", "a.b")]
    [InlineData("/{key}", "...")]
    [InlineData("/{key}", ".a")]
    [InlineData("/x{key}", ".")]
    [InlineData("/a?q={key}", ".")]
    [InlineData("/a?q={key}", "..")]
    [InlineData("/a?q=/{key}", "..")]
    public void Dots_that_are_not_a_whole_path_segment_render(string path, string key)
    {
        var buffer = new char[8];
        var template = Compile(Base + path);

        Assert.True(template.TryRenderUrl(Context(id: 1, key: key), ref buffer, out var value, out var fault));
        Assert.Null(fault);
        Assert.Equal(Base + path.Replace("{key}", key, StringComparison.Ordinal), value);
    }

    [Fact]
    public void Url_buffer_grows_when_short()
    {
        var template = Compile(Base + "/{key}");
        var buffer = new char[4];

        Assert.True(template.TryRenderUrl(Context(id: 1, key: "abcdef"), ref buffer, out var value, out _));
        Assert.Equal(Base + "/abcdef", value);
        Assert.True(buffer.Length >= value.Length);
    }

    [Fact]
    public void Many_segments_render_with_pooled_scratch()
    {
        var text = Base + "/" + string.Join("/", Enumerable.Repeat("{id}", 40));
        var buffer = new char[16];

        Assert.True(Compile(text).TryRenderUrl(Row, ref buffer, out var value, out _));
        Assert.Equal(Base + "/" + string.Join("/", Enumerable.Repeat("42", 40)), value);
    }

    [Fact]
    public void Wrong_target_throws()
    {
        var buffer = new char[16];

        Assert.Throws<InvalidOperationException>(() => Compile("{id}", TemplateTarget.HeaderValue).TryRenderUrl(Row, ref buffer, out _, out _));
        Assert.Throws<InvalidOperationException>(() => Compile(Base + "/{id}").TryRenderHeaderValue(Row, out _, out _));
    }

    [Fact]
    public void Rendered_constant_allocates_nothing()
    {
        var url = Compile(Base + "/{outbox}/{route}", outbox: "o", route: "r");
        var header = Compile("{outbox}-{destination}", TemplateTarget.HeaderValue, outbox: "o", destination: "d");
        var buffer = new char[64];
        var row = Row;
        url.TryRenderUrl(row, ref buffer, out _, out _);
        header.TryRenderHeaderValue(row, out _, out _);

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 100; i++)
        {
            url.TryRenderUrl(row, ref buffer, out _, out _);
            header.TryRenderHeaderValue(row, out _, out _);
        }

        Assert.Equal(0, GC.GetAllocatedBytesForCurrentThread() - before);
    }

    private static ValueTemplate Compile(
        string text,
        TemplateTarget target = TemplateTarget.Url,
        string outbox = "orders",
        string destination = "hooks",
        string? route = null)
    {
        Assert.True(ValueTemplate.TryCompile(text, target, outbox, destination, route, out var template, out var error), error);
        return template;
    }

    private static string RenderUrl(string text, RenderContext row)
    {
        var buffer = new char[16];
        Assert.True(Compile(text).TryRenderUrl(row, ref buffer, out var value, out var fault), (fault as DeliveryResult.MessageFault)?.Error);
        return value;
    }

    private static void AssertUrlFault(string text, RenderContext row, string error)
    {
        var buffer = new char[16];
        Assert.False(Compile(text).TryRenderUrl(row, ref buffer, out var value, out var fault));
        Assert.Null(value);
        Assert.Equal(new DeliveryResult.MessageFault(error, Permanent: true), fault);
    }

    private static RenderContext Context(long id, string? key, params (string Name, string Value)[] headers) =>
        new(
            id,
            key is null ? null : Encoding.UTF8.GetBytes(key),
            TestHeaders.Of(headers.Select(h => new KeyValuePair<string, string>(h.Name, h.Value))));
}
