using System.Text;
using Pernix.OutboxRelay.Outbox.PostgreSql;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.PostgreSql;

public sealed class HeadersParserTests
{
    private readonly HeadersParser parser = new();

    [Fact]
    public void Object_of_strings_keeps_order_and_unescaped_utf8_values()
    {
        var headers = Parse("""{"type": "Ordér 注文", "trace": "a\"b\\c", "empty": ""}""");

        Assert.NotNull(headers);
        Assert.Equal(["type", "trace", "empty"], headers.Select(h => h.Name));
        Assert.Equal(["Ordér 注文", "a\"b\\c", ""], headers.Select(h => Encoding.UTF8.GetString(h.Value)));
    }

    [Fact]
    public void Empty_object_is_no_headers()
    {
        Assert.Empty(Parse("{}")!);
    }

    [Theory]
    [InlineData("""{"attempt":1}""")]
    [InlineData("""{"a":"x","b":null}""")]
    [InlineData("""{"a":{"b":"c"}}""")]
    [InlineData("""["a"]""")]
    [InlineData("\"a\"")]
    [InlineData("""{"a":"x"} {}""")]
    [InlineData("""{"a":"x",}""")]
    [InlineData("""{"a":"x\""")]
    [InlineData("")]
    public void Anything_but_a_json_object_of_strings_is_malformed(string json)
    {
        Assert.Null(Parse(json));
    }

    [Fact]
    public void Repeated_name_keeps_first_position_and_last_value()
    {
        var headers = Parse("""{"a":"1","b":"2","a":"3"}""");

        Assert.NotNull(headers);
        Assert.Equal(["a", "b"], headers.Select(h => h.Name));
        Assert.Equal(["3", "2"], headers.Select(h => Encoding.UTF8.GetString(h.Value)));
    }

    [Fact]
    public void Repeated_name_matches_after_unescaping_and_keeps_first_position_and_last_value()
    {
        var headers = Parse("""{"a":"1","b":"2","\u0061":"3"}""");

        Assert.NotNull(headers);
        Assert.Equal(["a", "b"], headers.Select(h => h.Name));
        Assert.Equal(["3", "2"], headers.Select(h => Encoding.UTF8.GetString(h.Value)));
    }

    [Theory]
    [InlineData("""{"a":"\uD800"}""")]
    [InlineData("""{"a":"x\uDC00y"}""")]
    [InlineData("""{"\uD800":"x"}""")]
    public void Lone_surrogate_escape_is_malformed(string json)
    {
        Assert.Null(Parse(json));

        Assert.Equal(["c"], Parse("""{"c":"3"}""")!.Select(h => h.Name));
    }

    [Fact]
    public void Outsized_row_does_not_keep_its_scratch_capacity()
    {
        var names = Enumerable.Range(0, 1000).Select(i => $"h{i}").ToArray();
        var json = "{" + string.Join(",", names.Select(n => $"\"{n}\":\"{n}\"")) + ",\"h3\":\"x\"}";

        var headers = Parse(json);

        Assert.NotNull(headers);
        Assert.Equal(names, headers.Select(h => h.Name));
        Assert.Equal("x", Encoding.UTF8.GetString(headers[3].Value));
        Assert.True(parser.ScratchCapacity < names.Length);
    }

    [Fact]
    public void Parser_is_reusable_after_a_malformed_row()
    {
        Assert.Null(Parse("""{"a":"1","b":2}"""));

        Assert.Equal(["c"], Parse("""{"c":"3"}""")!.Select(h => h.Name));
    }

    [Fact]
    public void Repeated_name_returns_the_same_string_instance()
    {
        var first = Parse("""{"type":"a","trace":"b"}""")!;
        var second = Parse("""{"trace":"c","type":"d"}""")!;

        Assert.Same(first[0].Name, second[1].Name);
        Assert.Same(first[1].Name, second[0].Name);
        Assert.Equal(2, parser.CachedNameCount);
    }

    [Fact]
    public void Escaped_name_is_parsed_and_not_cached()
    {
        var headers = Parse("""{"\u0074ype":"a"}""");

        Assert.NotNull(headers);
        Assert.Equal(["type"], headers.Select(h => h.Name));
        Assert.Equal(0, parser.CachedNameCount);
        Assert.Equal(0, parser.CachedNameBytes);
    }

    [Fact]
    public void Name_longer_than_64_bytes_is_parsed_and_not_cached()
    {
        var name = new string('n', 65);

        var first = Parse($$"""{"{{name}}":"a"}""")!;
        var second = Parse($$"""{"{{name}}":"b"}""")!;

        Assert.Equal(name, first[0].Name);
        Assert.Equal(name, second[0].Name);
        Assert.NotSame(first[0].Name, second[0].Name);
        Assert.Equal(0, parser.CachedNameCount);
        Assert.Equal(0, parser.CachedNameBytes);
    }

    [Fact]
    public void Names_beyond_32_parse_and_are_not_cached()
    {
        var names = Enumerable.Range(0, 40).Select(i => $"h{i:D2}").ToArray();
        var json = "{" + string.Join(",", names.Select(n => $"\"{n}\":\"{n}\"")) + "}";

        var first = Parse(json)!;
        var second = Parse(json)!;

        Assert.Equal(names, first.Select(h => h.Name));
        Assert.Equal(names, second.Select(h => h.Name));
        Assert.Same(first[31].Name, second[31].Name);
        Assert.NotSame(first[32].Name, second[32].Name);
        Assert.Equal(32, parser.CachedNameCount);
        Assert.Equal(32 * 3, parser.CachedNameBytes);
    }

    [Fact]
    public void Name_cache_stays_bounded_after_10000_distinct_names()
    {
        for (var i = 0; i < 10_000; i++)
        {
            var name = new string('n', 60) + i.ToString("D4");
            Assert.Equal([name], Parse($$"""{"{{name}}":"v"}""")!.Select(h => h.Name));
        }

        Assert.True(parser.CachedNameCount <= 32);
        Assert.True(parser.CachedNameBytes <= 32 * 64);
        Assert.Equal(32, parser.CachedNameCount);
        Assert.Equal(32 * 64, parser.CachedNameBytes);
    }

    [Fact]
    public void Duplicate_name_keeps_first_position_and_last_value_with_a_warm_cache()
    {
        Parse("""{"a":"0","b":"0"}""");

        var headers = Parse("""{"a":"1","b":"2","a":"3"}""");

        Assert.NotNull(headers);
        Assert.Equal(["a", "b"], headers.Select(h => h.Name));
        Assert.Equal(["3", "2"], headers.Select(h => Encoding.UTF8.GetString(h.Value)));
        Assert.Equal(2, parser.CachedNameCount);
    }

    private Core.Abstractions.MessageHeaders? Parse(string json) => parser.Parse(Encoding.UTF8.GetBytes(json));
}
