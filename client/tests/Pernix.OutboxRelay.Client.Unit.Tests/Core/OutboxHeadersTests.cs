using System.Text;
using System.Text.Json;
using Xunit;

namespace Pernix.OutboxRelay.Client.Unit.Tests.Core;

public sealed class OutboxHeadersTests
{
    // Every name and value in the reject cases contains this, so a leak into the message is detectable.
    private const string Leak = "LEAK";

    [Fact]
    public void Build_keeps_add_order_through_count_indexer_and_enumerator()
    {
        var headers = OutboxHeaders.CreateBuilder().Add("b", "1").Add("a", "2").Add("c", "3").Build();

        Assert.Equal(3, headers.Count);
        Assert.Equal(new KeyValuePair<string, string>("a", "2"), headers[1]);
        Assert.Equal(
            [new("b", "1"), new("a", "2"), new("c", "3")],
            headers.ToArray());
    }

    [Fact]
    public void Build_accepts_names_differing_only_by_case_and_empty_values()
    {
        var headers = OutboxHeaders.CreateBuilder().Add("Name", "").Add("name", "").Add("NAME", "x").Build();

        Assert.Equal(3, headers.Count);
    }

    [Fact]
    public void Build_accepts_many_distinct_names()
    {
        var builder = OutboxHeaders.CreateBuilder(2);
        for (var i = 0; i < 40; i++)
        {
            builder.Add("h" + i, "v" + i);
        }

        var headers = builder.Build();

        Assert.Equal(40, headers.Count);
        Assert.Equal("h39", headers[39].Key);
    }

    [Fact]
    public void Builder_is_reusable_and_unchanged_by_build()
    {
        var builder = OutboxHeaders.CreateBuilder().Add("a", "1").Add("b", "2");

        var first = builder.Build();
        var second = builder.Build();
        var third = builder.Add("c", "3").Build();

        Assert.NotSame(first, second);
        Assert.Equal(first.ToArray(), second.ToArray());
        Assert.Equal(first.Json, second.Json);
        Assert.Equal(2, first.Count);
        Assert.Equal(3, third.Count);
    }

    [Fact]
    public void Failed_build_leaves_builder_usable()
    {
        var builder = OutboxHeaders.CreateBuilder().Add("a", "1").Add("a", "2");

        Assert.Throws<ArgumentException>(() => builder.Build());
        Assert.Throws<ArgumentException>(() => builder.Add("b", "3").Build());
    }

    [Fact]
    public void Create_builder_rejects_negative_capacity_and_accepts_zero()
    {
        var ex = Assert.Throws<ArgumentOutOfRangeException>(() => OutboxHeaders.CreateBuilder(-1));
        Assert.Equal("capacity", ex.ParamName);

        Assert.Single(OutboxHeaders.CreateBuilder(0).Add("a", "b").Build());
    }

    public static TheoryData<string, string, string> RejectCases => new()
    {
        { "name null", null!, Leak + "VALUE" },
        { "name empty", string.Empty, Leak + "VALUE" },
        { "value null", Leak + "NAME", null! },
        { "U+0000 in name", Leak + "\0NAME", Leak + "VALUE" },
        { "U+0000 in value", Leak + "NAME", Leak + "VAL\0UE" },
        { "lone high surrogate in name", Leak + "NAME\uD800", Leak + "VALUE" },
        { "lone low surrogate in name", "\uDC00" + Leak + "NAME", Leak + "VALUE" },
        { "lone high surrogate in value", Leak + "NAME", Leak + "\uD800VALUE" },
        { "lone low surrogate in value", Leak + "NAME", Leak + "VALUE\uDC00" },
        { "reversed pair in value", Leak + "NAME", Leak + "\uDC00\uD800" },
    };

    private static readonly Dictionary<string, string> RuleText = new()
    {
        ["name null"] = "name is null or empty",
        ["name empty"] = "name is null or empty",
        ["value null"] = "value is null",
        ["U+0000 in name"] = "name contains U+0000",
        ["U+0000 in value"] = "value contains U+0000",
        ["lone high surrogate in name"] = "name contains a lone surrogate",
        ["lone low surrogate in name"] = "name contains a lone surrogate",
        ["lone high surrogate in value"] = "value contains a lone surrogate",
        ["lone low surrogate in value"] = "value contains a lone surrogate",
        ["reversed pair in value"] = "value contains a lone surrogate",
    };

    [Theory]
    [MemberData(nameof(RejectCases))]
    public void Reject_message_names_rule_and_position_without_header_text(string rule, string name, string value)
    {
        var builder = OutboxHeaders.CreateBuilder()
            .Add(Leak + "N0", Leak + "V0")
            .Add(Leak + "N1", Leak + "V1")
            .Add(name, value)
            .Add(Leak + "N3", Leak + "V3");

        var ex = Assert.Throws<ArgumentException>(() => builder.Build());

        Assert.Equal($"Header 2: {RuleText[rule]}.", ex.Message);
        Assert.DoesNotContain(Leak, ex.Message, StringComparison.Ordinal);
        Assert.Null(ex.ParamName);
        Assert.Null(ex.InnerException);
    }

    // Pairwise path (count <= 8)
    [Fact]
    public void Duplicate_name_reports_both_positions_without_header_text()
    {
        var builder = OutboxHeaders.CreateBuilder()
            .Add(Leak + "N0", Leak + "V0")
            .Add(Leak + "DUP", Leak + "V1")
            .Add(Leak + "dup", Leak + "V2")
            .Add(Leak + "DUP", Leak + "V3");

        var ex = Assert.Throws<ArgumentException>(() => builder.Build());

        Assert.Equal("Header 3: name duplicates header 1 (ordinal).", ex.Message);
        Assert.DoesNotContain(Leak, ex.Message, StringComparison.Ordinal);
    }

    // Set path (count > 8)
    [Fact]
    public void Duplicate_name_in_large_set_reports_both_positions_without_header_text()
    {
        var builder = OutboxHeaders.CreateBuilder();
        for (var i = 0; i < 10; i++)
        {
            builder.Add(Leak + "N" + i, Leak + "V" + i);
        }

        builder.Add(Leak + "N3", Leak + "V10");

        var ex = Assert.Throws<ArgumentException>(() => builder.Build());

        Assert.Equal("Header 10: name duplicates header 3 (ordinal).", ex.Message);
        Assert.DoesNotContain(Leak, ex.Message, StringComparison.Ordinal);
    }

    // Rule order inside one entry
    // Attribute strings cannot carry lone surrogates, so these cases are built at run time.
    public static TheoryData<string?, string?, string> RuleOrderCases => new()
    {
        { null, null, "name is null or empty" },
        { "a\0\uD800", "b", "name contains U+0000" },
        { "a\uD800", "b\0", "value contains U+0000" },
        { "a\uD800", "b\uD800", "name contains a lone surrogate" },
        { "a", "b\uD800", "value contains a lone surrogate" },
    };

    [Theory]
    [MemberData(nameof(RuleOrderCases))]
    public void First_broken_rule_in_design_order_wins(string? name, string? value, string expected)
    {
        var ex = Assert.Throws<ArgumentException>(() => OutboxHeaders.CreateBuilder().Add(name!, value!).Build());

        Assert.Equal($"Header 0: {expected}.", ex.Message);
    }

    // Earliest broken entry wins
    [Fact]
    public void Earliest_broken_entry_wins()
    {
        var builder = OutboxHeaders.CreateBuilder().Add("a", "1").Add("b", null!).Add("a", "3");

        var ex = Assert.Throws<ArgumentException>(() => builder.Build());

        Assert.Equal("Header 1: value is null.", ex.Message);
    }

    [Fact]
    public void Json_is_one_instance_and_parses_back_to_the_built_pairs()
    {
        (string Name, string Value)[] pairs =
        [
            ("plain", "v"),
            ("quote\"name", "back\\slash"),
            ("ctrl", "\u0001\t\n\u001f"),
            ("unicode", "Ελληνικά ünïcødé"),
            ("emoji", "😀 pair"),
            ("empty", string.Empty),
            ("html", "<script>&'+"),
            ("Case", "x"),
            ("case", "y"),
            ("line sep", "\u2028\u2029"),
        ];

        var builder = OutboxHeaders.CreateBuilder();
        foreach (var (name, value) in pairs)
        {
            builder.Add(name, value);
        }

        var headers = builder.Build();

        Assert.Same(headers.Json, headers.Json);

        using var document = JsonDocument.Parse(headers.Json);
        var parsed = document.RootElement.EnumerateObject().ToArray();
        Assert.Equal(pairs.Length, parsed.Length);
        for (var i = 0; i < pairs.Length; i++)
        {
            Assert.Equal(Encoding.UTF8.GetBytes(pairs[i].Name), Encoding.UTF8.GetBytes(parsed[i].Name));
            Assert.Equal(JsonValueKind.String, parsed[i].Value.ValueKind);
            Assert.Equal(Encoding.UTF8.GetBytes(pairs[i].Value), Encoding.UTF8.GetBytes(parsed[i].Value.GetString()!));
        }
    }

    [Fact]
    public void Empty_json_is_an_empty_object_and_zero_entries_build_empty()
    {
        Assert.Equal("{}", OutboxHeaders.Empty.Json);
        Assert.Empty(OutboxHeaders.Empty);
        Assert.Same(OutboxHeaders.Empty, OutboxHeaders.CreateBuilder().Build());
        Assert.Same(OutboxHeaders.Empty, OutboxHeaders.CreateBuilder(0).Build());
    }

    [Fact]
    public void Outsized_build_does_not_keep_scratch_capacity()
    {
        OutboxHeaders.CreateBuilder().Add("small", "v").Build();
        Assert.InRange(HeadersJsonWriter.ScratchCapacity, 1, HeadersJsonWriter.RetainedCapacity);

        var large = new string('x', HeadersJsonWriter.RetainedCapacity * 4);
        var headers = OutboxHeaders.CreateBuilder().Add("big", large).Build();

        Assert.InRange(HeadersJsonWriter.ScratchCapacity, 0, HeadersJsonWriter.RetainedCapacity);
        using var document = JsonDocument.Parse(headers.Json);
        Assert.Equal(large, document.RootElement.GetProperty("big").GetString());

        var after = OutboxHeaders.CreateBuilder().Add("small", "v").Build();
        Assert.Equal("{\"small\":\"v\"}", after.Json);
    }
}
