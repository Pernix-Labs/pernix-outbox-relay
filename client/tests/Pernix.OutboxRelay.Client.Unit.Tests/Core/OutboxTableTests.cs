using Xunit;

namespace Pernix.OutboxRelay.Client.Unit.Tests.Core;

public sealed class OutboxTableTests
{
    private static readonly string Part63 = "a" + new string('b', 62);
    private static readonly string Part64 = "a" + new string('b', 63);

    public static TheoryData<string, string?, string> Accepted => new()
    {
        { "outbox", null, "outbox" },
        { "_", null, "_" },
        { "X1", null, "X1" },
        { "public.outbox", "public", "outbox" },
        { "Risk_Schema.Out_Box_2", "Risk_Schema", "Out_Box_2" },
        { "a.b", "a", "b" },
        { "_9._9", "_9", "_9" },
        { "orders", null, "orders" },
        { "user_outbox", null, "user_outbox" },
        { "users.order_outbox", "users", "order_outbox" },
        { "risk.selection", "risk", "selection" },
    };

    public static TheoryData<string?> Rejected => new()
    {
        (string?)null,
        "",
        " ",
        " outbox",
        "outbox ",
        "out box",
        "outbox\t",
        "\"outbox\"",
        "'outbox'",
        "public.\"outbox\"",
        "a.b.c",
        ".outbox",
        "outbox.",
        ".",
        "..",
        "1outbox",
        "a.1b",
        "out-box",
        "outbox;drop",
        "öutbox",
        "outbox\0",
    };

    [Theory]
    [MemberData(nameof(Accepted))]
    public void Parse_accepts_grammar(string text, string? schema, string name)
    {
        var table = OutboxTable.Parse(text);

        Assert.Equal(schema, table.Schema);
        Assert.Equal(name, table.Name);
    }

    [Theory]
    [MemberData(nameof(Rejected))]
    public void Parse_rejects_text_outside_grammar(string? text)
    {
        var ex = Assert.Throws<ArgumentException>(() => OutboxTable.Parse(text!));

        Assert.Equal("text", ex.ParamName);
    }

    // Reserved keywords (PostgreSQL 17 categories R and T) in either part, any case.
    [Theory]
    [InlineData("order", "order")]
    [InlineData("ORDER", "ORDER")]
    [InlineData("user.outbox", "user")]
    [InlineData("risk.select", "select")]
    [InlineData("risk.Select", "Select")]
    [InlineData("public.table", "table")]
    [InlineData("join", "join")]
    [InlineData("current_schema.outbox", "current_schema")]
    public void Parse_rejects_postgresql_reserved_keyword_parts(string text, string part)
    {
        var ex = Assert.Throws<ArgumentException>(() => OutboxTable.Parse(text));

        Assert.Equal("text", ex.ParamName);
        Assert.Contains($"'{part}' is a PostgreSQL reserved keyword", ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Parts_of_63_bytes_are_accepted_and_64_rejected()
    {
        Assert.Equal(Part63, OutboxTable.Parse(Part63).Name);
        Assert.Equal(Part63, OutboxTable.Parse(Part63 + "." + Part63).Schema);
        Assert.Equal(Part63, OutboxTable.Parse(Part63 + "." + Part63).Name);

        Assert.Throws<ArgumentException>(() => OutboxTable.Parse(Part64));
        Assert.Throws<ArgumentException>(() => OutboxTable.Parse(Part64 + ".n"));
        Assert.Throws<ArgumentException>(() => OutboxTable.Parse("s." + Part64));
    }

    [Theory]
    [InlineData("Risk.OutBox")]
    [InlineData("OUTBOX")]
    [InlineData("outbox")]
    public void To_string_returns_the_input_text_verbatim(string text)
    {
        var table = OutboxTable.Parse(text);

        Assert.Same(text, table.ToString());
    }

    [Fact]
    public void Default_table_has_empty_name_null_schema_and_empty_text()
    {
        var table = default(OutboxTable);

        Assert.Equal(string.Empty, table.Name);
        Assert.Null(table.Schema);
        Assert.Equal(string.Empty, table.ToString());
    }
}
