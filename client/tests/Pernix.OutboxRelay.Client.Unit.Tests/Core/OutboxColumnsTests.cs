using System.Reflection;
using Xunit;

namespace Pernix.OutboxRelay.Client.Unit.Tests.Core;

public sealed class OutboxColumnsTests
{
    private static readonly string[] TableOrder =
    [
        "id",
        "tx_id",
        "message_key",
        "payload",
        "headers",
        "route",
        "created_at",
        "retry_count",
        "next_attempt_at",
        "last_error",
    ];

    [Fact]
    public void All_lists_the_10_columns_in_table_order()
    {
        Assert.Equal(TableOrder, OutboxColumns.All);
    }

    [Fact]
    public void Constants_are_exactly_the_columns_of_all()
    {
        var constants = typeof(OutboxColumns)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(f => f.IsLiteral && f.FieldType == typeof(string))
            .Select(f => (string)f.GetRawConstantValue()!)
            .Order(StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(10, constants.Length);
        Assert.Equal(TableOrder.Order(StringComparer.Ordinal), constants);
    }

    [Fact]
    public void Producer_written_is_key_payload_headers_route()
    {
        Assert.Equal(["message_key", "payload", "headers", "route"], OutboxColumns.ProducerWritten);
    }

    [Fact]
    public void Route_is_route()
    {
        Assert.Equal("route", OutboxColumns.Route);
    }

    [Fact]
    public void Lists_are_cached_and_read_only()
    {
        Assert.Same(OutboxColumns.All, OutboxColumns.All);
        Assert.Same(OutboxColumns.ProducerWritten, OutboxColumns.ProducerWritten);
        Assert.Throws<NotSupportedException>(() => ((IList<string>)OutboxColumns.All)[0] = "x");
        Assert.Throws<NotSupportedException>(() => ((IList<string>)OutboxColumns.ProducerWritten)[0] = "x");
    }
}
