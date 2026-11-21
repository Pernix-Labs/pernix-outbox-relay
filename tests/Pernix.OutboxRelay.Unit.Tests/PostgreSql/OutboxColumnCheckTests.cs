using Pernix.OutboxRelay.PostgreSql;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.PostgreSql;

public sealed class OutboxColumnCheckTests
{
    [Fact]
    public void V001_columns_match()
    {
        var c = Columns.FromV001();

        Assert.Null(OutboxColumnCheck.Problem(c.Names, c.TypeOids, c.Typmods, c.Types, c.NotNull));
    }

    [Theory]
    [InlineData("retry_count", 20L, -1, "bigint", true, "retry_count bigint NOT NULL (expected integer NOT NULL)")]
    [InlineData("next_attempt_at", 1184L, 3, "timestamp(3) with time zone", true,
        "next_attempt_at timestamp(3) with time zone NOT NULL (expected timestamp with time zone NOT NULL)")]
    [InlineData("tx_id", 5069L, -1, "xid8", false, "tx_id xid8 NULL (expected xid8 NOT NULL)")]
    [InlineData("last_error", 25L, -1, "text", true, "last_error text NOT NULL (expected text NULL)")]
    public void Each_mismatch_class_is_named(string column, long typeOid, int typmod, string type, bool notNull, string expected)
    {
        var c = Columns.FromV001();
        var i = Array.IndexOf(c.Names, column);
        c.TypeOids[i] = typeOid;
        c.Typmods[i] = typmod;
        c.Types[i] = type;
        c.NotNull[i] = notNull;

        Assert.Equal(
            $"columns differ from V001: {expected}",
            OutboxColumnCheck.Problem(c.Names, c.TypeOids, c.Typmods, c.Types, c.NotNull));
    }

    [Fact]
    public void Mismatches_follow_V001_order()
    {
        var c = Columns.FromV001();
        Array.Reverse(c.Names);
        Array.Reverse(c.TypeOids);
        Array.Reverse(c.Typmods);
        Array.Reverse(c.Types);
        Array.Reverse(c.NotNull);
        var retryCount = Array.IndexOf(c.Names, "retry_count");
        c.TypeOids[retryCount] = 20;
        c.Types[retryCount] = "bigint";
        var route = Array.IndexOf(c.Names, "route");
        c.TypeOids[route] = 1043;
        c.Typmods[route] = 67;
        c.Types[route] = "character varying(63)";
        c.NotNull[Array.IndexOf(c.Names, "tx_id")] = false;

        Assert.Equal(
            "columns differ from V001: tx_id xid8 NULL (expected xid8 NOT NULL); "
            + "route character varying(63) NULL (expected text NULL); "
            + "retry_count bigint NOT NULL (expected integer NOT NULL)",
            OutboxColumnCheck.Problem(c.Names, c.TypeOids, c.Typmods, c.Types, c.NotNull));
    }

    [Fact]
    public void Names_are_V001_order()
    {
        Assert.Equal(
            ["id", "tx_id", "message_key", "payload", "headers", "route", "created_at", "retry_count", "next_attempt_at", "last_error"],
            OutboxColumnCheck.Names);
    }

    private sealed record Columns(string[] Names, long[] TypeOids, int[] Typmods, string[] Types, bool[] NotNull)
    {
        public static Columns FromV001()
        {
            var v001 = OutboxColumnCheck.V001;
            var c = new Columns(new string[v001.Length], new long[v001.Length], new int[v001.Length], new string[v001.Length], new bool[v001.Length]);
            for (var i = 0; i < v001.Length; i++)
            {
                c.Names[i] = v001[i].Name;
                c.TypeOids[i] = v001[i].TypeOid;
                c.Typmods[i] = -1;
                c.Types[i] = v001[i].Type;
                c.NotNull[i] = v001[i].NotNull;
            }

            return c;
        }
    }
}
