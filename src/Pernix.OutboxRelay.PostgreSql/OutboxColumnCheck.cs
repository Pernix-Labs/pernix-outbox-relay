using System.Text;

namespace Pernix.OutboxRelay.PostgreSql;

// V001 type and nullability of every outbox column the relay reads or writes, in V001 order.
internal static class OutboxColumnCheck
{
    // Built-in type OIDs are fixed (pg_type.dat), so search_path cannot change them.
    internal static readonly OutboxColumn[] V001 =
    [
        new("id", 20, "bigint", NotNull: true),
        new("tx_id", 5069, "xid8", NotNull: true),
        new("message_key", 17, "bytea", NotNull: false),
        new("payload", 17, "bytea", NotNull: true),
        new("headers", 3802, "jsonb", NotNull: true),
        new("route", 25, "text", NotNull: false),
        new("created_at", 1184, "timestamp with time zone", NotNull: true),
        new("retry_count", 23, "integer", NotNull: true),
        new("next_attempt_at", 1184, "timestamp with time zone", NotNull: true),
        new("last_error", 25, "text", NotNull: false),
    ];

    internal static readonly string[] Names = BuildNames();

    // Arrays aligned by index, one entry per relay column present, any order. Null when every column matches V001.
    // A V001 column absent from names is skipped: the missing-columns check reports it first.
    internal static string? Problem(string[] names, long[] typeOids, int[] typmods, string[] types, bool[] notNull)
    {
        StringBuilder? problem = null;
        foreach (var expected in V001)
        {
            var i = Array.IndexOf(names, expected.Name);
            if (i < 0 || (typeOids[i] == expected.TypeOid && typmods[i] == -1 && notNull[i] == expected.NotNull))
            {
                continue;
            }

            if (problem is null)
            {
                problem = new StringBuilder("columns differ from V001: ");
            }
            else
            {
                problem.Append("; ");
            }

            problem.Append(expected.Name).Append(' ').Append(types[i]).Append(' ').Append(Nullability(notNull[i]))
                .Append(" (expected ").Append(expected.Type).Append(' ').Append(Nullability(expected.NotNull)).Append(')');
        }

        return problem?.ToString();
    }

    private static string Nullability(bool notNull) => notNull ? "NOT NULL" : "NULL";

    private static string[] BuildNames()
    {
        var names = new string[V001.Length];
        for (var i = 0; i < V001.Length; i++)
        {
            names[i] = V001[i].Name;
        }

        return names;
    }
}

internal readonly record struct OutboxColumn(string Name, long TypeOid, string Type, bool NotNull);
