using System.Net.Sockets;
using Npgsql;

namespace Pernix.OutboxRelay.PostgreSql;

public static class PostgreSqlErrors
{
    // Type name plus SqlState, SocketError name or "timeout". Never Message: Npgsql messages carry host:port.
    public static string Describe(Exception exception)
    {
        var detail = Detail(exception);
        var type = exception.GetType().Name;
        return detail is null ? type : $"{type} {detail}";
    }

    // Excludes classes 08 (connection), 53 (resources) and 57 (shutdown), which a reconnect can fix.
    internal static bool IsServerRefusal(Exception exception) =>
        FindPostgres(exception) is { } postgres
        && !postgres.SqlState.StartsWith("08", StringComparison.Ordinal)
        && !postgres.SqlState.StartsWith("53", StringComparison.Ordinal)
        && !postgres.SqlState.StartsWith("57", StringComparison.Ordinal);

    private static PostgresException? FindPostgres(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            if (current is PostgresException postgres)
            {
                return postgres;
            }
        }

        return null;
    }

    private static string? Detail(Exception exception)
    {
        for (var current = exception; current is not null; current = current.InnerException)
        {
            switch (current)
            {
                case PostgresException postgres:
                    return postgres.SqlState;
                case SocketException socket:
                    return socket.SocketErrorCode.ToString();
                case TimeoutException:
                    return "timeout";
            }
        }

        return null;
    }
}
