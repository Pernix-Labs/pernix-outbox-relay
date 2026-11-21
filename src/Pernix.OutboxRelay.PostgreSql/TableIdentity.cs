using Npgsql;

namespace Pernix.OutboxRelay.PostgreSql;

// From server-side values. Over a Unix socket the configured host and port stand in.
internal sealed record DatabaseIdentity(string Host, int Port, string Database)
{
    public static DatabaseIdentity Of(string? serverAddress, int? serverPort, string database, string connectionString)
    {
        if (serverAddress is not null && serverPort is { } port)
        {
            return new DatabaseIdentity(serverAddress, port, database);
        }

        var configured = new NpgsqlConnectionStringBuilder(connectionString);
        return new DatabaseIdentity((configured.Host ?? string.Empty).ToLowerInvariant(), configured.Port, database);
    }
}

internal sealed record TableIdentity(DatabaseIdentity Database, string QualifiedTable);

internal sealed record LockKeyIdentity(DatabaseIdentity Database, int LockKey);
