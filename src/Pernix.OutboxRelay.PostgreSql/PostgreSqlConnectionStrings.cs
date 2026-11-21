using Npgsql;

namespace Pernix.OutboxRelay.PostgreSql;

public static class PostgreSqlConnectionStrings
{
    public const string DefaultApplicationName = "outbox-relay";

    public const string SessionTargetSessionAttributes = "read-write";

    public const string ValidationTargetSessionAttributes = "prefer-primary";

    public const string StatsApplicationNamePrefix = "outbox-relay-stats:";

    public static string ForSession(string configured) => Build(configured, SessionTargetSessionAttributes);

    public static string ForValidation(string configured) => Build(configured, ValidationTargetSessionAttributes);

    public static string ForStats(string configured, string outboxName) =>
        Build(configured, SessionTargetSessionAttributes, pooled: true, applicationName: StatsApplicationNamePrefix + outboxName);

    private static string Build(
        string configured,
        string targetSessionAttributes,
        bool pooled = false,
        string? applicationName = null)
    {
        var builder = new NpgsqlConnectionStringBuilder(configured)
        {
            Pooling = pooled,
            Multiplexing = false,
            Timeout = PostgreSqlSessionSettings.DeadlineSeconds,
            CommandTimeout = PostgreSqlSessionSettings.DeadlineSeconds,
            CancellationTimeout = -1,
            TcpKeepAlive = true,
            TcpKeepAliveTime = PostgreSqlSessionSettings.KeepAliveIdleSeconds,
            TcpKeepAliveInterval = PostgreSqlSessionSettings.KeepAliveIntervalSeconds,
            LoadBalanceHosts = false,
        };

        // Npgsql accepts target session attributes only with several hosts. With one host, pg_is_in_recovery() refuses a standby.
        builder.TargetSessionAttributes = builder.Host?.Contains(',', StringComparison.Ordinal) == true ? targetSessionAttributes : null;

        if (pooled)
        {
            builder.MinPoolSize = 0;
            builder.MaxPoolSize = 1;
            builder.ConnectionIdleLifetime = 60;

            // Appended, so these win over the same settings in configured Options. Other configured options stay.
            builder.Options = string.IsNullOrWhiteSpace(builder.Options)
                ? PostgreSqlSessionSettings.KeepAliveOptions
                : builder.Options + " " + PostgreSqlSessionSettings.KeepAliveOptions;
        }

        if (applicationName is not null)
        {
            builder.ApplicationName = applicationName;
        }
        else if (string.IsNullOrEmpty(builder.ApplicationName))
        {
            builder.ApplicationName = DefaultApplicationName;
        }

        return builder.ConnectionString;
    }
}
