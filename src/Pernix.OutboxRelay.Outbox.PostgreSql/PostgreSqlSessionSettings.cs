namespace Pernix.OutboxRelay.Outbox.PostgreSql;

// The leader-loss (<= 6 s) and lock-release (about 11 s) bounds depend on these values together.
internal static class PostgreSqlSessionSettings
{
    public const int DeadlineSeconds = 5;
    public const int KeepAliveIdleSeconds = 5;
    public const int KeepAliveIntervalSeconds = 2;
    public const int KeepAliveCount = 3;

    public const int TcpUserTimeoutMilliseconds = (KeepAliveIdleSeconds + (KeepAliveCount * KeepAliveIntervalSeconds)) * 1000;

    public static readonly TimeSpan Deadline = TimeSpan.FromSeconds(DeadlineSeconds);

    // Stats connections run no SET batch, so they take the server keepalives as startup options.
    public static readonly string KeepAliveOptions =
        $"-c tcp_keepalives_idle={KeepAliveIdleSeconds} " +
        $"-c tcp_keepalives_interval={KeepAliveIntervalSeconds} " +
        $"-c tcp_keepalives_count={KeepAliveCount}";

    // Server-side keepalives set after open win over any Options=-c in the configured string.
    public static readonly string SetSql =
        $"SET tcp_keepalives_idle = {KeepAliveIdleSeconds}; " +
        $"SET tcp_keepalives_interval = {KeepAliveIntervalSeconds}; " +
        $"SET tcp_keepalives_count = {KeepAliveCount}; " +
        $"SET tcp_user_timeout = {TcpUserTimeoutMilliseconds};";
}
