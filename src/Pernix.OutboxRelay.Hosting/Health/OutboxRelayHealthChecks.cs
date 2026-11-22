namespace Pernix.OutboxRelay.Hosting.Health;

public static class OutboxRelayHealthChecks
{
    public const string LiveTag = "live";
    public const string ReadyTag = "ready";

    public const string HostLifetime = "host-lifetime";
    public const string OutboxDatabase = "outbox-database";
    public const string WorkerLoop = "worker-loop";

    public const string LivePath = "/livez";
    public const string ReadyPath = "/readyz";

    public static readonly IReadOnlySet<string> Paths = new HashSet<string>(StringComparer.OrdinalIgnoreCase) { LivePath, ReadyPath };
}
