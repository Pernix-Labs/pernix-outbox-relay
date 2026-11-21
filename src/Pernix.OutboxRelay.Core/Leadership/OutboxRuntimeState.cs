namespace Pernix.OutboxRelay.Core.Leadership;

public sealed class OutboxRuntimeState
{
    private volatile IReadOnlyList<OutboxRuntimeStatus> outboxes = [];

    public IReadOnlyList<OutboxRuntimeStatus> Outboxes => outboxes;

    public int HeldCount => outboxes.Count(o => o.IsLeader);

    internal void SetOutboxes(IEnumerable<OutboxRuntimeStatus> statuses)
    {
        ArgumentNullException.ThrowIfNull(statuses);
        outboxes = [.. statuses];
    }
}

public sealed class OutboxRuntimeStatus(string name)
{
    internal static readonly TimeSpan ProgressGrace = TimeSpan.FromSeconds(10);

    private const long NoDeadline = long.MaxValue;

    private volatile bool isLeader;
    private long loopDueTicks = DateTimeOffset.MaxValue.UtcTicks;
    private long sendDeadlineTicks = NoDeadline;
    private volatile bool databaseReachable = true;
    private volatile Task? loop;

    public string Name { get; } = name;

    public bool IsLeader
    {
        get => isLeader;
        internal set => isLeader = value;
    }

    public bool DatabaseReachable
    {
        get => databaseReachable;
        internal set => databaseReachable = value;
    }

    public Task? Loop
    {
        get => loop;
        internal set => loop = value;
    }

    // min(loop due, earliest in-flight send deadline + 10 s).
    public DateTimeOffset ProgressDueAt
    {
        get
        {
            var loopDue = Interlocked.Read(ref loopDueTicks);
            var deadline = Interlocked.Read(ref sendDeadlineTicks);
            var due = deadline == NoDeadline ? loopDue : Math.Min(loopDue, AddClamped(deadline, ProgressGrace));
            return new DateTimeOffset(due, TimeSpan.Zero);
        }
    }

    internal void ExpectProgress(TimeSpan bound, TimeProvider time)
    {
        ArgumentNullException.ThrowIfNull(time);
        var due = AddClamped(AddClamped(time.GetUtcNow().UtcTicks, bound), ProgressGrace);
        Interlocked.Exchange(ref loopDueTicks, due);
    }

    internal void TrackSendDeadline(DateTimeOffset? deadline) =>
        Interlocked.Exchange(ref sendDeadlineTicks, deadline?.UtcTicks ?? NoDeadline);

    private static long AddClamped(long ticks, TimeSpan delta)
    {
        var max = DateTimeOffset.MaxValue.UtcTicks;
        return delta.Ticks >= max - ticks ? max : ticks + Math.Max(delta.Ticks, 0);
    }
}
