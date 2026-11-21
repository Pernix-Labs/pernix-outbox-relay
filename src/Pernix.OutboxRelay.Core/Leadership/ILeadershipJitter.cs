namespace Pernix.OutboxRelay.Core.Leadership;

public interface ILeadershipJitter
{
    ValueTask<TimeSpan> NextStartDelayAsync(string outboxName, CancellationToken ct);
}

public sealed class RandomLeadershipJitter : ILeadershipJitter
{
    private static readonly TimeSpan MaxDelay = TimeSpan.FromSeconds(2);

    public ValueTask<TimeSpan> NextStartDelayAsync(string outboxName, CancellationToken ct) =>
        ValueTask.FromResult(Random.Shared.NextDouble() * MaxDelay);
}
