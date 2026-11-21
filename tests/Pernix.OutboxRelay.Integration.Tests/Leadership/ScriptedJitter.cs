using Pernix.OutboxRelay.Core.Leadership;

namespace Pernix.OutboxRelay.Integration.Tests.Leadership;

// Workers await the gate, so scripted delays count from one shared instant.
public sealed class ScriptedJitter(IReadOnlyDictionary<string, TimeSpan> delays, Task? gate = null) : ILeadershipJitter
{
    private static readonly TimeSpan GateTimeout = TimeSpan.FromSeconds(30);

    public async ValueTask<TimeSpan> NextStartDelayAsync(string outboxName, CancellationToken ct)
    {
        if (gate is not null)
        {
            await gate.WaitAsync(GateTimeout, ct);
        }

        return delays[outboxName];
    }
}
