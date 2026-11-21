using System.Diagnostics;
using System.Diagnostics.Metrics;
using Pernix.OutboxRelay.Core.Diagnostics;
using Pernix.OutboxRelay.Core.Leadership;
using Pernix.OutboxRelay.Integration.Tests.Kafka;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.Leadership;

public sealed class LeadershipTests(PostgreSqlFixture postgres, KafkaFixture kafka)
{
    private static readonly TimeSpan SettleTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan TakeoverBound = TimeSpan.FromSeconds(6);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Two_instances_on_one_outbox_have_exactly_one_leader()
    {
        var (outbox, schema) = (RelayFactory.NewOutboxName(), await postgres.CreateOutboxAsync());
        await using var a = Relay((outbox, schema));
        await using var b = Relay((outbox, schema));
        var states = new[] { StateOf(a), StateOf(b) };
        Assert.True(await EventuallyAsync(() => LeaderCount(states, outbox) == 1, SettleTimeout));

        using var gauge = new LeaderGaugeReader(outbox);
        var samples = 0;
        var end = Stopwatch.StartNew();
        while (end.Elapsed < TimeSpan.FromSeconds(3))
        {
            Assert.Equal(1, LeaderCount(states, outbox));
            Assert.NotNull(await postgres.FindLockHolderPidAsync($"{schema}.outbox"));
            Assert.Equal(1, await postgres.CountLockHoldersAsync($"{schema}.outbox"));
            var values = gauge.Read();
            Assert.Equal(2, values.Count);
            Assert.Equal(1, values.Sum());
            samples++;
            await Task.Delay(100, Ct);
        }

        Assert.True(samples >= 10, $"only {samples} samples taken");
    }

    [Fact]
    public async Task Terminated_leader_steps_down_and_one_instance_takes_over()
    {
        var (outbox, schema) = (RelayFactory.NewOutboxName(), await postgres.CreateOutboxAsync());
        await using var a = Relay((outbox, schema));
        await using var b = Relay((outbox, schema));
        var states = new[] { StateOf(a), StateOf(b) };
        Assert.True(await EventuallyAsync(() => LeaderCount(states, outbox) == 1, SettleTimeout));
        var pid = await postgres.FindLockHolderPidAsync($"{schema}.outbox");
        Assert.NotNull(pid);

        var elapsed = Stopwatch.StartNew();
        await postgres.TerminateAsync(pid.Value);

        // The former leader can re-lock within 0.5 s, so a new holder pid is the step-down evidence.
        Assert.True(
            await EventuallyAsync(
                async () => await postgres.FindLockHolderPidAsync($"{schema}.outbox") is { } holder
                    && holder != pid
                    && LeaderCount(states, outbox) == 1,
                TakeoverBound - elapsed.Elapsed),
            "no single leader on a new session within 6 s");
    }

    [Fact]
    public async Task Instances_started_together_split_outboxes_by_jitter()
    {
        var outboxes = new (string Name, string Schema)[4];
        for (var i = 0; i < outboxes.Length; i++)
        {
            outboxes[i] = (RelayFactory.NewOutboxName(), await postgres.CreateOutboxAsync());
        }

        var late = TimeSpan.FromSeconds(1.5);
        var jitterA = outboxes.Select((o, i) => (o.Name, Delay: i % 2 == 0 ? TimeSpan.Zero : late)).ToDictionary(x => x.Name, x => x.Delay);
        var jitterB = outboxes.Select((o, i) => (o.Name, Delay: i % 2 == 0 ? late : TimeSpan.Zero)).ToDictionary(x => x.Name, x => x.Delay);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var a = Relay(outboxes, new ScriptedJitter(jitterA, started.Task));
        await using var b = Relay(outboxes, new ScriptedJitter(jitterB, started.Task));

        await Task.WhenAll(Task.Run(() => a.Services, Ct), Task.Run(() => b.Services, Ct));
        started.SetResult();
        var (stateA, stateB) = (StateOf(a), StateOf(b));
        Assert.True(await EventuallyAsync(
            () => outboxes.All(o => LeaderCount([stateA, stateB], o.Name) == 1),
            SettleTimeout));

        Assert.Equal([true, false, true, false], outboxes.Select(o => Leads(stateA, o.Name)));
        Assert.Equal([false, true, false, true], outboxes.Select(o => Leads(stateB, o.Name)));
    }

    [Fact]
    public async Task Stopped_leader_releases_lock_and_other_instance_takes_over()
    {
        var (outbox, schema) = (RelayFactory.NewOutboxName(), await postgres.CreateOutboxAsync());
        await using var a = Relay((outbox, schema));
        await using var b = Relay((outbox, schema));
        var (stateA, stateB) = (StateOf(a), StateOf(b));
        Assert.True(await EventuallyAsync(() => LeaderCount([stateA, stateB], outbox) == 1, SettleTimeout));
        var (leader, other) = Leads(stateA, outbox) ? (a, stateB) : (b, stateA);
        var pid = await postgres.FindLockHolderPidAsync($"{schema}.outbox");
        Assert.NotNull(pid);

        var elapsed = Stopwatch.StartNew();
        await leader.DisposeAsync();

        Assert.NotEqual(pid, await postgres.FindLockHolderPidAsync($"{schema}.outbox"));
        Assert.True(await EventuallyAsync(() => Leads(other, outbox), TakeoverBound - elapsed.Elapsed), "other instance did not lead within 6 s");
    }

    internal static Task<bool> EventuallyAsync(Func<bool> condition, TimeSpan timeout) =>
        EventuallyAsync(() => Task.FromResult(condition()), timeout);

    internal static async Task<bool> EventuallyAsync(Func<Task<bool>> condition, TimeSpan timeout)
    {
        var elapsed = Stopwatch.StartNew();
        while (!await condition())
        {
            if (elapsed.Elapsed >= timeout)
            {
                return false;
            }

            await Task.Delay(50, Ct);
        }

        return true;
    }

    private RelayFactory Relay(params (string Name, string Schema)[] outboxes) => Relay(outboxes, jitter: null);

    private RelayFactory Relay((string Name, string Schema)[] outboxes, ILeadershipJitter? jitter)
    {
        var factory = new RelayFactory().WithRelaySettings(RelayFactory.OutboxSettings(postgres.ConnectionString, kafka.BootstrapServers, outboxes));
        return jitter is null ? factory : factory.WithServices(s => s.AddSingleton(jitter));
    }

    private static OutboxRuntimeState StateOf(RelayFactory factory) => factory.Services.GetRequiredService<OutboxRuntimeState>();

    private static bool Leads(OutboxRuntimeState state, string outbox) => state.Outboxes.Single(o => o.Name == outbox).IsLeader;

    private static int LeaderCount(IEnumerable<OutboxRuntimeState> states, string outbox) => states.Count(s => Leads(s, outbox));

    // Reads outbox.leader the way an exporter would: one observation of every series for the outbox tag.
    private sealed class LeaderGaugeReader : IDisposable
    {
        private readonly MeterListener listener = new();
        private readonly List<int> values = [];

        public LeaderGaugeReader(string outbox)
        {
            listener.InstrumentPublished = (instrument, l) =>
            {
                if (instrument.Meter.Name == RelayDiagnostics.Name && instrument.Name == LeaderGauge.InstrumentName)
                {
                    l.EnableMeasurementEvents(instrument);
                }
            };
            listener.SetMeasurementEventCallback<int>((_, value, tags, _) =>
            {
                foreach (var tag in tags)
                {
                    if (tag.Key == LeaderGauge.OutboxTag && Equals(tag.Value, outbox))
                    {
                        values.Add(value);
                    }
                }
            });
            listener.Start();
        }

        public IReadOnlyList<int> Read()
        {
            values.Clear();
            listener.RecordObservableInstruments();
            return [.. values];
        }

        public void Dispose() => listener.Dispose();
    }
}
