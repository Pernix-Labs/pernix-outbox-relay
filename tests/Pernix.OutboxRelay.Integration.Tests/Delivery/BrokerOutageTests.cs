using System.Globalization;
using System.Net;
using System.Text;
using Pernix.OutboxRelay.Core.Leadership;
using Pernix.OutboxRelay.Integration.Tests.Kafka;
using Pernix.OutboxRelay.Integration.Tests.Leadership;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.Delivery;

// Opt-in test, selected by trait:
// OUTBOX_RELAY_BROKER_OUTAGE_TEST=1 dotnet test --project tests/Pernix.OutboxRelay.Integration.Tests -- --filter-trait "Category=BrokerOutage"
// Gate order source of truth: rows are inserted by serial transactions in ascending tag order, so (tx_id, id) order = tag order.
[Trait("Category", "BrokerOutage")]
public sealed class BrokerOutageTests(PostgreSqlFixture postgres)
{
    private const string EnvironmentVariable = "OUTBOX_RELAY_BROKER_OUTAGE_TEST";
    private const int Total = 3000;
    private const int Keys = 20;

    // Longer than librdkafka's 60 s socket timeout, so in-flight sends see the connection die.
    private static readonly TimeSpan Outage = TimeSpan.FromSeconds(180);
    private static readonly TimeSpan LivenessInterval = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan DeliveryTimeout = TimeSpan.FromSeconds(180);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("PerKey")]
    [InlineData("Strict")]
    public async Task Paused_broker_beyond_socket_timeout_keeps_liveness_and_delivers_every_row_in_gate_order(string mode)
    {
        Assert.SkipUnless(Environment.GetEnvironmentVariable(EnvironmentVariable) == "1", $"Opt-in broker-outage test: set {EnvironmentVariable}=1.");

        await using var own = await KafkaFixture.StartOwnContainerAsync();
        var setup = await DeliverySetup.CreateAsync(postgres, own, batchSize: 50, orderingMode: mode, partitions: mode == "PerKey" ? 3 : 1);
        await using var relay = setup.Relay();
        using var client = relay.CreateClient();
        var status = relay.Services.GetRequiredService<OutboxRuntimeState>().Outboxes.Single();
        Assert.True(await LeadershipTests.EventuallyAsync(() => status.IsLeader, TimeSpan.FromSeconds(10)), "no leader");

        using var inserter = new StreamingInserter(setup, Total, chunk: 100, pause: TimeSpan.FromMilliseconds(50), keys: Keys);
        Assert.True(await inserter.DeliveryStartedAsync(DeliveryTimeout), "no delivery before pause");

        await own.Container.PauseAsync(Ct);
        try
        {
            var outage = System.Diagnostics.Stopwatch.StartNew();
            while (outage.Elapsed < Outage)
            {
                using var response = await client.GetAsync("/livez", Ct);
                Assert.True(response.StatusCode == HttpStatusCode.OK, $"/livez {(int)response.StatusCode} after {outage.Elapsed:g} of broker outage");
                await Task.Delay(LivenessInterval, Ct);
            }

            Assert.True(await setup.CountAsync() > 0, "no rows held during outage");
        }
        finally
        {
            await own.Container.UnpauseAsync(Ct);
        }

        await inserter.CompleteAsync();
        Assert.True(await LeadershipTests.EventuallyAsync(async () => await setup.CountAsync() == 0, DeliveryTimeout), "table not empty after unpause");

        var first = DedupKeepFirst(await ConsumeDistinctAsync(setup));
        Assert.Equal(Enumerable.Range(1, Total), first.Select(r => r.Tag).Order());
        if (mode == "Strict")
        {
            Assert.Equal(Enumerable.Range(1, Total), first.Select(r => r.Tag));
        }
        else
        {
            var expected = Enumerable.Range(1, Total).GroupBy(t => $"k{t % Keys}").ToDictionary(g => g.Key, g => g.ToList());
            var actual = first.GroupBy(r => r.Key).ToDictionary(g => g.Key, g => g.Select(r => r.Tag).ToList());
            Assert.Equal(expected.Keys.Order(), actual.Keys.Order());
            Assert.All(expected, e => Assert.Equal(e.Value, actual[e.Key]));
        }
    }

    // Abandoned in-flight sends may land twice, so consume until every tag 1..Total is seen.
    private static async Task<IReadOnlyList<Record>> ConsumeDistinctAsync(DeliverySetup setup)
    {
        var deadline = DateTime.UtcNow + DeliveryTimeout;
        var expected = Total;
        while (true)
        {
            var records = (await KafkaFixture.ConsumeAllAsync(setup.BootstrapServers, setup.Topic, expected, deadline - DateTime.UtcNow))
                .Select(r => new Record(Encoding.UTF8.GetString(r.Message.Key), int.Parse(Encoding.UTF8.GetString(r.Message.Value), CultureInfo.InvariantCulture)))
                .ToList();
            var distinct = records.Select(r => r.Tag).Distinct().Count();
            if (distinct >= Total || DateTime.UtcNow >= deadline)
            {
                return records;
            }

            expected = records.Count + Total - distinct;
        }
    }

    private static List<Record> DedupKeepFirst(IEnumerable<Record> records)
    {
        var seen = new HashSet<int>();
        return [.. records.Where(r => seen.Add(r.Tag))];
    }

    private sealed record Record(string Key, int Tag);
}
