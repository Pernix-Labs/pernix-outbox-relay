using System.Diagnostics;
using Pernix.OutboxRelay.Core.Leadership;
using Pernix.OutboxRelay.Integration.Tests.Kafka;
using Pernix.OutboxRelay.Integration.Tests.Leadership;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.Delivery;

public sealed class DeliveryStopTests(PostgreSqlFixture postgres, KafkaFixture kafka)
{
    private static readonly TimeSpan ShutdownTimeout = TimeSpan.FromSeconds(28);
    private static readonly TimeSpan DeliveryTimeout = TimeSpan.FromSeconds(30);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Stop_mid_stream_deletes_only_rows_on_topic_within_shutdown_timeout()
    {
        const int total = 2000;
        var setup = await DeliverySetup.CreateAsync(postgres, kafka, batchSize: 50);
        await using var relay = setup.Relay();
        var host = StartedHost(relay);
        var status = relay.Services.GetRequiredService<OutboxRuntimeState>().Outboxes.Single();
        Assert.True(await LeadershipTests.EventuallyAsync(() => status.IsLeader, TimeSpan.FromSeconds(10)), "no leader");

        using var inserter = new StreamingInserter(setup, total, chunk: 100, pause: TimeSpan.FromMilliseconds(20));
        Assert.True(await inserter.DeliveryStartedAsync(DeliveryTimeout), "no delivery before stop");

        var elapsed = await StopAsync(host);
        await inserter.StopAsync();

        Assert.True(elapsed < ShutdownTimeout, $"stop took {elapsed}");
        var inserted = Enumerable.Range(1, inserter.Inserted).ToHashSet();
        var remaining = await setup.TagsInTableAsync();
        var deleted = inserted.Except(remaining).ToHashSet();
        var onTopic = (await setup.ConsumeTagsAsync(inserted.Count, TimeSpan.FromSeconds(10))).ToHashSet();

        Assert.NotEmpty(deleted);
        Assert.Subset(onTopic, deleted);
        Assert.Subset(inserted, onTopic);
    }

    [Fact]
    public async Task Stop_with_paused_broker_ends_within_shutdown_timeout_and_keeps_rows()
    {
        await using var own = await KafkaFixture.StartOwnContainerAsync();
        var setup = await DeliverySetup.CreateAsync(postgres, own);
        var recorder = new DestinationRecorder();
        await using var relay = RecordingDestinationFactory.Decorate(setup.Relay(), recorder);
        var host = StartedHost(relay);
        var status = relay.Services.GetRequiredService<OutboxRuntimeState>().Outboxes.Single();
        Assert.True(await LeadershipTests.EventuallyAsync(() => status.IsLeader, TimeSpan.FromSeconds(10)), "no leader");

        await setup.InsertTagsAsync(1, 1);
        Assert.True(await LeadershipTests.EventuallyAsync(async () => await setup.CountAsync() == 0, DeliveryTimeout), "warm-up row not delivered");

        await own.Container.PauseAsync(Ct);
        try
        {
            await setup.InsertTagsAsync(2, 10);
            Assert.True(await LeadershipTests.EventuallyAsync(() => recorder.Destinations.Count > 0 && recorder.Destinations[^1].InFlight > 0, TimeSpan.FromSeconds(10)), "no send in flight");

            var elapsed = await StopAsync(host);

            Assert.True(elapsed < ShutdownTimeout, $"stop took {elapsed}");
            Assert.Equal(10, await setup.CountAsync());
        }
        finally
        {
            await own.Container.UnpauseAsync(Ct);
        }
    }

    private static IHost StartedHost(RelayFactory relay)
    {
        var host = relay.Services.GetRequiredService<IHost>();
        Assert.Equal(ShutdownTimeout, relay.Services.GetRequiredService<IOptions<HostOptions>>().Value.ShutdownTimeout);
        return host;
    }

    private static async Task<TimeSpan> StopAsync(IHost host)
    {
        var elapsed = Stopwatch.StartNew();
        await host.StopAsync(CancellationToken.None);
        return elapsed.Elapsed;
    }
}
