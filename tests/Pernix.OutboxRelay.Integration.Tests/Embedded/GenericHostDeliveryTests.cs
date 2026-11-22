using Pernix.OutboxRelay.Hosting;
using Pernix.OutboxRelay.Integration.Tests.Delivery;
using Pernix.OutboxRelay.Integration.Tests.Kafka;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.Embedded;

public sealed class GenericHostDeliveryTests(PostgreSqlFixture postgres, KafkaFixture kafka)
{
    private static readonly TimeSpan DeliveryTimeout = TimeSpan.FromSeconds(30);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Generic_host_delivers_a_row()
    {
        var schema = await postgres.CreateOutboxAsync();
        var topic = KafkaFixture.NewTopic();
        await kafka.CreateTopicAsync(topic, 1);
        var settings = RelayFactory.OutboxSettings(postgres.ConnectionString, kafka.BootstrapServers, (RelayFactory.NewOutboxName(), schema));
        settings["Destinations:0:Kafka:Topic"] = topic;

        var builder = Microsoft.Extensions.Hosting.Host.CreateApplicationBuilder();
        builder.Configuration.AddInMemoryCollection(EmbeddedRelayHost.Prefixed(settings));
        builder.AddOutboxRelay();
        using var host = builder.Build();
        await host.StartAsync(Ct);

        try
        {
            var row = new OutboxRow("generic-1"u8.ToArray(), "{\"id\":1}"u8.ToArray(), new Dictionary<string, string> { ["type"] = "Generic" });
            await EmbeddedHostParityTests.InsertAsync(postgres.ConnectionString, schema, row);

            var record = Assert.Single(await kafka.ConsumeAllAsync(topic, 1, DeliveryTimeout));
            Assert.Equal(row.Key, record.Message.Key);
            Assert.Equal(row.Payload, record.Message.Value);
        }
        finally
        {
            await host.StopAsync(Ct);
        }
    }
}
