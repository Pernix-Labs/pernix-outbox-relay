using System.Net;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Core.Leadership;
using Pernix.OutboxRelay.Integration.Tests.Delivery;
using Pernix.OutboxRelay.Integration.Tests.Kafka;
using Pernix.OutboxRelay.Integration.Tests.Leadership;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Testcontainers.PostgreSql;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.Embedded;

public sealed class EmbeddedHostParityTests(PostgreSqlFixture postgres, KafkaFixture kafka)
{
    private static readonly TimeSpan DeliveryTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan LeaderTimeout = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan HealthBound = TimeSpan.FromSeconds(15);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Embedded_and_image_hosts_deliver_the_same_record()
    {
        var row = new OutboxRow(
            "parity-1"u8.ToArray(),
            "{\"id\":1,\"name\":\"Ordér\"}"u8.ToArray(),
            new Dictionary<string, string> { ["idempotency-key"] = Guid.NewGuid().ToString(), ["type"] = "Ordér", ["empty"] = "" });

        var (imageSchema, imageTopic, imageSettings) = await CreateOutboxAsync(postgres.ConnectionString);
        var (embeddedSchema, embeddedTopic, embeddedSettings) = await CreateOutboxAsync(postgres.ConnectionString);

        await using var image = new RelayFactory().WithRelaySettings(imageSettings);
        _ = image.Services;
        await using var embedded = await EmbeddedRelayHost.StartAsync(embeddedSettings, Ct);

        await InsertAsync(postgres.ConnectionString, imageSchema, row);
        await InsertAsync(postgres.ConnectionString, embeddedSchema, row);

        var imageRecord = Assert.Single(await kafka.ConsumeAllAsync(imageTopic, 1, DeliveryTimeout)).Message;
        var embeddedRecord = Assert.Single(await kafka.ConsumeAllAsync(embeddedTopic, 1, DeliveryTimeout)).Message;

        Assert.Equal(row.Key, imageRecord.Key);
        Assert.Equal(row.Payload, imageRecord.Value);
        Assert.Equal(imageRecord.Key, embeddedRecord.Key);
        Assert.Equal(imageRecord.Value, embeddedRecord.Value);
        Assert.Equal(HeaderList(imageRecord), HeaderList(embeddedRecord));
        Assert.Equal(
            row.Headers.OrderBy(h => h.Key, StringComparer.Ordinal),
            HeaderList(embeddedRecord).Select(h => KeyValuePair.Create(h.Key, Encoding.UTF8.GetString(Convert.FromBase64String(h.Value)))).OrderBy(h => h.Key, StringComparer.Ordinal));
    }

    // Own container: pausing the shared fixture would break every other test.
    [Fact]
    public async Task Embedded_and_image_hosts_report_the_same_health()
    {
        await using var container = new PostgreSqlBuilder("postgres:17.11").Build();
        await container.StartAsync(Ct);
        var (_, _, imageSettings) = await CreateOutboxAsync(container.GetConnectionString());
        var (_, _, embeddedSettings) = await CreateOutboxAsync(container.GetConnectionString());

        await using var image = new RelayFactory().WithRelaySettings(imageSettings);
        using var imageClient = image.CreateClient();
        await using var embedded = await EmbeddedRelayHost.StartAsync(embeddedSettings, Ct);
        var hosts = new[] { ("image", imageClient), ("embedded", embedded.HttpClient) };

        Assert.True(await LeadershipTests.EventuallyAsync(() => IsLeader(image.Services), LeaderTimeout), "image host not leader");
        Assert.True(await LeadershipTests.EventuallyAsync(() => IsLeader(embedded.Services), LeaderTimeout), "embedded host not leader");
        foreach (var (name, client) in hosts)
        {
            Assert.True(await EventuallyStatusAsync(client, "/readyz", HttpStatusCode.OK), $"{name} /readyz not 200");
            Assert.Equal(HttpStatusCode.OK, await StatusAsync(client, "/livez"));
        }

        await container.PauseAsync(Ct);
        try
        {
            foreach (var (name, client) in hosts)
            {
                Assert.True(await EventuallyStatusAsync(client, "/readyz", HttpStatusCode.ServiceUnavailable), $"{name} /readyz not 503 within 15 s");
                Assert.Equal(HttpStatusCode.OK, await StatusAsync(client, "/livez"));
            }
        }
        finally
        {
            await container.UnpauseAsync(Ct);
        }

        foreach (var (name, client) in hosts)
        {
            Assert.True(await EventuallyStatusAsync(client, "/readyz", HttpStatusCode.OK), $"{name} /readyz not 200 within 15 s after unpause");
        }
    }

    [Fact]
    public async Task Embedded_host_with_invalid_configuration_fails_to_start()
    {
        var settings = RelayFactory.OutboxSettings(postgres.ConnectionString, kafka.BootstrapServers, (RelayFactory.NewOutboxName(), PostgreSqlFixture.NewSchema()));
        settings["Destinations:0:Transport"] = null;

        var exception = await Record.ExceptionAsync(async () => await (await EmbeddedRelayHost.StartAsync(settings, Ct)).DisposeAsync());

        var validation = Assert.IsType<RelayStartupValidationException>(exception is AggregateException aggregate ? Assert.Single(aggregate.InnerExceptions) : exception);
        Assert.Contains(validation.Failures, f => f.StartsWith("OutboxRelay.Destinations[Events].Transport: required", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Embedded_host_with_empty_relay_section_fails_to_start()
    {
        var exception = await Record.ExceptionAsync(async () => await (await EmbeddedRelayHost.StartAsync(new Dictionary<string, string?>(), Ct)).DisposeAsync());

        var validation = Assert.IsType<RelayStartupValidationException>(exception is AggregateException aggregate ? Assert.Single(aggregate.InnerExceptions) : exception);
        Assert.Equal(["OutboxRelay.Outboxes: at least one outbox is required"], validation.Failures);
    }

    internal static async Task InsertAsync(string connectionString, string schema, OutboxRow row)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(Ct);
        await using var command = new NpgsqlCommand($"INSERT INTO \"{schema}\".outbox (message_key, payload, headers) VALUES (@key, @payload, @headers::jsonb)", connection);
        command.Parameters.Add(new NpgsqlParameter("key", NpgsqlTypes.NpgsqlDbType.Bytea) { Value = (object?)row.Key ?? DBNull.Value });
        command.Parameters.AddWithValue("payload", row.Payload);
        command.Parameters.AddWithValue("headers", JsonSerializer.Serialize(row.Headers));
        await command.ExecuteNonQueryAsync(Ct);
    }

    // One schema and one topic per host, so each host's record is consumed on its own.
    private async Task<(string Schema, string Topic, Dictionary<string, string?> Settings)> CreateOutboxAsync(string connectionString)
    {
        var schema = PostgreSqlFixture.NewSchema();
        await PostgreSqlFixture.CreateOutboxSchemaAsync(connectionString, schema);
        var topic = KafkaFixture.NewTopic();
        await kafka.CreateTopicAsync(topic, 1);
        var settings = RelayFactory.OutboxSettings(connectionString, kafka.BootstrapServers, (RelayFactory.NewOutboxName(), schema));
        settings["Destinations:0:Kafka:Topic"] = topic;
        return (schema, topic, settings);
    }

    // Wire order and bytes. Base64 so values compare by content.
    private static List<(string Key, string Value)> HeaderList(Message<byte[], byte[]> message) =>
        [.. message.Headers.Select(h => (h.Key, Convert.ToBase64String(h.GetValueBytes())))];

    private static bool IsLeader(IServiceProvider services) =>
        services.GetRequiredService<OutboxRuntimeState>().Outboxes.SingleOrDefault()?.IsLeader == true;

    private static async Task<HttpStatusCode> StatusAsync(HttpClient client, string path)
    {
        using var response = await client.GetAsync(path, Ct);
        return response.StatusCode;
    }

    private static async Task<bool> EventuallyStatusAsync(HttpClient client, string path, HttpStatusCode expected)
    {
        var end = DateTime.UtcNow + HealthBound;
        while (DateTime.UtcNow < end)
        {
            if (await StatusAsync(client, path) == expected)
            {
                return true;
            }

            await Task.Delay(250, Ct);
        }

        return false;
    }
}
