using Pernix.OutboxRelay.Integration.Tests.Kafka;
using Confluent.Kafka;
using System.Text;
using Confluent.Kafka.Admin;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using Xunit;

[assembly: AssemblyFixture(typeof(KafkaFixture))]

namespace Pernix.OutboxRelay.Integration.Tests.Kafka;

public sealed class KafkaFixture : IAsyncLifetime
{
    public const string Image = "apache/kafka:4.3.1";

    public const string EventsTopic = "events";

    private const ushort BrokerPort = 9092;
    private const string StartScript = "/testcontainers.sh";

    // KafkaBuilder cannot drive this image (empty advertised.listeners at storage format), so start once the mapped port is known.
    private readonly IContainer container = new ContainerBuilder(Image)
        .WithPortBinding(BrokerPort, assignRandomHostPort: true)
        .WithEnvironment("KAFKA_NODE_ID", "1")
        .WithEnvironment("KAFKA_PROCESS_ROLES", "broker,controller")
        .WithEnvironment("KAFKA_LISTENERS", $"PLAINTEXT://0.0.0.0:{BrokerPort},CONTROLLER://0.0.0.0:9093")
        .WithEnvironment("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
        .WithEnvironment("KAFKA_CONTROLLER_LISTENER_NAMES", "CONTROLLER")
        .WithEnvironment("KAFKA_CONTROLLER_QUORUM_VOTERS", "1@localhost:9093")
        .WithEnvironment("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
        .WithEnvironment("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
        .WithEnvironment("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
        .WithEnvironment("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_REPLICATION_FACTOR", "1")
        .WithEnvironment("KAFKA_SHARE_COORDINATOR_STATE_TOPIC_MIN_ISR", "1")
        .WithEnvironment("KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS", "0")
        .WithEnvironment("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "false")
        .WithEntrypoint("/bin/sh", "-c")
        .WithCommand($"while [ ! -f {StartScript} ]; do sleep 0.1; done; exec {StartScript}")
        .WithStartupCallback((started, ct) => started.CopyAsync(
            Encoding.ASCII.GetBytes($"#!/bin/sh\nexport KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://{started.Hostname}:{started.GetMappedPublicPort(BrokerPort)}\nexec /etc/kafka/docker/run\n"),
            StartScript,
            fileMode: UnixFileModes.UserRead | UnixFileModes.UserWrite | UnixFileModes.UserExecute | UnixFileModes.GroupRead | UnixFileModes.GroupExecute | UnixFileModes.OtherRead | UnixFileModes.OtherExecute,
            ct: ct))
        .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Kafka Server started"))
        .Build();

    public string BootstrapServers => $"{container.Hostname}:{container.GetMappedPublicPort(BrokerPort)}";

    public IContainer Container => container;

    public async ValueTask InitializeAsync()
    {
        await container.StartBoundedAsync();
        await CreateTopicAsync(EventsTopic, 1);
    }

    public async ValueTask DisposeAsync() => await container.DisposeAsync();

    public static async Task<KafkaFixture> StartOwnContainerAsync()
    {
        var own = new KafkaFixture();
        await own.InitializeAsync();
        return own;
    }

    public Task CreateTopicAsync(string name, int partitions, IReadOnlyDictionary<string, string>? configs = null) =>
        CreateTopicAsync(BootstrapServers, name, partitions, configs);

    public static async Task CreateTopicAsync(string bootstrapServers, string name, int partitions, IReadOnlyDictionary<string, string>? configs = null)
    {
        using var admin = new AdminClientBuilder(new AdminClientConfig { BootstrapServers = bootstrapServers }).Build();
        await admin.CreateTopicsAsync(
        [
            new TopicSpecification
            {
                Name = name,
                NumPartitions = partitions,
                ReplicationFactor = 1,
                Configs = configs?.ToDictionary(),
            },
        ]);

        // CreateTopics returns before the broker's metadata has the topic, so the relay's startup describe could miss it.
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (true)
        {
            try
            {
                await admin.DescribeConfigsAsync([new ConfigResource { Type = ResourceType.Topic, Name = name }]);
                return;
            }
            catch (DescribeConfigsException) when (DateTime.UtcNow < deadline)
            {
                await Task.Delay(50);
            }
        }
    }

    public Task<IReadOnlyList<ConsumeResult<byte[], byte[]>>> ConsumeAllAsync(string topic, int expected, TimeSpan timeout) =>
        ConsumeAllAsync(BootstrapServers, topic, expected, timeout);

    public static Task<IReadOnlyList<ConsumeResult<byte[], byte[]>>> ConsumeAllAsync(string bootstrapServers, string topic, int expected, TimeSpan timeout) =>
        Task.Run<IReadOnlyList<ConsumeResult<byte[], byte[]>>>(() =>
        {
            var config = new ConsumerConfig
            {
                BootstrapServers = bootstrapServers,
                GroupId = $"test-{Guid.NewGuid():N}",
                AutoOffsetReset = AutoOffsetReset.Earliest,
                EnableAutoCommit = false,
            };
            using var consumer = new ConsumerBuilder<byte[], byte[]>(config).Build();
            consumer.Subscribe(topic);
            try
            {
                var records = new List<ConsumeResult<byte[], byte[]>>();
                var deadline = DateTime.UtcNow + timeout;
                while (records.Count < expected && DateTime.UtcNow < deadline)
                {
                    var record = consumer.Consume(TimeSpan.FromMilliseconds(200));
                    if (record is { IsPartitionEOF: false })
                    {
                        records.Add(record);
                    }
                }

                return records;
            }
            finally
            {
                consumer.Close();
            }
        });

    public static string NewTopic() => $"t-{Guid.NewGuid():N}";
}
