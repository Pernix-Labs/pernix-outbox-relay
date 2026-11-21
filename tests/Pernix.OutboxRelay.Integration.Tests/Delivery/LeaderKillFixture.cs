using System.Text;
using Pernix.OutboxRelay.Integration.Tests.Kafka;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Configurations;
using DotNet.Testcontainers.Containers;
using DotNet.Testcontainers.Networks;
using Npgsql;
using Testcontainers.PostgreSql;

namespace Pernix.OutboxRelay.Integration.Tests.Delivery;

// Own Docker network so the relay container reaches PostgreSQL and Kafka by alias. Started after the opt-in check.
internal sealed class LeaderKillFixture : IAsyncDisposable
{
    public const string PostgresAlias = "postgres";
    public const string KafkaAlias = "kafka";

    private const string Database = "outbox";
    private const string Username = "outbox";
    private const string Password = "outbox";
    private const ushort PostgresPort = 5432;
    private const ushort BrokerPort = 9092;
    private const ushort NetworkBrokerPort = 19092;
    private const string StartScript = "/testcontainers.sh";

    private readonly INetwork network;
    private readonly PostgreSqlContainer postgres;
    private readonly IContainer kafka;

    private LeaderKillFixture()
    {
        network = new NetworkBuilder().WithName($"leader-kill-{Guid.NewGuid():N}").Build();
        postgres = new PostgreSqlBuilder("postgres:17.11")
            .WithDatabase(Database)
            .WithUsername(Username)
            .WithPassword(Password)
            .WithNetwork(network)
            .WithNetworkAliases(PostgresAlias)
            .Build();

        kafka = new ContainerBuilder(KafkaFixture.Image)
            .WithNetwork(network)
            .WithNetworkAliases(KafkaAlias)
            .WithPortBinding(BrokerPort, assignRandomHostPort: true)
            .WithEnvironment("KAFKA_NODE_ID", "1")
            .WithEnvironment("KAFKA_PROCESS_ROLES", "broker,controller")
            .WithEnvironment("KAFKA_LISTENERS", $"PLAINTEXT://0.0.0.0:{BrokerPort},NETWORK://0.0.0.0:{NetworkBrokerPort},CONTROLLER://0.0.0.0:9093")
            .WithEnvironment("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "CONTROLLER:PLAINTEXT,PLAINTEXT:PLAINTEXT,NETWORK:PLAINTEXT")
            .WithEnvironment("KAFKA_INTER_BROKER_LISTENER_NAME", "NETWORK")
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
                Encoding.ASCII.GetBytes(
                    $"#!/bin/sh\nexport KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://{started.Hostname}:{started.GetMappedPublicPort(BrokerPort)},NETWORK://{KafkaAlias}:{NetworkBrokerPort}\nexec /etc/kafka/docker/run\n"),
                StartScript,
                fileMode: UnixFileModes.UserRead | UnixFileModes.UserWrite | UnixFileModes.UserExecute | UnixFileModes.GroupRead | UnixFileModes.GroupExecute | UnixFileModes.OtherRead | UnixFileModes.OtherExecute,
                ct: ct))
            .WithWaitStrategy(Wait.ForUnixContainer().UntilMessageIsLogged("Kafka Server started"))
            .Build();
    }

    public INetwork Network => network;

    public string ConnectionString => postgres.GetConnectionString();

    public string BootstrapServers => $"{kafka.Hostname}:{kafka.GetMappedPublicPort(BrokerPort)}";

    public static string NetworkConnectionString =>
        $"Host={PostgresAlias};Port={PostgresPort};Database={Database};Username={Username};Password={Password}";

    public static string NetworkBootstrapServers => $"{KafkaAlias}:{NetworkBrokerPort}";

    public static async Task<LeaderKillFixture> StartAsync()
    {
        var fixture = new LeaderKillFixture();
        try
        {
            await fixture.network.CreateAsync();
            await Task.WhenAll(fixture.postgres.StartAsync(), fixture.kafka.StartAsync());
            return fixture;
        }
        catch
        {
            await fixture.DisposeAsync();
            throw;
        }
    }

    public async Task<string> CreateOutboxAsync()
    {
        var schema = PostgreSqlFixture.NewSchema();
        await PostgreSqlFixture.CreateOutboxSchemaAsync(ConnectionString, schema);
        return schema;
    }

    public Task CreateTopicAsync(string name, int partitions) => KafkaFixture.CreateTopicAsync(BootstrapServers, name, partitions);

    public async Task<int?> FindLockHolderPidAsync(string qualifiedTable, string applicationName)
    {
        await using var connection = new NpgsqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            """
            SELECT l.pid FROM pg_locks l
            JOIN pg_stat_activity a ON a.pid = l.pid
            WHERE l.locktype = 'advisory' AND l.granted
              AND l.classid = hashtext('outbox-relay')::oid
              AND l.objid = hashtext(@table)::oid
              AND l.objsubid = 2
              AND a.application_name = @application
            """,
            connection);
        command.Parameters.AddWithValue("table", qualifiedTable);
        command.Parameters.AddWithValue("application", applicationName);
        return await command.ExecuteScalarAsync() is int pid ? pid : null;
    }

    public async ValueTask DisposeAsync()
    {
        await kafka.DisposeAsync();
        await postgres.DisposeAsync();
        await network.DisposeAsync();
    }
}
