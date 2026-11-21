using Pernix.OutboxRelay.Client;
using Pernix.OutboxRelay.Client.PostgreSql;
using Pernix.OutboxRelay.Integration.Tests.Kafka;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Npgsql;

namespace Pernix.OutboxRelay.Integration.Tests.ClientContract;

// Outbox table from package DDL only (non-default name and prefix), its own topic (or one topic per mapped route), one relay outbox on both.
internal sealed class ClientContractSetup
{
    public const string TableName = "client_out";
    public const string NamePrefix = "co";

    private readonly string connectionString;
    private readonly Dictionary<string, string?> settings;
    private readonly string? topic;

    private ClientContractSetup(string connectionString, OutboxTable table, string? topic, Dictionary<string, string?> settings)
    {
        this.connectionString = connectionString;
        this.settings = settings;
        this.topic = topic;
        Table = table;
        Publisher = new PostgreSqlOutboxPublisher(table);
    }

    public OutboxTable Table { get; }

    public string Topic => topic ?? throw new InvalidOperationException("A Routes setup has no single topic.");

    public PostgreSqlOutboxPublisher Publisher { get; }

    // routes (route -> topic): the destination sets Kafka.Routes instead of Kafka.Topic, each mapped topic created.
    public static async Task<ClientContractSetup> CreateAsync(
        PostgreSqlFixture postgres,
        KafkaFixture kafka,
        string orderingMode = "None",
        int partitions = 1,
        IReadOnlyDictionary<string, string>? routes = null)
    {
        var schema = PostgreSqlFixture.NewSchema();
        var table = OutboxTable.Parse($"{schema}.{TableName}");
        await postgres.ExecuteAsync($"CREATE SCHEMA {schema}");
        foreach (var script in PostgreSqlOutboxSchema.Scripts(table, NamePrefix))
        {
            await postgres.ExecuteAsync(script.Sql);
        }

        var settings = RelayFactory.OutboxSettings(postgres.ConnectionString, kafka.BootstrapServers, (RelayFactory.NewOutboxName(), schema));
        string? topic = null;
        if (routes is null)
        {
            topic = KafkaFixture.NewTopic();
            await kafka.CreateTopicAsync(topic, partitions);
            settings["Destinations:0:Kafka:Topic"] = topic;
        }
        else
        {
            settings.Remove("Destinations:0:Kafka:Topic");
            foreach (var mapped in routes.Values.Distinct(StringComparer.Ordinal))
            {
                await kafka.CreateTopicAsync(mapped, partitions);
            }

            foreach (var (route, mapped) in routes)
            {
                settings[$"Destinations:0:Kafka:Routes:{route}"] = mapped;
            }
        }

        settings["Outboxes:0:Database:Table"] = table.ToString();
        settings["Outboxes:0:Ordering:Mode"] = orderingMode;

        return new ClientContractSetup(postgres.ConnectionString, table, topic, settings);
    }

    public RelayFactory Relay() => new RelayFactory().WithRelaySettings(settings);

    public async Task<NpgsqlConnection> OpenAsync(CancellationToken ct)
    {
        var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(ct);
        return connection;
    }

    public async Task<long> CountAsync(CancellationToken ct)
    {
        await using var connection = await OpenAsync(ct);
        await using var command = new NpgsqlCommand($"SELECT count(*) FROM {Table}", connection);
        return (long)(await command.ExecuteScalarAsync(ct))!;
    }
}
