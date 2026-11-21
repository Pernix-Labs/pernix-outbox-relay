using System.Globalization;
using System.Text;
using Pernix.OutboxRelay.Client;
using Pernix.OutboxRelay.Integration.Tests.Kafka;
using Pernix.OutboxRelay.Integration.Tests.Leadership;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.ClientContract;

// Package DDL and package publishes against the in-process relay.
public sealed class ClientContractTests(PostgreSqlFixture postgres, KafkaFixture kafka)
{
    private static readonly TimeSpan DeliveryTimeout = TimeSpan.FromSeconds(60);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Relay_starts_on_package_ddl_and_delivers_a_published_message()
    {
        var setup = await ClientContractSetup.CreateAsync(postgres, kafka);
        await using var relay = setup.Relay();
        _ = relay.Services;

        await PublishAsync(setup, [OutboxMessage.Create("c1", "delivered")]);

        var record = Assert.Single(await kafka.ConsumeAllAsync(setup.Topic, 1, DeliveryTimeout));
        Assert.Equal("c1"u8.ToArray(), record.Message.Key);
        Assert.Equal("delivered"u8.ToArray(), record.Message.Value);
        Assert.True(await LeadershipTests.EventuallyAsync(async () => await setup.CountAsync(Ct) == 0, DeliveryTimeout), "table not empty");
    }

    [Fact]
    public async Task Key_payload_and_headers_arrive_byte_identical_and_null_key_stays_null()
    {
        var setup = await ClientContractSetup.CreateAsync(postgres, kafka);
        await using var relay = setup.Relay();
        _ = relay.Services;

        // Payload is not valid UTF-8. Key is an offset slice of a larger buffer.
        byte[] keyBuffer = [0x00, 0x01, 0xFF, 0x80, 0x00, 0x7F, 0xC3, 0x02];
        var key = keyBuffer.AsMemory(2, 5);
        byte[] payload = [0xFF, 0xFE, 0x00, 0xC3, 0x28, 0x80, 0xED, 0xA0, 0x80];
        byte[] nullKeyPayload = [0xC0, 0xAF, 0x00, 0xFF];
        var headers = OutboxHeaders.CreateBuilder().Add("type", "order.created").Add("lang", "é 中").Build();

        await PublishAsync(setup, [OutboxMessage.FromBytes(key, payload, headers), OutboxMessage.FromBytes(null, nullKeyPayload)]);

        var records = await kafka.ConsumeAllAsync(setup.Topic, 2, DeliveryTimeout);
        Assert.Equal(2, records.Count);

        var keyed = Assert.Single(records, r => r.Message.Value.AsSpan().SequenceEqual(payload)).Message;
        Assert.Equal(key.ToArray(), keyed.Key);
        Assert.Equal(
            [("lang", Convert.ToHexString(Encoding.UTF8.GetBytes("é 中"))), ("type", Convert.ToHexString("order.created"u8))],
            keyed.Headers.Select(h => (h.Key, Convert.ToHexString(h.GetValueBytes()))).OrderBy(h => h.Key, StringComparer.Ordinal));

        var unkeyed = Assert.Single(records, r => r.Message.Value.AsSpan().SequenceEqual(nullKeyPayload)).Message;
        Assert.Null(unkeyed.Key);
        Assert.Empty(unkeyed.Headers ?? []);
    }

    [Fact]
    public async Task Strict_batch_of_100_in_one_transaction_is_delivered_in_list_order()
    {
        const int count = 100;
        var setup = await ClientContractSetup.CreateAsync(postgres, kafka, orderingMode: "Strict", partitions: 1);
        await using var relay = setup.Relay();
        _ = relay.Services;

        var messages = new OutboxMessage[count];
        for (var i = 0; i < count; i++)
        {
            messages[i] = OutboxMessage.Create($"k{i % 7}", i.ToString(CultureInfo.InvariantCulture));
        }

        await PublishAsync(setup, messages);

        var records = await kafka.ConsumeAllAsync(setup.Topic, count, DeliveryTimeout);
        Assert.Equal(count, records.Count);
        for (var i = 0; i < count; i++)
        {
            Assert.Equal(messages[i].Key!.Value.ToArray(), records[i].Message.Key);
            Assert.Equal(messages[i].Payload.ToArray(), records[i].Message.Value);
        }
    }

    [Fact]
    public async Task Rolled_back_publish_writes_nothing_and_the_committed_marker_is_the_first_record()
    {
        var setup = await ClientContractSetup.CreateAsync(postgres, kafka);
        await using var relay = setup.Relay();
        _ = relay.Services;

        await using (var connection = await setup.OpenAsync(Ct))
        await using (var transaction = await connection.BeginTransactionAsync(Ct))
        {
            await setup.Publisher.PublishAsync(transaction, OutboxMessage.Create("rolled-back", "single"), Ct);
            await setup.Publisher.PublishAsync(transaction, [OutboxMessage.Create("rolled-back", "batch-1"), OutboxMessage.Create("rolled-back", "batch-2")], Ct);
            await transaction.RollbackAsync(Ct);
        }

        Assert.Equal(0, await setup.CountAsync(Ct));

        await PublishAsync(setup, [OutboxMessage.Create("marker", "marker")]);

        var first = (await kafka.ConsumeAllAsync(setup.Topic, 1, DeliveryTimeout))[0];
        Assert.Equal("marker"u8.ToArray(), first.Message.Key);
        Assert.Equal("marker"u8.ToArray(), first.Message.Value);
        Assert.True(await LeadershipTests.EventuallyAsync(async () => await setup.CountAsync(Ct) == 0, DeliveryTimeout), "table not empty");
    }

    // WithRoute rows reach their mapped topics. The route-less row is held (FailurePolicy.HoldPrefix + NoRoute) and produced nowhere.
    [Fact]
    public async Task Routed_publishes_reach_their_mapped_topics_and_a_route_less_row_is_held()
    {
        const string holdError = "route not mapped: (none)";
        var alertsTopic = KafkaFixture.NewTopic();
        var auditTopic = KafkaFixture.NewTopic();
        var setup = await ClientContractSetup.CreateAsync(postgres, kafka, routes: new Dictionary<string, string>
        {
            ["alerts"] = alertsTopic,
            ["audit"] = auditTopic,
        });
        await using var relay = setup.Relay();
        _ = relay.Services;

        await PublishAsync(setup,
        [
            OutboxMessage.Create("a1", "alert-1").WithRoute("alerts"),
            OutboxMessage.Create("u1", "audit-1").WithRoute("audit"),
            OutboxMessage.Create("n1", "no-route"),
            OutboxMessage.Create("a2", "alert-2").WithRoute("alerts"),
        ]);

        Assert.True(
            await LeadershipTests.EventuallyAsync(async () => await RowsAsync(setup) is [("n1", "no-route", holdError)], DeliveryTimeout),
            "routed rows not delivered or route-less row not held");

        // One more than expected, so a stray route-less record would show up within the wait.
        var alerts = await kafka.ConsumeAllAsync(alertsTopic, 3, TimeSpan.FromSeconds(5));
        var audit = await kafka.ConsumeAllAsync(auditTopic, 2, TimeSpan.FromSeconds(5));
        Assert.Equal(["a1:alert-1", "a2:alert-2"], alerts.Select(Text));
        Assert.Equal(["u1:audit-1"], audit.Select(Text));
    }

    private static string Text(Confluent.Kafka.ConsumeResult<byte[], byte[]> record) =>
        $"{Encoding.UTF8.GetString(record.Message.Key)}:{Encoding.UTF8.GetString(record.Message.Value)}";

    private static async Task<List<(string Key, string Payload, string? LastError)>> RowsAsync(ClientContractSetup setup)
    {
        await using var connection = await setup.OpenAsync(Ct);
        await using var command = new Npgsql.NpgsqlCommand(
            $"SELECT convert_from(message_key, 'UTF8'), convert_from(payload, 'UTF8'), last_error FROM {setup.Table} ORDER BY id",
            connection);
        await using var reader = await command.ExecuteReaderAsync(Ct);
        var rows = new List<(string, string, string?)>();
        while (await reader.ReadAsync(Ct))
        {
            rows.Add((reader.GetString(0), reader.GetString(1), reader.IsDBNull(2) ? null : reader.GetString(2)));
        }

        return rows;
    }

    private static async Task PublishAsync(ClientContractSetup setup, IReadOnlyList<OutboxMessage> messages)
    {
        await using var connection = await setup.OpenAsync(Ct);
        await using var transaction = await connection.BeginTransactionAsync(Ct);
        await setup.Publisher.PublishAsync(transaction, messages, Ct);
        await transaction.CommitAsync(Ct);
    }
}
