using System.Text;
using Pernix.OutboxRelay.Client;
using Pernix.OutboxRelay.ContractVectors;
using Pernix.OutboxRelay.Integration.Tests.Kafka;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Confluent.Kafka;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.ClientContract;

// Every clientAccepts vector, published by the client, reaches Kafka with its expected headers.
public sealed class ClientHeaderContractTests(PostgreSqlFixture postgres, KafkaFixture kafka)
{
    private static readonly TimeSpan DeliveryTimeout = TimeSpan.FromSeconds(60);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Every_client_accepted_vector_arrives_with_its_kafka_headers_as_a_set()
    {
        var vectors = new List<HeaderVector>();
        foreach (var vector in HeaderVectors.All)
        {
            if (vector.ClientAccepts == true)
            {
                vectors.Add(vector);
            }
        }

        Assert.NotEmpty(vectors);

        var setup = await ClientContractSetup.CreateAsync(postgres, kafka);
        await using var relay = setup.Relay();
        _ = relay.Services;

        var messages = new OutboxMessage[vectors.Count];
        for (var i = 0; i < vectors.Count; i++)
        {
            var builder = OutboxHeaders.CreateBuilder();
            foreach (var header in vectors[i].Client!)
            {
                builder.Add(header.Key, header.Value);
            }

            messages[i] = OutboxMessage.Create(vectors[i].Id, vectors[i].Id, builder.Build());
        }

        await using (var connection = await setup.OpenAsync(Ct))
        await using (var transaction = await connection.BeginTransactionAsync(Ct))
        {
            await setup.Publisher.PublishAsync(transaction, messages, Ct);
            await transaction.CommitAsync(Ct);
        }

        var records = await kafka.ConsumeAllAsync(setup.Topic, vectors.Count, DeliveryTimeout);
        Assert.Equal(vectors.Count, records.Count);

        foreach (var vector in vectors)
        {
            var record = Assert.Single(records, r => r.Message.Key is { } key && key.AsSpan().SequenceEqual(Encoding.UTF8.GetBytes(vector.Id)));
            Assert.True(
                Expected(vector).SetEquals(Actual(record.Message.Headers)),
                $"{vector.Id}: Kafka headers differ from kafkaHeaders.");
            Assert.Equal(vector.KafkaHeaders!.Count, record.Message.Headers?.Count ?? 0);
        }
    }

    private static HashSet<(string Name, string Value)> Expected(HeaderVector vector)
    {
        var set = new HashSet<(string, string)>();
        foreach (var header in vector.KafkaHeaders!)
        {
            set.Add((header.Key, Convert.ToHexString(Encoding.UTF8.GetBytes(header.Value))));
        }

        return set;
    }

    private static HashSet<(string Name, string Value)> Actual(Headers? headers)
    {
        var set = new HashSet<(string, string)>();
        foreach (var header in headers ?? [])
        {
            set.Add((header.Key, Convert.ToHexString(header.GetValueBytes())));
        }

        return set;
    }
}
