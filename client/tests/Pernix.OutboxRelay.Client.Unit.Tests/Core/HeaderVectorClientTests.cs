using Pernix.OutboxRelay.ContractVectors;
using Xunit;

namespace Pernix.OutboxRelay.Client.Unit.Tests.Core;

public sealed class HeaderVectorClientTests
{
    public static TheoryData<string> ClientVectorIds()
    {
        var ids = new TheoryData<string>();
        foreach (var vector in HeaderVectors.All)
        {
            if (vector.Client is not null)
            {
                ids.Add(vector.Id);
            }
        }

        return ids;
    }

    public static TheoryData<string> AllVectorIds()
    {
        var ids = new TheoryData<string>();
        foreach (var vector in HeaderVectors.All)
        {
            ids.Add(vector.Id);
        }

        return ids;
    }

    [Theory]
    [MemberData(nameof(ClientVectorIds))]
    public void Build_accepts_exactly_when_client_accepts(string id)
    {
        var vector = HeaderVectors.Get(id);
        Assert.NotNull(vector.ClientAccepts);

        var builder = OutboxHeaders.CreateBuilder();
        foreach (var header in vector.Client!)
        {
            builder.Add(header.Key, header.Value);
        }

        if (vector.ClientAccepts.Value)
        {
            var headers = builder.Build();
            Assert.Equal(vector.Client, headers.ToArray());
        }
        else
        {
            Assert.Throws<ArgumentException>(() => builder.Build());
        }
    }

    // The client is never looser than the relay.
    [Theory]
    [MemberData(nameof(AllVectorIds))]
    public void Client_accepted_vector_is_relay_accepted_with_stored_json_and_kafka_headers(string id)
    {
        var vector = HeaderVectors.Get(id);
        if (vector.ClientAccepts != true)
        {
            return;
        }

        Assert.True(vector.RelayAccepts == true, $"{id}: clientAccepts is true but relayAccepts is {vector.RelayAccepts?.ToString() ?? "null"}.");
        Assert.NotNull(vector.StoredJson);
        Assert.NotNull(vector.KafkaHeaders);
    }

    [Fact]
    public void File_holds_h1_to_h16_once_each()
    {
        var ids = HeaderVectors.All.Select(v => v.Id).ToArray();

        Assert.Equal(ids.Length, ids.Distinct(StringComparer.Ordinal).Count());
        for (var i = 1; i <= 16; i++)
        {
            Assert.Contains("H" + i, ids);
        }
    }
}
