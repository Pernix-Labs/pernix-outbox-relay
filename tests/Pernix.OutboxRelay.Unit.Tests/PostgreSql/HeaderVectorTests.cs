using System.Text;
using Pernix.OutboxRelay.ContractVectors;
using Pernix.OutboxRelay.PostgreSql;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.PostgreSql;

public sealed class HeaderVectorTests
{
    private readonly HeadersParser parser = new();

    public static TheoryData<string> StoredVectorIds()
    {
        var ids = new TheoryData<string>();
        foreach (var vector in HeaderVectors.All)
        {
            if (vector.StoredJson is not null)
            {
                ids.Add(vector.Id);
            }
        }

        return ids;
    }

    [Theory]
    [MemberData(nameof(StoredVectorIds))]
    public void Parser_accepts_exactly_when_relay_accepts_and_delivers_the_kafka_headers(string id)
    {
        var vector = HeaderVectors.Get(id);
        Assert.NotNull(vector.RelayAccepts);

        var headers = parser.Parse(Encoding.UTF8.GetBytes(vector.StoredJson!));

        Assert.Equal(vector.RelayAccepts.Value, headers is not null);
        if (headers is null || vector.KafkaHeaders is null)
        {
            return;
        }

        var expected = vector.KafkaHeaders
            .Select(h => (h.Key, Convert.ToHexString(Encoding.UTF8.GetBytes(h.Value))))
            .Order();
        var actual = headers.Select(h => (h.Name, Convert.ToHexString(h.Value))).Order();
        Assert.Equal(expected, actual);
    }

    [Fact]
    public void Every_stored_vector_parses_the_same_with_a_cold_and_a_warm_name_cache()
    {
        for (var pass = 0; pass < 2; pass++)
        {
            foreach (var vector in HeaderVectors.All)
            {
                if (vector.StoredJson is null)
                {
                    continue;
                }

                var headers = parser.Parse(Encoding.UTF8.GetBytes(vector.StoredJson));

                Assert.NotNull(vector.RelayAccepts);
                Assert.Equal(vector.RelayAccepts.Value, headers is not null);
                if (headers is null || vector.KafkaHeaders is null)
                {
                    continue;
                }

                var expected = vector.KafkaHeaders
                    .Select(h => (h.Key, Convert.ToHexString(Encoding.UTF8.GetBytes(h.Value))))
                    .Order();
                var actual = headers.Select(h => (h.Name, Convert.ToHexString(h.Value))).Order();
                Assert.Equal(expected, actual);
            }
        }
    }
}
