using Confluent.Kafka;
using Xunit;
using Nk = Nanov.Kafka;

namespace Pernix.OutboxRelay.Integration.Tests.Kafka;

// A zero-length key, value and header value must arrive zero-length non-null.
public sealed class NanovZeroLengthTests(KafkaFixture kafka)
{
    private const string HeaderName = "h";
    private static readonly TimeSpan ConsumeTimeout = TimeSpan.FromSeconds(20);
    private static readonly TimeSpan FlushTimeout = TimeSpan.FromSeconds(30);

    private static readonly byte[] EmptyKey = [];
    private static readonly byte[] EmptyValue = [];
    private static readonly byte[] EmptyHeaderValue = [];
    private static readonly byte[] SecondValue = [1];

    [Fact]
    public async Task Stock_produce_keeps_zero_length_non_null()
    {
        var topic = await NewTopicAsync();

        using (var producer = new ProducerBuilder<byte[], byte[]>(ProducerConfig()).Build())
        {
            producer.Produce(topic, new Message<byte[], byte[]>
            {
                Key = EmptyKey,
                Value = EmptyValue,
                Headers = new Headers { { HeaderName, EmptyHeaderValue } },
            });
            producer.Produce(topic, new Message<byte[], byte[]> { Key = null!, Value = SecondValue });
            Assert.Equal(0, producer.Flush(FlushTimeout));
        }

        await AssertConsumedAsync(topic);
    }

    [Fact]
    public async Task Raw_produce_keeps_zero_length_key_and_value_non_null()
    {
        var topic = await NewTopicAsync();

        using (var producer = new Nk.RawProducerBuilder(ProducerConfig()).BuildRaw())
        {
            producer.RawProduce(topic, Nk.KafkaNativeSpan.Present(EmptyKey), Nk.KafkaNativeSpan.Present(EmptyValue), 1);
            producer.RawProduce(topic, Nk.KafkaNativeSpan.Null, Nk.KafkaNativeSpan.Present(SecondValue), 2);
            Assert.Equal(0, producer.Flush(FlushTimeout));
        }

        await AssertConsumedAsync(topic, withHeader: false);
    }

    // Fails on Nanov.Kafka 2.16.0.3: the public header API sends a zero-length value as NULL. Explicit until an upstream release fixes header values.
    [Fact(Explicit = true)]
    public async Task Raw_produce_keeps_zero_length_header_value_non_null()
    {
        var topic = await NewTopicAsync();

        using (var producer = new Nk.RawProducerBuilder(ProducerConfig()).BuildRaw())
        {
            Nk.KafkaHeaders headers = default;
            headers.Add(HeaderName, EmptyHeaderValue);
            producer.RawProduce(topic, Nk.KafkaNativeSpan.Present(EmptyKey), Nk.KafkaNativeSpan.Present(EmptyValue), in headers, 1);
            producer.RawProduce(topic, Nk.KafkaNativeSpan.Null, Nk.KafkaNativeSpan.Present(SecondValue), 2);
            Assert.Equal(0, producer.Flush(FlushTimeout));
        }

        await AssertConsumedAsync(topic);
    }

    private async Task<string> NewTopicAsync()
    {
        var topic = KafkaFixture.NewTopic();
        await kafka.CreateTopicAsync(topic, 1);
        return topic;
    }

    private ProducerConfig ProducerConfig() => new() { BootstrapServers = kafka.BootstrapServers };

    private async Task AssertConsumedAsync(string topic, bool withHeader = true)
    {
        var consumed = await kafka.ConsumeAllAsync(topic, 2, ConsumeTimeout);
        Assert.Equal(2, consumed.Count);

        var first = consumed[0].Message;
        List<Action> checks =
        [
            () => Assert.True(first.Key is { Length: 0 }, $"key: expected zero-length non-null, got {Describe(first.Key)}"),
            () => Assert.True(first.Value is { Length: 0 }, $"value: expected zero-length non-null, got {Describe(first.Value)}"),
        ];
        if (withHeader)
        {
            Assert.Single(first.Headers);
            Assert.Equal(HeaderName, first.Headers[0].Key);
            var headerValue = first.Headers[0].GetValueBytes();
            checks.Add(() => Assert.True(headerValue is { Length: 0 }, $"header value: expected zero-length non-null, got {Describe(headerValue)}"));
        }

        Assert.Multiple([.. checks]);

        var second = consumed[1].Message;
        Assert.Null(second.Key);
        Assert.Equal(SecondValue, second.Value);
    }

    private static string Describe(byte[]? bytes) => bytes is null ? "null" : $"length {bytes.Length}";
}
