using System.Text;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Destination.Kafka;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Kafka;

public sealed class KafkaRecordSizeTests
{
    [Fact]
    public void Empty_record_is_batch_and_record_overhead()
    {
        Assert.Equal(61 + 36, KafkaRecordSize.UpperBound(null, [], MessageHeaders.Empty));
    }

    [Fact]
    public void Size_counts_key_value_and_utf8_header_bytes()
    {
        var headers = TestHeaders.Of(new Dictionary<string, string> { ["type"] = "Ordér", ["traceparent"] = "00-1" });

        var size = KafkaRecordSize.UpperBound(new byte[3], new byte[100], headers);

        Assert.Equal(61 + 36 + 3 + 100 + (10 + 4 + Encoding.UTF8.GetByteCount("Ordér")) + (10 + 11 + 4), size);
    }

    [Fact]
    public void Limit_is_smaller_of_topic_and_producer()
    {
        Assert.Equal(1024, KafkaRecordSize.Limit(new TopicConfig(1024, Compacted: false), 1_000_000));
        Assert.Equal(500, KafkaRecordSize.Limit(new TopicConfig(1024, Compacted: false), 500));
    }

    [Fact]
    public async Task Record_at_limit_is_sent()
    {
        var harness = new KafkaHarness();
        var payload = new byte[1024 - (61 + 36 + 1)];
        await using var destination = harness.Create(new TopicConfig(1024, Compacted: false));

        var send = destination.SendAsync(KafkaHarness.Message([1], payload), CancellationToken.None);
        Assert.Single(Assert.Single(harness.Producers).Sends).Persist();

        Assert.IsType<DeliveryResult.Delivered>(await send);
    }

    [Fact]
    public async Task Record_one_byte_over_limit_is_not_sent()
    {
        var harness = new KafkaHarness();
        var payload = new byte[1024 - (61 + 36 + 1) + 1];
        await using var destination = harness.Create(new TopicConfig(1024, Compacted: false));

        var result = await destination.SendAsync(KafkaHarness.Message([1], payload), CancellationToken.None);

        Assert.Equal(new DeliveryResult.MessageFault("MsgSizeTooLarge: record up to 1025 bytes exceeds limit 1024", Permanent: true), result);
        Assert.Empty(harness.Producers);
    }
}
