using System.Text;
using Pernix.OutboxRelay.Core.Abstractions;

namespace Pernix.OutboxRelay.Kafka;

internal static class KafkaRecordSize
{
    internal const int BatchOverhead = 61;

    // Maximum varint widths: record length 5, attributes 1, timestamp delta 10, offset delta 5, key length 5, value length 5, header count 5.
    internal const int RecordOverhead = 36;

    internal const int HeaderOverhead = 10;

    public static long UpperBound(byte[]? key, byte[] value, MessageHeaders headers)
    {
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(headers);

        long size = BatchOverhead + RecordOverhead + (key?.Length ?? 0) + value.Length;
        for (var i = 0; i < headers.Count; i++)
        {
            size += HeaderOverhead + Encoding.UTF8.GetByteCount(headers[i].Name) + headers[i].Value.Length;
        }

        return size;
    }

    public static int Limit(TopicConfig topic, int producerMessageMaxBytes) => Math.Min(topic.MaxMessageBytes, producerMessageMaxBytes);
}
