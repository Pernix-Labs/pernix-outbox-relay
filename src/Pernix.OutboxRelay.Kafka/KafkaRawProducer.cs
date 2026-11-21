using Pernix.OutboxRelay.Core.Abstractions;
using Confluent.Kafka;
using Nk = Nanov.Kafka;

namespace Pernix.OutboxRelay.Kafka;

internal sealed class KafkaRawProducer(Nk.IRawProducer raw) : IKafkaRawProducer
{
    public Error? Produce(KafkaAddress address, byte[]? key, byte[] value, MessageHeaders headers, nint opaque)
    {
        try
        {
            var keySpan = key is null ? Nk.KafkaNativeSpan.Null : Nk.KafkaNativeSpan.Present(key);
            var valueSpan = Nk.KafkaNativeSpan.Present(value);
            if (headers.Count == 0)
            {
                raw.RawProduce(address.Topic, keySpan, valueSpan, opaque);
                return null;
            }

            Nk.KafkaHeaders first = default, second = default;
            if (!KafkaHeaders.Fill(headers, ref first, ref second))
            {
                return KafkaEmptyHeaderValues.Produce(raw, address.Topic, key, value, headers, opaque);
            }

            if (headers.Count <= KafkaHeaders.Inline)
            {
                raw.RawProduce(address.Topic, keySpan, valueSpan, in first, opaque);
            }
            else
            {
                raw.RawProduce(address.Topic, keySpan, valueSpan, in first, in second, opaque);
            }

            return null;
        }
        catch (Nk.KafkaException ex)
        {
            return ToStock(ex.Error);
        }
    }

    public void Dispose() => raw.Dispose();

    internal static Nk.RawDeliveryReportHandler ReportHandler(KafkaReportHandler onReport) =>
        (in Nk.RawDeliveryReport report) => Cross(report.Opaque, report.ErrorCode, report.Status, onReport);

    internal static void Cross(nint opaque, Nk.ErrorCode code, Nk.PersistenceStatus status, KafkaReportHandler onReport)
    {
        if (code == Nk.ErrorCode.Local_Fatal)
        {
            return;
        }

        onReport(opaque, code == Nk.ErrorCode.NoError ? null : new Error((ErrorCode)(int)code), (PersistenceStatus)(int)status);
    }

    internal static Error ToStock(Nk.Error error) => new((ErrorCode)(int)error.Code, error.Reason, error.IsFatal);
}
