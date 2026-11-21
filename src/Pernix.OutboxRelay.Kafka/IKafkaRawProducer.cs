using Pernix.OutboxRelay.Core.Abstractions;
using Confluent.Kafka;

namespace Pernix.OutboxRelay.Kafka;

internal interface IKafkaRawProducer : IDisposable
{
    // Null: enqueued, at most one report follows (none after a fatal error or dispose, see Retire). Non-null: refused synchronously, no report follows.
    Error? Produce(KafkaAddress address, byte[]? key, byte[] value, MessageHeaders headers, nint opaque);
}

internal delegate void KafkaReportHandler(nint opaque, Error? error, PersistenceStatus status);
