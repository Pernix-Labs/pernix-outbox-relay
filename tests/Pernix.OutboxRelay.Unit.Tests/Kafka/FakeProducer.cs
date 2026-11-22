using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Destination.Kafka;
using Confluent.Kafka;
using Nk = Nanov.Kafka;

namespace Pernix.OutboxRelay.Unit.Tests.Kafka;

internal sealed class FakeProducer : IKafkaRawProducer
{
    private readonly List<Pending> pending = [];
    private Error? refusal;
    private int refusals;
    private Exception? thrown;

    public IReadOnlyList<Pending> Sends
    {
        get
        {
            lock (pending)
            {
                return [.. pending];
            }
        }
    }

    public KafkaReportHandler? OnReport { get; set; }

    public int FlushCalls { get; }

    public int DisposeCalls { get; private set; }

    public Error? Produce(KafkaAddress address, byte[]? key, byte[] value, MessageHeaders headers, nint opaque)
    {
        lock (pending)
        {
            if (thrown is { } next)
            {
                thrown = null;
                throw next;
            }

            if (refusals > 0)
            {
                refusals--;
                return refusal;
            }

            pending.Add(new Pending(this, address, key, value, headers, opaque));
        }

        return null;
    }

    public void RefuseNext(Error error, int count)
    {
        lock (pending)
        {
            refusal = error;
            refusals = count;
        }
    }

    public void ThrowNext(Exception exception)
    {
        lock (pending)
        {
            thrown = exception;
        }
    }

    public void Dispose() => DisposeCalls++;

    internal sealed class Pending
    {
        private readonly FakeProducer owner;

        public Pending(FakeProducer owner, KafkaAddress address, byte[]? key, byte[] value, MessageHeaders headers, nint opaque)
        {
            this.owner = owner;
            Topic = address.Topic;
            TopicPartition = address.TopicPartition;
            MessageHeaders = headers;
            Opaque = opaque;
            Headers = [];
            for (var i = 0; i < headers.Count; i++)
            {
                Headers.Add(headers[i].Name, headers[i].Value);
            }

            Message = new Message<byte[], byte[]> { Key = key!, Value = value, Headers = Headers };
        }

        public string Topic { get; }

        public TopicPartition TopicPartition { get; }

        public MessageHeaders MessageHeaders { get; }

        public Headers Headers { get; }

        public Message<byte[], byte[]> Message { get; }

        public nint Opaque { get; }

        public void Persist() => Complete(PersistenceStatus.Persisted);

        public void Complete(PersistenceStatus status) => owner.OnReport?.Invoke(Opaque, null, status);

        public void Fail(Error error) => owner.OnReport?.Invoke(Opaque, error, PersistenceStatus.NotPersisted);

        public void ReportRaw(Nk.ErrorCode code) => KafkaRawProducer.Cross(Opaque, code, Nk.PersistenceStatus.NotPersisted, owner.OnReport!);
    }
}
