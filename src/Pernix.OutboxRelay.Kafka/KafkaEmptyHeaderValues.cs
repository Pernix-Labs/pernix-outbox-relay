// Nanov.Kafka 2.16.0.3 public produce APIs send a zero-length header value as NULL (fork RawProducer.cs:249), so such records use an own header list.
// Upstream: https://github.com/nanov/confluent-kafka-dotnet/pull/23
// Removal on a fixed release: pin it, delete this file and its call branch in KafkaRawProducer.Produce, Fill back to void, drop AllowUnsafeBlocks,
// remove Explicit from the raw header fact, re-run gates, parity tests and measurement.
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using System.Text;
using Pernix.OutboxRelay.Core.Abstractions;
using Confluent.Kafka;
using Nk = Nanov.Kafka;

namespace Pernix.OutboxRelay.Kafka;

internal static unsafe class KafkaEmptyHeaderValues
{
    private const int MsgFCopy = 2;
    private const int NameBufferBytes = 256;
    private const string Librdkafka = "Nanov.Kafka.Impl.Librdkafka, Nanov.Kafka";

    public static Error? Produce(Nk.IRawProducer raw, string topic, byte[]? key, byte[] value, MessageHeaders headers, nint opaque)
    {
        var list = HeadersNew(null, headers.Count);
        if (list == IntPtr.Zero)
        {
            return new Error(ErrorCode.Local_Fail, "Failed to allocate headers list.");
        }

        var owned = false;
        try
        {
            Span<byte> nameBuffer = stackalloc byte[NameBufferBytes];
            for (var i = 0; i < headers.Count; i++)
            {
                var header = headers[i];
                var nameLength = Encoding.UTF8.GetByteCount(header.Name);
                var name = nameLength <= NameBufferBytes ? nameBuffer[..nameLength] : new byte[nameLength];
                Encoding.UTF8.GetBytes(header.Name, name);
                Nk.ErrorCode added;
                fixed (byte* namePtr = &MemoryMarshal.GetReference(name))
                fixed (byte* headerValuePtr = &MemoryMarshal.GetArrayDataReference(header.Value))
                {
                    added = HeadersAdd(null, list, (IntPtr)namePtr, nameLength, (IntPtr)headerValuePtr, header.Value.Length);
                }

                if (added != Nk.ErrorCode.NoError)
                {
                    return new Error((ErrorCode)(int)added, "Failed to add header.");
                }
            }

            ref var keyRef = ref key is null ? ref Unsafe.NullRef<byte>() : ref MemoryMarshal.GetArrayDataReference(key);
            Nk.ErrorCode code;
            fixed (byte* keyPtr = &keyRef)
            fixed (byte* valuePtr = &MemoryMarshal.GetArrayDataReference(value))
            {
                code = ProduceRawCore(
                    raw, topic, Partition.Any.Value, (IntPtr)keyPtr, key?.Length ?? 0, (IntPtr)valuePtr, value.Length, list, MsgFCopy, opaque);
            }

            if (code == Nk.ErrorCode.NoError)
            {
                owned = true;
                return null;
            }

            ThrowIfError(raw, code);
            return new Error((ErrorCode)(int)code);
        }
        finally
        {
            if (!owned)
            {
                HeadersDestroy(null, list);
            }
        }
    }

    [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "ProduceRawCore")]
    private static extern Nk.ErrorCode ProduceRawCore(
        Nk.IRawProducer producer, string topic, int partition, IntPtr keyPtr, int keyLen, IntPtr valuePtr, int valueLen, IntPtr headers, IntPtr msgFlags, IntPtr opaque);

    [UnsafeAccessor(UnsafeAccessorKind.Method, Name = "ThrowIfError")]
    private static extern void ThrowIfError(Nk.IRawProducer producer, Nk.ErrorCode err);

    [UnsafeAccessor(UnsafeAccessorKind.StaticMethod, Name = "headers_new")]
    private static extern IntPtr HeadersNew([UnsafeAccessorType(Librdkafka)] object? type, IntPtr size);

    [UnsafeAccessor(UnsafeAccessorKind.StaticMethod, Name = "headers_add")]
    private static extern Nk.ErrorCode HeadersAdd(
        [UnsafeAccessorType(Librdkafka)] object? type, IntPtr headers, IntPtr name, IntPtr nameSize, IntPtr value, IntPtr valueSize);

    [UnsafeAccessor(UnsafeAccessorKind.StaticMethod, Name = "headers_destroy")]
    private static extern void HeadersDestroy([UnsafeAccessorType(Librdkafka)] object? type, IntPtr headers);
}
