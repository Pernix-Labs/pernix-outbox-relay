using System.Text;
using Pernix.OutboxRelay.Core.Abstractions;

namespace Pernix.OutboxRelay.Benchmarks.Infrastructure;

internal static class Workload
{
    public const int Keys = 100;
    public const string Outbox = "bench-outbox";
    public const string Destination = "bench";

    public static readonly byte[][] KeyBytes = BuildKeys();
    public static readonly byte[] Payload = "123456"u8.ToArray();

    // One shared dead-letter detail for every kafka-dlq send.
    public static readonly DeadLetterInfo DeadLetter = new("destination fault: boom", 5, new DateTimeOffset(2026, 1, 1, 0, 0, 0, TimeSpan.Zero));

    private static readonly MessageHeader IdempotencyKey = new("idempotency-key", "123456"u8.ToArray());
    private static readonly MessageHeader TraceParent = new("traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"u8.ToArray());
    private static readonly MessageHeader ContentType = new("content-type", "application/json"u8.ToArray());
    private static readonly MessageHeader Type = new("type", "order.created"u8.ToArray());

    private static readonly MessageHeaders One = new([IdempotencyKey]);
    private static readonly MessageHeaders Three = new([IdempotencyKey, TraceParent, ContentType]);
    private static readonly MessageHeaders Four = new([IdempotencyKey, TraceParent, ContentType, Type]);

    public static MessageHeaders Headers(int count) => count switch
    {
        0 => MessageHeaders.Empty,
        1 => One,
        3 => Three,
        4 => Four,
        _ => throw new ArgumentOutOfRangeException(nameof(count), count, "header count must be 0, 1, 3 or 4"),
    };

    // Route stays null: single-address destinations take NULL-route rows only.
    public static OutboxMessage Row(long id, MessageHeaders headers) => new()
    {
        Id = id,
        Position = new OutboxPosition((ulong)id, id),
        Key = KeyBytes[id % Keys],
        Payload = Payload,
        Headers = headers,
        CreatedAt = DateTimeOffset.UnixEpoch,
        RetryCount = 0,
        IsDue = true,
    };

    public static OutgoingMessage Message(string outbox, long id, int headerCount, DeadLetterInfo? deadLetter = null) => new()
    {
        Source = new MessageSource(outbox, id, Destination),
        Key = KeyBytes[id % Keys],
        Payload = Payload,
        Headers = Headers(headerCount),
        DeadLetter = deadLetter,
    };

    public static OutgoingMessage[] Messages(long firstId, int count, int headerCount, DeadLetterInfo? deadLetter = null)
    {
        var messages = new OutgoingMessage[count];
        for (var i = 0; i < count; i++)
        {
            messages[i] = Message(Outbox, firstId + i, headerCount, deadLetter);
        }

        return messages;
    }

    // BDN runs the workload 2 + (warmup + iterations) x invocations times; pre-built data covers 1.1x that, never wraps.
    public static int Capacity(int warmupCount, int iterationCount, int invocationCount, int operationsPerInvoke)
    {
        var operations = (2L + ((long)warmupCount + iterationCount) * invocationCount) * operationsPerInvoke;
        return checked((int)Math.Ceiling(1.1 * operations));
    }

    public static InvalidOperationException CapacityExhausted(string suite) => new($"bench capacity exhausted: {suite}");

    private static byte[][] BuildKeys()
    {
        var keys = new byte[Keys][];
        for (var i = 0; i < Keys; i++)
        {
            keys[i] = Encoding.UTF8.GetBytes("k" + i);
        }

        return keys;
    }
}

internal sealed class RowBlock
{
    private RowBlock(OutboxMessage[] rows, long firstId)
    {
        Rows = rows;
        FirstId = firstId;
    }

    public OutboxMessage[] Rows { get; }

    public long FirstId { get; }

    public int Count => Rows.Length;

    public static RowBlock Create(long firstId, int count, int headerCount)
    {
        var headers = Workload.Headers(headerCount);
        var rows = new OutboxMessage[count];
        for (var i = 0; i < count; i++)
        {
            rows[i] = Workload.Row(firstId + i, headers);
        }

        return new RowBlock(rows, firstId);
    }
}
