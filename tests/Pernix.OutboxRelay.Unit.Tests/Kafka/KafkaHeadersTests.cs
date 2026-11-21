using System.Runtime.InteropServices;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Kafka;
using Xunit;
using NanovHeaders = Nanov.Kafka.KafkaHeaders;

namespace Pernix.OutboxRelay.Unit.Tests.Kafka;

public sealed class KafkaHeadersTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(8)]
    [InlineData(9)]
    [InlineData(16)]
    [InlineData(17)]
    [InlineData(20)]
    public void Fill_puts_the_first_eight_in_first_and_the_rest_in_second_in_order(int count)
    {
        var items = new MessageHeader[count];
        for (var i = 0; i < count; i++)
        {
            items[i] = new MessageHeader($"h{i}", [(byte)i, 0xFF]);
        }

        AssertFilled(items);
    }

    [Fact]
    public void Fill_keeps_non_ascii_names()
    {
        AssertFilled(
        [
            new MessageHeader("Ordér", [1]),
            new MessageHeader("注文", [2]),
            new MessageHeader("ключ", [3]),
            new MessageHeader("traceparent", [4]),
        ]);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(8)]
    [InlineData(9)]
    [InlineData(16)]
    public void Fill_allocates_nothing_up_to_16(int count)
    {
        Assert.Equal(0, FillBytes(Headers(count)));
    }

    [Theory]
    [InlineData(17, 120)]
    [InlineData(18, 120)]
    [InlineData(19, 120)]
    [InlineData(20, 120)]
    [InlineData(21, 336)]
    [InlineData(22, 336)]
    [InlineData(23, 336)]
    [InlineData(24, 336)]
    public void Fill_bytes_beyond_16(int count, long expected)
    {
        var bytes = FillBytes(Headers(count));
        TestContext.Current.TestOutputHelper?.WriteLine($"Fill {count} headers: {bytes} B");

        Assert.Equal(expected, bytes);
    }

    [Theory]
    [InlineData(1, 0)]
    [InlineData(4, 3)]
    [InlineData(9, 0)]
    [InlineData(9, 8)]
    [InlineData(17, 16)]
    [InlineData(20, 12)]
    public void Fill_returns_false_on_zero_length_value(int count, int empty)
    {
        var items = new MessageHeader[count];
        for (var i = 0; i < count; i++)
        {
            items[i] = new MessageHeader($"h{i}", i == empty ? [] : [(byte)i]);
        }

        NanovHeaders first = default, second = default;

        Assert.False(KafkaHeaders.Fill(new MessageHeaders(items), ref first, ref second));
    }

    private static MessageHeaders Headers(int count)
    {
        var items = new MessageHeader[count];
        for (var i = 0; i < count; i++)
        {
            items[i] = new MessageHeader($"h{i}", [(byte)i, 0xFF]);
        }

        return new MessageHeaders(items);
    }

    private static long FillBytes(MessageHeaders headers)
    {
        NanovHeaders warmFirst = default, warmSecond = default;
        KafkaHeaders.Fill(headers, ref warmFirst, ref warmSecond);

        NanovHeaders first = default, second = default;
        var before = GC.GetAllocatedBytesForCurrentThread();
        var filled = KafkaHeaders.Fill(headers, ref first, ref second);
        var bytes = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.True(filled);
        Assert.Equal(headers.Count, first.Count + second.Count);
        return bytes;
    }

    private static void AssertFilled(MessageHeader[] items)
    {
        NanovHeaders first = default, second = default;

        Assert.True(KafkaHeaders.Fill(new MessageHeaders(items), ref first, ref second));

        Assert.Equal(Math.Min(items.Length, KafkaHeaders.Inline), first.Count);
        Assert.Equal(Math.Max(items.Length - KafkaHeaders.Inline, 0), second.Count);
        for (var i = 0; i < items.Length; i++)
        {
            var filled = i < KafkaHeaders.Inline ? first[i] : second[i - KafkaHeaders.Inline];
            Assert.Same(items[i].Name, filled.Name);
            Assert.True(MemoryMarshal.TryGetArray(filled.Value, out var segment));
            Assert.Same(items[i].Value, segment.Array);
            Assert.Equal(0, segment.Offset);
            Assert.Equal(items[i].Value.Length, segment.Count);
        }
    }
}
