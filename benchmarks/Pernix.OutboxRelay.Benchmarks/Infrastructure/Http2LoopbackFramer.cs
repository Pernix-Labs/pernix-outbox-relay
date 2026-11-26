using System.Buffers.Binary;
using System.Text;
using System.Threading.Tasks.Sources;

namespace Pernix.OutboxRelay.Benchmarks.Infrastructure;

// Prior-knowledge h2c server side of HttpLoopback: fixed buffers and preformatted frames, no HPACK encoder or decoder.
internal sealed class Http2LoopbackStream : Stream, IValueTaskSource<int>
{
    // Kestrel's default SETTINGS_MAX_CONCURRENT_STREAMS.
    public const int MaxStreams = 100;

    private const int FrameHeader = 9;
    private const int OutCap = 256 * 1024;
    private const byte DataType = 0x0;
    private const byte HeadersType = 0x1;
    private const byte SettingsType = 0x4;
    private const byte WindowUpdateType = 0x8;
    private const byte EndStream = 0x1;
    private const byte Ack = 0x1;
    private const byte EndHeaders = 0x4;

    private static ReadOnlySpan<byte> Preface => "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"u8;

    private readonly HttpLoopback server;
    private readonly Template template;
    private readonly byte[] response;
    private readonly byte[] frame = new byte[FrameHeader];
    private readonly byte[] windowUpdate = new byte[FrameHeader + 4];
    private readonly byte[] output = new byte[OutCap];
    private readonly Lock gate = new();
    private ManualResetValueTaskSourceCore<int> reader = new() { RunContinuationsAsynchronously = true };
    private int prefaceLeft = 24;
    private int frameFill;
    private int payloadLeft;
    private int outHead;
    private int outCount;
    private int openStreams;
    private Memory<byte> readBuffer;
    private bool readPending;
    private bool closed;

    public Http2LoopbackStream(HttpLoopback server, Template template)
    {
        this.server = server;
        this.template = template;
        response = (byte[])template.Bytes.Clone();
        WriteFrameHeader(windowUpdate, 4, WindowUpdateType, 0, 0);

        Span<byte> settings = stackalloc byte[FrameHeader + 6];
        WriteFrameHeader(settings, 6, SettingsType, 0, 0);
        BinaryPrimitives.WriteUInt16BigEndian(settings[FrameHeader..], 0x3);
        BinaryPrimitives.WriteUInt32BigEndian(settings[(FrameHeader + 2)..], MaxStreams);
        Append(settings);
    }

    public override bool CanRead => true;

    public override bool CanWrite => true;

    public override bool CanSeek => false;

    public override long Length => throw new NotSupportedException();

    public override long Position
    {
        get => throw new NotSupportedException();
        set => throw new NotSupportedException();
    }

    public static Template Build(int status, string body)
    {
        var block = new List<byte>();
        if (status == 200)
        {
            block.Add(0x88);
            Literal(block, "date", "Thu, 26 Nov 2026 00:00:00 GMT");
            Literal(block, "content-type", "application/json");
            Literal(block, "content-length", "100");
            Literal(block, "server", "loopback");
            var requestId = FrameHeader + Literal(block, "x-request-id", "0000000000000000");
            Literal(block, "cache-control", "no-store");
            var bodyBytes = Encoding.ASCII.GetBytes(body);
            var bytes = new byte[FrameHeader + block.Count + FrameHeader + bodyBytes.Length];
            WriteFrameHeader(bytes, block.Count, HeadersType, EndHeaders, 0);
            block.CopyTo(bytes, FrameHeader);
            var dataAt = FrameHeader + block.Count;
            WriteFrameHeader(bytes.AsSpan(dataAt), bodyBytes.Length, DataType, EndStream, 0);
            bodyBytes.CopyTo(bytes, dataAt + FrameHeader);
            return new Template(bytes, dataAt + 5, requestId);
        }

        block.Add(0x89);
        Literal(block, "date", "Thu, 26 Nov 2026 00:00:00 GMT");
        Literal(block, "server", "loopback");
        var noContent = new byte[FrameHeader + block.Count];
        WriteFrameHeader(noContent, block.Count, HeadersType, EndHeaders | EndStream, 0);
        block.CopyTo(noContent, FrameHeader);
        return new Template(noContent, -1, -1);
    }

    public override ValueTask WriteAsync(ReadOnlyMemory<byte> buffer, CancellationToken cancellationToken = default)
    {
        Write(buffer.Span);
        return ValueTask.CompletedTask;
    }

    public override Task WriteAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken)
    {
        Write(buffer.AsSpan(offset, count));
        return Task.CompletedTask;
    }

    public override void Write(byte[] buffer, int offset, int count) => Write(buffer.AsSpan(offset, count));

    public override void Write(ReadOnlySpan<byte> buffer)
    {
        lock (gate)
        {
            ObjectDisposedException.ThrowIf(closed, this);
            Consume(buffer);
            if (readPending && outCount > 0)
            {
                readPending = false;
                var n = CopyOut(readBuffer.Span);
                readBuffer = default;
                reader.SetResult(n);
            }
        }
    }

    public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
    {
        lock (gate)
        {
            if (outCount > 0)
            {
                return new ValueTask<int>(CopyOut(buffer.Span));
            }

            if (closed || buffer.IsEmpty)
            {
                return new ValueTask<int>(0);
            }

            if (readPending)
            {
                throw new InvalidOperationException("loopback h2: concurrent reads");
            }

            reader.Reset();
            readBuffer = buffer;
            readPending = true;
            return new ValueTask<int>(this, reader.Version);
        }
    }

    public override Task<int> ReadAsync(byte[] buffer, int offset, int count, CancellationToken cancellationToken) =>
        ReadAsync(buffer.AsMemory(offset, count), cancellationToken).AsTask();

    public override int Read(byte[] buffer, int offset, int count) => Read(buffer.AsSpan(offset, count));

    public override int Read(Span<byte> buffer)
    {
        lock (gate)
        {
            return outCount > 0 ? CopyOut(buffer) : throw new InvalidOperationException("loopback h2: synchronous read with nothing pending");
        }
    }

    public override void Flush()
    {
    }

    public override Task FlushAsync(CancellationToken cancellationToken) => Task.CompletedTask;

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    int IValueTaskSource<int>.GetResult(short token) => reader.GetResult(token);

    ValueTaskSourceStatus IValueTaskSource<int>.GetStatus(short token) => reader.GetStatus(token);

    void IValueTaskSource<int>.OnCompleted(Action<object?> continuation, object? state, short token, ValueTaskSourceOnCompletedFlags flags) =>
        reader.OnCompleted(continuation, state, token, flags);

    protected override void Dispose(bool disposing)
    {
        lock (gate)
        {
            if (closed)
            {
                return;
            }

            closed = true;
            for (; openStreams > 0; openStreams--)
            {
                server.Finished();
            }

            if (readPending)
            {
                readPending = false;
                readBuffer = default;
                reader.SetResult(0);
            }
        }

        base.Dispose(disposing);
    }

    private static void WriteFrameHeader(Span<byte> destination, int length, byte type, byte flags, int streamId)
    {
        destination[0] = (byte)(length >> 16);
        destination[1] = (byte)(length >> 8);
        destination[2] = (byte)length;
        destination[3] = type;
        destination[4] = flags;
        BinaryPrimitives.WriteInt32BigEndian(destination[5..], streamId);
    }

    // Literal header field without indexing, new name (RFC 7541 6.2.2). Returns the value's offset in the block.
    private static int Literal(List<byte> block, string name, string value)
    {
        block.Add(0x00);
        block.Add(checked((byte)name.Length));
        block.AddRange(Encoding.ASCII.GetBytes(name));
        block.Add(checked((byte)value.Length));
        var at = block.Count;
        block.AddRange(Encoding.ASCII.GetBytes(value));
        return at;
    }

    private void Consume(ReadOnlySpan<byte> data)
    {
        while (!data.IsEmpty)
        {
            if (prefaceLeft > 0)
            {
                var take = Math.Min(prefaceLeft, data.Length);
                if (!data[..take].SequenceEqual(Preface.Slice(Preface.Length - prefaceLeft, take)))
                {
                    throw new InvalidOperationException("loopback h2: bad client preface");
                }

                prefaceLeft -= take;
                data = data[take..];
            }
            else if (payloadLeft > 0)
            {
                var skip = Math.Min(payloadLeft, data.Length);
                payloadLeft -= skip;
                data = data[skip..];
            }
            else
            {
                var take = Math.Min(FrameHeader - frameFill, data.Length);
                data[..take].CopyTo(frame.AsSpan(frameFill));
                frameFill += take;
                data = data[take..];
                if (frameFill == FrameHeader)
                {
                    frameFill = 0;
                    OnFrame();
                }
            }
        }
    }

    // PRIORITY, PING, WINDOW_UPDATE, RST_STREAM, GOAWAY and CONTINUATION are skipped.
    private void OnFrame()
    {
        var length = (frame[0] << 16) | (frame[1] << 8) | frame[2];
        var type = frame[3];
        var flags = frame[4];
        var streamId = BinaryPrimitives.ReadInt32BigEndian(frame.AsSpan(5)) & int.MaxValue;
        payloadLeft = length;
        switch (type)
        {
            case SettingsType when (flags & Ack) == 0:
                Span<byte> ack = stackalloc byte[FrameHeader];
                WriteFrameHeader(ack, 0, SettingsType, Ack, 0);
                Append(ack);
                break;
            case HeadersType:
                openStreams++;
                server.Started();
                if ((flags & EndStream) != 0)
                {
                    Respond(streamId);
                }

                break;
            case DataType:
                if (length > 0)
                {
                    WindowUpdate(0, length);
                    if ((flags & EndStream) == 0)
                    {
                        WindowUpdate(streamId, length);
                    }
                }

                if ((flags & EndStream) != 0)
                {
                    Respond(streamId);
                }

                break;
        }
    }

    private void WindowUpdate(int streamId, int increment)
    {
        BinaryPrimitives.WriteInt32BigEndian(windowUpdate.AsSpan(5), streamId);
        BinaryPrimitives.WriteInt32BigEndian(windowUpdate.AsSpan(FrameHeader), increment);
        Append(windowUpdate);
    }

    private void Respond(int streamId)
    {
        BinaryPrimitives.WriteInt32BigEndian(response.AsSpan(5), streamId);
        if (template.DataStreamIdOffset >= 0)
        {
            BinaryPrimitives.WriteInt32BigEndian(response.AsSpan(template.DataStreamIdOffset), streamId);
            HttpLoopback.WriteRequestId(response.AsSpan(template.RequestIdOffset, 16));
        }

        Append(response);
        openStreams--;
        server.Finished();
    }

    private void Append(ReadOnlySpan<byte> bytes)
    {
        if (outCount + bytes.Length > OutCap)
        {
            throw new InvalidOperationException("loopback h2: output over 256 KB, client is not reading");
        }

        var tail = (outHead + outCount) % OutCap;
        var first = Math.Min(bytes.Length, OutCap - tail);
        bytes[..first].CopyTo(output.AsSpan(tail));
        bytes[first..].CopyTo(output);
        outCount += bytes.Length;
    }

    private int CopyOut(Span<byte> destination)
    {
        var n = Math.Min(destination.Length, outCount);
        var first = Math.Min(n, OutCap - outHead);
        output.AsSpan(outHead, first).CopyTo(destination);
        output.AsSpan(0, n - first).CopyTo(destination[first..]);
        outHead = (outHead + n) % OutCap;
        outCount -= n;
        return n;
    }

    internal sealed record Template(byte[] Bytes, int DataStreamIdOffset, int RequestIdOffset);
}
