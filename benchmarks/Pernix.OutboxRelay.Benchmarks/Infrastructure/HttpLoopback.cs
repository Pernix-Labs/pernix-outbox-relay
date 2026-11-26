using System.Net.Http;
using System.Text;
using System.Threading.Tasks.Sources;

namespace Pernix.OutboxRelay.Benchmarks.Infrastructure;

// In-memory HTTP/1.1 server for SocketsHttpHandler.ConnectCallback, allocation-free after warm-up.
internal sealed class HttpLoopback
{
    public const string Host = "loopback.invalid";

    private static long requestIds;

    private readonly byte[] response;
    private readonly int requestIdOffset;
    private int connects;
    private int inFlight;
    private int peakInFlight;

    public HttpLoopback(int status)
    {
        (response, requestIdOffset) = status switch
        {
            200 => Ok(),
            204 => NoContent(),
            _ => throw new ArgumentOutOfRangeException(nameof(status), status, "loopback answers 200 or 204"),
        };
    }

    public int Connects => Volatile.Read(ref connects);

    public int PeakInFlight => Volatile.Read(ref peakInFlight);

    public ValueTask<Stream> ConnectAsync(SocketsHttpConnectionContext context, CancellationToken cancellationToken) =>
        new(Open());

    public LoopbackStream Open()
    {
        Interlocked.Increment(ref connects);
        return new LoopbackStream(this);
    }

    private void Started()
    {
        var now = Interlocked.Increment(ref inFlight);
        var peak = Volatile.Read(ref peakInFlight);
        while (now > peak)
        {
            var seen = Interlocked.CompareExchange(ref peakInFlight, now, peak);
            if (seen == peak)
            {
                break;
            }

            peak = seen;
        }
    }

    private void Finished() => Interlocked.Decrement(ref inFlight);

    private static (byte[], int) Ok()
    {
        var body = "{\"id\":\"0000000000000000\",\"status\":\"accepted\",\"detail\":\"queued for processing\",\"padding\":\"xxxxxxxxx\"}";
        if (body.Length != 100)
        {
            throw new InvalidOperationException($"loopback body is {body.Length} B, expected 100");
        }

        const string Head =
            "HTTP/1.1 200 OK\r\n" +
            "Date: Thu, 26 Nov 2026 00:00:00 GMT\r\n" +
            "Content-Type: application/json\r\n" +
            "Content-Length: 100\r\n" +
            "Server: loopback\r\n" +
            "X-Request-Id: ";
        var text = Head + "0000000000000000\r\nCache-Control: no-store\r\n\r\n" + body;
        return (Encoding.ASCII.GetBytes(text), Head.Length);
    }

    private static (byte[], int) NoContent() =>
        (Encoding.ASCII.GetBytes("HTTP/1.1 204 No Content\r\nDate: Thu, 26 Nov 2026 00:00:00 GMT\r\nServer: loopback\r\n\r\n"), -1);

    internal sealed class LoopbackStream : Stream, IValueTaskSource<int>
    {
        private const int HeaderCap = 16 * 1024;
        private static ReadOnlySpan<byte> ContentLength => "content-length:"u8;

        private readonly HttpLoopback server;
        private readonly byte[] response;
        private readonly byte[] head = new byte[HeaderCap];
        private readonly Lock gate = new();
        private ManualResetValueTaskSourceCore<int> reader = new() { RunContinuationsAsynchronously = true };
        private int headLength;
        private long bodyLeft = -1;
        private bool requestOpen;
        private int pendingResponses;
        private int responseOffset;
        private Memory<byte> readBuffer;
        private bool readPending;
        private bool closed;

        public LoopbackStream(HttpLoopback server)
        {
            this.server = server;
            response = (byte[])server.response.Clone();
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
            var completed = false;
            lock (gate)
            {
                ObjectDisposedException.ThrowIf(closed, this);
                while (!buffer.IsEmpty)
                {
                    buffer = Consume(buffer, ref completed);
                }

                completed &= readPending && pendingResponses > 0;
                if (completed)
                {
                    readPending = false;
                    var n = CopyResponse(readBuffer.Span);
                    readBuffer = default;
                    reader.SetResult(n);
                }
            }
        }

        public override ValueTask<int> ReadAsync(Memory<byte> buffer, CancellationToken cancellationToken = default)
        {
            lock (gate)
            {
                if (pendingResponses > 0)
                {
                    return new ValueTask<int>(CopyResponse(buffer.Span));
                }

                if (closed || buffer.IsEmpty)
                {
                    return new ValueTask<int>(0);
                }

                if (readPending)
                {
                    throw new InvalidOperationException("loopback: concurrent reads");
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
                if (pendingResponses == 0)
                {
                    throw new InvalidOperationException("loopback: synchronous read with no response pending");
                }

                return CopyResponse(buffer);
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
                if (requestOpen || pendingResponses > 0)
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

        private ReadOnlySpan<byte> Consume(ReadOnlySpan<byte> data, scoped ref bool completed)
        {
            if (!requestOpen)
            {
                requestOpen = true;
                server.Started();
            }

            if (bodyLeft < 0)
            {
                var take = Math.Min(data.Length, HeaderCap - headLength);
                if (take == 0)
                {
                    throw new InvalidOperationException("loopback: request headers over 16 KB");
                }

                // Scan from 3 bytes back so a terminator split across writes is found.
                var from = Math.Max(0, headLength - 3);
                data[..take].CopyTo(head.AsSpan(headLength));
                headLength += take;
                var end = head.AsSpan(from, headLength - from).IndexOf("\r\n\r\n"u8);
                if (end < 0)
                {
                    return data[take..];
                }

                var headEnd = from + end + 4;
                bodyLeft = ParseContentLength(head.AsSpan(0, headEnd));
                var overshoot = headLength - headEnd;
                headLength = 0;
                data = data[(take - overshoot)..];
            }

            var body = (int)Math.Min(bodyLeft, data.Length);
            bodyLeft -= body;
            data = data[body..];
            if (bodyLeft == 0)
            {
                bodyLeft = -1;
                requestOpen = false;
                pendingResponses++;
                completed = true;
            }

            return data;
        }

        private int CopyResponse(Span<byte> destination)
        {
            if (responseOffset == 0 && server.requestIdOffset >= 0)
            {
                var id = (ulong)Interlocked.Increment(ref requestIds);
                var digits = response.AsSpan(server.requestIdOffset, 16);
                for (var i = 15; i >= 0; i--)
                {
                    digits[i] = (byte)"0123456789abcdef"[(int)(id & 0xF)];
                    id >>= 4;
                }
            }

            var n = Math.Min(destination.Length, response.Length - responseOffset);
            response.AsSpan(responseOffset, n).CopyTo(destination);
            responseOffset += n;
            if (responseOffset == response.Length)
            {
                responseOffset = 0;
                pendingResponses--;
                server.Finished();
            }

            return n;
        }

        private static long ParseContentLength(ReadOnlySpan<byte> headers)
        {
            while (!headers.IsEmpty)
            {
                var eol = headers.IndexOf("\r\n"u8);
                var line = eol < 0 ? headers : headers[..eol];
                headers = eol < 0 ? default : headers[(eol + 2)..];
                if (line.Length <= ContentLength.Length || !Ascii.EqualsIgnoreCase(line[..ContentLength.Length], ContentLength))
                {
                    continue;
                }

                var value = line[ContentLength.Length..].Trim((byte)' ');
                long length = 0;
                foreach (var b in value)
                {
                    length = checked((length * 10) + (b - '0'));
                }

                return length;
            }

            return 0;
        }
    }
}
