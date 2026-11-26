using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Http;
using System.Net.Sockets;
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

    // Child-process side of the real-socket run: each accepted socket is pumped through one fake stream. Ends at stdin EOF.
    public static async Task ServeAsync(int status)
    {
        var server = new HttpLoopback(status);
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        using var stop = new CancellationTokenSource();
        Console.WriteLine($"port {((IPEndPoint)listener.LocalEndpoint).Port}");
        Console.Out.Flush();
        _ = Task.Run(async () =>
        {
            while (await Console.In.ReadLineAsync() is not null)
            {
            }

            await stop.CancelAsync();
        });

        try
        {
            while (true)
            {
                var socket = await listener.AcceptSocketAsync(stop.Token);
                socket.NoDelay = true;
                _ = PumpAsync(server, socket);
            }
        }
        catch (OperationCanceledException)
        {
        }
    }

    private static async Task PumpAsync(HttpLoopback server, Socket socket)
    {
        await using var network = new NetworkStream(socket, ownsSocket: true);
        var fake = server.Open();
        var responses = Task.Run(async () =>
        {
            var buffer = new byte[16 * 1024];
            int n;
            while ((n = await fake.ReadAsync(buffer)) > 0)
            {
                await network.WriteAsync(buffer.AsMemory(0, n));
            }
        });

        try
        {
            var buffer = new byte[16 * 1024];
            int n;
            while ((n = await network.ReadAsync(buffer)) > 0)
            {
                fake.Write(buffer.AsSpan(0, n));
            }
        }
        catch (IOException)
        {
        }
        finally
        {
            await fake.DisposeAsync();
        }

        try
        {
            await responses;
        }
        catch (IOException)
        {
        }
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

// Parent side: runs this harness as `http-server` and reads the port it listens on.
internal sealed class LoopbackProcess : IAsyncDisposable
{
    private static readonly TimeSpan StartTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan ExitTimeout = TimeSpan.FromSeconds(5);
    private readonly Process process;

    private LoopbackProcess(Process process, int port)
    {
        this.process = process;
        Port = port;
    }

    public int Port { get; }

    public static async Task<LoopbackProcess> StartAsync(int status)
    {
        var path = Environment.ProcessPath ?? throw new InvalidOperationException("http-server: no process path");
        var info = new ProcessStartInfo(path)
        {
            RedirectStandardInput = true,
            RedirectStandardOutput = true,
            UseShellExecute = false,
        };
        if (Path.GetFileNameWithoutExtension(path) == "dotnet")
        {
            info.ArgumentList.Add(typeof(LoopbackProcess).Assembly.Location);
        }

        info.ArgumentList.Add("http-server");
        info.ArgumentList.Add("--response");
        info.ArgumentList.Add(status.ToString(CultureInfo.InvariantCulture));
        var process = Process.Start(info) ?? throw new InvalidOperationException("http-server: process did not start");
        try
        {
            var line = await process.StandardOutput.ReadLineAsync().WaitAsync(StartTimeout);
            if (line is null || !line.StartsWith("port ", StringComparison.Ordinal)
                || !int.TryParse(line.AsSpan(5), NumberStyles.None, CultureInfo.InvariantCulture, out var port))
            {
                throw new InvalidOperationException($"http-server: expected 'port <n>', got '{line}'");
            }

            return new LoopbackProcess(process, port);
        }
        catch
        {
            process.Kill();
            process.Dispose();
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        process.StandardInput.Close();
        try
        {
            await process.WaitForExitAsync().WaitAsync(ExitTimeout);
        }
        catch (TimeoutException)
        {
            process.Kill();
        }

        process.Dispose();
    }
}
