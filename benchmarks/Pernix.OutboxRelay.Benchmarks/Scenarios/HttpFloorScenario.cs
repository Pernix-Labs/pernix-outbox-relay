using System.Buffers.Binary;
using System.Globalization;
using System.Net;
using System.Runtime.CompilerServices;
using System.Text;
using Pernix.OutboxRelay.Benchmarks.Infrastructure;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;

namespace Pernix.OutboxRelay.Benchmarks.Scenarios;

internal static class HttpFloorScenario
{
    private const int Window = 500;
    private const int LoopbackLimit = 8;
    private const string ContentType = "application/json";
    private static readonly TimeSpan SendTimeout = TimeSpan.FromSeconds(10);
    private static readonly Uri StaticUri = new($"http://{HttpLoopback.Host}/orders");

    public static async Task LoopbackAsync(HarnessOptions options)
    {
        var server = new HttpLoopback(options.Response, options.Http2);
        await using var stream = server.Open();
        var request = Encoding.ASCII.GetBytes(
            $"POST /orders HTTP/1.1\r\nHost: {HttpLoopback.Host}\r\nidempotency-key: 123456\r\n" +
            $"Content-Type: {ContentType}\r\nContent-Length: {Workload.Payload.Length}\r\n\r\n");
        var h2 = options.Http2 ? new H2Exchange(options.Response) : null;
        var buffer = new byte[4096];
        var n = options.Count ?? 1_000_000;
        var warm = options.Warm ?? 100_000;
        var name = $"loopback{(options.Http2 ? " h2" : string.Empty)} {options.Response}";
        Func<long, Task> exchange = h2 is null
            ? count => ExchangeAsync(stream, request, buffer, count)
            : count => h2.RunAsync(stream, buffer, count);

        var last = await RoundRunner.RunAsync(
            "req",
            () => exchange(warm),
            options.Rounds,
            async (round, reported) =>
            {
                await Task.Yield();
                var start = Snapshot.Take();
                await exchange(n);
                var end = Snapshot.Take();
                return Measurement.Of(name, round, reported, n, "req", start, end);
            });

        if (last.BytesPerUnit > LoopbackLimit)
        {
            throw new InvalidOperationException(string.Create(
                CultureInfo.InvariantCulture,
                $"loopback allocates {last.BytesPerUnit:F1} B/req, limit {LoopbackLimit}: Http cells would include fake bytes"));
        }
    }

    public static async Task RunAsync(HarnessOptions options)
    {
        foreach (var url in options.Urls)
        {
            foreach (var mode in options.Modes)
            {
                foreach (var headers in options.Headers)
                {
                    var strict = mode == OrderingMode.Strict;
                    var n = options.Count ?? (strict ? 20_000 : 200_000);
                    var warm = options.Warm ?? (strict ? 5_000 : 50_000);
                    await CellAsync(url, mode, headers, strict ? 1 : Window, n, warm, options);
                }
            }
        }
    }

    private static async Task CellAsync(string url, OrderingMode mode, int headers, int window, long n, long warm, HarnessOptions options)
    {
        var name = $"http-floor {mode} w{window} h{headers} {url} {options.Response}";
        var sends = RoundUp(n, window);
        var server = new HttpLoopback(options.Response);
        using var sender = new Sender(server, window, url, options.Response);
        var messages = Workload.Messages(1, window, headers);
        var tasks = new ValueTask<HttpStatusCode>[window];
        var connectsAfterWarm = 0;

        await RoundRunner.RunAsync(
            "req",
            async () =>
            {
                await SendAllAsync(sender, messages, tasks, RoundUp(warm, window));
                connectsAfterWarm = server.Connects;
            },
            options.Rounds,
            async (round, reported) =>
            {
                await Task.Yield();
                var start = Snapshot.Take();
                await SendAllAsync(sender, messages, tasks, sends);
                var end = Snapshot.Take();
                return Measurement.Of(name, round, reported, sends, "req", start, end);
            });

        // Pool may open a connection for a queued request another connection serves: connects >= fake's peak.
        var connects = server.Connects;
        Console.WriteLine($"connects {connects} (after warm-up {connectsAfterWarm}), peak in-flight {server.PeakInFlight}, window {window}");
        if (connects != connectsAfterWarm || connects > window)
        {
            throw new InvalidOperationException(
                $"{name}: {connects} connects, {connectsAfterWarm} after warm-up, window {window}: connection not reused");
        }
    }

    private static async Task SendAllAsync(Sender sender, OutgoingMessage[] messages, ValueTask<HttpStatusCode>[] tasks, long count)
    {
        for (long sent = 0; sent < count; sent += messages.Length)
        {
            for (var i = 0; i < messages.Length; i++)
            {
                tasks[i] = sender.SendAsync(messages[i]);
            }

            for (var i = 0; i < messages.Length; i++)
            {
                var status = await tasks[i];
                tasks[i] = default;
                if (status != sender.Expected)
                {
                    throw new InvalidOperationException($"http-floor send answered {(int)status}, expected {(int)sender.Expected}");
                }
            }
        }
    }

    private sealed class H2Exchange
    {
        private const int FrameHeader = 9;
        private static readonly byte[] Preface = [.. "PRI * HTTP/2.0\r\n\r\nSM\r\n\r\n"u8, 0, 0, 0, 0x4, 0, 0, 0, 0, 0];

        private readonly byte[] request;
        private readonly int dataStreamIdOffset;
        private readonly int responseLength;
        private bool started;
        private int streamId = 1;

        public H2Exchange(int status)
        {
            // :method POST, :scheme http, :path /, content-type literal: the fake does not decode HPACK.
            byte[] block = [0x83, 0x86, 0x84, 0x0f, 0x10, (byte)ContentType.Length, .. Encoding.ASCII.GetBytes(ContentType)];
            var body = Workload.Payload.ToArray();
            request = new byte[FrameHeader + block.Length + FrameHeader + body.Length];
            Frame(request, block.Length, 0x1, 0x4);
            block.CopyTo(request, FrameHeader);
            dataStreamIdOffset = FrameHeader + block.Length;
            Frame(request.AsSpan(dataStreamIdOffset), body.Length, 0x0, 0x1);
            body.CopyTo(request, dataStreamIdOffset + FrameHeader);
            dataStreamIdOffset += 5;

            // Connection WINDOW_UPDATE (13 B) plus the response frames, for a non-empty body.
            responseLength = 13 + Http2LoopbackStream.Build(status, "{}".PadRight(100)).Bytes.Length;
        }

        public async Task RunAsync(Stream stream, byte[] buffer, long count)
        {
            if (!started)
            {
                started = true;
                var read = stream.ReadAsync(buffer);
                await stream.WriteAsync(Preface);

                await ReadExactlyAsync(stream, buffer, read, 24);
            }

            for (long i = 0; i < count; i++)
            {
                BinaryPrimitives.WriteInt32BigEndian(request.AsSpan(5), streamId);
                BinaryPrimitives.WriteInt32BigEndian(request.AsSpan(dataStreamIdOffset), streamId);
                streamId += 2;
                var read = stream.ReadAsync(buffer);
                await stream.WriteAsync(request);
                await ReadExactlyAsync(stream, buffer, read, responseLength);
            }
        }

        private static void Frame(Span<byte> destination, int length, byte type, byte flags)
        {
            destination[0] = (byte)(length >> 16);
            destination[1] = (byte)(length >> 8);
            destination[2] = (byte)length;
            destination[3] = type;
            destination[4] = flags;
        }

        private static async ValueTask ReadExactlyAsync(Stream stream, byte[] buffer, ValueTask<int> first, int expected)
        {
            var got = await first;
            while (got < expected)
            {
                var n = got == 0 ? 0 : await stream.ReadAsync(buffer);
                if (n == 0)
                {
                    throw new InvalidOperationException("loopback h2: no response");
                }

                got += n;
            }

            if (got != expected)
            {
                throw new InvalidOperationException($"loopback h2: read {got} B, expected {expected}");
            }
        }
    }

    private static async Task ExchangeAsync(Stream stream, byte[] request, byte[] buffer, long count)
    {
        var body = Workload.Payload;
        for (long i = 0; i < count; i++)
        {
            // Read posted first, like the handler's idle read-ahead: completes through the pending-read path.
            var read = stream.ReadAsync(buffer);
            await stream.WriteAsync(request);
            await stream.WriteAsync(body);
            if (await read == 0)
            {
                throw new InvalidOperationException("loopback: no response");
            }
        }
    }

    private static long RoundUp(long count, int window) => (count + window - 1) / window * window;

    private sealed class Sender : IDisposable
    {
        private readonly HttpMessageInvoker invoker;
        private readonly Stack<CancellationTokenSource> deadlines = new();
        private readonly Lock gate = new();
        private readonly string url;
        private readonly Dictionary<string, string> values = new(StringComparer.Ordinal);

        [ThreadStatic]
        private static char[]? urlBuffer;

        public Sender(HttpLoopback server, int window, string url, int status)
        {
            this.url = url;
            Expected = (HttpStatusCode)status;

            var handler = new SocketsHttpHandler
            {
                ConnectCallback = server.ConnectAsync,
                AllowAutoRedirect = false,
                UseCookies = false,
                AutomaticDecompression = DecompressionMethods.None,
                ActivityHeadersPropagator = null,
                PreAuthenticate = false,
                Credentials = null,
                MaxConnectionsPerServer = window,
                PooledConnectionLifetime = TimeSpan.FromMinutes(5),
                PooledConnectionIdleTimeout = TimeSpan.FromSeconds(90),
                ConnectTimeout = TimeSpan.FromSeconds(3),
                MaxResponseDrainSize = 64 * 1024,
                ResponseDrainTimeout = TimeSpan.FromSeconds(2),
            };
            invoker = new HttpMessageInvoker(handler, disposeHandler: true);
        }

        public HttpStatusCode Expected { get; }

        [AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]
        public async ValueTask<HttpStatusCode> SendAsync(OutgoingMessage message)
        {
            var deadline = Rent();
            try
            {
                deadline.CancelAfter(SendTimeout);
                using var request = new HttpRequestMessage(HttpMethod.Post, Target(message))
                {
                    Version = HttpVersion.Version11,
                    VersionPolicy = HttpVersionPolicy.RequestVersionOrLower,
                    Content = new ReadOnlyMemoryContent(message.Payload),
                };
                request.Content.Headers.TryAddWithoutValidation("Content-Type", ContentType);
                var headers = message.Headers;
                for (var i = 0; i < headers.Count; i++)
                {
                    request.Headers.TryAddWithoutValidation(headers[i].Name, Value(headers[i]));
                }

                using var response = await invoker.SendAsync(request, deadline.Token).ConfigureAwait(false);
                return response.StatusCode;
            }
            finally
            {
                Return(deadline);
            }
        }

        public void Dispose()
        {
            invoker.Dispose();
            foreach (var deadline in deadlines)
            {
                deadline.Dispose();
            }
        }

        private Uri Target(OutgoingMessage message)
        {
            if (url == "static")
            {
                return StaticUri;
            }

            var buffer = urlBuffer ??= new char[256];
            const string Prefix = $"http://{HttpLoopback.Host}/orders/";
            Prefix.CopyTo(buffer);
            message.Source.Id.TryFormat(buffer.AsSpan(Prefix.Length), out var written, provider: CultureInfo.InvariantCulture);
            var text = new string(buffer, 0, Prefix.Length + written);
            return url == "tpl-raw"
                ? new Uri(text, new UriCreationOptions { DangerousDisablePathAndQueryCanonicalization = true })
                : new Uri(text);
        }

        // Stands in for the slot HeaderValueCache (O5).
        private string Value(MessageHeader header)
        {
            lock (gate)
            {
                if (!values.TryGetValue(header.Name, out var value))
                {
                    value = Encoding.UTF8.GetString(header.Value);
                    values[header.Name] = value;
                }

                return value;
            }
        }

        private CancellationTokenSource Rent()
        {
            lock (gate)
            {
                return deadlines.TryPop(out var deadline) ? deadline : new CancellationTokenSource();
            }
        }

        private void Return(CancellationTokenSource deadline)
        {
            if (!deadline.TryReset())
            {
                deadline.Dispose();
                deadline = new CancellationTokenSource();
            }

            lock (gate)
            {
                deadlines.Push(deadline);
            }
        }
    }
}
