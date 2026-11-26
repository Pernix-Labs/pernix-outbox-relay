using Pernix.OutboxRelay.Benchmarks.Infrastructure;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Destination.Http;

namespace Pernix.OutboxRelay.Benchmarks.Scenarios;

internal static class HttpOnlyScenario
{
    private const int Window = 500;
    private const int DefaultInFlight = HttpDestinationPlan.DefaultMaxRequestsInFlight;

    public static async Task RunAsync(HarnessOptions options)
    {
        if (!options.Socket)
        {
            await CellsAsync(options, "http", "msg", deadLetter: false, Backend.Loopback);
            return;
        }

        await using var child = await LoopbackProcess.StartAsync(options.Response);
        await CellsAsync(options, "http socket", "msg", deadLetter: false, Backend.Socket, child.Port);
    }

    public static Task DeadLetterAsync(HarnessOptions options) => CellsAsync(options, "http-dlq", "dlq-msg", deadLetter: true, Backend.Loopback);

    public static async Task StubAsync(HarnessOptions options)
    {
        await CellsAsync(options, "http-stub", "msg", deadLetter: false, Backend.Stub);
        await CellsAsync(options with { Urls = ["static"] }, "http-stub-dlq", "dlq-msg", deadLetter: true, Backend.Stub);
    }

    private static async Task CellsAsync(HarnessOptions options, string label, string unit, bool deadLetter, Backend backend, int port = 0)
    {
        foreach (var url in options.Urls)
        {
            if (!HttpMock.UrlKinds.Contains(url, StringComparer.Ordinal))
            {
                throw new InvalidOperationException($"{label}: --urls '{url}' is not one of {string.Join(", ", HttpMock.UrlKinds)}");
            }

            foreach (var mode in options.Modes)
            {
                foreach (var headers in options.Headers)
                {
                    var strict = mode == OrderingMode.Strict;
                    var n = options.Count ?? (strict ? 20_000 : 200_000);
                    var warm = options.Warm ?? (strict ? 5_000 : 50_000);
                    var name = $"{label} {mode} w{(strict ? 1 : Window)} h{headers} {url} {options.Response}";
                    await CellAsync(name, unit, deadLetter, backend, port, url, mode, headers, strict ? 1 : Window, n, warm, options);
                }
            }
        }
    }

    private static async Task CellAsync(
        string name, string unit, bool deadLetter, Backend backend, int port, string url, OrderingMode mode, int headers, int window, long n, long warm,
        HarnessOptions options)
    {
        var loopback = backend == Backend.Loopback ? new HttpLoopback(options.Response) : null;
        var stub = backend == Backend.Stub ? new HttpStub(options.Response) : null;
        Func<HandlerSettings, HttpMessageHandler> handlers = backend switch
        {
            Backend.Loopback => HttpMock.Loopback(loopback!),
            Backend.Stub => _ => stub!,
            _ => static settings => HttpHandlerFactory.Create(settings),
        };
        var address = HttpMock.Url(backend == Backend.Socket ? $"127.0.0.1:{port}" : HttpLoopback.Host, url);
        await using var destination = deadLetter
            ? HttpMock.CreateDeadLetter(mode, handlers, address)
            : HttpMock.Create(mode, handlers, address);
        await destination.ValidateAsync(CancellationToken.None);

        var sends = RoundUp(n, window);
        var messages = Workload.Messages(1, window, headers, deadLetter ? Workload.DeadLetter : null);
        var tasks = new ValueTask<DeliveryResult>[window];
        var connectsAfterWarm = 0;

        await RoundRunner.RunAsync(
            unit,
            async () =>
            {
                await SendAsync(destination, messages, tasks, RoundUp(warm, window), name);
                connectsAfterWarm = loopback?.Connects ?? 0;
            },
            options.Rounds,
            async (round, reported) =>
            {
                // Suspend once first so the state machine box is allocated outside the window.
                await Task.Yield();
                var start = Snapshot.Take();
                await SendAsync(destination, messages, tasks, sends, name);
                var end = Snapshot.Take();
                return Measurement.Of(name, round, reported, sends, unit, start, end);
            });

        // Retained after the run: full blocking GC, then heap and process working set.
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
        GC.WaitForPendingFinalizers();
        GC.Collect(GC.MaxGeneration, GCCollectionMode.Forced, blocking: true, compacting: true);
        Console.WriteLine(
            $"retained heap {GC.GetGCMemoryInfo(GCKind.FullBlocking).HeapSizeBytes / 1024.0:N0} KB, working set {Environment.WorkingSet / 1048576.0:N1} MB");

        if (loopback is not null)
        {
            // C6: no connection growth after warm-up, connects <= MaxRequestsInFlight.
            var connects = loopback.Connects;
            Console.WriteLine($"connects {connects} (after warm-up {connectsAfterWarm}), peak in-flight {loopback.PeakInFlight}, window {window}");
            if (connects != connectsAfterWarm || connects > Math.Min(window, DefaultInFlight))
            {
                throw new InvalidOperationException(
                    $"{name}: {connects} connects, {connectsAfterWarm} after warm-up, limit {Math.Min(window, DefaultInFlight)}: connection not reused");
            }
        }

        if (stub is not null && stub.Requests != RoundUp(warm, window) + options.Rounds * sends)
        {
            throw new InvalidOperationException($"{name}: stub saw {stub.Requests} requests");
        }
    }

    private static async Task SendAsync(IDestination destination, OutgoingMessage[] messages, ValueTask<DeliveryResult>[] tasks, long count, string name)
    {
        for (long sent = 0; sent < count; sent += messages.Length)
        {
            for (var i = 0; i < messages.Length; i++)
            {
                tasks[i] = destination.SendAsync(messages[i], CancellationToken.None);
            }

            for (var i = 0; i < messages.Length; i++)
            {
                var result = await tasks[i];
                tasks[i] = default;
                if (result is not DeliveryResult.Delivered)
                {
                    throw new InvalidOperationException($"{name}: send not delivered: {result}");
                }
            }
        }
    }

    private static long RoundUp(long count, int window) => (count + window - 1) / window * window;

    private enum Backend
    {
        Loopback,
        Socket,
        Stub,
    }
}
