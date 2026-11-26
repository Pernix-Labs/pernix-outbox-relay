using Pernix.OutboxRelay.Benchmarks.Infrastructure;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Core.Delivery;

namespace Pernix.OutboxRelay.Benchmarks.Scenarios;

internal static class TermScenario
{
    private const long FlatCheckpoint = 100_000;
    private static readonly TimeSpan IdleWarmUp = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan StopTimeout = TimeSpan.FromMinutes(1);

    public static async Task RunAsync(HarnessOptions options)
    {
        foreach (var mode in options.Modes)
        {
            foreach (var dest in options.Dests)
            {
                foreach (var headers in options.Headers)
                {
                    var (n, warm) = Defaults(mode, dest);
                    await SteadyCellAsync(mode, dest, headers, options.Count ?? n, options.Warm ?? warm, options.Rounds);
                }
            }
        }
    }

    public static async Task IdleAsync(HarnessOptions options)
    {
        foreach (var mode in options.Modes)
        {
            foreach (var dest in options.Dests)
            {
                if (dest is not ("null" or "kafka" or "http"))
                {
                    throw new InvalidOperationException($"idle: destination '{dest}' not supported, use null, kafka or http");
                }

                await IdleCellAsync(mode, dest, options.Seconds, options.Rounds);
            }
        }
    }

    // Default PerKey kafka; a single --modes / --dests value overrides it.
    public static async Task FlatAsync(HarnessOptions options)
    {
        var mode = options.Modes.Length == 1 ? options.Modes[0] : OrderingMode.PerKey;
        var dest = options.Dests.Length == 1 ? options.Dests[0] : "kafka";
        var headers = options.Headers[0];
        var n = options.Count ?? 1_000_000;
        var warm = options.Warm ?? FlatCheckpoint;
        if (n < FlatCheckpoint)
        {
            throw new InvalidOperationException($"flat term: --n must be at least {FlatCheckpoint:N0}");
        }

        var name = $"flat term {mode} {dest} b{TermHost.BatchSize} h{headers}";
        var checkpoints = new long[(int)(n / FlatCheckpoint)];
        var taken = 0;
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var session = InMemorySession.Fresh(warm + n, headers);
        session.OnRemoved = count =>
        {
            var measured = count - warm;
            if (measured <= 0 || measured % FlatCheckpoint != 0)
            {
                return;
            }

            // GetTotalMemory(true) can stop after one Collect, so finalizer-held objects are collected first.
            GC.Collect();
            GC.WaitForPendingFinalizers();
            GC.Collect();
            var heap = GC.GetTotalMemory(forceFullCollection: true);
            var committed = GC.GetGCMemoryInfo().TotalCommittedBytes;
            checkpoints[taken++] = heap;
            // AllocationGuardTests parses this line.
            Console.WriteLine($"| {measured:N0} | {heap / 1024:N0} | {committed / 1024:N0} |");
            if (taken == checkpoints.Length)
            {
                done.TrySetResult();
            }
        };

        await using var cell = TermCell.Create(mode, dest, session);
        // AllocationGuardTests parses this line.
        Console.WriteLine($"{name}: heap after full GC every {FlatCheckpoint:N0} msgs (fresh rows)");
        // AllocationGuardTests parses this line.
        Console.WriteLine("| count | heap after full GC (KB) | committed (KB) |");
        // AllocationGuardTests parses this line.
        Console.WriteLine("|---:|---:|---:|");
        cell.Start(ex => done.TrySetException(ex));
        cell.Arm(WindowTimeout(warm + n), $"{name}: not every message delivered");
        await done.Task;
        await cell.StopAsync();

        var max = 0L;
        for (var i = 0; i < taken; i++)
        {
            max = Math.Max(max, checkpoints[i]);
        }

        Console.WriteLine($"trend: first {checkpoints[0] / 1024:N0} KB, last {checkpoints[taken - 1] / 1024:N0} KB, max {max / 1024:N0} KB");
    }

    private static (long N, long Warm) Defaults(OrderingMode mode, string dest) => dest switch
    {
        _ when mode == OrderingMode.Strict => (20_000, 5_000),
        "dlq" or "http-dlq" => (50_000, 10_000),
        _ => (200_000, 50_000),
    };

    // max(2 min, count / 1,000 s).
    private static TimeSpan WindowTimeout(long count) => TimeSpan.FromSeconds(Math.Max(120, count / 1_000));

    private static async Task SteadyCellAsync(OrderingMode mode, string dest, int headers, long n, long warm, int rounds)
    {
        var name = $"term {mode} {dest} b{TermHost.BatchSize} h{headers}";
        var unit = dest is "dlq" or "http-dlq" ? "dlq-msg" : "msg";
        var session = InMemorySession.Preallocated(RowBlock.Create(1, checked((int)(warm + rounds * n)), headers));
        var windows = new Windows(warm, n, rounds);
        session.OnRemoved = windows.OnRemoved;
        var failures = new string[rounds + 1];
        for (var i = 0; i < failures.Length; i++)
        {
            failures[i] = $"{name} {(i == 0 ? "warm-up" : $"round {i}")}: not every message delivered";
        }

        await using var cell = TermCell.Create(mode, dest, session);
        if (warm == 0)
        {
            windows.OnRemoved(0);
        }

        cell.Start(windows.Fail);
        var warmTimeout = WindowTimeout(warm);
        var roundTimeout = WindowTimeout(n);
        await RoundRunner.RunAsync(
            unit,
            () =>
            {
                cell.Arm(warmTimeout, failures[0]);
                return windows.Edges[0].Task;
            },
            rounds,
            async (round, reported) =>
            {
                cell.Arm(roundTimeout, failures[round]);
                await windows.Edges[round].Task;
                return Measurement.Of(name, round, reported, n, unit, windows.Snapshots[round - 1], windows.Snapshots[round]);
            });

        await cell.StopAsync();
    }

    private static async Task IdleCellAsync(OrderingMode mode, string dest, int seconds, int rounds)
    {
        var name = $"idle {mode} {dest} interval {TermHost.IdleInterval.TotalSeconds:0}s";
        var window = TimeSpan.FromSeconds(seconds);
        Exception? failure = null;
        await using var cell = TermCell.Create(mode, dest, InMemorySession.Empty(), TermHost.IdleInterval);
        cell.Start(ex => failure ??= ex);

        // AllocationGuardTests parses this line.
        Console.WriteLine($"{name} warm-up {IdleWarmUp.TotalSeconds:0}s: {seconds:N0} s per round");
        await RoundRunner.RunAsync(
            "s",
            () => Task.Delay(IdleWarmUp),
            rounds,
            async (round, reported) =>
            {
                var delay = Task.Delay(window);

                // Suspend once first so the state machine box is allocated outside the window.
                await Task.Yield();
                var start = Snapshot.Take();
                await delay;
                var end = Snapshot.Take();
                if (failure is not null)
                {
                    throw new InvalidOperationException($"{name}: term ended early", failure);
                }

                return Measurement.Of(name, round, reported, seconds, "s", start, end);
            });

        await cell.StopAsync();
    }

    // Snapshots[k] taken at Remove #(warm + k·n), so the term runs continuously across rounds.
    private sealed class Windows
    {
        private readonly long warm;
        private readonly long n;
        private long nextEdge;
        private int next;

        public Windows(long warm, long n, int rounds)
        {
            this.warm = warm;
            this.n = n;
            nextEdge = warm;
            Snapshots = new Snapshot[rounds + 1];
            Edges = new TaskCompletionSource[rounds + 1];
            for (var i = 0; i < Edges.Length; i++)
            {
                Edges[i] = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
            }
        }

        public Snapshot[] Snapshots { get; }

        public TaskCompletionSource[] Edges { get; }

        public void OnRemoved(long count)
        {
            if (count != nextEdge || next >= Edges.Length)
            {
                return;
            }

            Snapshots[next] = Snapshot.Take();
            Edges[next].TrySetResult();
            next++;
            nextEdge = warm + next * n;
        }

        public void Fail(Exception ex)
        {
            for (var i = 0; i < Edges.Length; i++)
            {
                Edges[i].TrySetException(ex);
            }
        }
    }

    // Fault and deadline wiring is set up once per cell so a measured window allocates nothing for it.
    private sealed class TermCell : IAsyncDisposable
    {
        private readonly DeliveryTerm term;
        private readonly CancellationTokenSource stopping = new();
        private readonly CancellationTokenSource deadline = new();
        private Task<DeliveryTermResult>? run;
        private string failure = string.Empty;
        private TimeSpan timeout;

        private TermCell(DeliveryTerm term, InMemorySession session)
        {
            this.term = term;
            Session = session;
        }

        public InMemorySession Session { get; }

        public static TermCell Create(OrderingMode mode, string dest, InMemorySession session, TimeSpan? interval = null)
        {
            var loopback = HttpMock.Loopback(new HttpLoopback(200));
            var staticUrl = HttpMock.Url(HttpLoopback.Host, "static");
            DeadLetterTarget? deadLetter = null;
            Func<IDestination> createDestination;
            switch (dest)
            {
                case "null":
                    createDestination = static () => new NullDestination();
                    break;
                case "kafka":
                    createDestination = () => KafkaMock.Create(mode);
                    break;
                case "dlq":
                    createDestination = static () => new PermanentFaultDestination();
                    deadLetter = new DeadLetterTarget(KafkaMock.DeadLetterName, () => KafkaMock.CreateDeadLetter(mode));
                    break;
                case "http":
                    createDestination = () => HttpMock.Create(mode, loopback, staticUrl);
                    break;
                case "http-tpl":
                    createDestination = () => HttpMock.Create(mode, loopback, HttpMock.Url(HttpLoopback.Host, "tpl"));
                    break;
                case "http-sat":
                    createDestination = () => HttpMock.Create(mode, loopback, staticUrl, HttpMock.SaturatedInFlight);
                    break;
                case "http-dlq":
                    createDestination = static () => new PermanentFaultDestination();
                    deadLetter = new DeadLetterTarget(HttpMock.DeadLetterName, () => HttpMock.CreateDeadLetter(mode, loopback, staticUrl));
                    break;
                default:
                    throw new InvalidOperationException($"term: unknown destination '{dest}'");
            }

            var term = TermHost.Create(mode, createDestination, interval ?? TermHost.SteadyInterval, deadLetter: deadLetter);
            return new TermCell(term, session);
        }

        public void Start(Action<Exception> fail)
        {
            deadline.Token.Register(() => fail(new TimeoutException($"{failure} within {timeout.TotalSeconds:N0} s")));
            run = term.RunAsync(Session, stopping.Token);
            _ = run.ContinueWith(
                t => fail(t.Exception?.GetBaseException() ?? new InvalidOperationException($"term ended early: {t.Result}")),
                CancellationToken.None,
                TaskContinuationOptions.ExecuteSynchronously,
                TaskScheduler.Default);
        }

        public void Arm(TimeSpan window, string message)
        {
            failure = message;
            timeout = window;
            deadline.CancelAfter(window);
        }

        public async Task StopAsync()
        {
            deadline.CancelAfter(System.Threading.Timeout.InfiniteTimeSpan);
            await stopping.CancelAsync();
            await run!.WaitAsync(StopTimeout);
        }

        public async ValueTask DisposeAsync()
        {
            deadline.CancelAfter(System.Threading.Timeout.InfiniteTimeSpan);
            if (run is { IsCompleted: false })
            {
                await stopping.CancelAsync();
                await Task.WhenAny(run, Task.Delay(StopTimeout));
            }

            stopping.Dispose();
            deadline.Dispose();
        }
    }
}
