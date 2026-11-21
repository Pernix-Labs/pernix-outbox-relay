using Pernix.OutboxRelay.Benchmarks.Infrastructure;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Core.Delivery;
using BenchmarkDotNet.Attributes;

namespace Pernix.OutboxRelay.Benchmarks.Suites;

[Config(typeof(BenchConfig))]
[WarmupCount(Warmups)]
[IterationCount(Iterations)]
[InvocationCount(Invocations)]
public class DeliveryTermBenchmarks
{
    private const int Warmups = 3;
    private const int Iterations = 10;
    private const int Invocations = 16;
    private const int PerInvocation = 5_000;
    private const int HeaderCount = 1;
    private static readonly TimeSpan StopTimeout = TimeSpan.FromMinutes(1);

    private readonly CancellationTokenSource stopping = new();
    private InMemorySession session = null!;
    private IDestination? destination;
    private Task<DeliveryTermResult>? run;

    [Params(OrderingMode.None, OrderingMode.PerKey)]
    public OrderingMode Mode { get; set; }

    [Params(TermDestination.Null, TermDestination.Kafka)]
    public TermDestination Destination { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        destination = Destination switch
        {
            TermDestination.Null => new NullDestination(),
            TermDestination.Kafka => KafkaMock.Create(Mode),
            _ => throw new ArgumentOutOfRangeException(nameof(Destination), Destination, "unknown term destination"),
        };

        var capacity = Workload.Capacity(Warmups, Iterations, Invocations, PerInvocation);
        session = InMemorySession.Gated(RowBlock.Create(1, capacity, HeaderCount), nameof(DeliveryTermBenchmarks));
        var created = destination;
        var term = TermHost.Create(Mode, () => created, TermHost.SteadyInterval);
        run = term.RunAsync(session, stopping.Token);
    }

    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        await stopping.CancelAsync();
        if (run is not null)
        {
            await run.WaitAsync(StopTimeout);
        }

        // Idempotent; covers a setup that failed before the term started.
        if (destination is not null)
        {
            await destination.DisposeAsync();
        }

        stopping.Dispose();
        await PgEnvironment.DisposeCurrentAsync();
    }

    [Benchmark(OperationsPerInvoke = PerInvocation)]
    public Task Deliver() => session.ReleaseAsync(PerInvocation);
}
