using Pernix.OutboxRelay.Benchmarks.Infrastructure;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using BenchmarkDotNet.Attributes;

namespace Pernix.OutboxRelay.Benchmarks.Suites;

// Read* reread the same 500 seeded rows, Commit* remove absent ids (table unchanged), ReadEmpty on a second empty schema.
[Config(typeof(BenchConfig))]
[NeedsPostgreSql]
[WarmupCount(3)]
[IterationCount(10)]
[InvocationCount(1000)]
public class SessionBenchmarks
{
    private const int Seeded = 500;
    private const int CommitCount = 100;
    private const long AbsentId = 10_000_000;

    private static readonly ReadRequest Read500Request = new(Seeded, null, Array.Empty<long>());
    private static readonly ReadRequest Read1Request = new(1, null, Array.Empty<long>());

    private IOutboxSession session = null!;
    private IOutboxSession empty = null!;
    private List<Outcome> commit100 = null!;
    private List<Outcome> commit1 = null!;

    [Params(OrderingMode.None, OrderingMode.PerKey)]
    public OrderingMode Mode { get; set; }

    [GlobalSetup]
    public async Task SetupAsync()
    {
        var pg = await PgEnvironment.GetAsync();
        var schema = await pg.CreateSchemaAsync();
        await pg.SeedAsync(schema, 1, Seeded);
        var emptySchema = await pg.CreateSchemaAsync();
        session = await pg.OpenSessionAsync(schema, Mode);
        empty = await pg.OpenSessionAsync(emptySchema, Mode);

        commit100 = new List<Outcome>(CommitCount);
        for (var i = 0; i < CommitCount; i++)
        {
            commit100.Add(new Outcome.Remove(AbsentId + i));
        }

        commit1 = [new Outcome.Remove(AbsentId + CommitCount)];
    }

    [GlobalCleanup]
    public async Task CleanupAsync()
    {
        if (session is not null)
        {
            await session.DisposeAsync();
        }

        if (empty is not null)
        {
            await empty.DisposeAsync();
        }

        await PgEnvironment.DisposeCurrentAsync();
    }

    [Benchmark(OperationsPerInvoke = Seeded)]
    public Task<IReadOnlyList<OutboxMessage>> Read500() => session.ReadAsync(Read500Request, CancellationToken.None);

    [Benchmark]
    public Task<IReadOnlyList<OutboxMessage>> Read1() => session.ReadAsync(Read1Request, CancellationToken.None);

    [Benchmark]
    public Task<IReadOnlyList<OutboxMessage>> ReadEmpty() => empty.ReadAsync(Read500Request, CancellationToken.None);

    [Benchmark(OperationsPerInvoke = CommitCount)]
    public Task Commit100() => session.CommitAsync(commit100, CancellationToken.None);

    [Benchmark]
    public Task Commit1() => session.CommitAsync(commit1, CancellationToken.None);

    [Benchmark]
    public Task Check() => session.CheckAsync(CancellationToken.None);

    [Benchmark]
    public Task<FailedRowsSummary> Summary() => session.ReadFailedSummaryAsync(CancellationToken.None);
}
