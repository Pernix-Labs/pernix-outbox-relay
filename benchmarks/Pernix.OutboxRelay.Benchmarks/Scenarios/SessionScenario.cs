using Pernix.OutboxRelay.Benchmarks.Infrastructure;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Core.Delivery;

namespace Pernix.OutboxRelay.Benchmarks.Scenarios;

internal static class SessionScenario
{
    private const OrderingMode FlatMode = OrderingMode.None;
    private const int FlatStep = 100_000;
    private const int FlatBatch = 500;
    private const int FlatHeaders = 1;

    public static async Task RunAsync(HarnessOptions options)
    {
        var pg = await PgEnvironment.GetAsync();
        foreach (var mode in options.Modes)
        {
            foreach (var batch in options.Batches)
            {
                foreach (var headers in options.Headers)
                {
                    await CellAsync(pg, options, mode, batch, headers);
                }
            }
        }
    }

    public static async Task FlatAsync(HarnessOptions options)
    {
        var pg = await PgEnvironment.GetAsync();
        var total = options.Count ?? 1_000_000;
        var schema = await pg.CreateSchemaAsync();
        await using var session = await pg.OpenSessionAsync(schema, FlatMode);
        var drain = new Drain(session, FlatMode, FlatBatch);
        // AllocationGuardTests parses this line.
        Console.WriteLine($"flat session {FlatMode} b{FlatBatch} h{FlatHeaders}: heap after full GC every {FlatStep:N0} rows (fresh rows)");
        var log = new FlatLog("rows");
        long seeded = 0;
        while (seeded < total)
        {
            var step = (int)Math.Min(FlatStep, total - seeded);
            await pg.SeedAsync(schema, seeded + 1, step, FlatHeaders);
            await drain.RunAsync(seeded + 1, step);
            seeded += step;
            log.Checkpoint(seeded);
        }

        log.PrintTrend();
    }

    private static async Task CellAsync(PgEnvironment pg, HarnessOptions options, OrderingMode mode, int batch, int headers)
    {
        var (count, warm) = Counts(options, batch);
        var schema = await pg.CreateSchemaAsync();
        await using var session = await pg.OpenSessionAsync(schema, mode);
        var drain = new Drain(session, mode, batch);
        var name = $"session {mode} batch {batch} h{headers}";
        long next = 1;

        // Seeding happens outside every window: warm-up seeds round 1, each round seeds the next.
        await RoundRunner.RunAsync(
            "row",
            async () =>
            {
                if (warm > 0)
                {
                    await pg.SeedAsync(schema, next, warm, headers);
                    await drain.RunAsync(next, warm);
                    next += warm;
                }

                await pg.SeedAsync(schema, next, count, headers);
            },
            options.Rounds,
            async (round, reported) =>
            {
                var result = await drain.RunAsync(next, count);
                next += count;
                if (!reported)
                {
                    await pg.SeedAsync(schema, next, count, headers);
                }

                return Measurement.Of($"{name} (read {result.Read / (double)count:N0}, commit {result.Commit / (double)count:N0})", round, reported, count, "row", result.Start, result.End)
                    with { BytesPerUnit = (result.Read + result.Commit) / (double)count };
            });
    }

    private static (int Count, int Warm) Counts(HarnessOptions options, int batch)
    {
        var (count, warm) = batch switch
        {
            >= 100 => (100_000, 50_000),
            >= 10 => (10_000, 5_000),
            _ => (5_000, 2_000),
        };

        return (checked((int)(options.Count ?? count)), checked((int)(options.Warm ?? warm)));
    }

    private sealed class Drain(IOutboxSession session, OrderingMode mode, int batch)
    {
        private readonly List<Outcome> group = new(DeliveryTerm.CommitGroupMax);
        private readonly ReadRequest noneRequest = new(batch, null, Array.Empty<long>());
        private OutboxPosition? after;

        public async Task<DrainResult> RunAsync(long firstId, int rows)
        {
            // Fresh schema and contiguous seeds: ids are firstId .. firstId + rows - 1.
            var outcomes = new Outcome[rows];
            for (var i = 0; i < rows; i++)
            {
                outcomes[i] = new Outcome.Remove(firstId + i);
            }

            long read = 0;
            long commit = 0;
            var count = 0;
            var start = Snapshot.Take();
            while (true)
            {
                var request = mode == OrderingMode.None ? noneRequest : new ReadRequest(batch, after, Array.Empty<long>());
                var a0 = GC.GetTotalAllocatedBytes(precise: true);
                var page = await session.ReadAsync(request, CancellationToken.None);
                read += GC.GetTotalAllocatedBytes(precise: true) - a0;
                if (page.Count == 0)
                {
                    break;
                }

                count += page.Count;
                after = page[page.Count - 1].Position;
                for (var i = 0; i < page.Count; i += DeliveryTerm.CommitGroupMax)
                {
                    group.Clear();
                    var end = Math.Min(page.Count, i + DeliveryTerm.CommitGroupMax);
                    for (var j = i; j < end; j++)
                    {
                        var index = page[j].Id - firstId;
                        if ((ulong)index >= (ulong)rows)
                        {
                            throw new InvalidOperationException($"session drain read id {page[j].Id}, expected {firstId}..{firstId + rows - 1}");
                        }

                        group.Add(outcomes[index]);
                    }

                    var c0 = GC.GetTotalAllocatedBytes(precise: true);
                    await session.CommitAsync(group, CancellationToken.None);
                    commit += GC.GetTotalAllocatedBytes(precise: true) - c0;
                }
            }

            var stop = Snapshot.Take();

            if (count != rows)
            {
                throw new InvalidOperationException($"session drain read {count} of {rows} rows");
            }

            return new DrainResult(read, commit, start, stop);
        }
    }

    private readonly record struct DrainResult(long Read, long Commit, Snapshot Start, Snapshot End);
}

internal sealed class FlatLog
{
    private long first = -1;
    private long last;
    private long max;

    public FlatLog(string unit)
    {
        // AllocationGuardTests parses this line.
        Console.WriteLine($"| {unit} | heap after full GC (KB) | committed (KB) |");
        // AllocationGuardTests parses this line.
        Console.WriteLine("|---:|---:|---:|");
    }

    public void Checkpoint(long count)
    {
        var heap = GC.GetTotalMemory(forceFullCollection: true) / 1024;
        var committed = GC.GetGCMemoryInfo().TotalCommittedBytes / 1024;
        // AllocationGuardTests parses this line.
        Console.WriteLine($"| {count:N0} | {heap:N0} | {committed:N0} |");
        if (first < 0)
        {
            first = heap;
        }

        last = heap;
        max = Math.Max(max, heap);
    }

    public void PrintTrend() => Console.WriteLine($"trend: first {first:N0} KB, last {last:N0} KB, max {max:N0} KB");
}
