using Pernix.OutboxRelay.Benchmarks.Infrastructure;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;

namespace Pernix.OutboxRelay.Benchmarks.Scenarios;

internal static class ChurnScenario
{
    private const int Quarters = 4;
    private const OrderingMode Mode = OrderingMode.PerKey;
    private const int SeededRows = 10;

    public static async Task RunAsync(HarnessOptions options)
    {
        var pg = await PgEnvironment.GetAsync();
        var total = options.Sessions;
        var schema = await pg.CreateSchemaAsync();
        await pg.SeedAsync(schema, 1, SeededRows);
        var request = new ReadRequest(1, null, Array.Empty<long>());

        // One validated store per table (same-table check in PostgreSqlOutboxStoreProvider).
        var store = await pg.ValidatedStoreAsync(schema, Mode);

        Outcome[] commit = [new Outcome.Remove(10_000_000)];

        var before = await pg.CountRelayBackendsAsync();
        Console.WriteLine($"relay backends before the first session: {before:N0}");
        // AllocationGuardTests parses this line.
        Console.WriteLine($"churn {Mode} seeded {SeededRows}: heap after full GC per quarter of {total:N0} sessions");
        // AllocationGuardTests parses this line.
        Console.WriteLine("| sessions | heap after full GC (KB) | backends | delta |");
        // AllocationGuardTests parses this line.
        Console.WriteLine("|---:|---:|---:|---:|");
        var quarter = 1;
        for (var i = 1; i <= total; i++)
        {
            await using (var session = await store.OpenSessionAsync(CancellationToken.None))
            {
                if (!await session.TryLockAsync(CancellationToken.None))
                {
                    throw new InvalidOperationException($"churn session {i} did not get the lock");
                }

                await session.CheckAsync(CancellationToken.None);
                var rows = await session.ReadAsync(request, CancellationToken.None);
                if (rows.Count != 1)
                {
                    throw new InvalidOperationException($"churn session {i} read {rows.Count} rows, expected 1");
                }

                await session.ReadFailedSummaryAsync(CancellationToken.None);
                await session.CommitAsync(commit, CancellationToken.None);
            }

            if (quarter <= Quarters && i == Checkpoint(total, quarter))
            {
                // Give closed connections time to leave pg_stat_activity.
                await Task.Delay(500);

                // GetTotalMemory(true) can stop after one Collect, so finalizer-held objects are collected first.
                GC.Collect();
                GC.WaitForPendingFinalizers();
                GC.Collect();
                var heap = GC.GetTotalMemory(forceFullCollection: true) / 1024;
                var backends = await pg.CountRelayBackendsAsync();
                // AllocationGuardTests parses this line.
                Console.WriteLine($"| {i:N0} | {heap:N0} | {backends:N0} | {backends - before:N0} |");
                quarter++;
            }
        }
    }

    private static int Checkpoint(int total, int quarter) => Math.Max(1, (int)(((long)total * quarter + Quarters - 1) / Quarters));
}
