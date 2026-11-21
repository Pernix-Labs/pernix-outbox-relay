using Pernix.OutboxRelay.Benchmarks.Infrastructure;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;

namespace Pernix.OutboxRelay.Benchmarks.Scenarios;

internal static class FixedCostScenario
{
    private const int ReadRows = 1_000;
    private const long AbsentId = 10_000_000;

    private enum Op
    {
        Read,
        Commit,
        Check,
        Summary,
        Stats,
    }

    public static async Task RunAsync(HarnessOptions options)
    {
        var pg = await PgEnvironment.GetAsync();
        foreach (var mode in options.Modes)
        {
            await ModeAsync(pg, options.Rounds, mode);
        }
    }

    private static async Task ModeAsync(PgEnvironment pg, int rounds, OrderingMode mode)
    {
        var schema = await pg.CreateSchemaAsync();
        // One validated store per table (same-table check in PostgreSqlOutboxStoreProvider).
        var store = await pg.ValidatedStoreAsync(schema, mode);
        await using var session = await store.OpenSessionAsync(CancellationToken.None);
        if (!await session.TryLockAsync(CancellationToken.None))
        {
            throw new InvalidOperationException($"fixed {mode}: session did not get the lock");
        }

        await pg.SeedAsync(schema, 1, 50_000);
        await WarmDrainAsync(session, mode, 500, 50_000);
        await pg.SeedAsync(schema, 1, 3_000);
        await WarmDrainAsync(session, mode, 1, 3_000);
        await pg.SeedAsync(schema, 1, ReadRows);

        foreach (var n in (int[])[1, 10, 100, 500, 1_000])
        {
            await GroupAsync(new Call($"{mode} read {n}", Op.Read, session, store, rounds, 100, 200) { Request = Read(n), Rows = n });
        }

        foreach (var n in (int[])[1, 10, 100])
        {
            var outcomes = new Outcome[n];
            for (var i = 0; i < n; i++)
            {
                outcomes[i] = new Outcome.Remove(AbsentId + i);
            }

            await GroupAsync(new Call($"{mode} commit-remove {n}", Op.Commit, session, store, rounds, 20, 500) { Outcomes = outcomes, Rows = n });
        }

        var fails = new Outcome[10];
        for (var i = 0; i < fails.Length; i++)
        {
            fails[i] = new Outcome.Fail(AbsentId + i) { ReadRetryCount = 0, RetryCount = 1, Delay = TimeSpan.FromSeconds(10), Error = "boom" };
        }

        await GroupAsync(new Call($"{mode} commit-fail (tx) 10", Op.Commit, session, store, rounds, 20, 200) { Outcomes = fails, Rows = fails.Length });

        // PerKey and Strict sessions refuse a non-empty exclude list.
        if (mode == OrderingMode.None)
        {
            foreach (var n in (int[])[0, 500, 5_000])
            {
                var exclude = new long[n];
                for (var i = 0; i < n; i++)
                {
                    exclude[i] = AbsentId + i;
                }

                await GroupAsync(new Call($"{mode} read 500 exclude {n}", Op.Read, session, store, rounds, 10, 100)
                {
                    Request = new ReadRequest(500, null, exclude),
                    Rows = 500,
                });
            }
        }

        await pg.ExecuteAsync($"DELETE FROM {schema}.outbox");
        await GroupAsync(new Call($"{mode} read-empty", Op.Read, session, store, rounds, 300, 1_000) { Request = Read(500), Rows = 0 });
        await GroupAsync(new Call($"{mode} check", Op.Check, session, store, rounds, 300, 1_000));
        await GroupAsync(new Call($"{mode} summary", Op.Summary, session, store, rounds, 300, 1_000));
        await GroupAsync(new Call($"{mode} stats", Op.Stats, session, store, rounds, 300, 1_000));
    }

    private static ReadRequest Read(int max) => new(max, null, Array.Empty<long>());

    private static Task<Measurement> GroupAsync(Call call)
    {
        // AllocationGuardTests parses this line.
        Console.WriteLine($"fixed {call.Name} warm {call.Warm}: {call.Calls:N0} calls per round");
        return RoundRunner.RunAsync(
            "call",
            () => InvokeAsync(call, call.Warm),
            call.Rounds,
            async (round, reported) =>
            {
                var start = Snapshot.Take();
                await InvokeAsync(call, call.Calls);
                var end = Snapshot.Take();
                var measurement = Measurement.Of(call.Name, round, reported, call.Calls, "call", start, end);
                return call.Op == Op.Read && call.Rows > 0
                    ? measurement with { Scenario = $"{call.Name} (B/row {measurement.BytesPerUnit / call.Rows:N0})" }
                    : measurement;
            });
    }

    private static async Task InvokeAsync(Call call, int calls)
    {
        var session = call.Session;
        switch (call.Op)
        {
            case Op.Read:
                var request = call.Request!;
                for (var i = 0; i < calls; i++)
                {
                    var rows = await session.ReadAsync(request, CancellationToken.None);
                    if (rows.Count != call.Rows)
                    {
                        throw new InvalidOperationException($"{call.Name}: read {rows.Count} rows, expected {call.Rows}");
                    }
                }

                break;
            case Op.Commit:
                var outcomes = call.Outcomes!;
                for (var i = 0; i < calls; i++)
                {
                    await session.CommitAsync(outcomes, CancellationToken.None);
                }

                break;
            case Op.Check:
                for (var i = 0; i < calls; i++)
                {
                    await session.CheckAsync(CancellationToken.None);
                }

                break;
            case Op.Summary:
                for (var i = 0; i < calls; i++)
                {
                    await session.ReadFailedSummaryAsync(CancellationToken.None);
                }

                break;
            case Op.Stats:
                for (var i = 0; i < calls; i++)
                {
                    await call.Store.GetStatsAsync(CancellationToken.None);
                }

                break;
            default:
                throw new InvalidOperationException($"unknown op {call.Op}");
        }
    }

    private static async Task WarmDrainAsync(IOutboxSession session, OrderingMode mode, int batch, int rows)
    {
        OutboxPosition? after = null;
        var drained = 0;
        while (true)
        {
            var page = await session.ReadAsync(new ReadRequest(batch, mode == OrderingMode.None ? null : after, Array.Empty<long>()), CancellationToken.None);
            if (page.Count == 0)
            {
                break;
            }

            drained += page.Count;
            after = page[page.Count - 1].Position;
            var outcomes = new Outcome[page.Count];
            for (var i = 0; i < page.Count; i++)
            {
                outcomes[i] = new Outcome.Remove(page[i].Id);
            }

            await session.CommitAsync(outcomes, CancellationToken.None);
        }

        if (drained != rows)
        {
            throw new InvalidOperationException($"fixed warm-up drained {drained} of {rows} rows");
        }
    }

    private sealed record Call(string Name, Op Op, IOutboxSession Session, IValidatedOutboxStore Store, int Rounds, int Warm, int Calls)
    {
        public ReadRequest? Request { get; init; }

        public IReadOnlyList<Outcome>? Outcomes { get; init; }

        public int Rows { get; init; }
    }
}
