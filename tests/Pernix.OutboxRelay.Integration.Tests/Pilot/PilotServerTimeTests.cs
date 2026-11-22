using System.Diagnostics;
using System.Globalization;
using System.Text;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Pernix.OutboxRelay.Outbox.PostgreSql;
using Xunit;
using static Pernix.OutboxRelay.Integration.Tests.Pilot.PilotDatasets;

namespace Pernix.OutboxRelay.Integration.Tests.Pilot;

// Explicit. Prints figures.
public sealed class PilotServerTimeTests(PostgreSqlFixture postgres)
{
    private const int Runs = 200;
    private const int WarmUpRuns = 20;
    private const int Walks = 3;
    private const int HeldIds = 10_000;
    private const int NoneBatch = 500;
    private const double P95LimitMs = 100;
    private const double MaxLimitMs = 1_000;

    private static readonly int[] BlockedBacklogs = [10_000, 100_000, 1_000_000];
    private static readonly int[] BatchSizes = [500, 1_000];
    private static readonly int[] FailedRows = [100_000, 500_000];
    private static readonly OrderingMode[] Modes = [OrderingMode.None, OrderingMode.PerKey, OrderingMode.Strict];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact(Explicit = true)]
    public async Task M1_blocked_key_rewalk_read_cost()
    {
        var output = await HeaderAsync("M1a/b blocked-key re-walk: PerKeyReadSql(cursor: true) per read, server ms");
        output.AppendLine(CultureInfo.InvariantCulture, $"Per case: 1 warm-up walk, then {Walks} measured walks pooled. Reads and rows are per walk.")
            .AppendLine("| N | BatchSize | Rows | Reads | p50 | p95 | max | Within |")
            .AppendLine("|---:|---:|---:|---:|---:|---:|---:|---|");

        string? explain = null;
        foreach (var n in BlockedBacklogs)
        {
            var schema = await CreateAsync(postgres);
            try
            {
                await SeedBlockedKeyAsync(postgres, schema, n);
                var sql = Positional(PostgreSqlOutboxSession.PerKeyReadSql($"{schema}.outbox", cursor: true), "max", "after_tx", "after_id");
                foreach (var batch in BatchSizes)
                {
                    await WalkAsync(postgres, schema, sql, batch, walks: 1);
                    var walk = await WalkAsync(postgres, schema, sql, batch, Walks);
                    output.AppendLine(CultureInfo.InvariantCulture, $"| {n:N0} | {batch} | {walk.Rows:N0} | {walk.Reads:N0} | {Row(walk.Timing)} |");
                }

                if (n == BlockedBacklogs[^1])
                {
                    explain = await ExplainAsync(postgres, sql, "int, text, bigint", $"{BatchSizes[^1]}, '0', 0");
                }
            }
            finally
            {
                await DropAsync(postgres, schema);
            }
        }

        Print(output, $"N = {BlockedBacklogs[^1]:N0}, BatchSize {BatchSizes[^1]}, first read from the head", explain);
    }

    [Fact(Explicit = true)]
    public async Task M2_failed_rows_summary()
    {
        var output = await HeaderAsync("M2 failed-rows summary: SummarySql server ms, and ReadFailedSummaryAsync round trip on a real session (client ms)");
        output.AppendLine(CultureInfo.InvariantCulture, $"{Runs} runs each, round trip after {WarmUpRuns} warm-up calls. Plus {FreshRows:N0} fresh rows.")
            .AppendLine("| Failed rows | State | Measure | p50 | p95 | max | Within |")
            .AppendLine("|---:|---|---|---:|---:|---:|---|");

        string? explain = null;
        foreach (var f in FailedRows)
        {
            var schema = await CreateAsync(postgres);
            try
            {
                await SeedFailedRowsAsync(postgres, schema, f);
                var sql = PostgreSqlOutboxSession.SummarySql($"{schema}.outbox");
                foreach (var state in new[] { "A vacuumed", "B unvacuumed" })
                {
                    if (state[0] == 'B')
                    {
                        await TouchFailedRowsAsync(postgres, schema);
                    }

                    var server = await BenchAsync(postgres, schema, sql, Runs);
                    var roundTrip = await SessionRoundTripAsync(schema);
                    output.AppendLine(CultureInfo.InvariantCulture, $"| {f:N0} | {state} | server | {Row(server)} |")
                        .AppendLine(CultureInfo.InvariantCulture, $"| {f:N0} | {state} | round trip | {Row(roundTrip)} |");
                }

                if (f == FailedRows[^1])
                {
                    explain = await ExplainAsync(postgres, sql);
                }
            }
            finally
            {
                await DropAsync(postgres, schema);
            }
        }

        Print(output, $"F = {FailedRows[^1]:N0}, state B", explain);
    }

    [Fact(Explicit = true)]
    public async Task M4_stats_with_unvacuumed_failed_rows()
    {
        var output = await HeaderAsync("M4 stats statement: BuildStatsSql(partitioned: false) server ms, per mode");
        output.AppendLine(CultureInfo.InvariantCulture, $"{Runs} runs each. Plus {FreshRows:N0} fresh rows. State A is the vacuumed baseline, state B gates.")
            .AppendLine("| Failed rows | State | Mode | p50 | p95 | max | Within |")
            .AppendLine("|---:|---|---|---:|---:|---:|---|");

        string? explain = null;
        foreach (var f in FailedRows)
        {
            var schema = await CreateAsync(postgres);
            try
            {
                await SeedFailedRowsAsync(postgres, schema, f);
                foreach (var state in new[] { "A vacuumed", "B unvacuumed" })
                {
                    if (state[0] == 'B')
                    {
                        await TouchFailedRowsAsync(postgres, schema);
                    }

                    foreach (var mode in Modes)
                    {
                        var sql = Positional(PostgreSqlValidatedOutboxStore.BuildStatsSql($"{schema}.outbox", mode, partitioned: false));
                        var timing = await BenchAsync(postgres, schema, sql, Runs);
                        output.AppendLine(CultureInfo.InvariantCulture, $"| {f:N0} | {state} | {mode} | {Row(timing)} |");
                    }
                }

                if (f == FailedRows[^1])
                {
                    explain = await ExplainAsync(
                        postgres, Positional(PostgreSqlValidatedOutboxStore.BuildStatsSql($"{schema}.outbox", OrderingMode.PerKey, partitioned: false)));
                }
            }
            finally
            {
                await DropAsync(postgres, schema);
            }
        }

        Print(output, $"F = {FailedRows[^1]:N0}, state B, PerKey", explain);
    }

    [Fact(Explicit = true)]
    public async Task M5_none_read_with_not_due_failed_rows()
    {
        var output = await HeaderAsync("M5 None read with not-due failed rows at head: NoneReadSql server ms ($2 = 500)");
        output.AppendLine(CultureInfo.InvariantCulture, $"{Runs} runs each. Plus {FreshRows:N0} due rows. Held ids = the first due rows by id. 0 held ids is the control.")
            .AppendLine("| Not-due failed rows | Held ids | p50 | p95 | max | Within |")
            .AppendLine("|---:|---:|---:|---:|---:|---|");

        string? explain = null;
        foreach (var n in FailedRows)
        {
            var schema = await CreateAsync(postgres);
            try
            {
                await SeedNotDueAsync(postgres, schema, n);
                var sql = Positional(PostgreSqlOutboxSession.NoneReadSql($"{schema}.outbox"), "exclude", "max");
                var held = await FirstDueIdsAsync(postgres, schema, HeldIds);
                foreach (var ids in new long[][] { held, [] })
                {
                    var timing = await BenchAsync(postgres, schema, sql, Runs, ids, NoneBatch);
                    output.AppendLine(CultureInfo.InvariantCulture, $"| {n:N0} | {ids.Length:N0} | {Row(timing)} |");
                }

                if (n == FailedRows[^1])
                {
                    explain = await ExplainAsync(postgres, sql, "bigint[], int", $"'{{{string.Join(',', held)}}}'::bigint[], {NoneBatch}");
                }
            }
            finally
            {
                await DropAsync(postgres, schema);
            }
        }

        Print(output, $"N = {FailedRows[^1]:N0}, {HeldIds:N0} held ids", explain);
    }

    private async Task<Timing> SessionRoundTripAsync(string schema)
    {
        var store = await PostgreSqlOutboxStoreTests.Provider(("Db", postgres.ConnectionString))
            .CreateStore(PostgreSqlOutboxStoreTests.Outbox("Pilot", "Db", $"{schema}.outbox", OrderingMode.PerKey))
            .ValidateAsync(Ct);
        await using var session = await store.OpenSessionAsync(Ct);

        for (var i = 0; i < WarmUpRuns; i++)
        {
            await session.ReadFailedSummaryAsync(Ct);
        }

        var samples = new double[Runs];
        for (var i = 0; i < Runs; i++)
        {
            var start = Stopwatch.GetTimestamp();
            await session.ReadFailedSummaryAsync(Ct);
            samples[i] = Stopwatch.GetElapsedTime(start).TotalMilliseconds;
        }

        Array.Sort(samples);
        return new Timing(Percentile(samples, 0.5), Percentile(samples, 0.95), samples[^1]);
    }

    private static double Percentile(double[] sorted, double p)
    {
        var rank = p * (sorted.Length - 1);
        var lower = (int)Math.Floor(rank);
        var upper = Math.Min(lower + 1, sorted.Length - 1);
        return sorted[lower] + ((rank - lower) * (sorted[upper] - sorted[lower]));
    }

    private static string Row(Timing t) =>
        $"{Ms(t.P50)} | {Ms(t.P95)} | {Ms(t.Max)} | {(t.P95 <= P95LimitMs && t.Max <= MaxLimitMs ? "yes" : "NO")}";

    private async Task<StringBuilder> HeaderAsync(string title) =>
        new StringBuilder()
            .AppendLine(title)
            .AppendLine(CultureInfo.InvariantCulture, $"Server: {await ServerAsync(postgres)}")
            .AppendLine(CultureInfo.InvariantCulture, $"Within = p95 <= {P95LimitMs} ms and max <= {MaxLimitMs} ms.");

    private static void Print(StringBuilder output, string explainCase, string? explain)
    {
        output.AppendLine()
            .AppendLine(CultureInfo.InvariantCulture, $"EXPLAIN (ANALYZE, BUFFERS), {explainCase}:")
            .AppendLine(explain);
        TestContext.Current.TestOutputHelper!.WriteLine(output.ToString());
    }
}
