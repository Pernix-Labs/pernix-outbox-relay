using System.Globalization;
using System.Text;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Allocation;

[CollectionDefinition(nameof(AllocationGuardTests), DisableParallelization = true)]
public sealed class AllocationGuardCollection;

// Allocation guard. Explicit: CI runners are too noisy. Figures come from harness child processes.
[Collection(nameof(AllocationGuardTests))]
public sealed class AllocationGuardTests
{
    private const string Rerun = "dotnet run -c Release --project benchmarks/Pernix.OutboxRelay.Benchmarks -- ";
    private const string Repeat = "If it repeats, measure base vs branch per docs/development.md \"Measuring allocations\".";
    private const double RoundingTolerance = 0.1;
    private const double MedianMarginFraction = 0.10;
    private const double IdleMinimumMargin = 200;
    private const double IdleStopSpread = 1_000;
    private const int FlatCheckpoints = 10;
    private const long FlatStep = 100_000;
    private const int ChurnQuarters = 4;
    private const long ChurnSessionsPerQuarter = 1_000;
    private const int FixedWarm = 300;
    private const int IdleWarmUpSeconds = 5;

    private const string FlatTermArgs = "flat --target term --modes PerKey --dests kafka --headers 1 --batches 500 --n 1000000 --warm 100000";
    private const string FlatSessionArgs = "flat --target session --n 1000000";
    private const string ChurnArgs = "churn --sessions 4000";

    private static readonly TimeSpan RunTimeout = TimeSpan.FromMinutes(15);
    private static readonly TimeSpan IdleTimeout = TimeSpan.FromMinutes(25);

    private static readonly CellRun[] TermRuns =
    [
        new("Term/1", "term --modes None,PerKey --dests null,kafka,http --headers 1 --batches 500 --n 200000 --warm 50000 --rounds 3", 200_000, Shape.Term, RunTimeout),
        new("Term/2", "term --modes None,PerKey --dests dlq --headers 1 --batches 500 --n 50000 --warm 10000 --rounds 3", 50_000, Shape.Term, RunTimeout),
        new("Term/3", "term --modes Strict --dests null,kafka,dlq,http --headers 1 --batches 500 --n 20000 --warm 5000 --rounds 3", 20_000, Shape.Term, RunTimeout),
    ];

    private static readonly CellRun[] HttpRuns =
    [
        new("Http/1", "term --modes None --dests http-tpl,http-sat,http-dlq,http2 --headers 1 --batches 500 --n 200000 --warm 50000 --rounds 3", 200_000, Shape.Term, RunTimeout),
        new("Http/2", "http-stub --modes None --headers 1 --urls static --n 200000 --warm 50000 --rounds 3", 200_000, Shape.Stub, RunTimeout),
    ];

    private static readonly CellRun SessionRun =
        new("Session", "session --modes None,PerKey,Strict --batches 500 --headers 1 --n 100000 --warm 50000 --rounds 3", 100_000, Shape.Session, RunTimeout);

    private static readonly CellRun FixedRun = new("Fixed", "fixed --modes None,PerKey,Strict --rounds 3", 1_000, Shape.Fixed, RunTimeout);

    private static readonly CellRun[] IdleRuns =
    [
        new("Idle/1", "idle --modes None,PerKey,Strict --dests null,kafka --seconds 120 --rounds 1", 120, Shape.Idle, IdleTimeout),
        new("Idle/2", "idle --modes None --dests http --seconds 120 --rounds 1", 120, Shape.Idle, IdleTimeout),
    ];

    private enum Shape
    {
        Term,
        Session,
        Fixed,
        Idle,
        Stub,
    }

    [Fact(Explicit = true)]
    public Task Term() => CellsAsync(TermRuns);

    [Fact(Explicit = true)]
    public Task Http() => CellsAsync(HttpRuns);

    [Fact(Explicit = true)]
    public Task Session() => CellsAsync([SessionRun]);

    [Fact(Explicit = true)]
    public Task Fixed() => CellsAsync([FixedRun]);

    [Fact(Explicit = true)]
    public Task Idle() => CellsAsync(IdleRuns);

    [Fact(Explicit = true)]
    public Task FlatTerm() => FlatAsync("FlatTerm", FlatTermArgs, term: true);

    [Fact(Explicit = true)]
    public Task FlatSession() => FlatAsync("FlatSession", FlatSessionArgs, term: false);

    [Fact(Explicit = true)]
    public Task Churn() => ChurnAsync();

    private static async Task CellsAsync(CellRun[] runs)
    {
        var output = Output();
        var failures = new List<string>();

        foreach (var run in runs)
        {
            foreach (var cell in run.Cells())
            {
                var ceiling = Find(cell.Name);
                var problem = ceiling is null ? $"{cell.Name}: no row in {nameof(AllocationCeilings)}.cs." : RowProblem(ceiling);
                if (problem is not null)
                {
                    failures.Add(problem);
                }
            }
        }

        var children = new HarnessRun[runs.Length];
        var lines = new string[runs.Length][];
        var headers = new string[runs.Length];
        for (var i = 0; i < runs.Length; i++)
        {
            children[i] = await HarnessProcess.RunAsync(runs[i].Args, runs[i].Timeout, TestContext.Current.CancellationToken);
            lines[i] = HarnessOutput.Lines(children[i].Stdout);
            headers[i] = RequireCleanHeader(runs[i].Label, children[i], lines[i]);
        }

        foreach (var header in headers)
        {
            output.WriteLine(header);
        }

        var measured = AllocationCeilings.MeasuredOn;
        var measuredOn = string.Create(
            CultureInfo.InvariantCulture,
            $"Ceilings measured on {measured.Machine}, commit {measured.Commit}, {measured.Date}, {measured.Runs} runs.");
        output.WriteLine(measuredOn);
        output.WriteLine("| cell | figure | ceiling | median | margin | reason | verdict |");
        output.WriteLine("|---|---:|---:|---:|---:|---|---|");

        for (var i = 0; i < runs.Length; i++)
        {
            var run = runs[i];
            var child = children[i];
            var rows = HarnessOutput.ReportedRows(lines[i]);
            var outputProblems = 0;
            foreach (var cell in run.Cells())
            {
                var ceiling = Find(cell.Name);
                var (figure, problem) = ReadFigure(run, cell, rows, lines[i], child);
                string verdict;
                if (problem is not null)
                {
                    failures.Add(problem);
                    outputProblems++;
                    verdict = "fail (output)";
                }
                else if (ceiling is null || RowProblem(ceiling) is not null)
                {
                    verdict = ceiling is not null && double.IsNaN(ceiling.Limit) ? "unset" : "fail (ceiling row)";
                }
                else if (figure > ceiling.Limit)
                {
                    failures.Add(string.Create(
                        CultureInfo.InvariantCulture,
                        $"{cell.Name}: {figure:N1} > {ceiling.Limit:N1} (median {ceiling.Median:N1}, margin {ceiling.Margin:N1}, {ceiling.MarginReason}). " +
                        $"Re-run alone: {Rerun}{Narrow(run.Args, cell.Mode, cell.Dest)}. {Repeat}"));
                    verdict = "fail";
                }
                else
                {
                    verdict = "pass";
                }

                output.WriteLine(
                    $"| {cell.Name} | {Number(figure)} | {Number(ceiling?.Limit)} | {Number(ceiling?.Median)} | {Number(ceiling?.Margin)} | " +
                    $"{ceiling?.MarginReason ?? "-"} | {verdict} |");
            }

            if (ChildFailed(child) && outputProblems == 0)
            {
                failures.Add(ChildFailure(run.Label, child));
            }
        }

        for (var i = 0; i < runs.Length; i++)
        {
            WriteChild(output, runs[i].Label, runs[i].Args, children[i]);
        }

        if (failures.Count > 0)
        {
            failures.Add(measuredOn);
        }

        Finish(failures);
    }

    private static async Task FlatAsync(string label, string args, bool term)
    {
        var output = Output();
        var child = await HarnessProcess.RunAsync(args, RunTimeout, TestContext.Current.CancellationToken);
        var lines = HarnessOutput.Lines(child.Stdout);
        output.WriteLine(RequireCleanHeader(label, child, lines));

        var points = HarnessOutput.FlatPoints(lines, HarnessOutput.FlatHeaderIndex(lines, term));
        output.WriteLine(term ? HarnessOutput.FlatTermHeader : HarnessOutput.FlatSessionHeader);
        output.WriteLine(HarnessOutput.FlatSeparator);
        foreach (var point in points)
        {
            output.WriteLine(string.Create(CultureInfo.InvariantCulture, $"| {point.Count:N0} | {point.HeapKb:N0} | {point.CommittedKb:N0} |"));
        }

        var failures = new List<string>();
        if (!HeapChecks.Pinned(points.ConvertAll(p => p.Count), FlatCheckpoints, FlatStep))
        {
            failures.Add(OutputProblem(
                label,
                child,
                $"{FlatCheckpoints} checkpoints every {FlatStep:N0}, last {FlatCheckpoints * FlatStep:N0}",
                points.Count == 0 ? "no checkpoints" : $"{points.Count} checkpoints, last {points[^1].Count:N0}"));
        }
        else
        {
            foreach (var rising in HeapChecks.Rising(label, points))
            {
                failures.Add($"{rising} Re-run alone: {Rerun}{args}. {Repeat}");
            }
        }

        if (ChildFailed(child) && failures.Count == 0)
        {
            failures.Add(ChildFailure(label, child));
        }

        output.WriteLine($"{label}: {(failures.Count == 0 ? "pass" : "fail")}");
        WriteChild(output, label, args, child);
        Finish(failures);
    }

    private static async Task ChurnAsync()
    {
        const string label = "Churn";
        var output = Output();
        var child = await HarnessProcess.RunAsync(ChurnArgs, RunTimeout, TestContext.Current.CancellationToken);
        var lines = HarnessOutput.Lines(child.Stdout);
        output.WriteLine(RequireCleanHeader(label, child, lines));

        var points = HarnessOutput.ChurnPoints(lines);
        output.WriteLine(HarnessOutput.ChurnHeader);
        output.WriteLine(HarnessOutput.ChurnSeparator);
        foreach (var point in points)
        {
            output.WriteLine(string.Create(
                CultureInfo.InvariantCulture,
                $"| {point.Sessions:N0} | {point.HeapKb:N0} | {point.Backends:N0} | {point.Delta:N0} |"));
        }

        var failures = new List<string>();
        if (!HeapChecks.Pinned(points.ConvertAll(p => p.Sessions), ChurnQuarters, ChurnSessionsPerQuarter))
        {
            failures.Add(OutputProblem(
                label,
                child,
                $"{ChurnQuarters} quarters, last {ChurnQuarters * ChurnSessionsPerQuarter:N0}",
                points.Count == 0 ? "no quarters" : $"{points.Count} quarters, last {points[^1].Sessions:N0}"));
        }
        else
        {
            foreach (var delta in HeapChecks.NonZeroBackendDeltas(label, points))
            {
                failures.Add($"{delta} Re-run alone: {Rerun}{ChurnArgs}. {Repeat}");
            }
        }

        if (ChildFailed(child) && failures.Count == 0)
        {
            failures.Add(ChildFailure(label, child));
        }

        output.WriteLine($"{label}: {(failures.Count == 0 ? "pass" : "fail")} (heap printed, not asserted)");
        WriteChild(output, label, ChurnArgs, child);
        Finish(failures);
    }

    private static (double? Figure, string? Problem) ReadFigure(CellRun run, ExpectedCell cell, List<ReportedRow> rows, string[] lines, HarnessRun child)
    {
        if (cell.ShapeLine is { } shape)
        {
            var printed = HarnessOutput.LineCount(lines, shape);
            if (printed != 1)
            {
                return (null, OutputProblem(cell.Name, child, $"1 line \"{shape}\"", $"{printed}"));
            }
        }

        ReportedRow? found = null;
        var matches = 0;
        foreach (var row in rows)
        {
            if (HarnessOutput.Matches(row.Name, cell.Row ?? cell.Name, cell.Prefix))
            {
                found = row;
                matches++;
            }
        }

        if (matches != 1)
        {
            return (null, OutputProblem(cell.Name, child, "1 reported row", $"{matches} reported rows"));
        }

        var reported = found!.Value;
        if (reported.Round != run.Rounds)
        {
            return (null, OutputProblem(cell.Name, child, $"round {run.Rounds}", $"round {reported.Round}"));
        }

        if (reported.Count != run.Count)
        {
            return (null, OutputProblem(
                cell.Name,
                child,
                string.Create(CultureInfo.InvariantCulture, $"count {run.Count:N0}"),
                string.Create(CultureInfo.InvariantCulture, $"count {reported.Count:N0}")));
        }

        return (reported.Figure, null);
    }

    private static string? RowProblem(Ceiling ceiling)
    {
        if (double.IsNaN(ceiling.Limit))
        {
            return $"{ceiling.Cell}: ceiling unset ({nameof(AllocationCeilings)}.cs).";
        }

        if (Math.Abs(ceiling.Limit - (ceiling.Median + ceiling.Margin)) > RoundingTolerance)
        {
            return string.Create(
                CultureInfo.InvariantCulture,
                $"{ceiling.Cell}: ceiling row inconsistent: limit {ceiling.Limit:N1} != median {ceiling.Median:N1} + margin {ceiling.Margin:N1}.");
        }

        double required;
        string reason;
        if (ceiling.Cell.StartsWith("idle ", StringComparison.Ordinal))
        {
            if (3 * ceiling.Spread > IdleStopSpread)
            {
                return string.Create(
                    CultureInfo.InvariantCulture,
                    $"{ceiling.Cell}: 3 × spread {3 * ceiling.Spread:N1} B/s > {IdleStopSpread:N0} B/s, too noisy to keep the idle reduction (STOP).");
            }

            required = Math.Max(3 * ceiling.Spread, IdleMinimumMargin);
            reason = AllocationCeilings.IdleNoise;
        }
        else
        {
            var percent = MedianMarginFraction * ceiling.Median;
            var spread = 3 * ceiling.Spread;
            required = Math.Max(percent, spread);
            reason = spread > percent ? AllocationCeilings.ThreeSpread : AllocationCeilings.TenPercentReason;
        }

        if (ceiling.Margin < required - RoundingTolerance)
        {
            return string.Create(
                CultureInfo.InvariantCulture,
                $"{ceiling.Cell}: margin {ceiling.Margin:N1} below the margin rule {required:N1}.");
        }

        return ceiling.MarginReason == reason ? null : $"{ceiling.Cell}: margin reason \"{ceiling.MarginReason}\", the margin rule gives \"{reason}\".";
    }

    private static string RequireCleanHeader(string label, HarnessRun child, string[] lines)
    {
        var header = HarnessOutput.Header(lines);
        if (header is not null && HarnessOutput.IsClean(header))
        {
            return header;
        }

        var output = Output();
        output.WriteLine(header ?? $"{label}: no harness header");
        WriteChild(output, label, string.Empty, child);
        Assert.Fail(
            $"{label}: harness header not clean, expected runtimeEvents=off profile=False otel=False gcServer=False. " +
            $"Header: {header ?? "missing"}. Exit code {child.ExitCode}. Stderr: {Stderr(child)}");
        return header!;
    }

    private static string OutputProblem(string name, HarnessRun child, string expected, string printed) =>
        $"{name}: expected {expected}, printed {printed}. Exit code {child.ExitCode}{TimedOut(child)}. Stderr: {Stderr(child)}";

    private static bool ChildFailed(HarnessRun child) => child.ExitCode != 0 || child.TimedOut;

    private static string ChildFailure(string label, HarnessRun child) =>
        $"{label}: exit code {child.ExitCode}{TimedOut(child)}. Stderr: {Stderr(child)}";

    private static string TimedOut(HarnessRun child) => child.TimedOut ? " (timed out, process tree killed)" : string.Empty;

    private static string Stderr(HarnessRun child)
    {
        var stderr = child.Stderr.Trim();
        return stderr.Length == 0 ? "(empty)" : stderr;
    }

    private static void WriteChild(ITestOutputHelper output, string label, string args, HarnessRun child)
    {
        output.WriteLine(string.Empty);
        output.WriteLine($"## {label} child stdout ({args}), exit code {child.ExitCode}{TimedOut(child)}");
        output.WriteLine(child.Stdout);
        if (child.Stderr.Length > 0)
        {
            output.WriteLine($"## {label} child stderr");
            output.WriteLine(child.Stderr);
        }
    }

    private static void Finish(List<string> failures)
    {
        if (failures.Count > 0)
        {
            Assert.Fail(string.Join('\n', failures));
        }
    }

    private static Ceiling? Find(string cell)
    {
        foreach (var ceiling in AllocationCeilings.All)
        {
            if (ceiling.Cell == cell)
            {
                return ceiling;
            }
        }

        return null;
    }

    private static string Number(double? value) =>
        value is null ? "-" : double.IsNaN(value.Value) ? "unset" : value.Value.ToString("N1", CultureInfo.InvariantCulture);

    private static string Narrow(string args, string mode, string? dest)
    {
        var tokens = args.Split(' ');
        for (var i = 0; i + 1 < tokens.Length; i++)
        {
            if (tokens[i] == "--modes")
            {
                tokens[i + 1] = mode;
            }
            else if (tokens[i] == "--dests" && dest is not null)
            {
                tokens[i + 1] = dest;
            }
        }

        return string.Join(' ', tokens);
    }

    private static ITestOutputHelper Output() =>
        TestContext.Current.TestOutputHelper ?? throw new InvalidOperationException("No test output helper");

    // Row: the harness row name when it differs from the ceiling cell name.
    private sealed record ExpectedCell(string Name, bool Prefix, string Mode, string? Dest, string? ShapeLine = null, string? Row = null);

    private sealed record CellRun(string Label, string Args, long Count, Shape Shape, TimeSpan Timeout)
    {
        public int Rounds => int.Parse(Option("--rounds")!, CultureInfo.InvariantCulture);

        public List<ExpectedCell> Cells()
        {
            var modes = Option("--modes")!.Split(',');
            var dests = Option("--dests")?.Split(',') ?? [];
            var cells = new List<ExpectedCell>();
            foreach (var mode in modes)
            {
                switch (Shape)
                {
                    case Shape.Term:
                        foreach (var dest in dests)
                        {
                            cells.Add(new($"term {mode} {dest} b500 h1", false, mode, dest));
                        }

                        break;
                    case Shape.Session:
                        cells.Add(new($"session {mode} batch 500 h1", true, mode, null));
                        break;
                    case Shape.Fixed:
                        foreach (var op in (string[])["read-empty", "check"])
                        {
                            var name = $"{mode} {op}";
                            cells.Add(new(name, false, mode, null, HarnessOutput.FixedShapeLine(name, FixedWarm, Count)));
                        }

                        break;
                    case Shape.Idle:
                        foreach (var dest in dests)
                        {
                            var name = $"idle {mode} {dest} interval 1s";
                            cells.Add(new(name, false, mode, dest, HarnessOutput.IdleShapeLine(name, IdleWarmUpSeconds, Count)));
                        }

                        break;
                    case Shape.Stub:
                        foreach (var headers in Option("--headers")!.Split(','))
                        {
                            var name = $"http-stub {mode} w{(mode == "Strict" ? 1 : 500)} h{headers}";
                            cells.Add(new(name, false, mode, null, Row: $"{name} {Option("--urls")} 200"));
                        }

                        break;
                }
            }

            return cells;
        }

        private string? Option(string name)
        {
            var tokens = Args.Split(' ');
            var index = Array.IndexOf(tokens, name);
            return index < 0 || index + 1 >= tokens.Length ? null : tokens[index + 1];
        }
    }
}
