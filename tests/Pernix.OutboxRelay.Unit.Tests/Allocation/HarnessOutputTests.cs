using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Allocation;

public sealed class HarnessOutputTests
{
    [Fact]
    public void Flat_session_table_is_found_after_its_shape_line()
    {
        var lines = FlatSession("flat session None b500 h1: heap after full GC every 100,000 rows (fresh rows)");

        var points = HarnessOutput.FlatPoints(lines, HarnessOutput.FlatHeaderIndex(lines, term: false));

        Assert.Equal([100_000L, 200_000L], points.ConvertAll(p => p.Count));
    }

    [Theory]
    [InlineData("flat session None b100 h1: heap after full GC every 100,000 rows (fresh rows)")]
    [InlineData("flat session PerKey b500 h1: heap after full GC every 100,000 rows (fresh rows)")]
    [InlineData("flat session None b500 h4: heap after full GC every 100,000 rows (fresh rows)")]
    [InlineData("")]
    public void Flat_session_table_with_another_shape_is_not_found(string shape)
    {
        var lines = FlatSession(shape);

        Assert.Equal(-1, HarnessOutput.FlatHeaderIndex(lines, term: false));
        Assert.Empty(HarnessOutput.FlatPoints(lines, HarnessOutput.FlatHeaderIndex(lines, term: false)));
    }

    [Fact]
    public void Churn_table_is_found_after_its_shape_line()
    {
        var points = HarnessOutput.ChurnPoints(Churn("churn PerKey seeded 10: heap after full GC per quarter of 4,000 sessions"));

        Assert.Equal([1_000L, 2_000L], points.ConvertAll(p => p.Sessions));
    }

    [Theory]
    [InlineData("churn None seeded 10: heap after full GC per quarter of 4,000 sessions")]
    [InlineData("churn PerKey seeded 100: heap after full GC per quarter of 4,000 sessions")]
    [InlineData("")]
    public void Churn_table_with_another_shape_is_not_found(string shape)
    {
        Assert.Empty(HarnessOutput.ChurnPoints(Churn(shape)));
    }

    [Fact]
    public void Fixed_shape_line_names_warm_and_calls()
    {
        Assert.Equal("fixed None read-empty warm 300: 1,000 calls per round", HarnessOutput.FixedShapeLine("None read-empty", 300, 1_000));
    }

    [Fact]
    public void Fixed_shape_line_with_another_warm_is_not_counted()
    {
        string[] lines = ["fixed None read-empty warm 200: 1,000 calls per round", "fixed None check warm 300: 1,000 calls per round"];

        Assert.Equal(0, HarnessOutput.LineCount(lines, HarnessOutput.FixedShapeLine("None read-empty", 300, 1_000)));
        Assert.Equal(1, HarnessOutput.LineCount(lines, HarnessOutput.FixedShapeLine("None check", 300, 1_000)));
    }

    [Fact]
    public void Idle_shape_line_names_warm_up_and_window()
    {
        Assert.Equal(
            "idle None null interval 1s warm-up 5s: 120 s per round",
            HarnessOutput.IdleShapeLine("idle None null interval 1s", 5, 120));
    }

    [Fact]
    public void Idle_shape_line_with_another_warm_up_is_not_counted()
    {
        string[] lines = ["idle None null interval 1s warm-up 1s: 120 s per round"];

        Assert.Equal(0, HarnessOutput.LineCount(lines, HarnessOutput.IdleShapeLine("idle None null interval 1s", 5, 120)));
    }

    [Theory]
    [InlineData("runtimeEvents=off", true)]
    [InlineData("runtimeEvents=on", false)]
    [InlineData("runtimeEvents=unknown", false)]
    public void Header_is_clean_only_when_runtime_events_are_off(string runtimeEvents, bool clean)
    {
        var header = $"# idle --seconds 120 | gcServer=False latency=Batch otel=False profile=False prepared=True {runtimeEvents}";

        Assert.Equal(clean, HarnessOutput.IsClean(header));
    }

    private static string[] FlatSession(string shape) => HarnessOutput.Lines(
        $"{shape}\n| rows | heap after full GC (KB) | committed (KB) |\n|---:|---:|---:|\n| 100,000 | 7,000 | 20,000 |\n| 200,000 | 7,010 | 20,000 |\ntrend: first 7,000 KB, last 7,010 KB, max 7,010 KB\n");

    private static string[] Churn(string shape) => HarnessOutput.Lines(
        $"relay backends before the first session: 0\n{shape}\n| sessions | heap after full GC (KB) | backends | delta |\n|---:|---:|---:|---:|\n| 1,000 | 8,000 | 0 | 0 |\n| 2,000 | 8,010 | 0 | 0 |\n");
}
