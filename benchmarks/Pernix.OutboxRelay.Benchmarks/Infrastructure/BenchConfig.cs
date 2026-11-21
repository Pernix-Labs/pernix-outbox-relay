using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Jobs;
using BenchmarkDotNet.Reports;
using Perfolizer.Metrology;

namespace Pernix.OutboxRelay.Benchmarks.Infrastructure;

public sealed class BenchConfig : ManualConfig
{
    public BenchConfig()
    {
        Add(DefaultConfig.Instance);
        AddDiagnoser(MemoryDiagnoser.Default);
        AddJob(Job.Default.WithGcServer(false).WithGcConcurrent(false));
        WithSummaryStyle(SummaryStyle.Default.WithSizeUnit(SizeUnit.B));
    }

    public static BenchConfig Instance { get; } = new();
}
