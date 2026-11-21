using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Reports;
using BenchmarkDotNet.Running;
using Perfolizer.Metrology;

// Run locally in Release, never in CI:
// dotnet run -c Release --project client/benchmarks/Pernix.OutboxRelay.Client.Benchmarks -- --filter '*'
var config = DefaultConfig.Instance.WithSummaryStyle(SummaryStyle.Default.WithSizeUnit(SizeUnit.B));
BenchmarkSwitcher.FromAssembly(typeof(Program).Assembly).Run(args, config);
