using System.Diagnostics;
using System.Diagnostics.Tracing;
using System.Globalization;
using System.Reflection;
using System.Runtime;
using System.Runtime.InteropServices;
using Pernix.OutboxRelay.Benchmarks.Infrastructure;
using Pernix.OutboxRelay.Benchmarks.Scenarios;
using Pernix.OutboxRelay.Core.Diagnostics;
using BenchmarkDotNet.Filters;
using BenchmarkDotNet.Running;
using OpenTelemetry;
using OpenTelemetry.Metrics;
using Testcontainers.PostgreSql;

CultureInfo.DefaultThreadCurrentCulture = CultureInfo.InvariantCulture;
CultureInfo.CurrentCulture = CultureInfo.InvariantCulture;

var verb = args.Length > 0 ? args[0] : "all";
var rest = args.Length > 0 ? args[1..] : [];

try
{
    if (verb == "bdn")
    {
        await Bdn.RunAsync(rest);
        return 0;
    }

    // Child of `http --socket`: the loopback server on a real socket.
    if (verb == "http-server")
    {
        await HttpLoopback.ServeAsync(HarnessOptions.Parse("http", rest).Response);
        return 0;
    }

    HarnessOptions options;
    try
    {
        options = HarnessOptions.Parse(verb, rest);
    }
    catch (UsageException ex)
    {
        Console.Error.WriteLine(ex.Message);
        Console.Error.WriteLine(HarnessOptions.Usage);
        return 2;
    }

    // AllocationGuardTests parses this line.
    Console.WriteLine(
        $"# {verb} {string.Join(' ', rest)} | runtime={RuntimeInformation.FrameworkDescription} os={RuntimeInformation.OSDescription} " +
        $"cpus={Environment.ProcessorCount} gcServer={GCSettings.IsServerGC} latency={GCSettings.LatencyMode} pg={PgEnvironment.Describe()} " +
        $"otel={options.Otel} profile={options.Profile} runtimeEvents={RuntimeEvents()}");

    using var profile = options.Profile ? AllocProfile.Enable() : null;
    using var meters = options.Otel
        ? Sdk.CreateMeterProviderBuilder()
            .AddMeter(RelayDiagnostics.Name)
            .AddInMemoryExporter(new List<Metric>(), reader => reader.PeriodicExportingMetricReaderOptions.ExportIntervalMilliseconds = 600_000)
            .Build()
        : null;

    await Verbs.RunAsync(options, rest);
    return 0;
}
catch (Exception ex)
{
    // Failure = no figure. Message chain only: no stack, nothing that could carry a connection string.
    for (var e = ex; e is not null; e = e.InnerException)
    {
        Console.Error.WriteLine($"{e.GetType().Name}: {e.Message}");
    }

    return 1;
}
finally
{
    await PgEnvironment.DisposeCurrentAsync();
}

static string RuntimeEvents()
{
    foreach (var source in EventSource.GetSources())
    {
        if (source.Name == "Microsoft-Windows-DotNETRuntime")
        {
            return source.IsEnabled() ? "on" : "off";
        }
    }

    return "unknown";
}

internal static class Verbs
{
    public static async Task RunAsync(HarnessOptions options, string[] args)
    {
        switch (options.Verb)
        {
            case "term":
                await TimedAsync("term", TermScenario.RunAsync, options);
                break;
            case "kafka":
                await TimedAsync("kafka", KafkaOnlyScenario.RunAsync, options);
                break;
            case "kafka-dlq":
                await TimedAsync("kafka-dlq", KafkaOnlyScenario.DeadLetterAsync, options);
                break;
            case "session":
                await TimedAsync("session", SessionScenario.RunAsync, options);
                break;
            case "fixed":
                await TimedAsync("fixed", FixedCostScenario.RunAsync, options);
                break;
            case "idle":
                await TimedAsync("idle", TermScenario.IdleAsync, options);
                break;
            case "churn":
                await TimedAsync("churn", ChurnScenario.RunAsync, options);
                break;
            case "loopback":
                await TimedAsync("loopback", HttpFloorScenario.LoopbackAsync, options);
                break;
            case "http-floor":
                await TimedAsync("http-floor", HttpFloorScenario.RunAsync, options);
                break;
            case "http":
                await TimedAsync(options.Socket ? "http socket" : "http", HttpOnlyScenario.RunAsync, options);
                break;
            case "http-dlq":
                await TimedAsync("http-dlq", HttpOnlyScenario.DeadLetterAsync, options);
                break;
            case "http-stub":
                await TimedAsync("http-stub", HttpOnlyScenario.StubAsync, options);
                break;
            case "flat":
                await FlatAsync(options);
                break;
            case "all":
                await AllAsync(args);
                break;
            default:
                throw new UsageException($"unknown verb '{options.Verb}'");
        }
    }

    private static async Task AllAsync(string[] args)
    {
        await TimedAsync("kafka", KafkaOnlyScenario.RunAsync, HarnessOptions.Parse("kafka", args));
        await TimedAsync("kafka-dlq", KafkaOnlyScenario.DeadLetterAsync, HarnessOptions.Parse("kafka-dlq", args));
        await TimedAsync("term null,kafka,dlq", TermScenario.RunAsync, HarnessOptions.Parse("term", args) with { Dests = ["null", "kafka", "dlq"] });
        await TimedAsync("idle", TermScenario.IdleAsync, HarnessOptions.Parse("idle", args));
        await TimedAsync("flat term", TermScenario.FlatAsync, HarnessOptions.Parse("flat", args) with { Target = "term" });
        await TimedAsync("session", SessionScenario.RunAsync, HarnessOptions.Parse("session", args));
        await TimedAsync("fixed", FixedCostScenario.RunAsync, HarnessOptions.Parse("fixed", args));
        await TimedAsync("flat session", SessionScenario.FlatAsync, HarnessOptions.Parse("flat", args) with { Target = "session" });
        await TimedAsync("churn", ChurnScenario.RunAsync, HarnessOptions.Parse("churn", args));
    }

    private static async Task FlatAsync(HarnessOptions options)
    {
        if (options.Target is null or "term")
        {
            await TimedAsync("flat term", TermScenario.FlatAsync, options with { Target = "term" });
        }

        if (options.Target is null or "session")
        {
            await TimedAsync("flat session", SessionScenario.FlatAsync, options with { Target = "session" });
        }
    }

    private static async Task TimedAsync(string name, Func<HarnessOptions, Task> scenario, HarnessOptions options)
    {
        Console.WriteLine();
        Console.WriteLine($"## {name}");
        var started = Stopwatch.GetTimestamp();
        await scenario(options);
        Console.WriteLine($"duration {Stopwatch.GetElapsedTime(started).TotalSeconds:F1} s");
    }
}

internal static class Bdn
{
    public static async Task RunAsync(string[] args)
    {
        PostgreSqlContainer? container = null;
        try
        {
            if (string.IsNullOrEmpty(Environment.GetEnvironmentVariable(PgEnvironment.ConnectionVariable)) && CanSelectPostgreSql(args))
            {
                try
                {
                    container = await PgEnvironment.StartContainerAsync();
                    Environment.SetEnvironmentVariable(PgEnvironment.ConnectionVariable, container.GetConnectionString());
                }
                catch (InvalidOperationException ex) when (ex.Message == PgEnvironment.DockerMissing)
                {
                    Console.Error.WriteLine(PgEnvironment.DockerMissing);
                }
            }

            BenchmarkSwitcher.FromAssembly(typeof(Bdn).Assembly).Run(args, BenchConfig.Instance);
        }
        finally
        {
            if (container is not null)
            {
                Environment.SetEnvironmentVariable(PgEnvironment.ConnectionVariable, null);
                await container.DisposeAsync();
            }
        }
    }

    private static bool CanSelectPostgreSql(string[] args)
    {
        var patterns = FilterPatterns(args);
        var filter = patterns.Length == 0 ? null : new GlobFilter(patterns);
        foreach (var type in typeof(Bdn).Assembly.GetTypes())
        {
            if (type.GetCustomAttribute<NeedsPostgreSqlAttribute>() is null)
            {
                continue;
            }

            foreach (var benchmarkCase in BenchmarkConverter.TypeToBenchmarks(type, BenchConfig.Instance).BenchmarksCases)
            {
                if (filter is null || filter.Predicate(benchmarkCase))
                {
                    return true;
                }
            }
        }

        return false;
    }

    private static string[] FilterPatterns(string[] args)
    {
        var patterns = new List<string>();
        for (var i = 0; i < args.Length; i++)
        {
            if (args[i].StartsWith("--filter=", StringComparison.Ordinal))
            {
                patterns.Add(args[i]["--filter=".Length..]);
                continue;
            }

            if (args[i] is not ("--filter" or "-f"))
            {
                continue;
            }

            while (i + 1 < args.Length && !args[i + 1].StartsWith('-'))
            {
                patterns.Add(args[++i]);
            }
        }

        return [.. patterns];
    }
}
