using System.Diagnostics;

namespace Pernix.OutboxRelay.Unit.Tests.Allocation;

internal sealed record HarnessRun(int ExitCode, string Stdout, string Stderr, bool TimedOut);

internal static class HarnessProcess
{
    private const string Project = "benchmarks/Pernix.OutboxRelay.Benchmarks";
    private const int BuildTailLines = 50;

    private static readonly TimeSpan BuildTimeout = TimeSpan.FromMinutes(15);

    // No EventPipe session, no diagnostic port, no GC mode override in the child.
    private static readonly string[] RemovedVariables =
    [
        "DOTNET_EnableEventPipe",
        "COMPlus_EnableEventPipe",
        "DOTNET_DiagnosticPorts",
        "DOTNET_gcServer",
        "COMPlus_gcServer",
        "DOTNET_gcConcurrent",
        "COMPlus_gcConcurrent",
    ];

    private static readonly string[] RemovedPrefixes = ["DOTNET_EventPipe", "COMPlus_EventPipe"];

    private static readonly Lazy<string> Root = new(FindRepoRoot);

    private static readonly Lazy<Task> Build = new(BuildAsync);

    public static string RepoRoot => Root.Value;

    public static async Task<HarnessRun> RunAsync(string args, TimeSpan timeout, CancellationToken cancellationToken)
    {
        await Build.Value.WaitAsync(cancellationToken);
        var dll = Path.Combine(RepoRoot, "benchmarks", "Pernix.OutboxRelay.Benchmarks", "bin", "Release", "net10.0", "Pernix.OutboxRelay.Benchmarks.dll");
        var arguments = new List<string> { dll };
        arguments.AddRange(args.Split(' '));
        return await ExecuteAsync(arguments, timeout, cancellationToken);
    }

    private static async Task BuildAsync()
    {
        string[] arguments = ["build", "-c", "Release", Project, "-nodeReuse:false", "-p:UseSharedCompilation=false", "-v", "q"];
        var run = await ExecuteAsync(arguments, BuildTimeout, CancellationToken.None);
        if (run.ExitCode == 0 && !run.TimedOut)
        {
            return;
        }

        var lines = (run.Stdout + run.Stderr).Split('\n');
        var tail = string.Join('\n', lines, Math.Max(0, lines.Length - BuildTailLines), Math.Min(lines.Length, BuildTailLines));
        throw new InvalidOperationException(
            $"Harness build failed{(run.TimedOut ? $" (timed out after {BuildTimeout.TotalMinutes:0} min)" : string.Empty)}, " +
            $"exit code {run.ExitCode}: dotnet {string.Join(' ', arguments)}\nLast {BuildTailLines} lines:\n{tail}");
    }

    private static async Task<HarnessRun> ExecuteAsync(IReadOnlyList<string> arguments, TimeSpan timeout, CancellationToken cancellationToken)
    {
        var start = new ProcessStartInfo(DotnetPath())
        {
            WorkingDirectory = RepoRoot,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };

        for (var i = 0; i < arguments.Count; i++)
        {
            start.ArgumentList.Add(arguments[i]);
        }

        Scrub(start.Environment);

        using var process = Process.Start(start) ?? throw new InvalidOperationException($"Could not start {start.FileName}");
        var stdout = process.StandardOutput.ReadToEndAsync(CancellationToken.None);
        var stderr = process.StandardError.ReadToEndAsync(CancellationToken.None);

        using var limit = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        limit.CancelAfter(timeout);
        var timedOut = false;
        try
        {
            await process.WaitForExitAsync(limit.Token);
        }
        catch (OperationCanceledException)
        {
            Kill(process);
            await process.WaitForExitAsync(CancellationToken.None);
            cancellationToken.ThrowIfCancellationRequested();
            timedOut = true;
        }

        return new HarnessRun(process.ExitCode, await stdout, await stderr, timedOut);
    }

    private static void Kill(Process process)
    {
        try
        {
            process.Kill(entireProcessTree: true);
        }
        catch (InvalidOperationException)
        {
            // Already exited.
        }
        catch (AggregateException)
        {
            // A descendant could not be killed. The caller still reports the run as timed out, with its stdout.
        }
    }

    private static void Scrub(IDictionary<string, string?> environment)
    {
        foreach (var key in new List<string>(environment.Keys))
        {
            if (IsScrubbed(key))
            {
                environment.Remove(key);
            }
        }
    }

    private static bool IsScrubbed(string key)
    {
        foreach (var name in RemovedVariables)
        {
            if (string.Equals(key, name, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        foreach (var prefix in RemovedPrefixes)
        {
            if (key.StartsWith(prefix, StringComparison.OrdinalIgnoreCase))
            {
                return true;
            }
        }

        return false;
    }

    private static string DotnetPath()
    {
        var host = Environment.GetEnvironmentVariable("DOTNET_HOST_PATH");
        return string.IsNullOrEmpty(host) ? "dotnet" : host;
    }

    private static string FindRepoRoot()
    {
        for (var dir = new DirectoryInfo(AppContext.BaseDirectory); dir is not null; dir = dir.Parent)
        {
            if (File.Exists(Path.Combine(dir.FullName, "OutboxRelay.slnx")))
            {
                return dir.FullName;
            }
        }

        throw new InvalidOperationException($"No parent of {AppContext.BaseDirectory} holds OutboxRelay.slnx");
    }
}
