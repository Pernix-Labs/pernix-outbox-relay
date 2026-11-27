using DotNet.Testcontainers.Containers;
using Testcontainers.PostgreSql;

namespace Pernix.OutboxRelay.Benchmarks.Infrastructure;

internal static class TestContainer
{
    // Testcontainers' default readiness wait is 1 h. A healthy container is ready in seconds.
    public static readonly TimeSpan StartTimeout = TimeSpan.FromMinutes(2);

    // As root the entrypoint re-execs through gosu, which intermittently spins at 100% CPU on Docker Desktop arm64 and never starts PostgreSQL.
    public static PostgreSqlBuilder PostgreSql(string image) => new PostgreSqlBuilder(image).WithCreateParameterModifier(p => p.User = "postgres");

    public static async Task StartBoundedAsync(this IContainer container, CancellationToken ct = default)
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(ct);
        timeout.CancelAfter(StartTimeout);
        try
        {
            await container.StartAsync(timeout.Token);
        }
        catch (OperationCanceledException) when (!ct.IsCancellationRequested)
        {
            var (stdout, stderr) = await container.GetLogsAsync(ct: CancellationToken.None);
            throw new TimeoutException(
                $"Container {container.Image.FullName} ({container.Id}) not ready within {StartTimeout}. stdout: {Tail(stdout)} stderr: {Tail(stderr)}");
        }
    }

    private static string Tail(string log) => log.Length <= 2000 ? log : log[^2000..];
}
