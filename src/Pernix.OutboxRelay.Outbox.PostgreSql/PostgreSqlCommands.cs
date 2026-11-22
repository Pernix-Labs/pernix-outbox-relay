using Npgsql;

namespace Pernix.OutboxRelay.Outbox.PostgreSql;

// Wall-clock bound. The caller token never reaches Npgsql, whose cancel request blocks far
// past the deadline when the server stops answering.
internal static class PostgreSqlCommands
{
    public const string DeadlineReason = "command deadline timeout";

    public static Task<T> WithDeadline<T>(Task<T> running, CancellationToken ct) =>
        WithDeadline(running, PostgreSqlSessionSettings.Deadline, ct);

    public static Task<T> WithDeadline<T>(Task<T> running, TimeSpan remaining, CancellationToken ct) =>
        running.WaitAsync(remaining > TimeSpan.Zero ? remaining : TimeSpan.Zero, ct);

    public static bool IsDeadline(Exception exception, Task running) => exception is TimeoutException && !running.IsCompleted;

    public static Task Abandon(Task running)
    {
        if (running.IsCompleted)
        {
            _ = running.Exception;
            return Task.CompletedTask;
        }

        return running.ContinueWith(
            t => _ = t.Exception,
            CancellationToken.None,
            TaskContinuationOptions.None,
            TaskScheduler.Default);
    }

    public static async ValueTask<bool> DisposeAsync(NpgsqlConnection connection, Task? abandoned)
    {
        if (abandoned is { IsCompleted: false } inFlight)
        {
            // Observed so a failed close never surfaces as an unobserved task exception.
            _ = Abandon(inFlight.ContinueWith(
                _ => connection.DisposeAsync().AsTask(),
                CancellationToken.None,
                TaskContinuationOptions.None,
                TaskScheduler.Default).Unwrap());
            return true;
        }

        // No pool: dispose closes the socket and the server drops the backend's locks.
        await connection.DisposeAsync().ConfigureAwait(false);
        return false;
    }
}
