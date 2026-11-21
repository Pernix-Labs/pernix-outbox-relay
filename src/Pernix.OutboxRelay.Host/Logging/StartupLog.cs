namespace Pernix.OutboxRelay.Host.Logging;

public static partial class StartupLog
{
    [LoggerMessage(
        EventId = 1,
        Level = LogLevel.Information,
        Message = "Relay starting: service {ServiceName} version {Version} commit {Commit} environment {EnvironmentName} component {ComponentName}")]
    public static partial void RelayStarting(
        this ILogger logger,
        string serviceName,
        string version,
        string commit,
        string environmentName,
        string componentName);

    [LoggerMessage(
        EventId = 2,
        Level = LogLevel.Critical,
        Message = "Relay startup failed")]
    public static partial void StartupFailed(this ILogger logger, Exception exception);
}
