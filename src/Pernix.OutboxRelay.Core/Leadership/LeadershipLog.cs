using Microsoft.Extensions.Logging;

namespace Pernix.OutboxRelay.Core.Leadership;

public static partial class LeadershipLog
{
    [LoggerMessage(EventId = 101, Level = LogLevel.Information, Message = "Leadership acquired for outbox {Outbox}")]
    public static partial void LeadershipAcquired(this ILogger logger, string outbox);

    [LoggerMessage(EventId = 102, Level = LogLevel.Warning, Message = "Leadership lost for outbox {Outbox}: {Reason}")]
    public static partial void LeadershipLost(this ILogger logger, string outbox, string reason);

    [LoggerMessage(EventId = 103, Level = LogLevel.Information, Message = "Leadership released for outbox {Outbox}")]
    public static partial void LeadershipReleased(this ILogger logger, string outbox);

    [LoggerMessage(EventId = 104, Level = LogLevel.Warning, Message = "Outbox database unreachable for outbox {Outbox}: {Reason}")]
    public static partial void OutboxDatabaseUnreachable(this ILogger logger, string outbox, string reason);

    [LoggerMessage(EventId = 105, Level = LogLevel.Information, Message = "Outbox database reachable again for outbox {Outbox}")]
    public static partial void OutboxDatabaseReachable(this ILogger logger, string outbox);

    [LoggerMessage(EventId = 106, Level = LogLevel.Critical, Message = "Worker loop faulted for outbox {Outbox}")]
    public static partial void WorkerFaulted(this ILogger logger, string outbox, Exception exception);

    [LoggerMessage(EventId = 107, Level = LogLevel.Warning, Message = "Leadership release pending for outbox {Outbox}: {Reason}")]
    public static partial void LeadershipReleasePending(this ILogger logger, string outbox, string reason);
}
