using Microsoft.Extensions.Logging;

namespace Pernix.OutboxRelay.Core.Stats;

internal static partial class StatsLog
{
    [LoggerMessage(EventId = 130, Level = LogLevel.Warning, Message = "Outbox stats unavailable for outbox {Outbox}: {Reason}")]
    public static partial void StatsUnavailable(this ILogger logger, string outbox, string reason);

    [LoggerMessage(EventId = 131, Level = LogLevel.Information, Message = "Outbox stats available again for outbox {Outbox}")]
    public static partial void StatsAvailable(this ILogger logger, string outbox);

    [LoggerMessage(EventId = 132, Level = LogLevel.Warning, Message = "Outbox gate lag unavailable for outbox {Outbox}: {Reason}")]
    public static partial void GateLagUnavailable(this ILogger logger, string outbox, string reason);
}
