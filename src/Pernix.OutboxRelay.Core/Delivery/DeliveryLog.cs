using Microsoft.Extensions.Logging;

namespace Pernix.OutboxRelay.Core.Delivery;

internal static partial class DeliveryLog
{
    [LoggerMessage(EventId = 110, Level = LogLevel.Warning, Message = "Delivery paused for outbox {Outbox} to destination {Destination} after {Kind}: {Error}, pause {Pause}")]
    public static partial void DeliveryPaused(this ILogger logger, string outbox, string destination, string kind, string error, TimeSpan pause);

    [LoggerMessage(EventId = 111, Level = LogLevel.Warning, Message = "Message {Id} of outbox {Outbox} to destination {Destination} faulted at retry count {RetryCount}, permanent {Permanent}: {Error}")]
    public static partial void MessageFaulted(this ILogger logger, string outbox, string destination, long id, int retryCount, bool permanent, string error);

    [LoggerMessage(EventId = 112, Level = LogLevel.Information, Message = "Delivery resumed for outbox {Outbox}")]
    public static partial void DeliveryResumed(this ILogger logger, string outbox);

    [LoggerMessage(EventId = 113, Level = LogLevel.Error, Message = "Send failed unexpectedly for outbox {Outbox}: {ExceptionType}")]
    public static partial void DeliverySendFailed(this ILogger logger, string outbox, string exceptionType);

    [LoggerMessage(EventId = 114, Level = LogLevel.Warning, Message = "Stop abandoned {InFlight} in-flight sends for outbox {Outbox}")]
    public static partial void DeliveryStopAbandoned(this ILogger logger, string outbox, int inFlight);

    [LoggerMessage(EventId = 115, Level = LogLevel.Warning, Message = "Stop left {Uncommitted} delivered messages uncommitted for outbox {Outbox}")]
    public static partial void DeliveryStopCommitIncomplete(this ILogger logger, string outbox, int uncommitted);

    [LoggerMessage(EventId = 116, Level = LogLevel.Debug, Message = "Bounded wait on {Step} for outbox {Outbox} abandoned: {ExceptionType}")]
    public static partial void DeliveryWaitAbandoned(this ILogger logger, string outbox, string step, string exceptionType);

    [LoggerMessage(EventId = 117, Level = LogLevel.Debug, Message = "Message {Id} of outbox {Outbox} scheduled for retry {RetryCount} in {Delay}")]
    public static partial void MessageRetryScheduled(this ILogger logger, string outbox, long id, int retryCount, TimeSpan delay);

    [LoggerMessage(EventId = 118, Level = LogLevel.Information, Message = "Message {Id} of outbox {Outbox} dead-lettered to destination {DeadLetterDestination} after {Attempts} attempts")]
    public static partial void MessageDeadLettered(this ILogger logger, string outbox, long id, string deadLetterDestination, int attempts);

    [LoggerMessage(EventId = 119, Level = LogLevel.Warning, Message = "Dead letter of message {Id} of outbox {Outbox} to destination {DeadLetterDestination} failed at retry count {RetryCount}: {Error}")]
    public static partial void DeadLetterFailed(this ILogger logger, string outbox, long id, string deadLetterDestination, int retryCount, string error);

    [LoggerMessage(EventId = 120, Level = LogLevel.Warning, Message = "Message {Id} of outbox {Outbox} to destination {Destination} dropped at retry count {RetryCount}: {Error}")]
    public static partial void MessageDropped(this ILogger logger, string outbox, long id, string destination, int retryCount, string error);

    [LoggerMessage(EventId = 121, Level = LogLevel.Information, Message = "Stop deferred {Count} dead letters for outbox {Outbox} to the next leader")]
    public static partial void DeadLetterDeferredByStop(this ILogger logger, string outbox, int count);

    [LoggerMessage(EventId = 122, Level = LogLevel.Debug, Message = "Dead letter of message {Id} of outbox {Outbox} to destination {DeadLetterDestination} scheduled for retry in {Delay}")]
    public static partial void DeadLetterRetryScheduled(this ILogger logger, string outbox, long id, string deadLetterDestination, TimeSpan delay);

    [LoggerMessage(EventId = 123, Level = LogLevel.Warning, Message = "Message {Id} of outbox {Outbox} to destination {Destination} blocked at retry count {RetryCount}: {Error}")]
    public static partial void MessageBlocked(this ILogger logger, string outbox, long id, string destination, int retryCount, string error);

    [LoggerMessage(EventId = 124, Level = LogLevel.Debug, Message = "Delivery of outbox {Outbox} rewound to head: {Reason}")]
    public static partial void DeliveryRewound(this ILogger logger, string outbox, string reason);

    [LoggerMessage(EventId = 125, Level = LogLevel.Warning, Message = "Message {Id} of outbox {Outbox} held: destination {Destination} does not accept route {Route}")]
    public static partial void MessageHeld(this ILogger logger, string outbox, string destination, long id, string route);
}
