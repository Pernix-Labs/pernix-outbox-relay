using Microsoft.Extensions.Logging;

namespace Pernix.OutboxRelay.Destination.Kafka;

internal static partial class KafkaLog
{
    [LoggerMessage(EventId = 200, Level = LogLevel.Information, Message = "Kafka client loaded, librdkafka {Version}")]
    public static partial void KafkaClientLoaded(this ILogger logger, string version);

    [LoggerMessage(EventId = 201, Message = "Kafka client {Client} {Facility}: {Text}")]
    public static partial void KafkaClientLog(this ILogger logger, LogLevel level, string client, string facility, string text);

    [LoggerMessage(EventId = 202, Level = LogLevel.Warning, Message = "Kafka client {Client} error: {Code}: {Reason}")]
    public static partial void KafkaClientError(this ILogger logger, string client, string code, string reason);

    [LoggerMessage(EventId = 203, Level = LogLevel.Error, Message = "Kafka producer fatal error for destination {Destination}: {Code}, producer is rebuilt before the next send")]
    public static partial void KafkaProducerFatal(this ILogger logger, string destination, string code);

    [LoggerMessage(EventId = 204, Level = LogLevel.Information, Message = "Kafka producer rebuilt for destination {Destination}")]
    public static partial void KafkaProducerRebuilt(this ILogger logger, string destination);
}
