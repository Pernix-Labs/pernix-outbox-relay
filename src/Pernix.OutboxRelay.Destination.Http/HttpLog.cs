using Microsoft.Extensions.Logging;

namespace Pernix.OutboxRelay.Destination.Http;

internal static partial class HttpLog
{
    [LoggerMessage(EventId = 300, Level = LogLevel.Debug, Message = "Http client created for destination {Destination}, host {Host}, max connections {MaxConnections}")]
    public static partial void HttpClientCreated(this ILogger logger, string destination, string host, int maxConnections);

    [LoggerMessage(EventId = 301, Level = LogLevel.Warning, Message = "Http token refresh failed for destination {Destination}, token host {TokenHost}: {Failure}")]
    public static partial void HttpTokenRefreshFailed(this ILogger logger, string destination, string tokenHost, string failure);

    [LoggerMessage(EventId = 302, Level = LogLevel.Warning, Message = "Http probe failed for destination {Destination}, host {Host}, method {Method}: {Failure}")]
    public static partial void HttpProbeFailed(this ILogger logger, string destination, string host, string method, string failure);
}
