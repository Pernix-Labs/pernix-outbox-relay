namespace Pernix.OutboxRelay.Core.Abstractions;

/// <summary>Dead-letter header names the relay stamps on dead-lettered messages. Shared by every transport.</summary>
public static class DeadLetterHeaderNames
{
    public const string Reason = "x-dead-letter-reason";
    public const string RetryAttempts = "x-retry-attempts";
    public const string OriginalDestination = "x-original-destination";
    public const string OriginalOutbox = "x-original-outbox";
    public const string OriginalId = "x-original-id";
    public const string FailedAt = "x-failed-at";
    public const string OriginalRoute = "x-original-route";

    /// <summary>Every relay dead-letter header name, compared case-insensitively.</summary>
    public static IReadOnlySet<string> All { get; } = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        Reason,
        RetryAttempts,
        OriginalDestination,
        OriginalOutbox,
        OriginalId,
        FailedAt,
        OriginalRoute,
    };
}
