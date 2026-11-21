using System.Globalization;
using System.Text;
using Pernix.OutboxRelay.Core.Abstractions;

namespace Pernix.OutboxRelay.Unit.Tests.Kafka;

// Test-only parity reference: verbatim KafkaDeadLetterHeaders.Merge at commit 2bb3ac1. Never edit to match product code.
internal static class KafkaDeadLetterHeadersLegacy
{
    public const string Reason = "x-dead-letter-reason";
    public const string RetryAttempts = "x-retry-attempts";
    public const string OriginalDestination = "x-original-destination";
    public const string OriginalOutbox = "x-original-outbox";
    public const string OriginalId = "x-original-id";
    public const string FailedAt = "x-failed-at";
    public const string OriginalRoute = "x-original-route";

    internal static readonly IReadOnlySet<string> Names = new HashSet<string>(StringComparer.OrdinalIgnoreCase)
    {
        Reason,
        RetryAttempts,
        OriginalDestination,
        OriginalOutbox,
        OriginalId,
        FailedAt,
        OriginalRoute,
    };

    // Relay values replace any producer header with the same name, so a spoofed x-original-id cannot survive.
    public static MessageHeaders Merge(OutgoingMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        var deadLetter = message.DeadLetter ?? throw new ArgumentException("Message has no dead-letter info.", nameof(message));

        var headers = new List<MessageHeader>(message.Headers.Count + Names.Count);
        for (var i = 0; i < message.Headers.Count; i++)
        {
            if (!Names.Contains(message.Headers[i].Name))
            {
                headers.Add(message.Headers[i]);
            }
        }

        headers.Add(Header(Reason, deadLetter.Reason));
        headers.Add(Header(RetryAttempts, deadLetter.Attempts.ToString(CultureInfo.InvariantCulture)));
        headers.Add(Header(OriginalDestination, message.Source.Destination));
        headers.Add(Header(OriginalOutbox, message.Source.Outbox));
        headers.Add(Header(OriginalId, message.Source.Id.ToString(CultureInfo.InvariantCulture)));
        headers.Add(Header(FailedAt, deadLetter.FailedAt.UtcDateTime.ToString("yyyy-MM-dd'T'HH:mm:ss.fff'Z'", CultureInfo.InvariantCulture)));
        if (message.Route is { } route)
        {
            headers.Add(Header(OriginalRoute, route));
        }

        return new MessageHeaders([.. headers]);
    }

    private static MessageHeader Header(string name, string value) => new(name, Encoding.UTF8.GetBytes(value));
}
