using System.Collections.Concurrent;
using System.Globalization;
using System.Text;
using Pernix.OutboxRelay.Core.Abstractions;

namespace Pernix.OutboxRelay.Destination.Kafka;

internal static class KafkaDeadLetterHeaders
{
    public const string Reason = "x-dead-letter-reason";
    public const string RetryAttempts = "x-retry-attempts";
    public const string OriginalDestination = "x-original-destination";
    public const string OriginalOutbox = "x-original-outbox";
    public const string OriginalId = "x-original-id";
    public const string FailedAt = "x-failed-at";
    public const string OriginalRoute = "x-original-route";

    private const string FailedAtFormat = "yyyy-MM-dd'T'HH:mm:ss.fff'Z'";

    private const int RelayCount = 6;

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

    // Outbox and destination name bytes. Keys are configured outbox and destination names only, never message content
    // (reason, route), so this process-wide cache stays bounded. Shared by every dead letter of that name: never written,
    // never handed to user code (ADR 0005).
    private static readonly ConcurrentDictionary<string, byte[]> NameBytes = new(StringComparer.Ordinal);

    // Test access: proves reason and route never become keys.
    internal static bool IsCached(string name) => NameBytes.ContainsKey(name);

    // Relay values replace any producer header with the same name, so a spoofed x-original-id cannot survive.
    public static MessageHeaders Merge(OutgoingMessage message)
    {
        ArgumentNullException.ThrowIfNull(message);
        var deadLetter = message.DeadLetter ?? throw new ArgumentException("Message has no dead-letter info.", nameof(message));

        var source = message.Headers;
        var kept = 0;
        for (var i = 0; i < source.Count; i++)
        {
            if (!Names.Contains(source[i].Name))
            {
                kept++;
            }
        }

        var route = message.Route;
        var headers = new MessageHeader[kept + RelayCount + (route is null ? 0 : 1)];
        var n = 0;
        for (var i = 0; i < source.Count; i++)
        {
            if (!Names.Contains(source[i].Name))
            {
                headers[n++] = source[i];
            }
        }

        headers[n++] = new(Reason, Encoding.UTF8.GetBytes(deadLetter.Reason));
        headers[n++] = new(RetryAttempts, Utf8(deadLetter.Attempts));
        headers[n++] = new(OriginalDestination, CachedName(message.Source.Destination));
        headers[n++] = new(OriginalOutbox, CachedName(message.Source.Outbox));
        headers[n++] = new(OriginalId, Utf8(message.Source.Id));
        headers[n++] = new(FailedAt, Utf8(deadLetter.FailedAt.UtcDateTime));
        if (route is not null)
        {
            headers[n] = new(OriginalRoute, Encoding.UTF8.GetBytes(route));
        }

        return new MessageHeaders(headers);
    }

    private static byte[] CachedName(string name) => NameBytes.GetOrAdd(name, static n => Encoding.UTF8.GetBytes(n));

    private static byte[] Utf8(int value)
    {
        Span<byte> span = stackalloc byte[11];
        return value.TryFormat(span, out var written, default, CultureInfo.InvariantCulture)
            ? span[..written].ToArray()
            : throw new InvalidOperationException("Int32 does not fit its UTF-8 buffer.");
    }

    private static byte[] Utf8(long value)
    {
        Span<byte> span = stackalloc byte[20];
        return value.TryFormat(span, out var written, default, CultureInfo.InvariantCulture)
            ? span[..written].ToArray()
            : throw new InvalidOperationException("Int64 does not fit its UTF-8 buffer.");
    }

    private static byte[] Utf8(DateTime value)
    {
        Span<byte> span = stackalloc byte[32];
        return value.TryFormat(span, out var written, FailedAtFormat, CultureInfo.InvariantCulture)
            ? span[..written].ToArray()
            : throw new InvalidOperationException("Failed-at time does not fit its UTF-8 buffer.");
    }
}
