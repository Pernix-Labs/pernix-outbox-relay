using System.Globalization;
using System.Net.Http.Headers;
using Pernix.OutboxRelay.Core.Abstractions;

namespace Pernix.OutboxRelay.Destination.Http;

internal static class HttpDeadLetterHeaders
{
    internal const int CachedAttempts = 100;

    private const string FailedAtFormat = "yyyy-MM-dd'T'HH:mm:ss.fff'Z'";

    private static readonly string[] Attempts =
        [.. Enumerable.Range(0, CachedAttempts + 1).Select(static i => i.ToString(CultureInfo.InvariantCulture))];

    // Outbox and destination values are the configured name instances: no per-send string, no cache keyed by content.
    public static void Apply(OutgoingMessage message, HttpRequestHeaders target)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(target);
        var deadLetter = message.DeadLetter ?? throw new ArgumentException("Message has no dead-letter info.", nameof(message));

        Set(target, DeadLetterHeaderNames.Reason, deadLetter.Reason);
        Set(target, DeadLetterHeaderNames.RetryAttempts, AttemptsString(deadLetter.Attempts));
        Set(target, DeadLetterHeaderNames.OriginalDestination, message.Source.Destination);
        Set(target, DeadLetterHeaderNames.OriginalOutbox, message.Source.Outbox);
        Set(target, DeadLetterHeaderNames.OriginalId, message.Source.Id.ToString(CultureInfo.InvariantCulture));
        Set(target, DeadLetterHeaderNames.FailedAt, deadLetter.FailedAt.UtcDateTime.ToString(FailedAtFormat, CultureInfo.InvariantCulture));
        if (message.Route is { } route)
        {
            Set(target, DeadLetterHeaderNames.OriginalRoute, route);
        }
        else
        {
            target.Remove(DeadLetterHeaderNames.OriginalRoute);
        }
    }

    internal static string AttemptsString(int attempts) =>
        (uint)attempts <= CachedAttempts ? Attempts[attempts] : attempts.ToString(CultureInfo.InvariantCulture);

    private static void Set(HttpRequestHeaders target, string name, string value)
    {
        target.Remove(name);
        if (!target.TryAddWithoutValidation(name, value))
        {
            throw new InvalidOperationException($"Dead-letter header '{name}' cannot be added.");
        }
    }
}
