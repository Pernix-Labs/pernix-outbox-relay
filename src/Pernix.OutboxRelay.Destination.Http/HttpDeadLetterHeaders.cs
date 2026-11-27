using System.Buffers;
using System.Diagnostics.CodeAnalysis;
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

    private static readonly SearchValues<char> ControlChars = SearchValues.Create("\r\n\0");

    // Outbox and destination values are the configured name instances: no per-send string, no cache keyed by content.
    // Null when every header is set. Names are relay constants, so a refused header is a relay defect: DestinationFault.
    public static DeliveryResult? Apply(OutgoingMessage message, HttpRequestHeaders target)
    {
        ArgumentNullException.ThrowIfNull(message);
        ArgumentNullException.ThrowIfNull(target);
        var deadLetter = message.DeadLetter ?? throw new ArgumentException("Message has no dead-letter info.", nameof(message));

        if (!TrySet(target, DeadLetterHeaderNames.Reason, SafeReason(deadLetter.Reason), out var fault)
            || !TrySet(target, DeadLetterHeaderNames.RetryAttempts, AttemptsString(deadLetter.Attempts), out fault)
            || !TrySet(target, DeadLetterHeaderNames.OriginalDestination, message.Source.Destination, out fault)
            || !TrySet(target, DeadLetterHeaderNames.OriginalOutbox, message.Source.Outbox, out fault)
            || !TrySet(target, DeadLetterHeaderNames.OriginalId, message.Source.Id.ToString(CultureInfo.InvariantCulture), out fault)
            || !TrySet(target, DeadLetterHeaderNames.FailedAt, deadLetter.FailedAt.UtcDateTime.ToString(FailedAtFormat, CultureInfo.InvariantCulture), out fault))
        {
            return fault;
        }

        if (message.Route is { } route)
        {
            return TrySet(target, DeadLetterHeaderNames.OriginalRoute, route, out fault) ? null : fault;
        }

        target.Remove(DeadLetterHeaderNames.OriginalRoute);
        return null;
    }

    internal static string AttemptsString(int attempts) =>
        (uint)attempts <= CachedAttempts ? Attempts[attempts] : attempts.ToString(CultureInfo.InvariantCulture);

    // Reason can carry exception text: CR, LF and NUL become spaces. A clean reason keeps its instance.
    internal static string SafeReason(string reason)
    {
        if (!reason.AsSpan().ContainsAny(ControlChars))
        {
            return reason;
        }

        return string.Create(reason.Length, reason, static (span, source) =>
        {
            source.AsSpan().CopyTo(span);
            span.Replace('\r', ' ');
            span.Replace('\n', ' ');
            span.Replace('\0', ' ');
        });
    }

    internal static bool TrySet(HttpRequestHeaders target, string name, string value, [NotNullWhen(false)] out DeliveryResult? fault)
    {
        try
        {
            target.Remove(name);
            if (target.TryAddWithoutValidation(name, value))
            {
                fault = null;
                return true;
            }
        }
        catch (Exception exception) when (exception is InvalidOperationException or FormatException)
        {
        }

        fault = new DeliveryResult.DestinationFault($"dead-letter header '{name}' cannot be sent");
        return false;
    }
}
