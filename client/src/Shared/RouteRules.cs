using System.Buffers;
using System.Diagnostics.CodeAnalysis;

namespace Pernix.OutboxRelay.Client;

// Compiled into each package by link. Conflict texts hold only routes that passed IsValid.
internal static class RouteRules
{
    public const int MaxLength = 63;

    public const string InvalidRouteMessage =
        "Route must match ^[a-z0-9][a-z0-9._-]{0,62}$: 1 to 63 lowercase ASCII letters, digits, '.', '-' or '_', starting with a letter or digit.";

    private static readonly SearchValues<char> First = SearchValues.Create("0123456789abcdefghijklmnopqrstuvwxyz");
    private static readonly SearchValues<char> Rest = SearchValues.Create("-.0123456789_abcdefghijklmnopqrstuvwxyz");

    // Ordinal, ASCII. Length checked first, so "a\n" and 64+ chars fail without a scan past 63.
    public static bool IsValid(string route) =>
        (uint)(route.Length - 1) < MaxLength && First.Contains(route[0]) && route.AsSpan(1).IndexOfAnyExcept(Rest) < 0;

    public static void Validate([NotNull] string? route, string paramName)
    {
        ArgumentNullException.ThrowIfNull(route, paramName);

        if (!IsValid(route))
        {
            throw new ArgumentException(InvalidRouteMessage, paramName);
        }
    }

    // index < 0: single message, else batch position.
    public static string Resolve(string? messageRoute, string scopedRoute, int index)
    {
        if (messageRoute is null || string.Equals(messageRoute, scopedRoute, StringComparison.Ordinal))
        {
            return scopedRoute;
        }

        throw Conflict(messageRoute, scopedRoute, index);
    }

    private static ArgumentException Conflict(string messageRoute, string scopedRoute, int index)
    {
        const string Advice =
            "A route-scoped outbox service never overrides a message's route: remove WithRoute, or use the service registered for '";

        return index < 0
            ? new ArgumentException(
                $"Message route '{messageRoute}' differs from scoped route '{scopedRoute}'. {Advice}{messageRoute}'.",
                "message")
            : new ArgumentException(
                $"Message {index} route '{messageRoute}' differs from scoped route '{scopedRoute}'. {Advice}{messageRoute}'.",
                "messages");
    }
}
