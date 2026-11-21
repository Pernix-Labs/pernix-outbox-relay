using System.Buffers;

namespace Pernix.OutboxRelay.Core.Configuration;

// Own copy of the route-key rule. Held to tests/contract-vectors/routes.json like the client copy.
internal static class RouteKeyRule
{
    public const string Pattern = "^[a-z0-9][a-z0-9._-]{0,62}$";

    private const int MaxLength = 63;

    private static readonly SearchValues<char> First = SearchValues.Create("0123456789abcdefghijklmnopqrstuvwxyz");
    private static readonly SearchValues<char> Rest = SearchValues.Create("-.0123456789_abcdefghijklmnopqrstuvwxyz");

    // Ordinal, ASCII. Length checked first, so "a\n" and 64+ chars fail without a scan past 63.
    public static bool IsValid(string? key) =>
        key is not null && (uint)(key.Length - 1) < MaxLength && First.Contains(key[0]) && key.AsSpan(1).IndexOfAnyExcept(Rest) < 0;
}
