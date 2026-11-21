namespace Pernix.OutboxRelay.Core.Delivery;

// The one route acceptance rule.
internal static class RouteAcceptance
{
    public static bool Accepts(IReadOnlySet<string>? routes, string? route) =>
        routes is null ? route is null : route is not null && routes.Contains(route);
}
