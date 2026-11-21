using System.Collections.Frozen;
using Pernix.OutboxRelay.Core.Delivery;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Delivery;

public sealed class RouteAcceptanceTests
{
    private const string Key = "orders";
    private const string Other = "billing";

    private static readonly FrozenSet<string> Routes = new[] { Key, "payments" }.ToFrozenSet(StringComparer.Ordinal);

    [Theory]
    [InlineData(null, true)]
    [InlineData(Key, false)]
    [InlineData(Other, false)]
    public void Single_address_accepts_only_rows_without_a_route(string? route, bool accepted)
    {
        Assert.Equal(accepted, RouteAcceptance.Accepts(null, route));
    }

    [Theory]
    [InlineData(null, false)]
    [InlineData(Key, true)]
    [InlineData(Other, false)]
    [InlineData("ORDERS", false)]
    public void Route_set_accepts_only_its_routes(string? route, bool accepted)
    {
        Assert.Equal(accepted, RouteAcceptance.Accepts(Routes, route));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Accepts_allocates_nothing_after_warm_up(bool routed)
    {
        IReadOnlySet<string>? routes = routed ? Routes : null;

        // Asserts stay outside the measured loop: they allocate.
        var accepted = Run(routes);
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 1000; i++)
        {
            accepted += Run(routes);
        }

        var bytes = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Equal(0, bytes);
        Assert.Equal(1001, accepted);
    }

    private static int Run(IReadOnlySet<string>? routes)
    {
        var accepted = 0;
        accepted += RouteAcceptance.Accepts(routes, null) ? 1 : 0;
        accepted += RouteAcceptance.Accepts(routes, Key) ? 1 : 0;
        accepted += RouteAcceptance.Accepts(routes, Other) ? 1 : 0;
        return accepted;
    }
}
