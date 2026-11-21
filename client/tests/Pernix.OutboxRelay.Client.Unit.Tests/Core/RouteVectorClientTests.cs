using Pernix.OutboxRelay.ContractVectors;
using Xunit;

namespace Pernix.OutboxRelay.Client.Unit.Tests.Core;

public sealed class RouteVectorClientTests
{
    private const string InvalidRouteText =
        "Route must match ^[a-z0-9][a-z0-9._-]{0,62}$: 1 to 63 lowercase ASCII letters, digits, '.', '-' or '_', starting with a letter or digit.";

    private const string SecretKey = "SECRETKEY";
    private const string SecretPayload = "SECRETPAYLOAD";
    private const string SecretHeader = "SECRETHEADER";

    public static TheoryData<string> VectorIds()
    {
        var ids = new TheoryData<string>();
        foreach (var vector in RouteVectors.All)
        {
            ids.Add(vector.Id);
        }

        return ids;
    }

    [Theory]
    [MemberData(nameof(VectorIds))]
    public void With_route_accepts_exactly_when_the_vector_accepts(string id)
    {
        var vector = RouteVectors.Get(id);
        var headers = OutboxHeaders.CreateBuilder().Add("h", SecretHeader).Build();
        var message = OutboxMessage.Create(SecretKey, SecretPayload, headers);

        if (vector.Accepts)
        {
            Assert.Same(vector.Route, message.WithRoute(vector.Route).Route);
            return;
        }

        var ex = Assert.Throws<ArgumentException>(() => message.WithRoute(vector.Route));
        Assert.Equal("route", ex.ParamName);
        Assert.Equal(new ArgumentException(InvalidRouteText, "route").Message, ex.Message);
        Assert.DoesNotContain(SecretKey, ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(SecretPayload, ex.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(SecretHeader, ex.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Null_route_throws_argument_null()
    {
        var ex = Assert.Throws<ArgumentNullException>(() => OutboxMessage.Create("k", "p").WithRoute(null!));

        Assert.Equal("route", ex.ParamName);
    }

    [Fact]
    public void File_holds_rv1_to_rv20_once_each_without_u0000()
    {
        var ids = new HashSet<string>(StringComparer.Ordinal);
        foreach (var vector in RouteVectors.All)
        {
            Assert.True(ids.Add(vector.Id), $"{vector.Id} appears twice.");
            Assert.DoesNotContain('\0', vector.Route);
        }

        for (var i = 1; i <= 20; i++)
        {
            Assert.Contains("RV" + i, ids);
        }
    }
}
