using Pernix.OutboxRelay.ContractVectors;
using Pernix.OutboxRelay.Core.Configuration;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Configuration;

public sealed class RouteKeyRuleTests
{
    public static TheoryData<string> VectorIds()
    {
        var data = new TheoryData<string>();
        foreach (var vector in RouteVectors.All)
        {
            data.Add(vector.Id);
        }

        return data;
    }

    [Theory]
    [MemberData(nameof(VectorIds))]
    public void Every_route_vector_matches_its_expected_verdict(string id)
    {
        var vector = RouteVectors.Get(id);

        Assert.Equal(vector.Accepts, RouteKeyRule.IsValid(vector.Route));
    }

    [Fact]
    public void Null_key_is_invalid() => Assert.False(RouteKeyRule.IsValid(null));

    [Fact]
    public void Pattern_is_the_contract_pattern() => Assert.Equal("^[a-z0-9][a-z0-9._-]{0,62}$", RouteKeyRule.Pattern);
}
