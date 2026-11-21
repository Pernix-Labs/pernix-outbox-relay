using System.Reflection;
using Pernix.OutboxRelay.Core.Abstractions;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Abstractions;

public sealed class ClosedHierarchyTests
{
    [Theory]
    [InlineData(typeof(DeliveryResult))]
    [InlineData(typeof(Outcome))]
    public void Only_nested_types_can_derive(Type type)
    {
        // The record copy constructor is protected by language rule. Every other constructor must be private.
        var constructors = type.GetConstructors(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)
            .Where(c => c.GetParameters() is not [{ } only] || only.ParameterType != type);

        Assert.All(constructors, c => Assert.True(c.IsPrivate, $"{type.Name}({string.Join(", ", c.GetParameters().Select(p => p.ParameterType.Name))}) is not private"));
        Assert.All(type.Assembly.GetTypes().Where(t => t.IsSubclassOf(type)), t => Assert.Equal(type, t.DeclaringType));
    }
}
