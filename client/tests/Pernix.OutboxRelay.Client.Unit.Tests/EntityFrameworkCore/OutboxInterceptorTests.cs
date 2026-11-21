using System.Reflection;
using Pernix.OutboxRelay.Client.EntityFrameworkCore;
using Xunit;

namespace Pernix.OutboxRelay.Client.Unit.Tests.EntityFrameworkCore;

public sealed class OutboxInterceptorTests
{
    // No per-context or per-transaction state.
    [Fact]
    public void Interceptor_holds_only_the_publisher()
    {
        var fields = typeof(OutboxInterceptor).GetFields(BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic);

        var field = Assert.Single(fields);
        Assert.Equal(typeof(IOutboxPublisher), field.FieldType);
        Assert.True(field.IsInitOnly);
    }
}
