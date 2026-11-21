using System.Reflection;
using Pernix.OutboxRelay.Client;
using Pernix.OutboxRelay.PostgreSql;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.PostgreSql;

public sealed class OutboxColumnsDriftTests
{
    [Fact]
    public void Relay_required_columns_are_client_outbox_columns()
    {
        // Read by reflection so the relay product code stays untouched. A missing or renamed field fails here.
        var field = typeof(PostgreSqlOutboxStore).GetField("RequiredColumns", BindingFlags.NonPublic | BindingFlags.Static);
        Assert.NotNull(field);
        var required = Assert.IsType<string[]>(field.GetValue(null));
        Assert.NotEmpty(required);

        Assert.Empty(required.Except(OutboxColumns.All, StringComparer.Ordinal));
    }
}
