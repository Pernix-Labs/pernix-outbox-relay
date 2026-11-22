using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Outbox.PostgreSql;
using Npgsql;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.PostgreSql;

// A bad argument is a faulted task, and the session is kept.
public sealed class PostgreSqlSessionArgumentTests
{
    private const string Table = "\"public\".\"outbox\"";

    [Theory]
    [InlineData(OrderingMode.None)]
    [InlineData(OrderingMode.PerKey)]
    [InlineData(OrderingMode.Strict)]
    public async Task Read_with_a_null_exclude_faults_with_ArgumentNullException_and_keeps_the_session(OrderingMode mode)
    {
        using var connection = new NpgsqlConnection();
        await using var session = new PostgreSqlOutboxSession("Orders", Table, mode, connection);

        var read = session.ReadAsync(new ReadRequest(10, null, null!), CancellationToken.None);

        Assert.True(read.IsFaulted);
        var error = await Assert.ThrowsAsync<ArgumentNullException>(() => read);
        Assert.Equal("request", error.ParamName);
        Assert.Equal("ReadRequest.Exclude must not be null. (Parameter 'request')", error.Message);
        Assert.False(session.Lost.IsCancellationRequested);
    }

    [Fact]
    public async Task None_read_with_a_cursor_and_a_null_exclude_faults_with_the_cursor_ArgumentException()
    {
        using var connection = new NpgsqlConnection();
        await using var session = new PostgreSqlOutboxSession("Orders", Table, OrderingMode.None, connection);

        var read = session.ReadAsync(new ReadRequest(10, new OutboxPosition(1, 1), null!), CancellationToken.None);

        Assert.True(read.IsFaulted);
        var error = await Assert.ThrowsAsync<ArgumentException>(() => read);
        Assert.IsType<ArgumentException>(error);
        Assert.Equal("request", error.ParamName);
        Assert.Equal("A position cursor is not supported for Ordering.Mode None. (Parameter 'request')", error.Message);
        Assert.False(session.Lost.IsCancellationRequested);
    }
}
