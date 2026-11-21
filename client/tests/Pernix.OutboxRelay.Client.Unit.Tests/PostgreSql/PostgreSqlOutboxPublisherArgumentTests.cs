using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Transactions;
using Pernix.OutboxRelay.Client.PostgreSql;
using Npgsql;
using Xunit;
using IsolationLevel = System.Data.IsolationLevel;

namespace Pernix.OutboxRelay.Client.Unit.Tests.PostgreSql;

// Argument-level checks, no database.
public sealed class PostgreSqlOutboxPublisherArgumentTests
{
    private static readonly OutboxMessage Message = OutboxMessage.Create("k", "p");
    private static readonly OutboxMessage[] Messages = [Message];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    private readonly PostgreSqlOutboxPublisher publisher = new(OutboxTable.Parse("s.t"));

    [Fact]
    public void Ctor_rejects_default_table()
    {
        var exception = Assert.Throws<ArgumentException>(() => new PostgreSqlOutboxPublisher(default));

        Assert.Equal("table", exception.ParamName);
    }

    [Fact]
    public void Table_is_the_table_passed_to_the_ctor()
    {
        Assert.Equal("s.t", publisher.Table.ToString());
    }

    [Fact]
    public async Task PublishAsync_rejects_null_transaction()
    {
        var single = await Assert.ThrowsAsync<ArgumentNullException>(() => publisher.PublishAsync(null!, Message, Ct));
        var batch = await Assert.ThrowsAsync<ArgumentNullException>(() => publisher.PublishAsync(null!, Messages, Ct));

        Assert.Equal("transaction", single.ParamName);
        Assert.Equal("transaction", batch.ParamName);
    }

    // Null messages wins over an unsupported transaction type.
    [Fact]
    public async Task PublishAsync_batch_rejects_null_messages()
    {
        var exception = await Assert.ThrowsAsync<ArgumentNullException>(
            () => publisher.PublishAsync(new FakeTransaction(), (IReadOnlyList<OutboxMessage>)null!, Ct));

        Assert.Equal("messages", exception.ParamName);
    }

    [Fact]
    public async Task PublishAsync_rejects_a_non_npgsql_transaction_naming_its_type()
    {
        var single = await Assert.ThrowsAsync<InvalidOperationException>(() => publisher.PublishAsync(new FakeTransaction(), Message, Ct));
        var batch = await Assert.ThrowsAsync<InvalidOperationException>(() => publisher.PublishAsync(new FakeTransaction(), Messages, Ct));

        Assert.Contains(typeof(FakeTransaction).FullName!, single.Message, StringComparison.Ordinal);
        Assert.Contains(typeof(FakeTransaction).FullName!, batch.Message, StringComparison.Ordinal);
    }

    // An empty batch returns only after the argument checks.
    [Fact]
    public async Task PublishAsync_empty_batch_still_rejects_a_non_npgsql_transaction()
    {
        await Assert.ThrowsAsync<InvalidOperationException>(() => publisher.PublishAsync(new FakeTransaction(), Array.Empty<OutboxMessage>(), Ct));
    }

    [Fact]
    public async Task PublishEnlistedAsync_rejects_null_connection()
    {
        var single = await Assert.ThrowsAsync<ArgumentNullException>(() => publisher.PublishEnlistedAsync(null!, Message, Ct));
        var batch = await Assert.ThrowsAsync<ArgumentNullException>(() => publisher.PublishEnlistedAsync(null!, Messages, Ct));

        Assert.Equal("connection", single.ParamName);
        Assert.Equal("connection", batch.ParamName);
    }

    [Fact]
    public async Task PublishEnlistedAsync_batch_rejects_null_messages()
    {
        await using var connection = new FakeConnection();

        var exception = await Assert.ThrowsAsync<ArgumentNullException>(
            () => publisher.PublishEnlistedAsync(connection, (IReadOnlyList<OutboxMessage>)null!, Ct));

        Assert.Equal("messages", exception.ParamName);
    }

    // The type check runs before the ambient transaction check.
    [Fact]
    public async Task PublishEnlistedAsync_rejects_a_non_npgsql_connection_naming_its_type()
    {
        using var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);
        await using var connection = new FakeConnection();

        var single = await Assert.ThrowsAsync<InvalidOperationException>(() => publisher.PublishEnlistedAsync(connection, Message, Ct));
        var batch = await Assert.ThrowsAsync<InvalidOperationException>(() => publisher.PublishEnlistedAsync(connection, Array.Empty<OutboxMessage>(), Ct));

        Assert.Contains(typeof(FakeConnection).FullName!, single.Message, StringComparison.Ordinal);
        Assert.Contains(typeof(FakeConnection).FullName!, batch.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task PublishEnlistedAsync_without_ambient_transaction_says_to_pass_the_transaction()
    {
        Assert.Null(Transaction.Current);
        await using var connection = new NpgsqlConnection();

        var single = await Assert.ThrowsAsync<InvalidOperationException>(() => publisher.PublishEnlistedAsync(connection, Message, Ct));
        var batch = await Assert.ThrowsAsync<InvalidOperationException>(() => publisher.PublishEnlistedAsync(connection, Array.Empty<OutboxMessage>(), Ct));

        Assert.Equal("No ambient TransactionScope. Pass the DbTransaction to PublishAsync.", single.Message);
        Assert.Equal(single.Message, batch.Message);
    }

    [Fact]
    public async Task PublishEnlistedAsync_rejects_a_closed_connection()
    {
        using var scope = new TransactionScope(TransactionScopeAsyncFlowOption.Enabled);
        await using var connection = new NpgsqlConnection();

        var single = await Assert.ThrowsAsync<InvalidOperationException>(() => publisher.PublishEnlistedAsync(connection, Message, Ct));
        var batch = await Assert.ThrowsAsync<InvalidOperationException>(() => publisher.PublishEnlistedAsync(connection, Messages, Ct));

        Assert.Contains("Closed", single.Message, StringComparison.Ordinal);
        Assert.Equal(single.Message, batch.Message);
    }

    private sealed class FakeTransaction : DbTransaction
    {
        public override IsolationLevel IsolationLevel => IsolationLevel.ReadCommitted;

        protected override DbConnection? DbConnection => null;

        public override void Commit() => throw new NotSupportedException();

        public override void Rollback() => throw new NotSupportedException();
    }

    private sealed class FakeConnection : DbConnection
    {
        [AllowNull]
        public override string ConnectionString { get; set; } = string.Empty;

        public override string Database => string.Empty;

        public override string DataSource => string.Empty;

        public override string ServerVersion => string.Empty;

        public override ConnectionState State => ConnectionState.Open;

        public override void ChangeDatabase(string databaseName) => throw new NotSupportedException();

        public override void Close()
        {
        }

        public override void Open() => throw new NotSupportedException();

        protected override DbTransaction BeginDbTransaction(IsolationLevel isolationLevel) => throw new NotSupportedException();

        protected override DbCommand CreateDbCommand() => throw new NotSupportedException();
    }
}
