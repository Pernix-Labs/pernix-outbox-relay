using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;
using System.Text;
using System.Transactions;
using Npgsql;
using Xunit;

namespace Pernix.OutboxRelay.Client.PostgreSql.Integration.Tests;

// System.Transactions path against PostgreSQL. One connection per scope (no two-phase commit).
public sealed class PostgreSqlOutboxPublisherEnlistedTests(PostgreSqlFixture postgres)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task No_ambient_scope_throws_with_pass_the_DbTransaction_text_and_writes_nothing()
    {
        var table = await postgres.CreateOutboxAsync();
        var publisher = new PostgreSqlOutboxPublisher(table);
        Assert.Null(Transaction.Current);

        await using (var connection = await OpenAsync())
        {
            var single = await Assert.ThrowsAsync<InvalidOperationException>(
                () => publisher.PublishEnlistedAsync(connection, OutboxMessage.Create("k", "p"), Ct));
            var batch = await Assert.ThrowsAsync<InvalidOperationException>(
                () => publisher.PublishEnlistedAsync(connection, [OutboxMessage.Create("k", "p")], Ct));
            var empty = await Assert.ThrowsAsync<InvalidOperationException>(
                () => publisher.PublishEnlistedAsync(connection, Array.Empty<OutboxMessage>(), Ct));

            Assert.Contains("Pass the DbTransaction", single.Message, StringComparison.Ordinal);
            Assert.Contains("Pass the DbTransaction", batch.Message, StringComparison.Ordinal);
            Assert.Contains("Pass the DbTransaction", empty.Message, StringComparison.Ordinal);
        }

        Assert.Empty(await ReadKeysAsync(table));
    }

    [Fact]
    public async Task Closed_connection_throws_and_writes_nothing()
    {
        var table = await postgres.CreateOutboxAsync();
        var publisher = new PostgreSqlOutboxPublisher(table);

        using (var scope = NewScope())
        await using (var connection = new NpgsqlConnection(EnlistingConnectionString()))
        {
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => publisher.PublishEnlistedAsync(connection, OutboxMessage.Create("k", "p"), Ct));
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => publisher.PublishEnlistedAsync(connection, [OutboxMessage.Create("k", "p")], Ct));
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => publisher.PublishEnlistedAsync(connection, Array.Empty<OutboxMessage>(), Ct));
            scope.Complete();
        }

        Assert.Empty(await ReadKeysAsync(table));
    }

    [Fact]
    public async Task Non_Npgsql_connection_throws_and_writes_nothing()
    {
        var table = await postgres.CreateOutboxAsync();
        var publisher = new PostgreSqlOutboxPublisher(table);

        using (var scope = NewScope())
        await using (var connection = new ForeignConnection())
        {
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => publisher.PublishEnlistedAsync(connection, OutboxMessage.Create("k", "p"), Ct));
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => publisher.PublishEnlistedAsync(connection, [OutboxMessage.Create("k", "p")], Ct));
            await Assert.ThrowsAsync<InvalidOperationException>(
                () => publisher.PublishEnlistedAsync(connection, Array.Empty<OutboxMessage>(), Ct));
            scope.Complete();
        }

        Assert.Empty(await ReadKeysAsync(table));
    }

    [Fact]
    public async Task Connection_opened_inside_scope_commits_with_Complete_and_leaves_nothing_without_it()
    {
        var table = await postgres.CreateOutboxAsync();
        var business = await CreateBusinessTableAsync(table);
        var publisher = new PostgreSqlOutboxPublisher(table);

        string businessTxId;
        using (var scope = NewScope())
        await using (var connection = await OpenAsync())
        {
            await ExecuteAsync(connection, $"INSERT INTO {business} VALUES (1)");
            businessTxId = await ScalarStringAsync(connection, "SELECT pg_current_xact_id()::text");
            await publisher.PublishEnlistedAsync(connection, OutboxMessage.Create("single", "p"), Ct);
            await publisher.PublishEnlistedAsync(connection, [OutboxMessage.Create("b1", "p"), OutboxMessage.Create("b2", "p")], Ct);
            scope.Complete();
        }

        using (NewScope())
        await using (var connection = await OpenAsync())
        {
            await ExecuteAsync(connection, $"INSERT INTO {business} VALUES (2)");
            await publisher.PublishEnlistedAsync(connection, OutboxMessage.Create("rolled-back", "p"), Ct);
            await publisher.PublishEnlistedAsync(connection, [OutboxMessage.Create("rolled-back-batch", "p")], Ct);
        }

        Assert.Equal(["1"], await ReadBusinessIdsAsync(business));
        var rows = await ReadRowsAsync(table);
        Assert.Equal(["single", "b1", "b2"], rows.ConvertAll(row => row.Key));
        Assert.All(rows, row => Assert.Equal(businessTxId, row.TxId));
    }

    // Pins Npgsql behaviour at the CPM version. The publisher never calls EnlistTransaction.
    [Fact]
    public async Task EnlistTransaction_with_the_current_transaction_is_a_no_op_on_an_enlisted_connection()
    {
        var table = await postgres.CreateOutboxAsync();
        var business = await CreateBusinessTableAsync(table);
        var publisher = new PostgreSqlOutboxPublisher(table);

        using (NewScope())
        await using (var connection = await OpenAsync())
        {
            await ExecuteAsync(connection, $"INSERT INTO {business} VALUES (1)");
            var before = await ScalarStringAsync(connection, "SELECT pg_current_xact_id()::text");

            connection.EnlistTransaction(Transaction.Current);

            var after = await ScalarStringAsync(connection, "SELECT pg_current_xact_id()::text");
            Assert.Equal(before, after);
            await publisher.PublishEnlistedAsync(connection, OutboxMessage.Create("k", "p"), Ct);
        }

        Assert.Empty(await ReadBusinessIdsAsync(business));
        Assert.Empty(await ReadKeysAsync(table));
    }

    // A connection opened before the scope is not enlisted. Publish autocommits like the business write.
    [Fact]
    public async Task Connection_opened_before_scope_autocommits_business_write_and_publish()
    {
        var table = await postgres.CreateOutboxAsync();
        var business = await CreateBusinessTableAsync(table);
        var publisher = new PostgreSqlOutboxPublisher(table);

        await using (var connection = await OpenAsync())
        {
            using (NewScope())
            {
                await ExecuteAsync(connection, $"INSERT INTO {business} VALUES (1)");
                await publisher.PublishEnlistedAsync(connection, OutboxMessage.Create("single", "p"), Ct);
                await publisher.PublishEnlistedAsync(connection, [OutboxMessage.Create("batch", "p")], Ct);
            }
        }

        Assert.Equal(["1"], await ReadBusinessIdsAsync(business));
        Assert.Equal(["single", "batch"], await ReadKeysAsync(table));
    }

    private static TransactionScope NewScope() => new(TransactionScopeOption.Required, TransactionScopeAsyncFlowOption.Enabled);

    private string EnlistingConnectionString() =>
        new NpgsqlConnectionStringBuilder(postgres.ConnectionString) { Enlist = true }.ConnectionString;

    private async Task<NpgsqlConnection> OpenAsync()
    {
        var connection = new NpgsqlConnection(EnlistingConnectionString());
        await connection.OpenAsync(Ct);
        return connection;
    }

    private async Task<string> CreateBusinessTableAsync(OutboxTable table)
    {
        var business = $"{table.Schema}.business";
        await postgres.ExecuteAsync($"CREATE TABLE {business} (id int NOT NULL)");
        return business;
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        await command.ExecuteNonQueryAsync(Ct);
    }

    private static async Task<string> ScalarStringAsync(NpgsqlConnection connection, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection);
        return (string)(await command.ExecuteScalarAsync(Ct))!;
    }

    private Task<List<string>> ReadBusinessIdsAsync(string business) =>
        ReadOutsideScopeAsync(() => PostgreSqlFixture.QueryStringsAsync(postgres.ConnectionString, $"SELECT id::text FROM {business} ORDER BY id"));

    private Task<List<string>> ReadKeysAsync(OutboxTable table) =>
        ReadOutsideScopeAsync(() => PostgreSqlFixture.QueryStringsAsync(
            postgres.ConnectionString, $"SELECT convert_from(message_key, 'UTF8') FROM {table} ORDER BY id"));

    private async Task<List<(string Key, string TxId)>> ReadRowsAsync(OutboxTable table)
    {
        Assert.Null(Transaction.Current);
        await using var connection = new NpgsqlConnection(postgres.ConnectionString);
        await connection.OpenAsync(Ct);
        await using var command = new NpgsqlCommand($"SELECT message_key, tx_id::text FROM {table} ORDER BY id", connection);
        await using var reader = await command.ExecuteReaderAsync(Ct);
        var rows = new List<(string Key, string TxId)>();
        while (await reader.ReadAsync(Ct))
        {
            rows.Add((Encoding.UTF8.GetString(reader.GetFieldValue<byte[]>(0)), reader.GetString(1)));
        }

        return rows;
    }

    private static Task<List<string>> ReadOutsideScopeAsync(Func<Task<List<string>>> read)
    {
        Assert.Null(Transaction.Current);
        return read();
    }

    private sealed class ForeignConnection : DbConnection
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

        protected override DbTransaction BeginDbTransaction(System.Data.IsolationLevel isolationLevel) => throw new NotSupportedException();

        protected override DbCommand CreateDbCommand() => throw new NotSupportedException();
    }
}
