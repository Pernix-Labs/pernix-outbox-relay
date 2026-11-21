using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Npgsql;
using Xunit;

namespace Pernix.OutboxRelay.Client.PostgreSql.Integration.Tests;

// DbTransaction path against PostgreSQL. Tables come from the package DDL only.
public sealed class PostgreSqlOutboxPublisherTests(PostgreSqlFixture postgres)
{
    private const string NpgsqlActivitySource = "Npgsql";

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Publish_writes_key_payload_and_headers_and_leaves_headers_default_when_empty()
    {
        var table = await postgres.CreateOutboxAsync();
        var publisher = new PostgreSqlOutboxPublisher(table);
        var headers = OutboxHeaders.CreateBuilder().Add("type", "order.created").Add("é", "ü \"q\"").Build();

        await using (var connection = await OpenAsync())
        await using (var transaction = await connection.BeginTransactionAsync(Ct))
        {
            await publisher.PublishAsync(transaction, OutboxMessage.Create("k1", "p1", headers), Ct);
            await publisher.PublishAsync(transaction, OutboxMessage.Create("k2", "p2"), Ct);
            await transaction.CommitAsync(Ct);
        }

        var rows = await ReadRowsAsync(table);
        Assert.Equal(2, rows.Count);
        Assert.Equal("k1"u8.ToArray(), rows[0].Key);
        Assert.Equal("p1"u8.ToArray(), rows[0].Payload);
        Assert.Equal(new Dictionary<string, string> { ["type"] = "order.created", ["é"] = "ü \"q\"" }, ParseHeaders(rows[0].Headers));
        Assert.Equal("k2"u8.ToArray(), rows[1].Key);
        Assert.Equal("p2"u8.ToArray(), rows[1].Payload);
        Assert.Equal("{}", rows[1].Headers);
    }

    [Fact]
    public async Task Commit_keeps_rows_with_the_business_tx_id_and_rollback_leaves_none()
    {
        var table = await postgres.CreateOutboxAsync();
        await postgres.ExecuteAsync($"CREATE TABLE {table.Schema}.business (id int NOT NULL)");
        var publisher = new PostgreSqlOutboxPublisher(table);

        string businessTxId;
        await using (var connection = await OpenAsync())
        await using (var transaction = await connection.BeginTransactionAsync(Ct))
        {
            await ExecuteAsync(connection, transaction, $"INSERT INTO {table.Schema}.business VALUES (1)");
            businessTxId = await ScalarStringAsync(connection, transaction, "SELECT pg_current_xact_id()::text");
            await publisher.PublishAsync(transaction, OutboxMessage.Create("single", "p"), Ct);
            await publisher.PublishAsync(transaction, [OutboxMessage.Create("b1", "p"), OutboxMessage.Create("b2", "p")], Ct);
            await transaction.CommitAsync(Ct);
        }

        await using (var connection = await OpenAsync())
        await using (var transaction = await connection.BeginTransactionAsync(Ct))
        {
            await ExecuteAsync(connection, transaction, $"INSERT INTO {table.Schema}.business VALUES (2)");
            await publisher.PublishAsync(transaction, OutboxMessage.Create("rolled-back", "p"), Ct);
            await publisher.PublishAsync(transaction, [OutboxMessage.Create("rolled-back-batch", "p")], Ct);
            await transaction.RollbackAsync(Ct);
        }

        var rows = await ReadRowsAsync(table);
        Assert.Equal(["single", "b1", "b2"], Keys(rows));
        Assert.All(rows, row => Assert.Equal(businessTxId, row.TxId));
    }

    [Fact]
    public async Task Completed_transaction_throws_and_writes_nothing()
    {
        var table = await postgres.CreateOutboxAsync();
        var publisher = new PostgreSqlOutboxPublisher(table);

        await using var connection = await OpenAsync();
        await using var committed = await connection.BeginTransactionAsync(Ct);
        await committed.CommitAsync(Ct);

        await Assert.ThrowsAsync<InvalidOperationException>(() => publisher.PublishAsync(committed, OutboxMessage.Create("k", "p"), Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(() => publisher.PublishAsync(committed, [OutboxMessage.Create("k", "p")], Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(() => publisher.PublishAsync(committed, Array.Empty<OutboxMessage>(), Ct));

        await using var rolledBack = await connection.BeginTransactionAsync(Ct);
        await rolledBack.RollbackAsync(Ct);

        await Assert.ThrowsAsync<InvalidOperationException>(() => publisher.PublishAsync(rolledBack, OutboxMessage.Create("k", "p"), Ct));
        await Assert.ThrowsAsync<InvalidOperationException>(() => publisher.PublishAsync(rolledBack, [OutboxMessage.Create("k", "p")], Ct));

        var disposed = await connection.BeginTransactionAsync(Ct);
        await disposed.DisposeAsync();

        await Assert.ThrowsAnyAsync<InvalidOperationException>(() => publisher.PublishAsync(disposed, OutboxMessage.Create("k", "p"), Ct));
        await Assert.ThrowsAnyAsync<InvalidOperationException>(() => publisher.PublishAsync(disposed, [OutboxMessage.Create("k", "p")], Ct));

        Assert.Empty(await ReadRowsAsync(table));
    }

    [Fact]
    public async Task Batch_of_100_is_one_command_and_ids_ascend_in_list_order()
    {
        var table = await postgres.CreateOutboxAsync();
        var publisher = new PostgreSqlOutboxPublisher(table);
        var headers = OutboxHeaders.CreateBuilder().Add("h", "v").Build();
        var messages = new OutboxMessage[100];
        var expectedKeys = new string[messages.Length];
        for (var i = 0; i < messages.Length; i++)
        {
            // Reverse key order, alternating header variants: ids must follow list order, not key order.
            expectedKeys[i] = $"k{messages.Length - i:D3}";
            messages[i] = OutboxMessage.Create(expectedKeys[i], $"p{i}", i % 2 == 0 ? headers : null);
        }

        await using (var connection = await OpenAsync())
        await using (var transaction = await connection.BeginTransactionAsync(Ct))
        {
            using var activities = new NpgsqlActivityRecorder();
            await publisher.PublishAsync(transaction, messages, Ct);
            Assert.Equal(1, activities.Stop());
            await transaction.CommitAsync(Ct);
        }

        var rows = await ReadRowsAsync(table);
        Assert.Equal(expectedKeys, Keys(rows));
        for (var i = 0; i < rows.Count; i++)
        {
            Assert.Equal(Encoding.UTF8.GetBytes($"p{i}"), rows[i].Payload);
            Assert.Equal(i % 2 == 0 ? "{\"h\": \"v\"}" : "{}", rows[i].Headers);
        }
    }

    [Fact]
    public async Task Empty_batch_sends_no_command()
    {
        var table = await postgres.CreateOutboxAsync();
        var publisher = new PostgreSqlOutboxPublisher(table);

        await using (var connection = await OpenAsync())
        await using (var transaction = await connection.BeginTransactionAsync(Ct))
        {
            using (var activities = new NpgsqlActivityRecorder())
            {
                await publisher.PublishAsync(transaction, Array.Empty<OutboxMessage>(), Ct);
                await publisher.PublishAsync(transaction, new List<OutboxMessage>(), Ct);
                Assert.Equal(0, activities.Stop());
            }

            // The recorder sees a real publish, so the zero above is not a silent listener.
            using (var activities = new NpgsqlActivityRecorder())
            {
                await publisher.PublishAsync(transaction, OutboxMessage.Create("k", "p"), Ct);
                Assert.Equal(1, activities.Stop());
            }

            await transaction.CommitAsync(Ct);
        }

        Assert.Equal(["k"], Keys(await ReadRowsAsync(table)));
    }

    [Fact]
    public async Task Null_key_is_null_and_empty_key_is_empty_bytea()
    {
        var table = await postgres.CreateOutboxAsync();
        var publisher = new PostgreSqlOutboxPublisher(table);

        await using (var connection = await OpenAsync())
        await using (var transaction = await connection.BeginTransactionAsync(Ct))
        {
            await publisher.PublishAsync(transaction, OutboxMessage.FromBytes(null, "p"u8.ToArray()), Ct);
            await publisher.PublishAsync(transaction, OutboxMessage.FromBytes(ReadOnlyMemory<byte>.Empty, "p"u8.ToArray()), Ct);
            await publisher.PublishAsync(transaction, [OutboxMessage.FromBytes(null, "p"u8.ToArray()), OutboxMessage.FromBytes(Array.Empty<byte>(), "p"u8.ToArray())], Ct);
            await transaction.CommitAsync(Ct);
        }

        var keys = await PostgreSqlFixture.QueryStringsAsync(
            postgres.ConnectionString,
            $"SELECT CASE WHEN message_key IS NULL THEN 'null' ELSE 'len' || octet_length(message_key) END FROM {table} ORDER BY id");
        Assert.Equal(["null", "len0", "null", "len0"], keys);
    }

    // USAGE on the schema and INSERT on the table, nothing else (no SELECT, no sequence grant).
    [Fact]
    public async Task Insert_only_role_publishes_single_and_batch_with_and_without_headers()
    {
        var table = await postgres.CreateOutboxAsync();
        var role = $"r_{Guid.NewGuid():N}";
        const string password = "insert-only";
        await postgres.ExecuteAsync($"CREATE ROLE {role} LOGIN PASSWORD '{password}'");
        await postgres.ExecuteAsync($"GRANT USAGE ON SCHEMA {table.Schema} TO {role}");
        await postgres.ExecuteAsync($"GRANT INSERT ON {table} TO {role}");
        var connectionString = new NpgsqlConnectionStringBuilder(postgres.ConnectionString) { Username = role, Password = password }.ConnectionString;
        var publisher = new PostgreSqlOutboxPublisher(table);
        var headers = OutboxHeaders.CreateBuilder().Add("h", "v").Build();

        await using (var connection = new NpgsqlConnection(connectionString))
        {
            await connection.OpenAsync(Ct);
            await using var transaction = await connection.BeginTransactionAsync(Ct);
            await publisher.PublishAsync(transaction, OutboxMessage.Create("s1", "p", headers), Ct);
            await publisher.PublishAsync(transaction, OutboxMessage.Create("s2", "p"), Ct);
            await publisher.PublishAsync(transaction, [OutboxMessage.Create("b1", "p", headers), OutboxMessage.Create("b2", "p")], Ct);
            await transaction.CommitAsync(Ct);
        }

        // The role cannot read the table, so the check runs as the superuser.
        var rows = await ReadRowsAsync(table);
        Assert.Equal(["s1", "s2", "b1", "b2"], Keys(rows));
        Assert.Equal(["{\"h\": \"v\"}", "{}", "{\"h\": \"v\"}", "{}"], rows.ConvertAll(row => row.Headers));
    }

    [Fact]
    public async Task Offset_slices_of_a_larger_array_round_trip_exactly()
    {
        var table = await postgres.CreateOutboxAsync();
        var publisher = new PostgreSqlOutboxPublisher(table);
        var buffer = new byte[256];
        for (var i = 0; i < buffer.Length; i++)
        {
            buffer[i] = (byte)i;
        }

        var key = new ReadOnlyMemory<byte>(buffer, 3, 7);
        var payload = new ReadOnlyMemory<byte>(buffer, 129, 100);
        var batchKey = new ReadOnlyMemory<byte>(buffer, 250, 6);
        var batchPayload = new ReadOnlyMemory<byte>(buffer, 1, 255);

        await using (var connection = await OpenAsync())
        await using (var transaction = await connection.BeginTransactionAsync(Ct))
        {
            await publisher.PublishAsync(transaction, OutboxMessage.FromBytes(key, payload), Ct);
            await publisher.PublishAsync(transaction, [OutboxMessage.FromBytes(batchKey, batchPayload), OutboxMessage.FromBytes(key, payload)], Ct);
            await transaction.CommitAsync(Ct);
        }

        var rows = await ReadRowsAsync(table);
        Assert.Equal(3, rows.Count);
        Assert.Equal(key.ToArray(), rows[0].Key);
        Assert.Equal(payload.ToArray(), rows[0].Payload);
        Assert.Equal(batchKey.ToArray(), rows[1].Key);
        Assert.Equal(batchPayload.ToArray(), rows[1].Payload);
        Assert.Equal(key.ToArray(), rows[2].Key);
        Assert.Equal(payload.ToArray(), rows[2].Payload);
    }

    [Fact]
    public async Task Cancelled_token_throws_and_writes_nothing_and_leaves_the_transaction_to_the_caller()
    {
        var table = await postgres.CreateOutboxAsync();
        var publisher = new PostgreSqlOutboxPublisher(table);
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        await using (var connection = await OpenAsync())
        await using (var transaction = await connection.BeginTransactionAsync(Ct))
        {
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => publisher.PublishAsync(transaction, OutboxMessage.Create("k", "p"), cancelled.Token));
            await Assert.ThrowsAnyAsync<OperationCanceledException>(
                () => publisher.PublishAsync(transaction, [OutboxMessage.Create("k", "p")], cancelled.Token));

            // Still the caller's open transaction: it commits, and nothing was written.
            Assert.Same(connection, transaction.Connection);
            await transaction.CommitAsync(Ct);
        }

        Assert.Empty(await ReadRowsAsync(table));
    }

    [Fact]
    public async Task One_publisher_serves_16_concurrent_transactions()
    {
        const int transactions = 16;
        var table = await postgres.CreateOutboxAsync();
        var publisher = new PostgreSqlOutboxPublisher(table);
        var headers = OutboxHeaders.CreateBuilder().Add("h", "v").Build();
        var txIds = new string[transactions];

        var tasks = new Task[transactions];
        for (var t = 0; t < transactions; t++)
        {
            var index = t;
            tasks[t] = Task.Run(async () =>
            {
                await using var connection = await OpenAsync();
                await using var transaction = await connection.BeginTransactionAsync(Ct);
                txIds[index] = await ScalarStringAsync(connection, transaction, "SELECT pg_current_xact_id()::text");
                await publisher.PublishAsync(transaction, OutboxMessage.Create($"t{index:D2}-0", $"p{index}", headers), Ct);
                await publisher.PublishAsync(
                    transaction,
                    [OutboxMessage.Create($"t{index:D2}-1", $"p{index}"), OutboxMessage.Create($"t{index:D2}-2", $"p{index}", headers)],
                    Ct);
                await transaction.CommitAsync(Ct);
            }, Ct);
        }

        await Task.WhenAll(tasks);

        var rows = await ReadRowsAsync(table);
        Assert.Equal(transactions * 3, rows.Count);
        for (var t = 0; t < transactions; t++)
        {
            var own = rows.FindAll(row => row.TxId == txIds[t]);
            Assert.Equal([$"t{t:D2}-0", $"t{t:D2}-1", $"t{t:D2}-2"], Keys(own));
            Assert.All(own, row => Assert.Equal(Encoding.UTF8.GetBytes($"p{t}"), row.Payload));
            Assert.Equal(["{\"h\": \"v\"}", "{}", "{\"h\": \"v\"}"], own.ConvertAll(row => row.Headers));
        }
    }

    private async Task<NpgsqlConnection> OpenAsync()
    {
        var connection = new NpgsqlConnection(postgres.ConnectionString);
        await connection.OpenAsync(Ct);
        return connection;
    }

    private static async Task ExecuteAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        await command.ExecuteNonQueryAsync(Ct);
    }

    private static async Task<string> ScalarStringAsync(NpgsqlConnection connection, NpgsqlTransaction transaction, string sql)
    {
        await using var command = new NpgsqlCommand(sql, connection, transaction);
        return (string)(await command.ExecuteScalarAsync(Ct))!;
    }

    private async Task<List<OutboxRow>> ReadRowsAsync(OutboxTable table)
    {
        await using var connection = await OpenAsync();
        await using var command = new NpgsqlCommand(
            $"SELECT id, tx_id::text, message_key, payload, headers::text FROM {table} ORDER BY id", connection);
        await using var reader = await command.ExecuteReaderAsync(Ct);
        var rows = new List<OutboxRow>();
        while (await reader.ReadAsync(Ct))
        {
            rows.Add(new OutboxRow(
                reader.GetInt64(0),
                reader.GetString(1),
                reader.IsDBNull(2) ? null : reader.GetFieldValue<byte[]>(2),
                reader.GetFieldValue<byte[]>(3),
                reader.GetString(4)));
        }

        return rows;
    }

    private static List<string?> Keys(List<OutboxRow> rows) =>
        rows.ConvertAll(row => row.Key is null ? null : Encoding.UTF8.GetString(row.Key));

    private static Dictionary<string, string> ParseHeaders(string json) =>
        JsonSerializer.Deserialize<Dictionary<string, string>>(json)!;

    private sealed record OutboxRow(long Id, string TxId, byte[]? Key, byte[] Payload, string Headers);

    // Counts Npgsql command activities started under a private root activity, so commands of tests running in
    // parallel (other traces) are not counted.
    private sealed class NpgsqlActivityRecorder : IDisposable
    {
        private readonly ConcurrentQueue<Activity> stopped = new();
        private readonly ActivityListener listener;
        private readonly Activity root;

        public NpgsqlActivityRecorder()
        {
            root = new Activity("outbox-publisher-test");
            listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == NpgsqlActivitySource,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) => ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = activity =>
                {
                    if (activity.TraceId == root.TraceId)
                    {
                        stopped.Enqueue(activity);
                    }
                },
            };
            ActivitySource.AddActivityListener(listener);
            root.SetIdFormat(ActivityIdFormat.W3C);
            root.Start();
        }

        public int Stop()
        {
            root.Stop();
            return stopped.Count;
        }

        public void Dispose()
        {
            if (Activity.Current == root)
            {
                root.Stop();
            }

            listener.Dispose();
        }
    }
}
