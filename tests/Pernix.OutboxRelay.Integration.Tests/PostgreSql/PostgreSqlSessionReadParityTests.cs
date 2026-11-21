using System.Globalization;
using System.Text;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.PostgreSql;

// Public IOutboxSession API only, so the same file runs on base. Expectations are fixed to base.
public sealed class PostgreSqlSessionReadParityTests(PostgreSqlFixture postgres)
{
    private const string Past = "'2000-01-01 00:00:00+00'";
    private const string Future = "'2999-01-01 00:00:00+00'";
    private const int LargeValueLength = 1024 * 1024;

    private static readonly DateTimeOffset CreatedAt = new DateTimeOffset(2026, 1, 2, 1, 4, 5, TimeSpan.Zero).AddTicks(1_234_560);
    private static readonly DateTimeOffset PastAt = new(2000, 1, 1, 0, 0, 0, TimeSpan.Zero);
    private static readonly DateTimeOffset FutureAt = new(2999, 1, 1, 0, 0, 0, TimeSpan.Zero);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData(OrderingMode.None)]
    [InlineData(OrderingMode.PerKey)]
    [InlineData(OrderingMode.Strict)]
    public async Task Read_maps_every_edge_row_column(OrderingMode mode)
    {
        var schema = PostgreSqlFixture.NewSchema();
        await postgres.CreateOutboxSchemaAsync(schema);
        var store = await PostgreSqlOutboxStoreTests.Provider(("Db", postgres.ConnectionString))
            .CreateStore(PostgreSqlOutboxStoreTests.Outbox("Orders", "Db", $"{schema}.outbox", mode))
            .ValidateAsync(Ct);

        // json keeps duplicate names and escapes as written. NULL headers are only reachable once the constraints are gone.
        await postgres.ExecuteAsync(
            $"""
            ALTER TABLE {schema}.outbox DROP CONSTRAINT outbox_headers_is_object, DROP CONSTRAINT outbox_header_values_are_strings,
                ALTER COLUMN headers DROP DEFAULT, ALTER COLUMN headers DROP NOT NULL, ALTER COLUMN headers TYPE json USING headers::json
            """);

        var rows = new List<Row>
        {
            await InsertAsync(schema, new Row("null key", Key: null, Payload: "p1", Headers: """'{"a":"1"}'""", Route: "orders.v1")),
            await InsertAsync(schema, new Row("empty payload", Key: [0x01], Payload: "", Headers: "'{}'")),
            await InsertAsync(schema, new Row("null headers due", Key: [0x02], Payload: "p3", Headers: "NULL")),
            await InsertAsync(schema, new Row(
                "null headers not due", Key: [0x03], Payload: "p4", Headers: "NULL", RetryCount: 1, NextAttemptAt: Future, LastError: "waiting")),
            await InsertAsync(schema, new Row("array headers", Key: [0x04], Payload: "p5", Headers: """'["x"]'""")),
            await InsertAsync(schema, new Row("non-string value", Key: [0x05], Payload: "p6", Headers: """'{"a":1}'""")),
            await InsertAsync(schema, new Row("duplicate names", Key: [0x06], Payload: "p7", Headers: """'{"a":"1","b":"2","a":"3"}'""")),
            await InsertAsync(schema, new Row("escaped name", Key: [0x07], Payload: "p8", Headers: """'{"é\u0001":"v"}'""")),
            await InsertAsync(schema, new Row(
                "blocked", Key: [0x08], Payload: "p9", Headers: """'{"h":"9"}'""", RetryCount: 2, NextAttemptAt: "'infinity'", LastError: "blocked")),
            await InsertAsync(schema, new Row(
                "failed head", Key: [0x09], Payload: "p10", Headers: """'{"h":"10"}'""", RetryCount: 1, NextAttemptAt: Future, LastError: "failed")),
            await InsertAsync(schema, new Row("behind failed key", Key: [0x09], Payload: "p11", Headers: """'{"h":"11"}'""", Route: "orders.v2")),
            await InsertAsync(schema, new Row("last error due", Key: [0x0a], Payload: "p12", Headers: "'{}'", RetryCount: 3, LastError: "boom")),
            await InsertAsync(schema, new Row(
                "large headers", Key: [0x0b], Payload: "p13", Headers: $"pg_catalog.json_build_object('big', pg_catalog.repeat('x', {LargeValueLength}))")),
        };

        var expected = rows.Where(r => mode != OrderingMode.None || r.Due).ToList();
        await using var session = await store.OpenSessionAsync(Ct);

        var read = await PostgreSqlOutboxSessionTests.ReadUntilAsync(session, new ReadRequest(100, null, []), expected.Count);

        Assert.Equal(expected.Select(r => r.Id), read.Select(m => m.Id));
        for (var i = 0; i < expected.Count; i++)
        {
            AssertRow(mode, expected[i], read[i]);
        }

        Assert.False(session.Lost.IsCancellationRequested);
    }

    private static void AssertRow(OrderingMode mode, Row row, OutboxMessage actual)
    {
        var isDue = mode switch
        {
            OrderingMode.None => true,
            OrderingMode.Strict => row.Due,
            _ => row.Due && row.Name != "behind failed key",
        };

        // PerKey omits payload, headers and route of a not-due row. A not-due NULL headers value reads as no headers.
        var omitted = mode == OrderingMode.PerKey && !isDue;
        var malformed = !omitted && row.Name is "null headers due" or "array headers" or "non-string value";
        (string Name, string Value)[] headers = omitted || malformed ? [] : row.Name switch
        {
            "null key" => [("a", "1")],
            "duplicate names" => [("a", "3"), ("b", "2")],
            "escaped name" => [("é\u0001", "v")],
            "blocked" => [("h", "9")],
            "failed head" => [("h", "10")],
            "behind failed key" => [("h", "11")],
            "large headers" => [("big", new string('x', LargeValueLength))],
            _ => [],
        };

        var because = $"{mode} row '{row.Name}'";
        Assert.True(row.Id == actual.Id, because);
        Assert.True(new OutboxPosition(row.Order, row.Id) == actual.Position, $"{because}: Position");
        Assert.True(Same(row.Key, actual.Key), $"{because}: Key");
        Assert.True(Same(omitted ? [] : Encoding.UTF8.GetBytes(row.Payload), actual.Payload), $"{because}: Payload");
        Assert.True(malformed == actual.HeadersMalformed, $"{because}: HeadersMalformed");
        Assert.True(headers.Length == actual.Headers.Count, $"{because}: header count {actual.Headers.Count}");
        for (var i = 0; i < headers.Length; i++)
        {
            Assert.True(headers[i].Name == actual.Headers[i].Name, $"{because}: header {i} name '{actual.Headers[i].Name}'");
            Assert.True(Same(Encoding.UTF8.GetBytes(headers[i].Value), actual.Headers[i].Value), $"{because}: header {i} value");
        }

        Assert.True(CreatedAt == actual.CreatedAt && actual.CreatedAt.Offset == TimeSpan.Zero, $"{because}: CreatedAt {actual.CreatedAt:O}");
        Assert.True(row.RetryCount == actual.RetryCount, $"{because}: RetryCount");
        DateTimeOffset? nextAttemptAt = row.NextAttemptAt switch
        {
            Past => PastAt,
            Future => FutureAt,
            _ => null,
        };
        Assert.True(nextAttemptAt == actual.NextAttemptAt, $"{because}: NextAttemptAt {actual.NextAttemptAt:O}");
        Assert.True(actual.NextAttemptAt is not { } next || next.Offset == TimeSpan.Zero, $"{because}: NextAttemptAt offset");
        Assert.True(isDue == actual.IsDue, $"{because}: IsDue");
        Assert.True(row.LastError == actual.LastError, $"{because}: LastError '{actual.LastError}'");
        Assert.True((omitted ? null : row.Route) == actual.Route, $"{because}: Route '{actual.Route}'");
    }

    private static bool Same(byte[]? expected, byte[]? actual) =>
        expected is null ? actual is null : actual is not null && expected.AsSpan().SequenceEqual(actual);

    private async Task<Row> InsertAsync(string schema, Row row)
    {
        var key = row.Key is null ? "NULL" : $"'\\x{Convert.ToHexString(row.Key)}'::bytea";
        var payload = $"'\\x{Convert.ToHexString(Encoding.UTF8.GetBytes(row.Payload))}'::bytea";
        var lastError = row.LastError is null ? "NULL" : $"'{row.LastError}'";
        var route = row.Route is null ? "NULL" : $"'{row.Route}'";
        var id = await postgres.ScalarAsync<long>(
            $"""
            INSERT INTO {schema}.outbox (message_key, payload, headers, created_at, retry_count, next_attempt_at, last_error, route)
            VALUES ({key}, {payload}, {row.Headers}, '2026-01-02 03:04:05.123456+02', {row.RetryCount}, {row.NextAttemptAt}, {lastError}, {route})
            RETURNING id
            """);
        var order = await postgres.ScalarAsync<string>($"SELECT tx_id::text FROM {schema}.outbox WHERE id = {id}");
        return row with { Id = id, Order = ulong.Parse(order!, CultureInfo.InvariantCulture) };
    }

    private sealed record Row(
        string Name,
        byte[]? Key,
        string Payload,
        string Headers,
        int RetryCount = 0,
        string NextAttemptAt = Past,
        string? LastError = null,
        string? Route = null)
    {
        public long Id { get; init; }

        public ulong Order { get; init; }

        public bool Due => NextAttemptAt == Past;
    }
}
