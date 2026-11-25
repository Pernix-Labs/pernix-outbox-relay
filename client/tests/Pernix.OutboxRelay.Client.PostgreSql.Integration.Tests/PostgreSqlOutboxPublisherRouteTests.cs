using System.Collections.Concurrent;
using System.Diagnostics;
using System.Text;
using System.Transactions;
using Npgsql;
using Xunit;

namespace Pernix.OutboxRelay.Client.PostgreSql.Integration.Tests;

// The publisher writes each message's route. Tables come from the package DDL only.
public sealed class PostgreSqlOutboxPublisherRouteTests(PostgreSqlFixture postgres)
{
    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Theory]
    [MemberData(nameof(RoutePublish.Methods), MemberType = typeof(RoutePublish))]
    public async Task Routed_rows_hold_their_route_and_route_less_rows_hold_null(RoutePublishMethod method)
    {
        var table = await postgres.CreateOutboxAsync();
        var publisher = new PostgreSqlOutboxPublisher(table);
        var headers = OutboxHeaders.CreateBuilder().Add("h", "v").Build();
        OutboxMessage[] messages =
        [
            OutboxMessage.Create("k1", "p").WithRoute("alerts"),
            OutboxMessage.Create("k2", "p"),
            OutboxMessage.Create("k3", "p", headers).WithRoute("audit.eu"),
            OutboxMessage.Create("k4", "p", headers),
        ];

        Assert.Null(await RoutePublish.PublishThenCommitAsync(postgres.ConnectionString, publisher, method, messages));

        var rows = await RoutePublish.ReadRowsAsync(postgres.ConnectionString, table);
        Assert.Equal([("k1", "alerts"), ("k2", null), ("k3", "audit.eu"), ("k4", null)], rows.ConvertAll(row => (row.Key, row.Route)));
        Assert.Equal(["{}", "{}", "{\"h\": \"v\"}", "{\"h\": \"v\"}"], rows.ConvertAll(row => row.Headers));
    }

    [Theory]
    [InlineData(RoutePublishMethod.TransactionBatch)]
    [InlineData(RoutePublishMethod.EnlistedBatch)]
    public async Task Mixed_batch_writes_per_row_routes_in_one_round_trip_with_ids_in_list_order(RoutePublishMethod method)
    {
        var table = await postgres.CreateOutboxAsync();
        var publisher = new PostgreSqlOutboxPublisher(table);
        var headers = OutboxHeaders.CreateBuilder().Add("h", "v").Build();
        var messages = new OutboxMessage[40];
        var expected = new (string Key, string? Route)[messages.Length];
        for (var i = 0; i < messages.Length; i++)
        {
            var key = $"k{messages.Length - i:D3}";
            var message = OutboxMessage.Create(key, $"p{i}", i % 3 == 0 ? headers : null);
            var route = (i % 4) switch { 0 => null, 1 => "alerts", 2 => "audit", _ => "alerts" };
            messages[i] = route is null ? message : message.WithRoute(route);
            expected[i] = (key, route);
        }

        // The enlisted path has its own pool. Warm it so a physical connect is not counted as a round trip.
        var connectionString = method == RoutePublishMethod.EnlistedBatch
            ? new NpgsqlConnectionStringBuilder(postgres.ConnectionString) { Enlist = true }.ConnectionString
            : postgres.ConnectionString;
        await using (var warm = new NpgsqlConnection(connectionString))
        {
            await warm.OpenAsync(Ct);
        }

        using (var activities = new NpgsqlActivityRecorder())
        {
            Assert.Null(await RoutePublish.PublishThenCommitAsync(postgres.ConnectionString, publisher, method, messages));
            Assert.Equal(1, activities.Stop());
        }

        var rows = await RoutePublish.ReadRowsAsync(postgres.ConnectionString, table);
        Assert.Equal(expected, rows.ConvertAll(row => (row.Key, row.Route)));
        for (var i = 1; i < rows.Count; i++)
        {
            Assert.True(rows[i].Id > rows[i - 1].Id);
        }
    }

    private sealed class NpgsqlActivityRecorder : IDisposable
    {
        private readonly ConcurrentQueue<Activity> stopped = new();
        private readonly ActivityListener listener;
        private readonly Activity root;

        public NpgsqlActivityRecorder()
        {
            root = new Activity("outbox-route-test");
            listener = new ActivityListener
            {
                ShouldListenTo = source => source.Name == "Npgsql",
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
            root.Stop();
            listener.Dispose();
        }
    }
}

public enum RoutePublishMethod
{
    Transaction,
    TransactionBatch,
    Enlisted,
    EnlistedBatch,
}

public sealed record RouteRow(long Id, string Key, string? Route, string Headers);

internal static class RoutePublish
{
    public static TheoryData<RoutePublishMethod> Methods =>
    [
        RoutePublishMethod.Transaction,
        RoutePublishMethod.TransactionBatch,
        RoutePublishMethod.Enlisted,
        RoutePublishMethod.EnlistedBatch,
    ];

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    // Commits after an ArgumentException too, so a refused message proves the publisher wrote nothing.
    public static async Task<Exception?> PublishThenCommitAsync(
        string connectionString, IOutboxPublisher publisher, RoutePublishMethod method, params OutboxMessage[] messages)
    {
        if (method is RoutePublishMethod.Transaction or RoutePublishMethod.TransactionBatch)
        {
            await using var connection = new NpgsqlConnection(connectionString);
            await connection.OpenAsync(Ct);
            await using var transaction = await connection.BeginTransactionAsync(Ct);
            var exception = await CaptureAsync(method == RoutePublishMethod.Transaction
                ? async () =>
                {
                    foreach (var message in messages)
                    {
                        await publisher.PublishAsync(transaction, message, Ct);
                    }
                }
                : () => publisher.PublishAsync(transaction, messages, Ct));
            if (exception is null or ArgumentException)
            {
                await transaction.CommitAsync(Ct);
            }

            return exception;
        }

        var enlisting = new NpgsqlConnectionStringBuilder(connectionString) { Enlist = true }.ConnectionString;
        using (var scope = new TransactionScope(TransactionScopeOption.Required, TransactionScopeAsyncFlowOption.Enabled))
        {
            await using var connection = new NpgsqlConnection(enlisting);
            await connection.OpenAsync(Ct);
            var exception = await CaptureAsync(method == RoutePublishMethod.Enlisted
                ? async () =>
                {
                    foreach (var message in messages)
                    {
                        await publisher.PublishEnlistedAsync(connection, message, Ct);
                    }
                }
                : () => publisher.PublishEnlistedAsync(connection, messages, Ct));
            if (exception is null or ArgumentException)
            {
                scope.Complete();
            }

            return exception;
        }
    }

    public static async Task<List<RouteRow>> ReadRowsAsync(string connectionString, OutboxTable table)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync(Ct);
        await using var command = new NpgsqlCommand(
            $"SELECT id, message_key, route, headers::text FROM {table} ORDER BY id", connection);
        await using var reader = await command.ExecuteReaderAsync(Ct);
        var rows = new List<RouteRow>();
        while (await reader.ReadAsync(Ct))
        {
            rows.Add(new RouteRow(
                reader.GetInt64(0),
                Encoding.UTF8.GetString(reader.GetFieldValue<byte[]>(1)),
                reader.IsDBNull(2) ? null : reader.GetString(2),
                reader.GetString(3)));
        }

        return rows;
    }

    private static async Task<Exception?> CaptureAsync(Func<Task> publish)
    {
        try
        {
            await publish();
            return null;
        }
        catch (Exception exception) when (exception is ArgumentException or PostgresException)
        {
            return exception;
        }
    }
}
