using System.Text;
using Pernix.OutboxRelay.Integration.Tests.Kafka;
using Pernix.OutboxRelay.Integration.Tests.Leadership;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Npgsql;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.Delivery;

// A Kafka destination with Topic has one address (Routes null), so a routed row is held and route-less rows of other keys flow.
public sealed class RoutingHoldTests(PostgreSqlFixture postgres, KafkaFixture kafka)
{
    private const string Route = "orders.eu";
    private const string HoldError = "route not mapped: " + Route;

    // Far longer than the test runs, so the row is held exactly once and never re-held.
    private static readonly TimeSpan MaxDelay = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan DeliveryTimeout = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan AbsenceWait = TimeSpan.FromSeconds(4);

    // Covers the gap between the hold commit and the state read: the 50 ms poll interval plus query latency.
    private static readonly TimeSpan ClockTolerance = TimeSpan.FromSeconds(2);

    [Fact]
    public async Task PerKey_routed_row_on_kafka_destination_is_held_and_route_less_rows_of_other_keys_are_delivered()
    {
        var setup = await DeliverySetup.CreateAsync(postgres, kafka, orderingMode: "PerKey");
        var backoff = new Dictionary<string, string?>
        {
            ["Outboxes:0:Retry:InitialDelay"] = "00:00:01",
            ["Outboxes:0:Retry:MaxDelay"] = MaxDelay.ToString("c", System.Globalization.CultureInfo.InvariantCulture),
        };

        var heldId = await InsertAsync(setup, "a", "routed", Route);
        await InsertAsync(setup, "b", "b1", route: null);
        await InsertAsync(setup, "c", "c1", route: null);

        await using var relay = setup.Relay().WithRelaySettings(backoff);
        _ = relay.Services;

        Assert.True(
            await LeadershipTests.EventuallyAsync(
                async () => await setup.FailureStatesAsync() is [var only] && only.Id == heldId && only.LastError == HoldError,
                DeliveryTimeout),
            "route-less rows not delivered or routed row not held");

        var held = await HeldStateAsync(setup, heldId);
        Assert.Equal(HoldError, held.LastError);
        Assert.Equal(1, held.RetryCount);

        // The single hold wrote next_attempt_at = its now() + MaxDelay, at most ClockTolerance before this read.
        Assert.InRange(held.NextAttemptAt, held.Now + MaxDelay - ClockTolerance, held.Now + MaxDelay);

        var records = await KafkaFixture.ConsumeAllAsync(setup.BootstrapServers, setup.Topic, 3, AbsenceWait);
        Assert.Equal(
            ["b:b1", "c:c1"],
            records.Select(r => $"{Encoding.UTF8.GetString(r.Message.Key)}:{Encoding.UTF8.GetString(r.Message.Value)}").Order(StringComparer.Ordinal));
    }

    private static async Task<long> InsertAsync(DeliverySetup setup, string key, string payload, string? route)
    {
        await using var connection = await setup.OpenAsync();
        await using var command = new NpgsqlCommand(
            $"INSERT INTO {setup.Table} (message_key, payload, route) VALUES (convert_to(@key, 'UTF8'), convert_to(@payload, 'UTF8'), @route) RETURNING id",
            connection);
        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("payload", payload);
        command.Parameters.Add(new NpgsqlParameter("route", NpgsqlTypes.NpgsqlDbType.Text) { Value = (object?)route ?? DBNull.Value });
        return (long)(await command.ExecuteScalarAsync(TestContext.Current.CancellationToken))!;
    }

    private static async Task<HeldState> HeldStateAsync(DeliverySetup setup, long id)
    {
        await using var connection = await setup.OpenAsync();
        await using var command = new NpgsqlCommand($"SELECT retry_count, last_error, next_attempt_at, now() FROM {setup.Table} WHERE id = @id", connection);
        command.Parameters.AddWithValue("id", id);
        await using var reader = await command.ExecuteReaderAsync(TestContext.Current.CancellationToken);
        Assert.True(await reader.ReadAsync(TestContext.Current.CancellationToken), $"row {id} gone");
        return new HeldState(reader.GetInt32(0), reader.IsDBNull(1) ? null : reader.GetString(1), reader.GetDateTime(2), reader.GetDateTime(3));
    }

    private sealed record HeldState(int RetryCount, string? LastError, DateTime NextAttemptAt, DateTime Now);
}
