using System.Globalization;
using System.Text;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Leadership;
using Pernix.OutboxRelay.Integration.Tests.Kafka;
using Pernix.OutboxRelay.Integration.Tests.Leadership;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Npgsql;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.Delivery;

// Gate order source of truth: rows are inserted by serial transactions in ascending tag order, so (tx_id, id) order = tag order.
public sealed class OrderedDeliveryTests(PostgreSqlFixture postgres, KafkaFixture kafka)
{
    private const string PerKey = "PerKey";
    private const string Strict = "Strict";
    private const string TransientError = "injected transient fault";

    private static readonly TimeSpan DeliveryTimeout = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan AbsenceWait = TimeSpan.FromSeconds(4);
    private static readonly IReadOnlyDictionary<string, string> SmallMessages = new Dictionary<string, string> { ["max.message.bytes"] = "1024" };

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task PerKey_keeps_gate_order_per_key_under_transient_head_faults_and_other_keys_flow()
    {
        const int keys = 20;
        const int perKey = 50;
        var setup = await DeliverySetup.CreateAsync(postgres, kafka, orderingMode: PerKey, partitions: 3);
        var rules = new FaultRules();
        int[] faultedHeads = [1, 2, 3];
        var faults = faultedHeads.Select(tag => rules.Add(RelayFactory.DestinationName, Tag(tag), 3, Transient)).ToList();
        var midFault = rules.Add(RelayFactory.DestinationName, Tag(500), 2, Transient);

        var rows = Enumerable.Range(1, keys * perKey).Select(t => Row(KeyOf(t, keys), t, fault: true)).ToList();
        await setup.InsertAsync(rows);

        // Backoff 2 s + 4 s + 4 s holds the faulted keys long after the other keys drain.
        await using var relay = FaultInjectingDestinationFactory.Decorate(setup.Relay().WithRelaySettings(Backoff("00:00:02", "00:00:04")), rules);
        _ = relay.Services;

        var faultedKeys = faultedHeads.Select(t => KeyOf(t, keys)).Append(KeyOf(500, keys)).ToHashSet();
        Assert.True(
            await LeadershipTests.EventuallyAsync(async () => (await KeysInTableAsync(setup)).All(faultedKeys.Contains), DeliveryTimeout),
            "unfaulted keys not drained");
        Assert.Subset((await KeysInTableAsync(setup)).ToHashSet(), faultedHeads.Select(t => KeyOf(t, keys)).ToHashSet());

        var records = await ConsumeExactlyAsync(setup, setup.Topic, rows.Count);
        AssertPerKeyOrder(records, rows.Select(r => (Encoding.UTF8.GetString(r.Key!), TagOf(r))));
        Assert.All(faults, f => Assert.Equal(4, f.Sends));
        Assert.Equal(3, midFault.Sends);
        Assert.Equal(0, await setup.CountAsync());
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task PerKey_leader_loss_keeps_first_occurrences_in_gate_order_and_delivers_every_row(bool terminateBackend)
    {
        const int total = 2000;
        const int keys = 20;
        var setup = await DeliverySetup.CreateAsync(postgres, kafka, batchSize: 50, orderingMode: PerKey, partitions: 3);
        await using var first = setup.Relay();
        var firstStatus = first.Services.GetRequiredService<OutboxRuntimeState>().Outboxes.Single();
        Assert.True(await LeadershipTests.EventuallyAsync(() => firstStatus.IsLeader, TimeSpan.FromSeconds(10)), "first relay not leader");
        await using var second = setup.Relay();
        var secondStatus = second.Services.GetRequiredService<OutboxRuntimeState>().Outboxes.Single();

        using var inserter = new StreamingInserter(setup, total, chunk: 100, pause: TimeSpan.FromMilliseconds(50), keys: keys);
        Assert.True(await inserter.DeliveryStartedAsync(DeliveryTimeout), "no delivery before leader loss");

        if (terminateBackend)
        {
            var pid = await postgres.FindLockHolderPidAsync(setup.Table);
            Assert.NotNull(pid);
            await postgres.TerminateAsync(pid.Value);
            Assert.True(
                await LeadershipTests.EventuallyAsync(async () => await postgres.FindLockHolderPidAsync(setup.Table) is { } holder && holder != pid, TimeSpan.FromSeconds(15)),
                "no new leader session");
        }
        else
        {
            await first.Services.GetRequiredService<IHost>().StopAsync(CancellationToken.None);
            Assert.True(await LeadershipTests.EventuallyAsync(() => secondStatus.IsLeader, TimeSpan.FromSeconds(15)), "second relay not leader");
        }

        await inserter.CompleteAsync();
        Assert.True(await LeadershipTests.EventuallyAsync(async () => await setup.CountAsync() == 0, DeliveryTimeout), "table not empty");

        var records = await ConsumeDistinctAsync(setup, total);
        var gate = Enumerable.Range(1, total).Select(t => ($"k{t % keys}", t));
        AssertPerKeyOrder(DedupKeepFirst(records), gate);
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Strict_leader_loss_keeps_gate_order_and_delivers_every_row(bool terminateBackend)
    {
        const int total = 2000;
        const int keys = 20;
        const int batchSize = 50;
        var setup = await DeliverySetup.CreateAsync(postgres, kafka, batchSize: batchSize, orderingMode: Strict, partitions: 1);
        await using var first = setup.Relay();
        var firstStatus = first.Services.GetRequiredService<OutboxRuntimeState>().Outboxes.Single();
        Assert.True(await LeadershipTests.EventuallyAsync(() => firstStatus.IsLeader, TimeSpan.FromSeconds(10)), "first relay not leader");
        await using var second = setup.Relay();
        var secondStatus = second.Services.GetRequiredService<OutboxRuntimeState>().Outboxes.Single();

        using var inserter = new StreamingInserter(setup, total, chunk: 100, pause: TimeSpan.FromMilliseconds(50), keys: keys);
        Assert.True(await inserter.DeliveryStartedAsync(DeliveryTimeout), "no delivery before leader loss");

        if (terminateBackend)
        {
            var pid = await postgres.FindLockHolderPidAsync(setup.Table);
            Assert.NotNull(pid);
            await postgres.TerminateAsync(pid.Value);
            Assert.True(
                await LeadershipTests.EventuallyAsync(async () => await postgres.FindLockHolderPidAsync(setup.Table) is { } holder && holder != pid, TimeSpan.FromSeconds(15)),
                "no new leader session");
        }
        else
        {
            await first.Services.GetRequiredService<IHost>().StopAsync(CancellationToken.None);
            Assert.True(await LeadershipTests.EventuallyAsync(() => secondStatus.IsLeader, TimeSpan.FromSeconds(15)), "second relay not leader");
        }

        await inserter.CompleteAsync();
        Assert.True(await LeadershipTests.EventuallyAsync(async () => await setup.CountAsync() == 0, DeliveryTimeout), "table not empty");

        var records = await ConsumeDistinctAsync(setup, total);
        AssertOrder(Strict, DedupKeepFirst(records), Enumerable.Range(1, total).Select(t => ($"k{t % keys}", t)));

        // One loss event re-sends at most the old leader's held rows, which are the new leader's first rows.
        var duplicated = records.GroupBy(r => r.Tag).Where(g => g.Count() > 1).Select(g => g.Key).Order().ToList();
        TestContext.Current.TestOutputHelper?.WriteLine(
            $"terminateBackend={terminateBackend}: {records.Count - total} duplicate records over {duplicated.Count} tags" +
            (duplicated.Count > 0 ? $" ({duplicated[0]}..{duplicated[^1]})" : string.Empty));
        if (duplicated.Count > 0)
        {
            Assert.Equal(Enumerable.Range(duplicated[0], duplicated.Count), duplicated);
            Assert.InRange(duplicated.Count, 1, batchSize);
        }

        Assert.Equal(0, await setup.CountAsync());
    }

    [Fact]
    public async Task Strict_keeps_total_gate_order_including_after_retried_heads()
    {
        const int total = 200;
        var setup = await DeliverySetup.CreateAsync(postgres, kafka, orderingMode: Strict);
        var rules = new FaultRules();
        var firstHead = rules.Add(RelayFactory.DestinationName, Tag(1), 2, Transient);
        var midHead = rules.Add(RelayFactory.DestinationName, Tag(100), 2, Transient);
        var rows = Enumerable.Range(1, total).Select(t => Row(KeyOf(t, 5), t, fault: true)).ToList();
        await setup.InsertAsync(rows);

        await using var relay = FaultInjectingDestinationFactory.Decorate(setup.Relay(), rules);
        _ = relay.Services;

        var records = await ConsumeExactlyAsync(setup, setup.Topic, total);
        Assert.Equal(Enumerable.Range(1, total), records.Select(r => r.Tag));
        Assert.Equal(3, firstHead.Sends);
        Assert.Equal(3, midHead.Sends);
    }

    [Theory]
    [InlineData(PerKey, true)]
    [InlineData(PerKey, false)]
    [InlineData(Strict, true)]
    [InlineData(Strict, false)]
    public async Task Gate_holds_later_commits_behind_open_transaction(string mode, bool commitA)
    {
        var setup = await DeliverySetup.CreateAsync(postgres, kafka, orderingMode: mode);
        await using var relay = setup.Relay();
        var status = relay.Services.GetRequiredService<OutboxRuntimeState>().Outboxes.Single();
        Assert.True(await LeadershipTests.EventuallyAsync(() => status.IsLeader, TimeSpan.FromSeconds(10)), "no leader");

        await using var a = await setup.OpenAsync();
        await using var tx = await a.BeginTransactionAsync(Ct);
        await using (var insert = new NpgsqlCommand($"INSERT INTO {setup.Table} (message_key, payload) VALUES (convert_to('k', 'UTF8'), convert_to('1', 'UTF8'))", a, tx))
        {
            await insert.ExecuteNonQueryAsync(Ct);
        }

        await setup.InsertAsync([Row("k", 2), Row("k", 3), Row("k", 4)]);

        Assert.Empty(await KafkaFixture.ConsumeAllAsync(setup.BootstrapServers, setup.Topic, 1, AbsenceWait));
        Assert.Equal(3, await setup.CountAsync());

        if (commitA)
        {
            await tx.CommitAsync(Ct);
        }
        else
        {
            await tx.RollbackAsync(Ct);
        }

        int[] expected = commitA ? [1, 2, 3, 4] : [2, 3, 4];
        var records = await ConsumeExactlyAsync(setup, setup.Topic, expected.Length);
        Assert.Equal(expected, records.Select(r => r.Tag));
        Assert.True(await LeadershipTests.EventuallyAsync(async () => await setup.CountAsync() == 0, DeliveryTimeout), "table not empty");
    }

    [Theory]
    [InlineData(PerKey, true)]
    [InlineData(PerKey, false)]
    [InlineData(Strict, true)]
    [InlineData(Strict, false)]
    public async Task Operator_delete_or_reset_of_failed_head_delivers_later_rows_without_restart(string mode, bool delete)
    {
        var setup = await DeliverySetup.CreateAsync(postgres, kafka, orderingMode: mode, partitions: mode == PerKey ? 2 : 1);
        var rules = new FaultRules();
        rules.Add(RelayFactory.DestinationName, Tag(1), 1, Transient);
        var ids = await setup.InsertAsync([Row("a", 1, fault: true), Row("a", 2), Row("b", 3), Row("a", 4)]);

        // Ten-minute backoff: only operator action can release the failed head within the test.
        await using var relay = FaultInjectingDestinationFactory.Decorate(setup.Relay().WithRelaySettings(Backoff("00:10:00", "00:10:00")), rules);
        _ = relay.Services;

        Assert.True(
            await LeadershipTests.EventuallyAsync(async () => (await setup.FailureStatesAsync()).Any(s => s.Id == ids[0] && s.RetryCount == 1), DeliveryTimeout),
            "head not failed");

        int[] flowing = mode == PerKey ? [3] : [];
        var whileHeld = await ConsumeExactlyAsync(setup, setup.Topic, flowing.Length);
        Assert.Equal(flowing, whileHeld.Select(r => r.Tag));
        int[] held = mode == PerKey ? [1, 2, 4] : [1, 2, 3, 4];
        Assert.Equal(held, (await setup.TagsInTableAsync()).Order());

        await postgres.ExecuteAsync(delete
            ? $"DELETE FROM {setup.Table} WHERE id = {ids[0]}"
            : $"UPDATE {setup.Table} SET retry_count = 0, next_attempt_at = now() WHERE id = {ids[0]}");

        Assert.True(await LeadershipTests.EventuallyAsync(async () => await setup.CountAsync() == 0, TimeSpan.FromSeconds(15)), "later rows not delivered after operator action");
        var expected = (delete ? new[] { 2, 3, 4 } : [1, 2, 3, 4]).Select(t => (t == 3 ? "b" : "a", t)).ToList();
        var records = await ConsumeExactlyAsync(setup, setup.Topic, expected.Count);
        AssertOrder(mode, records, expected);
    }

    [Theory]
    [InlineData(PerKey, true)]
    [InlineData(PerKey, false)]
    [InlineData(Strict, true)]
    [InlineData(Strict, false)]
    public async Task Block_holds_scope_at_infinity_until_operator_delete_then_delivers_in_order(string mode, bool deadLetter)
    {
        var setup = await DeliverySetup.CreateAsync(
            postgres, kafka, topicConfigs: SmallMessages, deadLetter: deadLetter, orderingMode: mode, onPermanentFailure: "Block", partitions: mode == PerKey ? 2 : 1);
        await using var relay = setup.Relay();
        _ = relay.Services;

        // Tag 2 is oversized for the main topic: a permanent fault.
        var ids = await setup.InsertAsync([Row("a", 1), Row("a", 2, oversized: true), Row("b", 3), Row("a", 4), Row("b", 5)]);
        var blocked = ids[1];

        Assert.True(
            await LeadershipTests.EventuallyAsync(
                async () => await postgres.ScalarAsync<bool?>($"SELECT next_attempt_at = 'infinity' AND retry_count >= 1 FROM {setup.Table} WHERE id = {blocked}") == true,
                DeliveryTimeout),
            "row not blocked");

        if (deadLetter)
        {
            var dead = Assert.Single(await KafkaFixture.ConsumeAllAsync(setup.BootstrapServers, setup.DeadLetterTopic!, 1, DeliveryTimeout));
            Assert.Equal(blocked.ToString(CultureInfo.InvariantCulture), Header(dead.Message.Headers, "x-original-id"));
        }

        var flowing = mode == PerKey ? new[] { ("a", 1), ("b", 3), ("b", 5) } : [("a", 1)];
        AssertOrder(mode, await ConsumeExactlyAsync(setup, setup.Topic, flowing.Length), flowing);
        var held = mode == PerKey ? new[] { 2, 4 } : [2, 3, 4, 5];
        Assert.Equal(held, (await setup.TagsInTableAsync()).Order());
        Assert.True(await postgres.ScalarAsync<bool>($"SELECT next_attempt_at = 'infinity' FROM {setup.Table} WHERE id = {blocked}"), "blocked row released without operator action");

        await postgres.ExecuteAsync($"DELETE FROM {setup.Table} WHERE id = {blocked}");

        Assert.True(await LeadershipTests.EventuallyAsync(async () => await setup.CountAsync() == 0, TimeSpan.FromSeconds(15)), "held rows not delivered after operator delete");
        AssertOrder(mode, await ConsumeExactlyAsync(setup, setup.Topic, 4), [("a", 1), ("b", 3), ("a", 4), ("b", 5)]);
        if (deadLetter)
        {
            Assert.Single(await KafkaFixture.ConsumeAllAsync(setup.BootstrapServers, setup.DeadLetterTopic!, 2, AbsenceWait));
        }
    }

    [Theory]
    [InlineData(PerKey, true)]
    [InlineData(PerKey, false)]
    [InlineData(Strict, true)]
    [InlineData(Strict, false)]
    public async Task Continue_dead_letters_or_drops_failed_row_and_scope_proceeds_in_order(string mode, bool deadLetter)
    {
        var setup = await DeliverySetup.CreateAsync(
            postgres, kafka, topicConfigs: SmallMessages, deadLetter: deadLetter, orderingMode: mode, onPermanentFailure: "Continue", partitions: mode == PerKey ? 2 : 1);
        await using var relay = setup.Relay();
        _ = relay.Services;

        var ids = await setup.InsertAsync([Row("a", 1), Row("a", 2, oversized: true), Row("b", 3), Row("a", 4), Row("b", 5)]);

        Assert.True(await LeadershipTests.EventuallyAsync(async () => await setup.CountAsync() == 0, DeliveryTimeout), "table not empty");
        AssertOrder(mode, await ConsumeExactlyAsync(setup, setup.Topic, 4), [("a", 1), ("b", 3), ("a", 4), ("b", 5)]);
        if (deadLetter)
        {
            var dead = Assert.Single(await ConsumeExactlyAsync(setup, setup.DeadLetterTopic!, 1, raw: true));
            Assert.Equal(ids[1].ToString(CultureInfo.InvariantCulture), dead.Id);
        }
    }

    [Fact]
    public async Task PerKey_retried_head_due_before_the_next_summary_is_not_overtaken_by_later_rows_of_its_key_beyond_one_batch()
    {
        const int keys = 10;
        const int backlog = 2000;
        var setup = await DeliverySetup.CreateAsync(postgres, kafka, batchSize: 10, orderingMode: PerKey, partitions: 3);
        var rules = new FaultRules();
        var head = rules.Add(RelayFactory.DestinationName, Tag(1), 1, Transient);

        // The backlog walk outlasts the 1 s backoff and the 10 s interval delays the summary, so a read reaches the head's later rows
        // after the head is due but before any rewind.
        List<OutboxRow> rows = [Row("x", 1, fault: true), .. Enumerable.Range(2, backlog).Select(t => Row(KeyOf(t, keys), t))];
        rows.AddRange(Enumerable.Range(backlog + 2, 5).Select(t => Row("x", t)));
        await setup.InsertAsync(rows);

        var settings = Backoff("00:00:01", "00:00:01");
        settings["Outboxes:0:Polling:Interval"] = "00:00:10";
        await using var relay = FaultInjectingDestinationFactory.Decorate(setup.Relay().WithRelaySettings(settings), rules);
        _ = relay.Services;

        var records = await ConsumeExactlyAsync(setup, setup.Topic, rows.Count);
        AssertPerKeyOrder(records, rows.Select(r => (Encoding.UTF8.GetString(r.Key!), TagOf(r))));
        Assert.Equal(2, head.Sends);
        Assert.Equal(0, await setup.CountAsync());
    }

    [Fact]
    public async Task PerKey_blocked_key_with_a_backlog_of_twenty_batches_does_not_stop_other_keys()
    {
        const int batchSize = 50;
        const int backlog = 20 * batchSize;
        var setup = await DeliverySetup.CreateAsync(
            postgres, kafka, batchSize: batchSize, topicConfigs: SmallMessages, orderingMode: PerKey, onPermanentFailure: "Block", partitions: 2);

        var blocked = (await setup.InsertAsync([Row("k0", 1, oversized: true)]))[0];
        await setup.InsertTagsAsync(2, backlog, keys: 1);
        await using var relay = setup.Relay();
        _ = relay.Services;

        Assert.True(
            await LeadershipTests.EventuallyAsync(
                async () => await postgres.ScalarAsync<bool?>($"SELECT next_attempt_at = 'infinity' FROM {setup.Table} WHERE id = {blocked}") == true,
                DeliveryTimeout),
            "row not blocked");

        var first = backlog + 2;
        await setup.InsertAsync([Row("a", first), Row("b", first + 1)]);
        Assert.True(await LeadershipTests.EventuallyAsync(async () => await setup.CountAsync() == backlog + 1, DeliveryTimeout), "other keys not delivered");

        await setup.InsertAsync([Row("a", first + 2), Row("c", first + 3)]);
        Assert.True(await LeadershipTests.EventuallyAsync(async () => await setup.CountAsync() == backlog + 1, DeliveryTimeout), "later writes of other keys not delivered");

        var records = await ConsumeExactlyAsync(setup, setup.Topic, 4);
        AssertPerKeyOrder(records, [("a", first), ("b", first + 1), ("a", first + 2), ("c", first + 3)]);
        Assert.Equal(["k0"], await KeysInTableAsync(setup));
        Assert.True(await postgres.ScalarAsync<bool>($"SELECT next_attempt_at = 'infinity' FROM {setup.Table} WHERE id = {blocked}"), "blocked row released without operator action");
    }

    private static DeliveryResult Transient => new DeliveryResult.MessageFault(TransientError, Permanent: false);

    private static string Tag(int tag) => $"t{tag}";

    private static string KeyOf(int tag, int keys) => $"k{tag % keys}";

    private static int TagOf(OutboxRow row) => int.Parse(Encoding.UTF8.GetString(row.Payload).Trim(), CultureInfo.InvariantCulture);

    private static Dictionary<string, string?> Backoff(string initial, string max) => new()
    {
        ["Outboxes:0:Retry:InitialDelay"] = initial,
        ["Outboxes:0:Retry:MaxDelay"] = max,
    };

    private static OutboxRow Row(string key, int tag, bool fault = false, bool oversized = false)
    {
        var payload = tag.ToString(CultureInfo.InvariantCulture);
        var headers = fault ? new Dictionary<string, string> { [FaultRules.TagHeader] = Tag(tag) } : [];
        return new OutboxRow(Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(oversized ? payload.PadRight(2048) : payload), headers);
    }

    private static string? Header(Confluent.Kafka.Headers headers, string name) =>
        headers.TryGetLastBytes(name, out var value) ? Encoding.UTF8.GetString(value) : null;

    private static async Task<IReadOnlyList<string>> KeysInTableAsync(DeliverySetup setup)
    {
        await using var connection = await setup.OpenAsync();
        await using var command = new NpgsqlCommand($"SELECT DISTINCT convert_from(message_key, 'UTF8') FROM {setup.Table}", connection);
        await using var reader = await command.ExecuteReaderAsync(Ct);
        var keys = new List<string>();
        while (await reader.ReadAsync(Ct))
        {
            keys.Add(reader.GetString(0));
        }

        return keys;
    }

    // The absence wait proves no extra record follows.
    private static async Task<IReadOnlyList<Record>> ConsumeExactlyAsync(DeliverySetup setup, string topic, int expected, bool raw = false)
    {
        if (expected > 0)
        {
            Assert.Equal(expected, (await KafkaFixture.ConsumeAllAsync(setup.BootstrapServers, topic, expected, DeliveryTimeout)).Count);
        }

        var records = (await KafkaFixture.ConsumeAllAsync(setup.BootstrapServers, topic, expected + 1, AbsenceWait)).Select(r => ToRecord(r, raw)).ToList();
        Assert.Equal(expected, records.Count);
        return records;
    }

    // Leader loss redelivers, so consume until every tag 1..total is seen.
    private static async Task<IReadOnlyList<Record>> ConsumeDistinctAsync(DeliverySetup setup, int total)
    {
        var deadline = DateTime.UtcNow + DeliveryTimeout;
        var expected = total;
        while (true)
        {
            var records = (await KafkaFixture.ConsumeAllAsync(setup.BootstrapServers, setup.Topic, expected, deadline - DateTime.UtcNow)).Select(r => ToRecord(r, raw: false)).ToList();
            var distinct = records.Select(r => r.Tag).Distinct().Count();
            if (distinct >= total || DateTime.UtcNow >= deadline)
            {
                return records;
            }

            expected = records.Count + total - distinct;
        }
    }

    private static Record ToRecord(Confluent.Kafka.ConsumeResult<byte[], byte[]> result, bool raw) => new(
        Encoding.UTF8.GetString(result.Message.Key),
        raw ? 0 : int.Parse(Encoding.UTF8.GetString(result.Message.Value), CultureInfo.InvariantCulture),
        raw ? Header(result.Message.Headers, "x-original-id") : null);

    private static IReadOnlyList<Record> DedupKeepFirst(IEnumerable<Record> records)
    {
        var seen = new HashSet<int>();
        return [.. records.Where(r => seen.Add(r.Tag))];
    }

    private static void AssertOrder(string mode, IReadOnlyList<Record> records, IEnumerable<(string Key, int Tag)> gate)
    {
        if (mode == Strict)
        {
            Assert.Equal(gate.Select(g => g.Tag), records.Select(r => r.Tag));
            Assert.Equal(gate.Select(g => g.Key), records.Select(r => r.Key));
        }
        else
        {
            AssertPerKeyOrder(records, gate);
        }
    }

    private static void AssertPerKeyOrder(IReadOnlyList<Record> records, IEnumerable<(string Key, int Tag)> gate)
    {
        var expected = gate.GroupBy(g => g.Key).ToDictionary(g => g.Key, g => g.Select(x => x.Tag).ToList());
        var actual = records.GroupBy(r => r.Key).ToDictionary(g => g.Key, g => g.Select(x => x.Tag).ToList());
        Assert.Equal(expected.Keys.Order(), actual.Keys.Order());
        Assert.All(expected, e => Assert.Equal(e.Value, actual[e.Key]));
    }

    private sealed record Record(string Key, int Tag, string? Id);
}
