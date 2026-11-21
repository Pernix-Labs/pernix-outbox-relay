using System.Diagnostics;
using System.Globalization;
using System.Text;
using Pernix.OutboxRelay.Core.Leadership;
using Pernix.OutboxRelay.Integration.Tests.Kafka;
using Pernix.OutboxRelay.Integration.Tests.Leadership;
using DotNet.Testcontainers.Builders;
using DotNet.Testcontainers.Containers;
using Microsoft.Extensions.DependencyInjection;
using Npgsql;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.Delivery;

// Opt-in SIGKILL variant, excluded by trait. Needs `docker build -t outbox-relay:local .` first:
// OUTBOX_RELAY_LEADER_KILL_TEST=1 dotnet test --project tests/Pernix.OutboxRelay.Integration.Tests -- --filter-trait "Category=LeaderKill"
// Gate order source of truth: rows are inserted by serial transactions in ascending tag order, so (tx_id, id) order = tag order.
[Trait("Category", "LeaderKill")]
public sealed class StrictLeaderKillTests
{
    private const string EnvironmentVariable = "OUTBOX_RELAY_LEADER_KILL_TEST";
    private const string ImageVariable = "OUTBOX_RELAY_TEST_IMAGE";
    private const string DefaultImage = "outbox-relay:local";

    // The container keeps the default application_name, so the in-process relay must not share it.
    private const string ContainerApplicationName = "outbox-relay";
    private const string InProcessApplicationName = "outbox-relay-in-process";

    private const int Total = 2000;
    private const int Keys = 20;
    private const int BatchSize = 50;
    private const int Chunk = 100;

    private static readonly TimeSpan InsertPause = TimeSpan.FromMilliseconds(50);
    private static readonly TimeSpan LeaderTimeout = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan DeliveryTimeout = TimeSpan.FromSeconds(60);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Strict_leader_sigkill_keeps_gate_order_and_delivers_every_row()
    {
        Assert.SkipUnless(Environment.GetEnvironmentVariable(EnvironmentVariable) == "1", $"Opt-in leader-kill test: set {EnvironmentVariable}=1.");
        var image = Environment.GetEnvironmentVariable(ImageVariable) is { Length: > 0 } configured ? configured : DefaultImage;

        await using var fixture = await LeaderKillFixture.StartAsync();
        var schema = await fixture.CreateOutboxAsync();
        var table = $"{schema}.outbox";
        var topic = KafkaFixture.NewTopic();
        await fixture.CreateTopicAsync(topic, 1);
        var outbox = RelayFactory.NewOutboxName();

        var containerSettings = Settings(LeaderKillFixture.NetworkConnectionString, LeaderKillFixture.NetworkBootstrapServers, outbox, schema, topic);
        var builder = new ContainerBuilder(image)
            .WithNetwork(fixture.Network);
        foreach (var (key, value) in containerSettings)
        {
            builder = builder.WithEnvironment(key.Replace(":", "__", StringComparison.Ordinal), value!);
        }

        await using var leader = builder.Build();
        await leader.StartAsync(Ct);
        try
        {
            Assert.True(
                await LeadershipTests.EventuallyAsync(async () => await fixture.FindLockHolderPidAsync(table, ContainerApplicationName) is not null, LeaderTimeout),
                "container relay not leader");

            // Started before inserts so the kill follows the records-flowing check directly, with a backlog left.
            var inProcessConnection = new NpgsqlConnectionStringBuilder(fixture.ConnectionString) { ApplicationName = InProcessApplicationName }.ConnectionString;
            await using var second = new RelayFactory().WithRelaySettings(Settings(inProcessConnection, fixture.BootstrapServers, outbox, schema, topic));
            var secondStatus = second.Services.GetRequiredService<OutboxRuntimeState>().Outboxes.Single();
            Assert.False(secondStatus.IsLeader, "in-process relay leader before kill");

            using var stopInserts = new CancellationTokenSource();
            var inserted = 0;
            var inserter = Task.Run(async () =>
            {
                while (inserted < Total && !stopInserts.IsCancellationRequested)
                {
                    var count = Math.Min(Chunk, Total - inserted);
                    await InsertTagsAsync(fixture.ConnectionString, table, inserted + 1, count);
                    Interlocked.Add(ref inserted, count);
                    await Task.Delay(InsertPause, Ct);
                }
            }, Ct);

            try
            {
                Assert.True(
                    await LeadershipTests.EventuallyAsync(
                        async () =>
                        {
                            var snapshot = Volatile.Read(ref inserted);
                            return snapshot > 0 && await CountAsync(fixture.ConnectionString, table) < snapshot;
                        },
                        DeliveryTimeout),
                    "no delivery before leader kill");
                Assert.NotNull(await fixture.FindLockHolderPidAsync(table, ContainerApplicationName));

                var insertedAtKill = Volatile.Read(ref inserted);
                var remainingAtKill = await CountAsync(fixture.ConnectionString, table);
                Assert.True(remainingAtKill > 0, "no backlog at kill");
                await DockerKillAsync(leader.Id);
                Assert.True(await LeadershipTests.EventuallyAsync(() => secondStatus.IsLeader, LeaderTimeout), "in-process relay not leader after kill");

                await inserter;
                Assert.True(await LeadershipTests.EventuallyAsync(async () => await CountAsync(fixture.ConnectionString, table) == 0, DeliveryTimeout), "table not empty");

                var records = await ConsumeDistinctAsync(fixture.BootstrapServers, topic);
                var first = DedupKeepFirst(records);
                Assert.Equal(Enumerable.Range(1, Total), first.Select(r => r.Tag));
                Assert.Equal(Enumerable.Range(1, Total).Select(t => $"k{t % Keys}"), first.Select(r => r.Key));

                // One loss event re-sends at most the old leader's held rows, which are the new leader's first rows.
                var duplicated = records.GroupBy(r => r.Tag).Where(g => g.Count() > 1).Select(g => g.Key).Order().ToList();
                TestContext.Current.TestOutputHelper?.WriteLine(
                    $"sigkill: inserted {insertedAtKill}, remaining {remainingAtKill} at kill; {records.Count - Total} duplicate records over {duplicated.Count} tags" +
                    (duplicated.Count > 0 ? $" ({duplicated[0]}..{duplicated[^1]})" : string.Empty));
                if (duplicated.Count > 0)
                {
                    Assert.Equal(Enumerable.Range(duplicated[0], duplicated.Count), duplicated);
                    Assert.InRange(duplicated.Count, 1, BatchSize);
                }

                Assert.Equal(0, await CountAsync(fixture.ConnectionString, table));
            }
            finally
            {
                await stopInserts.CancelAsync();
                await inserter;
            }
        }
        catch
        {
            await WriteLogsAsync(leader);
            throw;
        }
    }

    private static Dictionary<string, string?> Settings(string connectionString, string bootstrapServers, string outbox, string schema, string topic)
    {
        var settings = RelayFactory.OutboxSettings(connectionString, bootstrapServers, (outbox, schema));
        settings["Destinations:0:Kafka:Topic"] = topic;
        settings["Outboxes:0:Retry:InitialDelay"] = "00:00:01";
        settings["Outboxes:0:Retry:MaxDelay"] = "00:00:02";
        settings["Outboxes:0:Ordering:Mode"] = "Strict";
        settings["Outboxes:0:Polling:BatchSize"] = BatchSize.ToString(CultureInfo.InvariantCulture);
        return settings;
    }

    // Through the Docker CLI: Testcontainers stops with SIGTERM only.
    private static async Task DockerKillAsync(string containerId)
    {
        using var process = Process.Start(new ProcessStartInfo("docker", ["kill", "--signal", "KILL", containerId])
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        })!;
        var error = process.StandardError.ReadToEndAsync(Ct);
        await process.StandardOutput.ReadToEndAsync(Ct);
        await process.WaitForExitAsync(Ct);
        Assert.True(process.ExitCode == 0, $"docker kill exited {process.ExitCode}: {await error}");
    }

    private static async Task WriteLogsAsync(IContainer container)
    {
        try
        {
            var (stdout, stderr) = await container.GetLogsAsync(ct: CancellationToken.None);
            TestContext.Current.TestOutputHelper?.WriteLine($"----- relay container logs -----\n{stdout}\n{stderr}");
        }
        catch (Exception ex)
        {
            TestContext.Current.TestOutputHelper?.WriteLine($"relay container logs unavailable: {ex.Message}");
        }
    }

    private static async Task InsertTagsAsync(string connectionString, string table, int first, int count)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand(
            $"INSERT INTO {table} (message_key, payload) SELECT convert_to('k' || (g % @keys), 'UTF8'), convert_to(g::text, 'UTF8') FROM generate_series(@first, @last) g ORDER BY g",
            connection);
        command.Parameters.AddWithValue("keys", Keys);
        command.Parameters.AddWithValue("first", first);
        command.Parameters.AddWithValue("last", first + count - 1);
        await command.ExecuteNonQueryAsync();
    }

    private static async Task<long> CountAsync(string connectionString, string table)
    {
        await using var connection = new NpgsqlConnection(connectionString);
        await connection.OpenAsync();
        await using var command = new NpgsqlCommand($"SELECT count(*) FROM {table}", connection);
        return (long)(await command.ExecuteScalarAsync())!;
    }

    private static async Task<IReadOnlyList<Record>> ConsumeDistinctAsync(string bootstrapServers, string topic)
    {
        var deadline = DateTime.UtcNow + DeliveryTimeout;
        var expected = Total;
        while (true)
        {
            var records = (await KafkaFixture.ConsumeAllAsync(bootstrapServers, topic, expected, deadline - DateTime.UtcNow))
                .Select(r => new Record(Encoding.UTF8.GetString(r.Message.Key), int.Parse(Encoding.UTF8.GetString(r.Message.Value), CultureInfo.InvariantCulture)))
                .ToList();
            var distinct = records.Select(r => r.Tag).Distinct().Count();
            if (distinct >= Total || DateTime.UtcNow >= deadline)
            {
                return records;
            }

            expected = records.Count + Total - distinct;
        }
    }

    private static List<Record> DedupKeepFirst(IEnumerable<Record> records)
    {
        var seen = new HashSet<int>();
        return [.. records.Where(r => seen.Add(r.Tag))];
    }

    private sealed record Record(string Key, int Tag);
}
