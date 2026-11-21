using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Integration.Tests.Delivery;
using Pernix.OutboxRelay.Integration.Tests.Kafka;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Confluent.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Npgsql;
using Xunit;
using static Pernix.OutboxRelay.Integration.Tests.Pilot.PilotDatasets;

namespace Pernix.OutboxRelay.Integration.Tests.Pilot;

// A real relay and Kafka. Explicit. Prints figures.
public sealed class PilotDeliveryDelayTests(PostgreSqlFixture postgres, KafkaFixture kafka)
{
    private const int BatchSize = 500;
    private const string ChurnTag = "pilot-churn";
    private const string ChurnManyTag = "pilot-churn-many";
    private const int StreamRate = 50;
    private const int OtherKeys = 100;
    private const double T1Seconds = 30;
    private const double T2Seconds = 10;
    private const double FromRewindLimitSeconds = 30;
    private const int LargeBacklog = 1_000_000;
    private const double M3LimitSeconds = 60;
    private const int M3GatedHotKey = 1_000;
    private const double DrainCapRate = 100;
    private const double DrainCapFactor = 1.5;

    private static readonly int[] BlockedBacklogs = [100_000, 1_000_000];
    private static readonly int[] RewalkBatchSizes = [500, 1000];
    private static readonly int[] HotKeyBacklogs = [0, 1_000, 10_000];
    private static readonly TimeSpan StreamDuration = TimeSpan.FromSeconds(120);
    private static readonly TimeSpan StreamCollectTimeout = TimeSpan.FromSeconds(180);
    private static readonly TimeSpan StreamCutoff = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan SettleTimeout = TimeSpan.FromMinutes(30);
    private static readonly TimeSpan MarkerTimeout = TimeSpan.FromSeconds(120);
    private static readonly TimeSpan DrainTimeout = TimeSpan.FromMinutes(10);
    private static readonly TimeSpan RewindWindow = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan PhaseSampleInterval = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan DrainStallTimeout = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan BlockedRateWindow = TimeSpan.FromSeconds(120);
    private static readonly Func<OutgoingMessage, TimeSpan> ChurnManyDelay = ChurnManyFaultDelay;
    private static readonly byte[] BlockedKeyBytes = Encoding.UTF8.GetBytes(BlockedKey);
    private static readonly byte[][] MarkerKeys = [Encoding.UTF8.GetBytes(MarkerSingleKey), Encoding.UTF8.GetBytes(MarkerChurnKey), Encoding.UTF8.GetBytes(MarkerChurnManyKey)];
    private static readonly byte[] HotKey = "hot"u8.ToArray();

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact(Explicit = true)]
    public async Task Blocked_key_rewalk_delivery_delay()
    {
        var streamed = (int)(StreamRate * StreamDuration.TotalSeconds);
        var output = new StringBuilder()
            .AppendLine(CultureInfo.InvariantCulture, $"Blocked-key re-walk delivery delay: PerKey, Polling.Interval 1 s, Retry delay 1 s, Kafka 1 partition, churn-many {ChurnManyKeys} keys")
            .AppendLine(CultureInfo.InvariantCulture, $"Server: {await ServerAsync(postgres)}")
            .AppendLine("From rewind = marker Kafka timestamp - first DeliveryRewound (124) after the release, or the last one before the marker when the marker beat it.")
            .AppendLine("From release = marker Kafka timestamp - release commit start. Rewinds = DeliveryRewound events from the release to the marker.")
            .AppendLine(CultureInfo.InvariantCulture, $"Stream = {StreamRate} msg/s over {OtherKeys} keys for {StreamDuration.TotalSeconds:0} s after the marker, rows with payload time >= stream start - {StreamCutoff.TotalSeconds:0} s. Delay = Kafka CreateTime - payload time.")
            .AppendLine("Reasons and per 60 s = DeliveryRewound from churn start to stream end (recorded only).")
            .AppendLine(CultureInfo.InvariantCulture, $"Verdict: 1M marker <= {T1Seconds:0} s from release, churn stream {streamed:N0}/{streamed:N0}, p95 <= {T2Seconds:0} s, max <= {T1Seconds:0} s. 100k marker <= {FromRewindLimitSeconds:0} s from rewind and <= {T1Seconds:0} s from release.")
            .AppendLine("| N | BatchSize | Case | Settle s | From rewind s | From release s | Rewinds | Stream rows | p50 s | p95 s | max s | Rewinds by reason | Rewinds per 60 s | Verdict |")
            .AppendLine("|---:|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|---|---|---|");
        var phases = new StringBuilder()
            .AppendLine(CultureInfo.InvariantCulture, $"Churn-many phases: distinct next_attempt_at over the {ChurnManyKeys} churn-many rows, sampled every {PhaseSampleInterval.TotalSeconds:0} s from the third retry cycle to stream end. Expect about {ChurnManyKeys}.")
            .AppendLine("| N | BatchSize | Samples | Min | Median | Max |")
            .AppendLine("|---:|---:|---:|---:|---:|---:|");
        var drains = new StringBuilder()
            .AppendLine("Final drain: skip the blocked head and the churn-many rows, then by mode:")
            .AppendLine(CultureInfo.InvariantCulture, $"- full (N < {LargeBacklog:N0}): wait for 0 rows. Fails only if the row count does not fall for {DrainStallTimeout.TotalSeconds:0} s, cap N / {DrainCapRate:0} msg/s x {DrainCapFactor}.")
            .AppendLine(CultureInfo.InvariantCulture, $"- window (N >= {LargeBacklog:N0}): same wait for 0 rows not of key '{BlockedKey}', record the '{BlockedKey}' drain over {BlockedRateWindow.TotalSeconds:0} s, delete the rest of the key (docs/delivery.md 'Skip a message'), require 0 rows.")
            .AppendLine("Then scan the topic from the settle point. Order = delivered blocked rows strictly increasing by id. Drain s = skip to 0 rows.")
            .AppendLine("| N | BatchSize | Mode | Drain s | Blocked delivered | Blocked order | Blocked msg/s | Blocked in window | Blocked deleted | Markers | Stream rows | Verdict |")
            .AppendLine("|---:|---:|---|---:|---:|---|---:|---:|---:|---:|---:|---|");

        try
        {
            foreach (var n in BlockedBacklogs)
            {
                foreach (var batchSize in RewalkBatchSizes)
                {
                    await RewalkCellAsync(n, batchSize, streamed, output, phases, drains);
                }
            }
        }
        finally
        {
            TestContext.Current.TestOutputHelper!.WriteLine(output.AppendLine().Append(phases).AppendLine().Append(drains).ToString());
        }
    }

    private async Task RewalkCellAsync(int n, int batchSize, int streamed, StringBuilder output, StringBuilder phases, StringBuilder drains)
    {
        var setup = await DeliverySetup.CreateAsync(postgres, kafka, batchSize: batchSize, maxRetryAttempts: 1_000_000, orderingMode: "PerKey");
        await SeedRewalkAsync(postgres, setup.Schema, n, FaultRules.TagHeader, ChurnTag, ChurnManyTag);

        var rules = new FaultRules();
        var churn = rules.Add(RelayFactory.DestinationName, ChurnTag, int.MaxValue, new DeliveryResult.MessageFault("pilot churn", Permanent: false));
        var churnMany = rules.Add(RelayFactory.DestinationName, ChurnManyTag, int.MaxValue, new DeliveryResult.MessageFault("pilot churn-many", Permanent: false), ChurnManyDelay);
        var rewinds = new RewindLog();
        var settled = Stopwatch.StartNew();
        await using var relay = FaultInjectingDestinationFactory.Decorate(setup.Relay(), rules)
            .WithRelaySettings(new Dictionary<string, string?>
            {
                ["Outboxes:0:Retry:InitialDelay"] = "00:00:01",
                ["Outboxes:0:Retry:MaxDelay"] = "00:00:01",
                ["Outboxes:0:Retry:BackoffMultiplier"] = "1",
            })
            .WithServices(s =>
            {
                s.AddSingleton<ILoggerProvider>(rewinds);
                s.Configure<LoggerFilterOptions>(o => o.AddFilter<RewindLog>(null, LogLevel.Debug));
            });
        _ = relay.Services;

        // Only the blocked key and the extras left: the cursor has passed the backlog and every marker.
        await UntilAsync(async () => await CountAsync(postgres, setup.Schema) == n + RewalkExtraRows, SettleTimeout, $"N = {n:N0}, BatchSize {batchSize} did not settle");
        var settleSeconds = settled.Elapsed.TotalSeconds;
        var settledAt = HighWatermark(setup.BootstrapServers, setup.Topic);
        var cell = string.Create(CultureInfo.InvariantCulture, $"| {n:N0} | {batchSize} |");

        var single = await MeasureReleaseAsync(setup, rewinds, ResetSingleKey, MarkerSingleKey);
        output.AppendLine(CultureInfo.InvariantCulture, $"{cell} single | {settleSeconds:0.0} | {ReleaseCells(single)} | - | - | - | - | - | - | {Verdict(n, single, null, streamed)} |");

        var churnStartedAt = DateTimeOffset.UtcNow;
        await StartChurnAsync(postgres, setup.Schema);
        await UntilAsync(() => Task.FromResult(churn.Sends >= 3), TimeSpan.FromSeconds(60), "churn row was not retried");
        var churned = await MeasureReleaseAsync(setup, rewinds, ResetChurnKey, MarkerChurnKey);
        var churnStream = await MeasureStreamAsync(setup, streamed);
        output.AppendLine(CultureInfo.InvariantCulture,
            $"{cell} churn | {settleSeconds:0.0} | {ReleaseCells(churned)} | {StreamCells(churnStream, streamed)} | {rewinds.Reasons(churnStartedAt, DateTimeOffset.UtcNow)} | {rewinds.PerWindow(churnStartedAt, DateTimeOffset.UtcNow)} | {Verdict(n, churned, churnStream, streamed)} |");

        await SkipChurnAsync(postgres, setup.Schema);
        var churnManyStartedAt = DateTimeOffset.UtcNow;
        await StartChurnManyAsync(postgres, setup.Schema);
        await UntilAsync(() => Task.FromResult(churnMany.Sends >= 3 * ChurnManyKeys), TimeSpan.FromSeconds(60), "churn-many rows were not retried");
        using var sampling = CancellationTokenSource.CreateLinkedTokenSource(Ct);
        var phaseSamples = SamplePhasesAsync(setup.Schema, sampling.Token);
        var churnedMany = await MeasureReleaseAsync(setup, rewinds, ResetChurnManyKey, MarkerChurnManyKey);
        var churnManyStream = await MeasureStreamAsync(setup, streamed);
        await sampling.CancelAsync();
        phases.AppendLine(CultureInfo.InvariantCulture, $"{cell} {PhaseCells(await phaseSamples)} |");
        output.AppendLine(CultureInfo.InvariantCulture,
            $"{cell} churn-many | {settleSeconds:0.0} | {ReleaseCells(churnedMany)} | {StreamCells(churnManyStream, streamed)} | {rewinds.Reasons(churnManyStartedAt, DateTimeOffset.UtcNow)} | {rewinds.PerWindow(churnManyStartedAt, DateTimeOffset.UtcNow)} | {Verdict(n, churnedMany, churnManyStream, streamed)} |");

        var drained = Stopwatch.StartNew();
        await SkipBlockedAndChurnManyAsync(postgres, setup.Schema);
        var cap = TimeSpan.FromSeconds(n / DrainCapRate * DrainCapFactor);
        var full = n < LargeBacklog;
        string windowCells;
        if (full)
        {
            await DrainAsync(() => CountAsync(postgres, setup.Schema), cap, $"N = {n:N0}, BatchSize {batchSize} did not drain");
            windowCells = "- | - | -";
        }
        else
        {
            await DrainAsync(() => CountOtherThanBlockedKeyAsync(postgres, setup.Schema), cap, $"N = {n:N0}, BatchSize {batchSize} rows not of key '{BlockedKey}' did not drain");
            var before = await CountBlockedKeyAsync(postgres, setup.Schema);
            var window = Stopwatch.StartNew();
            await Task.Delay(BlockedRateWindow, Ct);
            var after = await CountBlockedKeyAsync(postgres, setup.Schema);
            var windowSeconds = window.Elapsed.TotalSeconds;
            var deleted = await SkipBlockedKeyAsync(postgres, setup.Schema);
            await DrainAsync(() => CountAsync(postgres, setup.Schema), cap, $"N = {n:N0}, BatchSize {batchSize} did not reach 0 rows after deleting key '{BlockedKey}'");
            windowCells = string.Create(CultureInfo.InvariantCulture, $"{(before - after) / windowSeconds:0.0} | {before - after:N0} | {deleted:N0}");
        }

        var drainSeconds = drained.Elapsed.TotalSeconds;
        var scan = await ScanAsync(setup.BootstrapServers, setup.Topic, settledAt, HighWatermark(setup.BootstrapServers, setup.Topic), churnStream.CutoffMs);
        var complete = (!full || scan.Blocked >= n) && scan.BlockedOrdered && scan.Markers == MarkerKeys.Length && scan.Stream == 2 * streamed;
        var blockedCell = full ? string.Create(CultureInfo.InvariantCulture, $"{scan.Blocked:N0} / {n:N0}") : string.Create(CultureInfo.InvariantCulture, $"{scan.Blocked:N0}");
        drains.AppendLine(CultureInfo.InvariantCulture,
            $"{cell} {(full ? "full" : "window")} | {drainSeconds:0.0} | {blockedCell} | {(scan.BlockedOrdered ? "yes" : "NO")} | {windowCells} | {scan.Markers} / {MarkerKeys.Length} | {scan.Stream:N0} / {2 * streamed:N0} | {(complete ? "PASS" : "FAIL")} |");
    }

    // Fails only when the count has not fallen for DrainStallTimeout, or at the overall cap.
    private static async Task DrainAsync(Func<Task<long>> count, TimeSpan cap, string failure)
    {
        var total = Stopwatch.StartNew();
        var sinceFall = Stopwatch.StartNew();
        var last = await count();
        while (last > 0)
        {
            Assert.True(sinceFall.Elapsed < DrainStallTimeout, string.Create(CultureInfo.InvariantCulture, $"{failure}: {last:N0} rows left, no fall in {DrainStallTimeout.TotalSeconds:0} s"));
            Assert.True(total.Elapsed < cap, string.Create(CultureInfo.InvariantCulture, $"{failure}: {last:N0} rows left at the {cap.TotalSeconds:0} s cap"));
            await Task.Delay(TimeSpan.FromMilliseconds(500), Ct);
            var current = await count();
            if (current < last)
            {
                sinceFall.Restart();
            }

            last = current;
        }
    }

    // A fail commit sets now() + 1 s for its whole group, so concurrent churn-many faults would share one phase. Key churn-m{g} (g < 10, one digit)
    // returns its fault g x 100 ms late, so the outcomes reach separate commits.
    private static TimeSpan ChurnManyFaultDelay(OutgoingMessage message) => TimeSpan.FromMilliseconds((message.Key![^1] - '0') * 100);

    private async Task<List<long>> SamplePhasesAsync(string schema, CancellationToken ct)
    {
        var samples = new List<long>();
        while (true)
        {
            try
            {
                await Task.Delay(PhaseSampleInterval, ct);
            }
            catch (OperationCanceledException)
            {
                return samples;
            }

            samples.Add(await ChurnManyPhasesAsync(postgres, schema));
        }
    }

    private static string PhaseCells(List<long> samples)
    {
        if (samples.Count == 0)
        {
            return "0 | - | - | -";
        }

        samples.Sort();
        return string.Create(CultureInfo.InvariantCulture, $"{samples.Count} | {samples[0]} | {samples[samples.Count / 2]} | {samples[^1]}");
    }

    [Fact(Explicit = true)]
    public async Task M3_hot_key_backlog_delay()
    {
        var output = new StringBuilder()
            .AppendLine(CultureInfo.InvariantCulture, $"M3 hot-key backlog delay: PerKey, BatchSize {BatchSize}, Kafka 1 partition, stream {StreamRate} msg/s (assumed, Q1) over {OtherKeys} keys for {StreamDuration.TotalSeconds:0} s")
            .AppendLine(CultureInfo.InvariantCulture, $"Server: {await ServerAsync(postgres)}")
            .AppendLine("Delay = Kafka record timestamp - insert time in the payload. Hot-key drain = H / (last hot record - hot backlog insert).")
            .AppendLine("| H | Other rows | p50 s | p95 s | max s | Over 5 s | Hot drain s | Hot msg/s | Within |")
            .AppendLine("|---:|---:|---:|---:|---:|---:|---:|---:|---|");

        foreach (var h in HotKeyBacklogs)
        {
            var setup = await DeliverySetup.CreateAsync(postgres, kafka, batchSize: BatchSize, orderingMode: "PerKey");
            await using var relay = setup.Relay();
            _ = relay.Services;

            await using var connection = new NpgsqlConnection(postgres.ConnectionString);
            await connection.OpenAsync(Ct);
            await InsertTimedAsync(connection, setup.Table, "warm-up"u8.ToArray(), new byte[PayloadSize]);
            await UntilAsync(async () => await CountAsync(postgres, setup.Schema) == 0, TimeSpan.FromSeconds(60), "warm-up row not delivered");

            var streamed = (int)(StreamRate * StreamDuration.TotalSeconds);
            var collect = CollectAsync(setup.BootstrapServers, setup.Topic, HighWatermark(setup.BootstrapServers, setup.Topic), h + streamed, StreamDuration + DrainTimeout);

            var hotInsertedAt = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
            if (h > 0)
            {
                await SeedHotKeyAsync(postgres, setup.Schema, HotKey, h);
            }

            await StreamAsync(connection, setup.Table, streamed);
            var records = await collect;

            var delays = new double[streamed];
            var others = 0;
            long lastHot = 0;
            var hot = 0;
            foreach (var record in records)
            {
                if (record.Hot)
                {
                    hot++;
                    lastHot = Math.Max(lastHot, record.TimestampMs);
                }
                else if (others < delays.Length)
                {
                    delays[others++] = (record.TimestampMs - record.InsertedMs) / 1000.0;
                }
            }

            Assert.True(others == streamed && hot == h, $"H = {h}: received {others}/{streamed} other and {hot}/{h} hot records");
            Array.Sort(delays);
            var p95 = Percentile(delays, 0.95);
            var firstSlow = Array.FindIndex(delays, d => d > 5);
            var slow = firstSlow < 0 ? 0 : delays.Length - firstSlow;
            var drain = h > 0 ? (lastHot - hotInsertedAt) / 1000.0 : 0;
            var verdict = h == M3GatedHotKey ? (p95 <= M3LimitSeconds ? "yes" : "NO") : "recorded";
            output.AppendLine(CultureInfo.InvariantCulture,
                $"| {h:N0} | {others:N0} | {Percentile(delays, 0.5):0.000} | {p95:0.000} | {delays[^1]:0.000} | {slow} | {(h > 0 ? drain.ToString("0.0", CultureInfo.InvariantCulture) : "-")} | {(h > 0 ? (h / drain).ToString("0", CultureInfo.InvariantCulture) : "-")} | {verdict} |");
        }

        TestContext.Current.TestOutputHelper!.WriteLine(output.ToString());
    }

    private async Task<Release> MeasureReleaseAsync(DeliverySetup setup, RewindLog rewinds, string resetKey, string markerKey)
    {
        var marker = Encoding.UTF8.GetBytes(markerKey);
        var find = TimestampOfKeyAsync(setup.BootstrapServers, setup.Topic, HighWatermark(setup.BootstrapServers, setup.Topic), marker, MarkerTimeout);
        var releasedAt = DateTimeOffset.UtcNow;
        await ReleaseAsync(postgres, setup.Schema, resetKey, markerKey);
        if (await find is not { } deliveredMs)
        {
            return new Release(null, null, rewinds.Between(releasedAt, DateTimeOffset.UtcNow).Count);
        }

        var delivered = DateTimeOffset.FromUnixTimeMilliseconds(deliveredMs);
        var after = rewinds.Between(releasedAt, delivered);
        var rewoundAt = after.Count > 0 ? after[0] : rewinds.LastBefore(delivered);
        return new Release((delivered - rewoundAt)?.TotalSeconds, (delivered - releasedAt).TotalSeconds, after.Count);
    }

    private static string ReleaseCells(Release release) =>
        release.FromRelease is { } fromRelease
            ? string.Create(CultureInfo.InvariantCulture, $"{release.FromRewind:0.000} | {fromRelease:0.000} | {release.Rewinds}")
            : string.Create(CultureInfo.InvariantCulture, $"not delivered in {MarkerTimeout.TotalSeconds:0} s | - | {release.Rewinds}");

    private static string StreamCells(StreamResult stream, int streamed) =>
        stream.Delays.Length == 0
            ? string.Create(CultureInfo.InvariantCulture, $"0 / {streamed:N0} | - | - | -")
            : string.Create(CultureInfo.InvariantCulture, $"{stream.Delays.Length:N0} / {streamed:N0} | {stream.P50:0.000} | {stream.P95:0.000} | {stream.Max:0.000}");

    // The stream is gated at 1M only.
    private static string Verdict(int n, Release release, StreamResult? stream, int streamed)
    {
        var pass = release.FromRelease is { } fromRelease && fromRelease <= T1Seconds;
        if (n < LargeBacklog)
        {
            pass &= release.FromRewind is { } fromRewind && fromRewind <= FromRewindLimitSeconds;
        }
        else if (stream is { } s)
        {
            pass &= s.Delays.Length == streamed && s.P95 <= T2Seconds && s.Max <= T1Seconds;
        }

        return pass ? "PASS" : "FAIL";
    }

    private async Task<StreamResult> MeasureStreamAsync(DeliverySetup setup, int streamed)
    {
        await using var connection = new NpgsqlConnection(postgres.ConnectionString);
        await connection.OpenAsync(Ct);
        var from = HighWatermark(setup.BootstrapServers, setup.Topic);
        var cutoffMs = DateTimeOffset.UtcNow.Subtract(StreamCutoff).ToUnixTimeMilliseconds();
        var collect = CollectStreamAsync(setup.BootstrapServers, setup.Topic, from, cutoffMs, streamed, StreamCollectTimeout);
        await StreamAsync(connection, setup.Table, streamed);
        var delays = await collect;
        Array.Sort(delays);
        return delays.Length == 0
            ? new StreamResult(delays, 0, 0, 0, cutoffMs)
            : new StreamResult(delays, Percentile(delays, 0.5), Percentile(delays, 0.95), delays[^1], cutoffMs);
    }

    private static async Task StreamAsync(NpgsqlConnection connection, string table, int count)
    {
        var keys = new byte[OtherKeys][];
        for (var i = 0; i < keys.Length; i++)
        {
            keys[i] = Encoding.UTF8.GetBytes($"o{i}");
        }

        var payload = new byte[PayloadSize];
        var interval = TimeSpan.FromSeconds(1.0 / StreamRate);
        var clock = Stopwatch.StartNew();
        for (var i = 0; i < count; i++)
        {
            var wait = (interval * i) - clock.Elapsed;
            if (wait > TimeSpan.Zero)
            {
                await Task.Delay(wait, Ct);
            }

            await InsertTimedAsync(connection, table, keys[i % keys.Length], payload);
        }
    }

    private static async Task InsertTimedAsync(NpgsqlConnection connection, string table, byte[] key, byte[] payload)
    {
        BinaryPrimitives.WriteInt64BigEndian(payload, DateTimeOffset.UtcNow.ToUnixTimeMilliseconds());
        await using var command = new NpgsqlCommand($"INSERT INTO {table} (message_key, payload) VALUES (@key, @payload)", connection);
        command.Parameters.AddWithValue("key", key);
        command.Parameters.AddWithValue("payload", payload);
        await command.ExecuteNonQueryAsync(Ct);
    }

    private static long HighWatermark(string bootstrapServers, string topic)
    {
        using var consumer = NewConsumer(bootstrapServers);
        return consumer.QueryWatermarkOffsets(new TopicPartition(topic, 0), TimeSpan.FromSeconds(10)).High.Value;
    }

    private static Task<long?> TimestampOfKeyAsync(string bootstrapServers, string topic, long from, byte[] key, TimeSpan timeout) =>
        Task.Run<long?>(() =>
        {
            using var consumer = NewConsumer(bootstrapServers);
            consumer.Assign(new TopicPartitionOffset(topic, 0, from));
            var deadline = Stopwatch.StartNew();
            while (deadline.Elapsed < timeout)
            {
                if (consumer.Consume(TimeSpan.FromMilliseconds(200)) is { IsPartitionEOF: false } record && key.AsSpan().SequenceEqual(record.Message.Key))
                {
                    return record.Message.Timestamp.UnixTimestampMs;
                }
            }

            return null;
        });

    private static Task<List<Received>> CollectAsync(string bootstrapServers, string topic, long from, int expected, TimeSpan timeout) =>
        Task.Run(() =>
        {
            using var consumer = NewConsumer(bootstrapServers);
            consumer.Assign(new TopicPartitionOffset(topic, 0, from));
            var records = new List<Received>(expected);
            var deadline = Stopwatch.StartNew();
            while (records.Count < expected && deadline.Elapsed < timeout)
            {
                if (consumer.Consume(TimeSpan.FromMilliseconds(200)) is { IsPartitionEOF: false } record)
                {
                    records.Add(new Received(
                        HotKey.AsSpan().SequenceEqual(record.Message.Key),
                        BinaryPrimitives.ReadInt64BigEndian(record.Message.Value),
                        record.Message.Timestamp.UnixTimestampMs));
                }
            }

            return records;
        });

    // Rows inserted before the cutoff (a released seeded row) are not stream rows.
    private static Task<double[]> CollectStreamAsync(string bootstrapServers, string topic, long from, long cutoffMs, int expected, TimeSpan timeout) =>
        Task.Run(() =>
        {
            using var consumer = NewConsumer(bootstrapServers);
            consumer.Assign(new TopicPartitionOffset(topic, 0, from));
            var delays = new double[expected];
            var count = 0;
            var deadline = Stopwatch.StartNew();
            while (count < expected && deadline.Elapsed < timeout)
            {
                if (consumer.Consume(TimeSpan.FromMilliseconds(200)) is { IsPartitionEOF: false } record
                    && BinaryPrimitives.ReadInt64BigEndian(record.Message.Value) is var insertedMs && insertedMs >= cutoffMs)
                {
                    delays[count++] = (record.Message.Timestamp.UnixTimestampMs - insertedMs) / 1000.0;
                }
            }

            return delays[..count];
        });

    private static Task<Scan> ScanAsync(string bootstrapServers, string topic, long from, long to, long streamCutoffMs) =>
        Task.Run(() =>
        {
            using var consumer = NewConsumer(bootstrapServers);
            consumer.Assign(new TopicPartitionOffset(topic, 0, from));
            var stream = new HashSet<long>();
            var markers = new bool[MarkerKeys.Length];
            var blocked = 0;
            var ordered = true;
            long lastSequence = 0;
            var deadline = Stopwatch.StartNew();
            var next = from;
            while (next < to && deadline.Elapsed < DrainTimeout)
            {
                if (consumer.Consume(TimeSpan.FromMilliseconds(200)) is not { IsPartitionEOF: false } record)
                {
                    continue;
                }

                next = record.Offset.Value + 1;
                var key = record.Message.Key.AsSpan();
                if (key.SequenceEqual(BlockedKeyBytes))
                {
                    blocked++;
                    var sequence = BinaryPrimitives.ReadInt64BigEndian(record.Message.Value.AsSpan(PayloadSequenceOffset));
                    ordered &= sequence > lastSequence;
                    lastSequence = sequence;
                }
                else if (key.Length is 2 or 3 && key[0] == (byte)'o' && BinaryPrimitives.ReadInt64BigEndian(record.Message.Value) is var insertedMs && insertedMs >= streamCutoffMs)
                {
                    var index = key.Length == 2 ? key[1] - '0' : ((key[1] - '0') * 10) + key[2] - '0';
                    stream.Add((insertedMs * OtherKeys) + index);
                }
                else
                {
                    for (var i = 0; i < MarkerKeys.Length; i++)
                    {
                        markers[i] |= key.SequenceEqual(MarkerKeys[i]);
                    }
                }
            }

            return new Scan(blocked, ordered, Array.FindAll(markers, m => m).Length, stream.Count);
        });

    private static IConsumer<byte[], byte[]> NewConsumer(string bootstrapServers) =>
        new ConsumerBuilder<byte[], byte[]>(new ConsumerConfig
        {
            BootstrapServers = bootstrapServers,
            GroupId = $"pilot-{Guid.NewGuid():N}",
            EnableAutoCommit = false,
        }).Build();

    private static async Task UntilAsync(Func<Task<bool>> condition, TimeSpan timeout, string failure)
    {
        var deadline = Stopwatch.StartNew();
        while (!await condition())
        {
            Assert.True(deadline.Elapsed < timeout, failure);
            await Task.Delay(TimeSpan.FromMilliseconds(500), Ct);
        }
    }

    private static double Percentile(double[] sorted, double p)
    {
        var rank = p * (sorted.Length - 1);
        var lower = (int)Math.Floor(rank);
        var upper = Math.Min(lower + 1, sorted.Length - 1);
        return sorted[lower] + ((rank - lower) * (sorted[upper] - sorted[lower]));
    }

    private readonly record struct Received(bool Hot, long InsertedMs, long TimestampMs);

    private readonly record struct Release(double? FromRewind, double? FromRelease, int Rewinds);

    private readonly record struct StreamResult(double[] Delays, double P50, double P95, double Max, long CutoffMs);

    private readonly record struct Scan(int Blocked, bool BlockedOrdered, int Markers, int Stream);

    // Times and reasons of DeliveryRewound (event 124, Debug), enabled for this provider only.
    private sealed class RewindLog : ILoggerProvider, ILogger
    {
        private const int DeliveryRewoundEvent = 124;
        private const string ReasonProperty = "Reason";

        private readonly ConcurrentQueue<(DateTimeOffset At, string Reason)> rewinds = new();

        public List<DateTimeOffset> Between(DateTimeOffset from, DateTimeOffset to)
        {
            var between = new List<DateTimeOffset>();
            foreach (var (at, _) in rewinds)
            {
                if (at >= from && at <= to)
                {
                    between.Add(at);
                }
            }

            between.Sort();
            return between;
        }

        public DateTimeOffset? LastBefore(DateTimeOffset to)
        {
            DateTimeOffset? last = null;
            foreach (var (at, _) in rewinds)
            {
                if (at <= to && (last is null || at > last))
                {
                    last = at;
                }
            }

            return last;
        }

        public string Reasons(DateTimeOffset from, DateTimeOffset to)
        {
            var counts = new SortedDictionary<string, int>(StringComparer.Ordinal);
            foreach (var (at, reason) in rewinds)
            {
                if (at >= from && at <= to)
                {
                    counts[reason] = counts.GetValueOrDefault(reason) + 1;
                }
            }

            return counts.Count == 0 ? "-" : string.Join(", ", counts.Select(c => $"{c.Key} {c.Value}"));
        }

        public string PerWindow(DateTimeOffset from, DateTimeOffset to)
        {
            var windows = new int[(int)Math.Ceiling((to - from) / RewindWindow)];
            foreach (var (at, _) in rewinds)
            {
                if (at >= from && at < to)
                {
                    windows[(int)((at - from) / RewindWindow)]++;
                }
            }

            return windows.Length == 0 ? "-" : string.Join(" / ", windows);
        }

        public ILogger CreateLogger(string categoryName) => this;

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (eventId.Id != DeliveryRewoundEvent)
            {
                return;
            }

            var reason = "?";
            if (state is IReadOnlyList<KeyValuePair<string, object?>> properties)
            {
                foreach (var property in properties)
                {
                    if (property.Key == ReasonProperty)
                    {
                        reason = property.Value as string ?? "?";
                    }
                }
            }

            rewinds.Enqueue((DateTimeOffset.UtcNow, reason));
        }

        public void Dispose()
        {
        }
    }
}
