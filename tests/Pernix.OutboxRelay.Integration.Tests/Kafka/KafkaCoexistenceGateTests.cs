using System.Diagnostics;
using System.Text;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Destination.Kafka;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;
using Nk = Nanov.Kafka;

namespace Pernix.OutboxRelay.Integration.Tests.Kafka;

// Stock Confluent.Kafka and Nanov.Kafka in one process over one librdkafka.
public sealed class KafkaCoexistenceGateTests(KafkaFixture kafka)
{
    private const string Destination = "Events";
    private static readonly TimeSpan ConsumeTimeout = TimeSpan.FromSeconds(20);

    [Fact]
    public async Task Stock_admin_raw_produce_and_stock_consume_share_one_librdkafka()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = KafkaFixture.NewTopic();
        await kafka.CreateTopicAsync(topic, 1);

        // Stock admin describe through the relay's own path.
        var (relay, clusters, factory) = Bind(topic);
        Assert.Empty(factory.Validate(relay.Destinations[0], $"Destinations[{Destination}]", relay));
        await using (var destination = factory.Create(relay.Destinations[0], new DestinationContext(OrderingMode.PerKey)))
        {
            await destination.ValidateAsync(ct);
        }

        var records = new[]
        {
            Record(null, "v-null-key-0h", 0),
            Record("k-1h", "v-1h", 1),
            Record(null, "v-null-key-9h", 9),
            Record("k-9h", "v-9h", 9),
            Record("k-17h", "v-17h", 17),
        };

        var succeeded = new int[records.Length];
        var failed = new int[records.Length];
        Assert.True(clusters.TryGet("Main", out var cluster));
        var settings = KafkaClientSettings.Build(cluster, OrderingMode.PerKey);
        using (var producer = new Nk.RawProducerBuilder(settings.Producer)
            .SetDeliveryReportHandler((in Nk.RawDeliveryReport report) =>
            {
                var index = (int)report.Opaque - 1;
                if (report.ErrorCode == Nk.ErrorCode.NoError && report.Status == Nk.PersistenceStatus.Persisted)
                {
                    Interlocked.Increment(ref succeeded[index]);
                }
                else
                {
                    Interlocked.Increment(ref failed[index]);
                }
            })
            .BuildRaw())
        {
            for (var i = 0; i < records.Length; i++)
            {
                RawProduce(producer, topic, records[i], i + 1);
            }

            Assert.Equal(0, producer.Flush(TimeSpan.FromSeconds(30)));
        }

        Assert.All(succeeded, count => Assert.Equal(1, count));
        Assert.All(failed, count => Assert.Equal(0, count));

        var consumed = await kafka.ConsumeAllAsync(topic, records.Length, ConsumeTimeout);

        Assert.Equal(records.Length, consumed.Count);
        for (var i = 0; i < records.Length; i++)
        {
            var message = consumed[i].Message;
            Assert.Equal(records[i].Key, message.Key);
            Assert.Equal(records[i].Value, message.Value);
            Assert.Equal(
                records[i].Headers.Select(h => (h.Name, h.Value)),
                message.Headers.Select(h => (h.Key, h.GetValueBytes())));
        }

        // Both managed bindings loaded, one native librdkafka in the process.
        Assert.True(Library.IsLoaded);
        Assert.True(Nk.Library.IsLoaded);
        var paths = LoadedLibrdkafkaPaths();
        TestContext.Current.TestOutputHelper?.WriteLine($"librdkafka modules ({paths.Count}): {string.Join(", ", paths)}");
        TestContext.Current.TestOutputHelper?.WriteLine($"stock {Library.VersionString}, Nanov.Kafka {Nk.Library.VersionString}");
        Assert.Single(paths);
    }

    private static void RawProduce(Nk.IRawProducer producer, string topic, SentRecord record, nint opaque)
    {
        if (record.Headers.Length == 0)
        {
            producer.RawProduce(topic, record.Key, record.Value, opaque);
            return;
        }

        Nk.KafkaHeaders first = default;
        Nk.KafkaHeaders second = default;
        for (var i = 0; i < record.Headers.Length; i++)
        {
            var (name, value) = record.Headers[i];
            if (i < 8)
            {
                first.Add(name, value);
            }
            else
            {
                second.Add(name, value);
            }
        }

        if (record.Headers.Length <= 8)
        {
            producer.RawProduce(topic, record.Key, record.Value, in first, opaque);
        }
        else
        {
            producer.RawProduce(topic, record.Key, record.Value, in first, in second, opaque);
        }
    }

    private static SentRecord Record(string? key, string value, int headers)
    {
        var list = new (string Name, byte[] Value)[headers];
        for (var i = 0; i < headers; i++)
        {
            // Repeated names keep order observable, non-ASCII checks the name bytes.
            list[i] = (i % 3 == 2 ? "dup" : $"h-{i}-é", Encoding.UTF8.GetBytes($"value-{i}-ü"));
        }

        return new SentRecord(key is null ? null : Encoding.UTF8.GetBytes(key), Encoding.UTF8.GetBytes(value), list);
    }

    private (RelayOptions Relay, KafkaClusterSection Clusters, KafkaDestinationFactory Factory) Bind(string topic)
    {
        var settings = new Dictionary<string, string?>
        {
            ["KafkaClusters:Main:BootstrapServers"] = kafka.BootstrapServers,
            ["Destinations:0:Name"] = Destination,
            ["Destinations:0:Transport"] = "Kafka",
            ["Destinations:0:Kafka:Cluster"] = "Main",
            ["Destinations:0:Kafka:Topic"] = topic,
        };

        var config = new ConfigurationBuilder().AddInMemoryCollection(settings).Build();
        var clusters = new KafkaClusterSection(new RelayConfiguration(config));
        return (RelayOptionsBinder.Bind(config), clusters, new KafkaDestinationFactory(NullLoggerFactory.Instance, clusters));
    }

    // Distinct loaded module paths containing "librdkafka": /proc/self/maps on Linux, lsof on macOS.
    private static IReadOnlyList<string> LoadedLibrdkafkaPaths()
    {
        IEnumerable<string> lines;
        if (OperatingSystem.IsLinux())
        {
            lines = File.ReadLines("/proc/self/maps")
                .Select(line => line.Split(' ', 6, StringSplitOptions.RemoveEmptyEntries))
                .Where(parts => parts.Length == 6)
                .Select(parts => parts[5].Trim());
        }
        else if (OperatingSystem.IsMacOS())
        {
            using var lsof = Process.Start(new ProcessStartInfo("lsof", $"-p {Environment.ProcessId} -Fn") { RedirectStandardOutput = true })!;
            var output = lsof.StandardOutput.ReadToEnd();
            lsof.WaitForExit();
            lines = output.Split('\n').Where(line => line.StartsWith('n')).Select(line => line[1..]);
        }
        else
        {
            throw new PlatformNotSupportedException("The librdkafka module listing runs on Linux or macOS.");
        }

        return lines.Where(path => path.Contains("librdkafka", StringComparison.Ordinal)).Distinct(StringComparer.Ordinal).ToList();
    }

    private sealed record SentRecord(byte[]? Key, byte[] Value, (string Name, byte[] Value)[] Headers);
}
