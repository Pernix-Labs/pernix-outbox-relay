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

public sealed class KafkaCoexistenceTests(KafkaFixture kafka)
{
    private const string Destination = "Events";
    private static readonly TimeSpan ConsumeTimeout = TimeSpan.FromSeconds(20);

    [Fact]
    public async Task Stock_admin_destination_sends_and_stock_consume_share_one_librdkafka()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = KafkaFixture.NewTopic();
        await kafka.CreateTopicAsync(topic, 1);

        var messages = new[]
        {
            Message(null, "v-null-key-0h"u8.ToArray(), Headers(0)),
            Message([], "v-empty-key-1h"u8.ToArray(), Headers(1)),
            Message("k-9h"u8.ToArray(), "v-9h"u8.ToArray(), Headers(9)),
            Message("k-17h"u8.ToArray(), "v-17h"u8.ToArray(), Headers(17)),
            Message("k-empty-value"u8.ToArray(), [], Headers(1)),
            Message("k-1h-empty"u8.ToArray(), "v-1h-empty"u8.ToArray(), Headers(1, empty: 0)),
            Message(null, "v-9h-empty"u8.ToArray(), Headers(9, empty: 4)),
            Message([], [], Headers(17, empty: 16)),
        };

        var relay = Bind(topic);
        var factory = new KafkaDestinationFactory(NullLoggerFactory.Instance);
        Assert.Empty(factory.Validate(relay.Destinations[0], $"Destinations[{Destination}]", relay));
        await using (var destination = factory.Create(relay.Destinations[0], new DestinationContext(OrderingMode.PerKey)))
        {
            await destination.ValidateAsync(ct);
            foreach (var message in messages)
            {
                Assert.Same(DeliveryResult.Delivered.Instance, await destination.SendAsync(message, ct));
            }
        }

        var consumed = await kafka.ConsumeAllAsync(topic, messages.Length, ConsumeTimeout);

        Assert.Equal(messages.Length, consumed.Count);
        for (var i = 0; i < messages.Length; i++)
        {
            var record = consumed[i].Message;
            Assert.Equal(Hex(messages[i].Key), Hex(record.Key));
            Assert.Equal(Hex(messages[i].Payload), Hex(record.Value));
            Assert.Equal(
                messages[i].Headers.Select(h => (h.Name, Hex(h.Value))),
                record.Headers.Select(h => (h.Key, Hex(h.GetValueBytes()))));
        }

        // After the sends above, so the process has run the common and the zero-length header value path.
        Assert.True(Library.IsLoaded);
        Assert.True(Nk.Library.IsLoaded);
        var paths = LoadedLibrdkafkaPaths();
        TestContext.Current.TestOutputHelper?.WriteLine($"librdkafka modules ({paths.Count}): {string.Join(", ", paths)}");
        TestContext.Current.TestOutputHelper?.WriteLine($"stock {Library.VersionString}, Nanov.Kafka {Nk.Library.VersionString}");
        Assert.Single(paths);
    }

    private static OutgoingMessage Message(byte[]? key, byte[] payload, MessageHeaders headers) => new()
    {
        Source = new MessageSource("Orders", 1, Destination),
        Key = key,
        Payload = payload,
        Headers = headers,
    };

    private static MessageHeaders Headers(int count, int empty = -1)
    {
        if (count == 0)
        {
            return MessageHeaders.Empty;
        }

        var items = new MessageHeader[count];
        for (var i = 0; i < count; i++)
        {
            items[i] = new MessageHeader(i % 3 == 2 ? "dup" : $"h-{i}-é", i == empty ? [] : Encoding.UTF8.GetBytes($"value-{i}-ü"));
        }

        return new MessageHeaders(items);
    }

    private static string Hex(byte[]? bytes) => bytes is null ? "null" : $"[{Convert.ToHexString(bytes)}]";

    private RelayOptions Bind(string topic) => RelayOptionsBinder.Bind(new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
    {
        ["KafkaClusters:Main:BootstrapServers"] = kafka.BootstrapServers,
        ["Destinations:0:Name"] = Destination,
        ["Destinations:0:Transport"] = "Kafka",
        ["Destinations:0:Kafka:Cluster"] = "Main",
        ["Destinations:0:Kafka:Topic"] = topic,
    }).Build());

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
}
