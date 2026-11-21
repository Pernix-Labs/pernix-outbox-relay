using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Kafka;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.Kafka;

public sealed class KafkaProducerConfigPassthroughTests(KafkaFixture kafka)
{
    private const string ConfFacility = "CONF";

    private static readonly Dictionary<string, string> ClusterProducer = new(StringComparer.Ordinal)
    {
        ["security.protocol"] = "PLAINTEXT",
        ["message.max.bytes"] = "500000",
        ["compression.type"] = "lz4",
        ["debug"] = "conf",
        ["queue.buffering.max.ms"] = "50",
        ["delivery.timeout.ms"] = "120000",
        ["request.required.acks"] = "1",
        ["metadata.broker.list"] = "localhost:1",
    };

    [Theory]
    [InlineData(OrderingMode.None)]
    [InlineData(OrderingMode.PerKey)]
    [InlineData(OrderingMode.Strict)]
    public async Task Relay_producer_config_dump_equals_stock_and_holds_every_relay_key(OrderingMode mode)
    {
        var settings = KafkaClientSettings.Build(new KafkaClusterOptions { BootstrapServers = kafka.BootstrapServers, Producer = ClusterProducer }, mode);

        var logger = new ConfLogger();
        using (KafkaDestinationFactory.BuildProducer(settings.Producer, logger, static _ => { }, static (_, _, _) => { }))
        {
            await WaitForDumpAsync(() => logger.Lines);
        }

        var stockLines = new List<string>();
        using (new ProducerBuilder<byte[], byte[]>(settings.Producer)
            .SetLogHandler((_, message) =>
            {
                if (message.Facility == ConfFacility)
                {
                    lock (stockLines)
                    {
                        stockLines.Add(message.Message);
                    }
                }
            })
            .Build())
        {
            await WaitForDumpAsync(() => Snapshot(stockLines));
        }

        var relay = logger.Lines;
        foreach (var line in relay)
        {
            TestContext.Current.TestOutputHelper?.WriteLine(line);
        }

        Assert.Equal(Comparable(Snapshot(stockLines)), Comparable(relay));

        var dumped = Properties(relay);
        foreach (var (key, value) in settings.Producer)
        {
            if (key.StartsWith("dotnet.producer.", StringComparison.Ordinal))
            {
                continue;
            }

            var name = LibrdkafkaName.GetValueOrDefault(key, key);
            Assert.True(dumped.TryGetValue(name, out var actual), $"{key} ({name}) missing from the debug=conf dump");
            Assert.Equal(LibrdkafkaValue.GetValueOrDefault((key, value), value), actual);
        }
    }

    private static readonly Dictionary<string, string> LibrdkafkaName = new(StringComparer.Ordinal)
    {
        ["bootstrap.servers"] = "metadata.broker.list",
        ["acks"] = "request.required.acks",
        ["linger.ms"] = "queue.buffering.max.ms",
        ["compression.type"] = "compression.codec",
    };

    private static readonly Dictionary<(string, string), string> LibrdkafkaValue = new()
    {
        [("acks", "all")] = "-1",
        [("security.protocol", "PLAINTEXT")] = "plaintext",
    };

    private static readonly string[] PointerProperties = ["_cb", "opaque", "default_topic_conf"];

    private static List<string> Comparable(IReadOnlyList<string> lines) =>
        [.. lines.Where(line => !PointerProperties.Any(p => Property(line).Split(" = ")[0].TrimEnd().EndsWith(p, StringComparison.Ordinal)))];

    private static string Property(string line) => line[(line.IndexOf("]:", StringComparison.Ordinal) + 2)..];

    private static Dictionary<string, string> Properties(IReadOnlyList<string> lines)
    {
        var properties = new Dictionary<string, string>(StringComparer.Ordinal);
        foreach (var line in lines)
        {
            var property = Property(line);
            var separator = property.IndexOf(" = ", StringComparison.Ordinal);
            if (separator > 0)
            {
                properties[property[..separator].Trim()] = property[(separator + 3)..].Trim();
            }
        }

        return properties;
    }

    private static List<string> Snapshot(List<string> lines)
    {
        lock (lines)
        {
            return [.. lines];
        }
    }

    private static async Task WaitForDumpAsync(Func<IReadOnlyList<string>> lines)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        var last = -1;
        while (DateTime.UtcNow < deadline)
        {
            var count = lines().Count;
            if (count > 0 && count == last)
            {
                return;
            }

            last = count;
            await Task.Delay(100, TestContext.Current.CancellationToken);
        }

        Assert.Fail("no stable debug=conf dump within 5 s");
    }

    private sealed class ConfLogger : ILogger
    {
        private readonly List<string> lines = [];

        public IReadOnlyList<string> Lines => Snapshot(lines);

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            if (eventId.Id != 201 || state is not IEnumerable<KeyValuePair<string, object?>> pairs)
            {
                return;
            }

            var fields = pairs.ToDictionary(p => p.Key, p => p.Value);
            if (Equals(fields["Facility"], ConfFacility))
            {
                lock (lines)
                {
                    lines.Add((string)fields["Text"]!);
                }
            }
        }
    }
}
