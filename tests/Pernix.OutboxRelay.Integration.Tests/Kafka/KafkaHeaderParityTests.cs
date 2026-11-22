using System.Globalization;
using System.Text;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Integration.Tests.Delivery;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Pernix.OutboxRelay.Destination.Kafka;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.Kafka;

public sealed class KafkaHeaderParityTests(PostgreSqlFixture postgres, KafkaFixture kafka)
{
    private const string Destination = "Events";
    private const int MixedRows = 40;
    private static readonly TimeSpan ConsumeTimeout = TimeSpan.FromSeconds(30);

    // Captured on e545671 (stock Confluent.Kafka 2.16.0).
    private static readonly string[] Expected =
    [
        "h0 ",
        "h1 h-0:[00FF00]",
        "h1-empty h-0:[]",
        "h4 h-0:[00FF00],h-1:[00FF01],dup:[00FF02],h-3:[00FF03]",
        "h8 h-0:[00FF00],h-1:[00FF01],dup:[00FF02],h-3:[00FF03],h-4:[00FF04],dup:[00FF05],h-6:[00FF06],h-7:[00FF07]",
        "h9 h-0:[00FF00],h-1:[00FF01],dup:[00FF02],h-3:[00FF03],h-4:[00FF04],dup:[00FF05],h-6:[00FF06],h-7:[00FF07],dup:[00FF08]",
        "h9-empty h-0:[00FF00],h-1:[00FF01],dup:[00FF02],h-3:[00FF03],h-4:[00FF04],dup:[00FF05],h-6:[00FF06],h-7:[00FF07],dup:[]",
        "h16 h-0:[00FF00],h-1:[00FF01],dup:[00FF02],h-3:[00FF03],h-4:[00FF04],dup:[00FF05],h-6:[00FF06],h-7:[00FF07],dup:[00FF08],h-9:[00FF09],h-10:[00FF0A],dup:[00FF0B],h-12:[00FF0C],h-13:[00FF0D],dup:[00FF0E],h-15:[00FF0F]",
        "h17 h-0:[00FF00],h-1:[00FF01],dup:[00FF02],h-3:[00FF03],h-4:[00FF04],dup:[00FF05],h-6:[00FF06],h-7:[00FF07],dup:[00FF08],h-9:[00FF09],h-10:[00FF0A],dup:[00FF0B],h-12:[00FF0C],h-13:[00FF0D],dup:[00FF0E],h-15:[00FF0F],h-16:[00FF10]",
        "h17-empty h-0:[00FF00],h-1:[00FF01],dup:[00FF02],h-3:[],h-4:[00FF04],dup:[00FF05],h-6:[00FF06],h-7:[00FF07],dup:[00FF08],h-9:[00FF09],h-10:[00FF0A],dup:[00FF0B],h-12:[00FF0C],h-13:[00FF0D],dup:[00FF0E],h-15:[00FF0F],h-16:[00FF10]",
        "h20 h-0:[00FF00],h-1:[00FF01],dup:[00FF02],h-3:[00FF03],h-4:[00FF04],dup:[00FF05],h-6:[00FF06],h-7:[00FF07],dup:[00FF08],h-9:[00FF09],h-10:[00FF0A],dup:[00FF0B],h-12:[00FF0C],h-13:[00FF0D],dup:[00FF0E],h-15:[00FF0F],h-16:[00FF10],dup:[00FF11],h-18:[00FF12],h-19:[00FF13]",
        "non-ascii Ordér:[76302DC3BC],注文:[76312DC3BC],ключ:[76322DC3BC],Ωmega:[76332DC3BC]",
        "zero-length a:[],b:[01],c:[]",
        "dead-letter type:[4F72646572],empty:[],x-dead-letter-reason:[496E76616C69645265636F72643A2072656A6563746564],x-retry-attempts:[34],x-original-destination:[4F7264657273],x-original-outbox:[4F7264657273],x-original-id:[3432],x-failed-at:[323032362D31312D30335431303A32303A33302E3435365A]",
    ];

    [Fact]
    public async Task Header_names_values_and_order_match_base()
    {
        var ct = TestContext.Current.CancellationToken;
        var topic = KafkaFixture.NewTopic();
        await kafka.CreateTopicAsync(topic, 1);

        (string Case, OutgoingMessage Message)[] cases =
        [
            ("h0", Message("h0", MessageHeaders.Empty)),
            ("h1", Message("h1", Headers(1))),
            ("h1-empty", Message("h1-empty", Headers(1, empty: 0))),
            ("h4", Message("h4", Headers(4))),
            ("h8", Message("h8", Headers(8))),
            ("h9", Message("h9", Headers(9))),
            ("h9-empty", Message("h9-empty", Headers(9, empty: 8))),
            ("h16", Message("h16", Headers(16))),
            ("h17", Message("h17", Headers(17))),
            ("h17-empty", Message("h17-empty", Headers(17, empty: 3))),
            ("h20", Message("h20", Headers(20))),
            ("non-ascii", Message("non-ascii", Named("Ordér", "注文", "ключ", "Ωmega"))),
            ("zero-length", Message("zero-length", new MessageHeaders([new MessageHeader("a", []), new MessageHeader("b", [1]), new MessageHeader("c", [])]))),
            ("dead-letter", Message("dead-letter", new MessageHeaders(
                [new MessageHeader("type", "Order"u8.ToArray()), new MessageHeader("empty", []), new MessageHeader("x-original-id", "spoofed"u8.ToArray())])) with
            {
                Source = new MessageSource("Orders", 42, "Orders"),
                DeadLetter = new DeadLetterInfo("InvalidRecord: rejected", 4, new DateTimeOffset(2026, 11, 3, 10, 20, 30, 456, TimeSpan.Zero)),
            }),
        ];

        var relay = RelayOptionsBinder.Bind(new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["KafkaClusters:Main:BootstrapServers"] = kafka.BootstrapServers,
            ["Destinations:0:Name"] = Destination,
            ["Destinations:0:Transport"] = "Kafka",
            ["Destinations:0:Kafka:Cluster"] = "Main",
            ["Destinations:0:Kafka:Topic"] = topic,
        }).Build());
        var factory = new KafkaDestinationFactory(NullLoggerFactory.Instance);
        Assert.Empty(factory.Validate(relay.Destinations[0], $"Destinations[{Destination}]", relay));
        await using (var destination = factory.Create(relay.Destinations[0], new DestinationContext(OrderingMode.PerKey)))
        {
            await destination.ValidateAsync(ct);
            foreach (var (_, message) in cases)
            {
                Assert.IsType<DeliveryResult.Delivered>(await destination.SendAsync(message, ct));
            }
        }

        var records = await kafka.ConsumeAllAsync(topic, cases.Length, ConsumeTimeout);

        var observed = records
            .Select(r => $"{Encoding.UTF8.GetString(r.Message.Key)} {string.Join(",", r.Message.Headers.Select(h => $"{h.Key}:{Hex(h.GetValueBytes())}"))}")
            .ToArray();
        foreach (var line in observed)
        {
            TestContext.Current.TestOutputHelper?.WriteLine(line);
        }

        Assert.Equal(Expected, observed);
    }

    [Theory]
    [InlineData("PerKey")]
    [InlineData("Strict")]
    public async Task Mixed_header_paths_keep_per_key_order(string mode)
    {
        var setup = await DeliverySetup.CreateAsync(postgres, kafka, orderingMode: mode, partitions: 6);
        await using var relay = setup.Relay();
        _ = relay.Services;

        var rows = new OutboxRow[MixedRows];
        for (var i = 0; i < MixedRows; i++)
        {
            rows[i] = new OutboxRow("mixed"u8.ToArray(), Encoding.UTF8.GetBytes(i.ToString(CultureInfo.InvariantCulture)), new Dictionary<string, string> { ["h"] = i % 2 == 0 ? "" : "v" });
        }

        await setup.InsertAsync(rows);

        var records = await KafkaFixture.ConsumeAllAsync(setup.BootstrapServers, setup.Topic, MixedRows, ConsumeTimeout);

        Assert.Equal(MixedRows, records.Count);
        Assert.Single(records.Select(r => r.Partition.Value).Distinct());
        var byOffset = records.OrderBy(r => r.Offset.Value).ToArray();
        for (var i = 0; i < MixedRows; i++)
        {
            Assert.Equal(i.ToString(CultureInfo.InvariantCulture), Encoding.UTF8.GetString(byOffset[i].Message.Value));
            Assert.Equal(i % 2 == 0 ? "[]" : "[76]", Hex(Assert.Single(byOffset[i].Message.Headers).GetValueBytes()));
        }
    }

    private static OutgoingMessage Message(string name, MessageHeaders headers) => new()
    {
        Source = new MessageSource("Orders", 1, Destination),
        Key = Encoding.UTF8.GetBytes(name),
        Payload = [1],
        Headers = headers,
    };

    private static MessageHeaders Headers(int count, int empty = -1)
    {
        var items = new MessageHeader[count];
        for (var i = 0; i < count; i++)
        {
            items[i] = new MessageHeader(i % 3 == 2 ? "dup" : $"h-{i}", i == empty ? [] : [0x00, 0xFF, (byte)i]);
        }

        return new MessageHeaders(items);
    }

    private static MessageHeaders Named(params string[] names) =>
        new([.. names.Select((name, i) => new MessageHeader(name, Encoding.UTF8.GetBytes($"v{i}-ü")))]);

    private static string Hex(byte[]? bytes) => bytes is null ? "null" : $"[{Convert.ToHexString(bytes)}]";
}
