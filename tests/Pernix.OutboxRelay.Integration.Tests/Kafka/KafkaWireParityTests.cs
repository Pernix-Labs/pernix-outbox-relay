using System.Globalization;
using System.Text;
using Pernix.OutboxRelay.Integration.Tests.Delivery;
using Pernix.OutboxRelay.Integration.Tests.PostgreSql;
using Confluent.Kafka;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.Kafka;

public sealed class KafkaWireParityTests(PostgreSqlFixture postgres, KafkaFixture kafka)
{
    private const int Partitions = 6;
    private const string RowHeader = "row";
    private static readonly TimeSpan DeliveryTimeout = TimeSpan.FromSeconds(60);

    // Captured on e545671 (stock Confluent.Kafka 2.16.0). NULL key: partition random under murmur2_random, not asserted.
    private static readonly string[] Expected =
    [
        "row=1 key=null value=[702D6E756C6C2D6B6579] partition=* headers=row:[31]",
        "row=2 key=[] value=[702D656D7074792D6B6579] partition=3 headers=row:[32]",
        "row=3 key=[6F726465722D31] value=[702D33] partition=4 headers=row:[33]",
        "row=4 key=[6F726465722D32] value=[702D34] partition=3 headers=row:[34]",
        "row=5 key=[637573746F6D65722D3137] value=[702D35] partition=4 headers=row:[35]",
        "row=6 key=[C3BC2D6B6579] value=[702D36] partition=5 headers=row:[36]",
        "row=7 key=[61] value=[702D37] partition=4 headers=row:[37]",
        "row=8 key=[7A2D39] value=[702D38] partition=2 headers=row:[38]",
        "row=9 key=[6B2D656D7074792D7061796C6F6164] value=[] partition=2 headers=row:[39]",
        "row=10 key=[6B2D656D7074792D686561646572] value=[702D3130] partition=3 headers=row:[3130],empty:[]",
    ];

    [Fact]
    public async Task Keys_values_header_values_and_partitions_match_base()
    {
        var setup = await DeliverySetup.CreateAsync(postgres, kafka, partitions: Partitions);
        await using var relay = setup.Relay();
        _ = relay.Services;

        OutboxRow[] rows =
        [
            Row(1, null, "p-null-key"u8.ToArray()),
            Row(2, [], "p-empty-key"u8.ToArray()),
            Row(3, "order-1"u8.ToArray(), "p-3"u8.ToArray()),
            Row(4, "order-2"u8.ToArray(), "p-4"u8.ToArray()),
            Row(5, "customer-17"u8.ToArray(), "p-5"u8.ToArray()),
            Row(6, Encoding.UTF8.GetBytes("ü-key"), "p-6"u8.ToArray()),
            Row(7, "a"u8.ToArray(), "p-7"u8.ToArray()),
            Row(8, "z-9"u8.ToArray(), "p-8"u8.ToArray()),
            Row(9, "k-empty-payload"u8.ToArray(), []),
            Row(10, "k-empty-header"u8.ToArray(), "p-10"u8.ToArray(), ("empty", "")),
        ];
        await setup.InsertAsync(rows);

        var records = await KafkaFixture.ConsumeAllAsync(setup.BootstrapServers, setup.Topic, rows.Length, DeliveryTimeout);

        var observed = records.Select(Describe).OrderBy(RowOf).ToArray();
        foreach (var line in observed)
        {
            TestContext.Current.TestOutputHelper?.WriteLine(line);
        }

        Assert.Equal(Expected, observed);
    }

    private static OutboxRow Row(int tag, byte[]? key, byte[] payload, params (string Name, string Value)[] headers)
    {
        var all = new Dictionary<string, string> { [RowHeader] = tag.ToString(CultureInfo.InvariantCulture) };
        foreach (var (name, value) in headers)
        {
            all[name] = value;
        }

        return new OutboxRow(key, payload, all);
    }

    private static string Describe(ConsumeResult<byte[], byte[]> record)
    {
        var message = record.Message;
        var partition = message.Key is null ? "*" : record.Partition.Value.ToString(CultureInfo.InvariantCulture);
        var headers = string.Join(",", message.Headers.Select(h => $"{h.Key}:{Hex(h.GetValueBytes())}"));
        return $"row={Encoding.UTF8.GetString(message.Headers.GetLastBytes(RowHeader))} key={Hex(message.Key)} value={Hex(message.Value)} partition={partition} headers={headers}";
    }

    private static int RowOf(string line) => int.Parse(line[4..line.IndexOf(' ', StringComparison.Ordinal)], CultureInfo.InvariantCulture);

    private static string Hex(byte[]? bytes) => bytes is null ? "null" : $"[{Convert.ToHexString(bytes)}]";
}
