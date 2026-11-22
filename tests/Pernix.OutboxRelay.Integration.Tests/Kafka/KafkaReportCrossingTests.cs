using System.Text;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Destination.Kafka;
using Confluent.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using Xunit;

namespace Pernix.OutboxRelay.Integration.Tests.Kafka;

public sealed class KafkaReportCrossingTests(KafkaFixture kafka)
{
    private const string Route = "r";
    private const int ReportTimeoutMs = 2000;
    private static readonly TimeSpan ReportWait = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan ConsumeTimeout = TimeSpan.FromSeconds(20);

    private static readonly MessageHeaders CommonHeaders = new([new MessageHeader("h", "v"u8.ToArray()), new MessageHeader("g", "w"u8.ToArray())]);
    private static readonly MessageHeaders RareHeaders = new([new MessageHeader("h", "v"u8.ToArray()), new MessageHeader("empty", [])]);

    private static CancellationToken Ct => TestContext.Current.CancellationToken;

    [Fact]
    public async Task Not_persisted_report_crosses_with_stock_code_reason_and_status()
    {
        var topic = KafkaFixture.NewTopic();
        var config = Settings(kafka.BootstrapServers, ReportTimeoutMs).Producer;

        var raw = await RawReportAsync(config, topic);
        var stock = await StockReportAsync(config, topic);

        Assert.Equal(ErrorCode.Local_MsgTimedOut, stock.Error.Code);
        Assert.Equal(PersistenceStatus.NotPersisted, stock.Status);
        AssertSameReport(stock, raw);

        await using var destination = Destination(Settings(kafka.BootstrapServers, ReportTimeoutMs), topic, described: false);
        var result = await destination.SendAsync(Message("p", CommonHeaders), Ct);
        Assert.Equal(new DeliveryResult.DestinationFault($"route '{Route}' topic '{topic}': {stock.Error.Code}: {stock.Error.Reason}"), result);
    }

    [Fact]
    public async Task Possibly_persisted_report_crosses_with_stock_code_reason_and_status()
    {
        await using var own = await KafkaFixture.StartOwnContainerAsync();
        var topic = KafkaFixture.NewTopic();
        await own.CreateTopicAsync(topic, 1);
        var config = Settings(own.BootstrapServers, ReportTimeoutMs).Producer;

        var rawReports = new ReportSink();
        var stockReports = new ReportSink();
        using var raw = KafkaDestinationFactory.BuildProducer(config, NullLogger.Instance, static _ => { }, rawReports.OnReport);
        using var stock = new ProducerBuilder<byte[], byte[]>(config).Build();
        var address = new KafkaAddress(topic);

        Assert.Null(raw.Produce(address, [1], [1], MessageHeaders.Empty, 1));
        var warmRaw = await rawReports.NextAsync();
        Assert.Null(warmRaw.Error);
        var warmStock = await stock.ProduceAsync(topic, new Message<byte[], byte[]> { Key = [1], Value = [1] }, Ct);
        Assert.Equal(PersistenceStatus.Persisted, warmStock.Status);

        await own.Container.PauseAsync(Ct);
        try
        {
            Assert.Null(raw.Produce(address, [2], [2], MessageHeaders.Empty, 2));
            stock.Produce(topic, new Message<byte[], byte[]> { Key = [2], Value = [2] }, stockReports.OnStock);
            var rawReport = await rawReports.NextAsync();
            var stockReport = await stockReports.NextAsync();

            Assert.Equal(ErrorCode.Local_MsgTimedOut, stockReport.Error?.Code);
            Assert.Equal(PersistenceStatus.PossiblyPersisted, stockReport.Status);
            Assert.Equal(stockReport.Status, rawReport.Status);
            Assert.NotNull(rawReport.Error);
            Assert.Equal(stockReport.Error!.Code, rawReport.Error.Code);
            Assert.Equal(stockReport.Error.Reason, rawReport.Error.Reason);
            Assert.Equal(stockReport.Error.IsFatal, rawReport.Error.IsFatal);
            Assert.Equal(KafkaErrorClassifier.Classify(stockReport.Error), KafkaErrorClassifier.Classify(rawReport.Error));
        }
        finally
        {
            await own.Container.UnpauseAsync(Ct);
        }
    }

    [Fact]
    public async Task Rare_path_reaches_same_slot_and_fault_mapping_as_common_path()
    {
        var topic = KafkaFixture.NewTopic();
        await kafka.CreateTopicAsync(topic, 1);
        await using (var destination = Destination(Settings(kafka.BootstrapServers), topic, described: true))
        {
            await destination.ValidateAsync(Ct);
            var common = destination.SendAsync(Message("common", CommonHeaders), Ct);
            var rare = destination.SendAsync(Message("rare", RareHeaders), Ct);

            Assert.Same(DeliveryResult.Delivered.Instance, await common);
            Assert.Same(DeliveryResult.Delivered.Instance, await rare);
            Assert.Equal(2, destination.SlotsCreated);
            Assert.Equal(2, destination.FreeSlots);
        }

        var records = await kafka.ConsumeAllAsync(topic, 2, ConsumeTimeout);
        Assert.Equal(2, records.Count);
        AssertRecord(Assert.Single(records, r => r.Message.Value.AsSpan().SequenceEqual("common"u8)), CommonHeaders);
        AssertRecord(Assert.Single(records, r => r.Message.Value.AsSpan().SequenceEqual("rare"u8)), RareHeaders);

        var unknown = KafkaFixture.NewTopic();
        await using (var destination = Destination(Settings(kafka.BootstrapServers, ReportTimeoutMs), unknown, described: false))
        {
            var common = destination.SendAsync(Message("common", CommonHeaders), Ct);
            var rare = destination.SendAsync(Message("rare", RareHeaders), Ct);
            var commonResult = await common;
            var rareResult = await rare;

            Assert.Equal(new DeliveryResult.DestinationFault($"route '{Route}' topic '{unknown}': {Text(ErrorCode.Local_MsgTimedOut)}"), commonResult);
            Assert.Equal(commonResult, rareResult);
            Assert.Equal(2, destination.SlotsCreated);
            Assert.Equal(2, destination.FreeSlots);
        }

        var held = KafkaFixture.NewTopic();
        var refusing = Settings(kafka.BootstrapServers);
        refusing.Producer.Set("queue.buffering.max.messages", "1");
        await using (var destination = Destination(refusing, held, described: false))
        {
            _ = destination.SendAsync(Message("held", CommonHeaders), Ct);
            await WaitUntilAsync(() => destination.SlotsCreated == 1);

            var commonResult = await AssertRefusedAsync(destination, Message("common", CommonHeaders));
            var rareResult = await AssertRefusedAsync(destination, Message("rare", RareHeaders));

            Assert.Equal(new DeliveryResult.DestinationFault($"route '{Route}' topic '{held}': {Text(ErrorCode.Local_QueueFull)}"), commonResult);
            Assert.Equal(commonResult, rareResult);
        }
    }

    private static async Task<DeliveryResult> AssertRefusedAsync(KafkaDestination destination, OutgoingMessage message)
    {
        var refused = destination.SendAsync(message, Ct);

        Assert.True(refused.IsCompleted);
        Assert.Equal(2, destination.SlotsCreated);
        Assert.Equal(1, destination.FreeSlots);
        var first = await refused;
        Assert.Equal(first, await refused);
        return first;
    }

    private static string Text(ErrorCode code) => $"{code}: {new Error(code).Reason}";

    private static void AssertRecord(ConsumeResult<byte[], byte[]> record, MessageHeaders sent)
    {
        Assert.Equal(
            sent.Select(h => (h.Name, Convert.ToHexString(h.Value))),
            record.Message.Headers.Select(h => (h.Key, h.GetValueBytes() is { } value ? Convert.ToHexString(value) : "null")));
    }

    private static void AssertSameReport(DeliveryReport<byte[], byte[]> stock, (Error? Error, PersistenceStatus Status) raw)
    {
        Assert.Equal(stock.Status, raw.Status);
        Assert.NotNull(raw.Error);
        Assert.Equal(stock.Error.Code, raw.Error.Code);
        Assert.Equal(stock.Error.Reason, raw.Error.Reason);
        Assert.Equal(stock.Error.IsFatal, raw.Error.IsFatal);
        Assert.Equal(KafkaErrorClassifier.Classify(stock.Error), KafkaErrorClassifier.Classify(raw.Error));
    }

    private static async Task<(Error? Error, PersistenceStatus Status)> RawReportAsync(ProducerConfig config, string topic)
    {
        var reports = new ReportSink();
        using var producer = KafkaDestinationFactory.BuildProducer(config, NullLogger.Instance, static _ => { }, reports.OnReport);
        Assert.Null(producer.Produce(new KafkaAddress(topic), [1], [1], MessageHeaders.Empty, 1));
        return await reports.NextAsync();
    }

    private static async Task<DeliveryReport<byte[], byte[]>> StockReportAsync(ProducerConfig config, string topic)
    {
        var report = new TaskCompletionSource<DeliveryReport<byte[], byte[]>>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var producer = new ProducerBuilder<byte[], byte[]>(config).Build();
        producer.Produce(topic, new Message<byte[], byte[]> { Key = [1], Value = [1] }, report.SetResult);
        return await report.Task.WaitAsync(ReportWait, Ct);
    }

    private static KafkaClientSettings Settings(string bootstrapServers, int? messageTimeoutMs = null)
    {
        var settings = KafkaClientSettings.Build(new KafkaClusterOptions { BootstrapServers = bootstrapServers }, OrderingMode.None);
        if (messageTimeoutMs is { } timeout)
        {
            settings.Producer.Set("message.timeout.ms", timeout.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        return settings;
    }

    // An undescribed topic gets the config ValidateAsync would set, so sends to a missing topic reach the producer.
    private static KafkaDestination Destination(KafkaClientSettings settings, string topic, bool described)
    {
        var addresses = KafkaAddressMap.ForRoutes(new Dictionary<string, string> { [Route] = topic });
        if (!described)
        {
            addresses.Distinct[0].Config = new TopicConfig(1_000_000, Compacted: false);
        }

        return new KafkaDestination("Events", addresses, "Main", settings, probe: false, [], KafkaDestinationFactory.BuildProducer, NullLogger.Instance);
    }

    private static OutgoingMessage Message(string payload, MessageHeaders headers) => new()
    {
        Source = new MessageSource("Orders", 1, "Events"),
        Key = "k"u8.ToArray(),
        Payload = Encoding.UTF8.GetBytes(payload),
        Headers = headers,
        Route = Route,
    };

    private static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow.AddSeconds(10);
        while (!condition())
        {
            Assert.True(DateTime.UtcNow < deadline, "condition not met within 10 s");
            await Task.Delay(10, Ct);
        }
    }

    private sealed class ReportSink
    {
        private readonly System.Threading.Channels.Channel<(Error? Error, PersistenceStatus Status)> reports =
            System.Threading.Channels.Channel.CreateUnbounded<(Error?, PersistenceStatus)>();

        public void OnReport(nint opaque, Error? error, PersistenceStatus status) => reports.Writer.TryWrite((error, status));

        public void OnStock(DeliveryReport<byte[], byte[]> report) =>
            reports.Writer.TryWrite((report.Error.IsError ? report.Error : null, report.Status));

        public async Task<(Error? Error, PersistenceStatus Status)> NextAsync() =>
            await reports.Reader.ReadAsync(Ct).AsTask().WaitAsync(ReportWait, Ct);
    }
}
