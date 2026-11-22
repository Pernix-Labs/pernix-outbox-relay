using System.Reflection;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Destination.Kafka;
using Confluent.Kafka;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using Xunit;
using Nk = Nanov.Kafka;

namespace Pernix.OutboxRelay.Unit.Tests.Kafka;

public sealed class KafkaRawProducerTests
{
    private const string Topic = "raw-producer-tests";
    private const int RareProduces = 100;
    private const long FillOverflowBytes17 = 120;

    private static readonly byte[] Value = [1, 2, 3];
    private static readonly MessageHeaders CommonHeaders = new([new MessageHeader("h", [1])]);
    private static readonly MessageHeaders RareHeaders = new([new MessageHeader("h", [1]), new MessageHeader("empty", [])]);

    [Fact]
    public void Raw_enums_match_stock_by_name_and_value()
    {
        AssertSameMembers<Nk.ErrorCode, ErrorCode>();
        AssertSameMembers<Nk.PersistenceStatus, PersistenceStatus>();
        AssertSameMembers<Nk.SyslogLevel, SyslogLevel>();
    }

    [Fact]
    public void Cross_drops_local_fatal_and_maps_the_rest()
    {
        foreach (var code in Enum.GetValues<Nk.ErrorCode>())
        {
            foreach (var status in Enum.GetValues<Nk.PersistenceStatus>())
            {
                var calls = 0;
                Error? crossed = null;
                PersistenceStatus crossedStatus = default;
                KafkaRawProducer.Cross(42, code, status, (opaque, error, s) =>
                {
                    Assert.Equal(42, opaque);
                    calls++;
                    crossed = error;
                    crossedStatus = s;
                });

                if (code == Nk.ErrorCode.Local_Fatal)
                {
                    Assert.Equal(0, calls);
                    continue;
                }

                Assert.Equal(1, calls);
                Assert.Equal((int)status, (int)crossedStatus);
                if (code == Nk.ErrorCode.NoError)
                {
                    Assert.Null(crossed);
                    continue;
                }

                var stock = new Error((ErrorCode)(int)code);
                Assert.NotNull(crossed);
                Assert.Equal(stock.Code, crossed.Code);
                Assert.Equal(stock.Reason, crossed.Reason);
                Assert.Equal(stock.IsFatal, crossed.IsFatal);
            }
        }
    }

    [Fact]
    public async Task Raw_local_fatal_report_is_dropped_and_send_retires()
    {
        var logs = new CapturingLoggerFactory();
        var fake = new FakeProducer();
        Action<Error>? onFatal = null;
        var factory = new KafkaDestinationFactory(logs, (_, _, fatal, report) =>
        {
            fake.OnReport = report;
            onFatal = fatal;
            return fake;
        });
        await using var destination = Create(factory);

        var send = destination.SendAsync(KafkaHarness.Message([1], [1]), CancellationToken.None);
        Assert.Single(fake.Sends).ReportRaw(Nk.ErrorCode.Local_Fatal);
        Assert.False(send.IsCompleted);

        onFatal!(new Error(ErrorCode.OutOfOrderSequenceNumber, "sequence", isFatal: true));

        Assert.Equal(new DeliveryResult.DestinationFault("Local_Fatal: producer failed fatally before delivery report"), await send);
        Assert.Empty(logs.Events(202));
        var fatal = Assert.Single(logs.Events(203));
        Assert.Equal(nameof(ErrorCode.OutOfOrderSequenceNumber), fatal["Code"]);
    }

    [Fact]
    public async Task Build_producer_error_handler_logs_fatal_once_and_reports_it()
    {
        var logs = new CapturingLoggerFactory();
        var fatals = new List<Error>();
        using var producer = KafkaDestinationFactory.BuildProducer(
            Config(), logs.CreateLogger("test"), error => { lock (fatals) { fatals.Add(error); } }, static (_, _, _) => { });

        var reason = Marshal.StringToHGlobalAnsi("sequence");
        try
        {
            Assert.Equal(0, TestFatalError(NativeHandle(producer), (int)ErrorCode.OutOfOrderSequenceNumber, reason));
        }
        finally
        {
            Marshal.FreeHGlobal(reason);
        }

        await KafkaHarness.WaitUntilAsync(() => FatalLogs(logs).Count > 0);
        await Task.Delay(200, TestContext.Current.CancellationToken);

        var logged = Assert.Single(FatalLogs(logs));
        Error fatal;
        lock (fatals)
        {
            fatal = Assert.Single(fatals);
        }

        Assert.True(fatal.IsFatal);
        Assert.Equal(ErrorCode.OutOfOrderSequenceNumber, fatal.Code);
        Assert.Contains("sequence", fatal.Reason, StringComparison.Ordinal);
        Assert.Equal(nameof(ErrorCode.OutOfOrderSequenceNumber), logged["Code"]);
        Assert.Equal(fatal.Reason, logged["Reason"]);
    }

    [Fact]
    public void Empty_header_value_path_binds_internal_members()
    {
        Assert.Equal(new Version(2, 16, 0, 3), typeof(Nk.IRawProducer).Assembly.GetName().Version);

        var config = Config();
        config.Set("queue.buffering.max.messages", "1");
        using var raw = new Nk.RawProducerBuilder(config).BuildRaw();

        // Accepted: headers_new, headers_add, ProduceRawCore. Refused: ThrowIfError, then headers_destroy. An unbound member throws MissingMethodException.
        Assert.Null(KafkaEmptyHeaderValues.Produce(raw, Topic, [], Value, RareHeaders, 1));
        var refused = Assert.Throws<Nk.KafkaException>(() => KafkaEmptyHeaderValues.Produce(raw, Topic, null, Value, RareHeaders, 2));
        Assert.Equal(Nk.ErrorCode.Local_QueueFull, refused.Error.Code);
    }

    [Fact]
    public void Rare_path_refusal_matches_public_path()
    {
        var common = Refusal(CommonHeaders);
        var rare = Refusal(RareHeaders);

        Assert.Equal(ErrorCode.Local_QueueFull, common.Code);
        Assert.Equal(common.Code, rare.Code);
        Assert.Equal(common.Reason, rare.Reason);
        Assert.Equal(common.IsFatal, rare.IsFatal);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(8)]
    [InlineData(9)]
    [InlineData(16)]
    public void Rare_path_allocates_nothing(int count)
    {
        Assert.Equal(0, RarePathBytes(count));
    }

    [Fact]
    public void Rare_path_with_zero_length_value_at_index_17_pays_fill_overflow()
    {
        var bytes = RarePathBytes(18);
        TestContext.Current.TestOutputHelper?.WriteLine($"Rare path 18 headers, zero-length at 17: {bytes / RareProduces} B per produce");

        Assert.Equal(FillOverflowBytes17 * RareProduces, bytes);
    }

    private static long RarePathBytes(int count)
    {
        var items = new MessageHeader[count];
        for (var i = 0; i < count; i++)
        {
            items[i] = new MessageHeader($"h{i}", i == count - 1 ? [] : [(byte)i]);
        }

        var headers = new MessageHeaders(items);
        using var producer = KafkaDestinationFactory.BuildProducer(Config(), Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, static _ => { }, static (_, _, _) => { });
        var address = new KafkaAddress(Topic);
        byte[] key = [7];
        Assert.Null(producer.Produce(address, key, Value, headers, 1));

        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < RareProduces; i++)
        {
            producer.Produce(address, key, Value, headers, i + 2);
        }

        return GC.GetAllocatedBytesForCurrentThread() - before;
    }

    private static IReadOnlyList<Dictionary<string, object?>> FatalLogs(CapturingLoggerFactory logs) =>
        [.. logs.Events(202).Where(e => Equals(e["Code"], nameof(ErrorCode.OutOfOrderSequenceNumber)))];

    private static Error Refusal(MessageHeaders headers)
    {
        var config = Config();
        config.Set("queue.buffering.max.messages", "1");
        using var producer = KafkaDestinationFactory.BuildProducer(config, Microsoft.Extensions.Logging.Abstractions.NullLogger.Instance, static _ => { }, static (_, _, _) => { });
        var address = new KafkaAddress(Topic);

        Assert.Null(producer.Produce(address, [1], Value, headers, 1));
        var refused = producer.Produce(address, [1], Value, headers, 2);
        Assert.NotNull(refused);
        return refused;
    }

    private static ProducerConfig Config()
    {
        return KafkaClientSettings.Build(new KafkaClusterOptions { BootstrapServers = "localhost:1" }, OrderingMode.None).Producer;
    }

    private static KafkaDestination Create(KafkaDestinationFactory factory)
    {
        var relay = RelayOptionsBinder.Bind(new ConfigurationBuilder().AddInMemoryCollection(new Dictionary<string, string?>
        {
            ["KafkaClusters:Main:BootstrapServers"] = "kafka-1:9092",
            ["Destinations:0:Name"] = KafkaHarness.Destination,
            ["Destinations:0:Transport"] = "Kafka",
            ["Destinations:0:Kafka:Cluster"] = "Main",
            ["Destinations:0:Kafka:Topic"] = KafkaHarness.Topic,
        }).Build());
        var options = relay.Destinations[0];
        Assert.Empty(factory.Validate(options, $"Destinations[{options.Name}]", relay));
        foreach (var address in factory.AddressesOf(KafkaHarness.Destination).Distinct)
        {
            address.Config = new TopicConfig(1_000_000, Compacted: false);
        }

        return Assert.IsType<KafkaDestination>(factory.Create(options, new DestinationContext(OrderingMode.None)));
    }

    private static void AssertSameMembers<TRaw, TStock>()
        where TRaw : struct, Enum
        where TStock : struct, Enum
    {
        var raw = Enum.GetNames<TRaw>().ToDictionary(name => name, name => Convert.ToInt64(Enum.Parse<TRaw>(name), System.Globalization.CultureInfo.InvariantCulture));
        var stock = Enum.GetNames<TStock>().ToDictionary(name => name, name => Convert.ToInt64(Enum.Parse<TStock>(name), System.Globalization.CultureInfo.InvariantCulture));

        Assert.Equal(stock.OrderBy(p => p.Key, StringComparer.Ordinal), raw.OrderBy(p => p.Key, StringComparer.Ordinal));
    }

    // The fork exposes no fatal-error trigger, so the test reaches the native handle and calls librdkafka's own test API.
    private static IntPtr NativeHandle(IKafkaRawProducer producer)
    {
        var raw = (Nk.IRawProducer)typeof(KafkaRawProducer)
            .GetFields(BindingFlags.Instance | BindingFlags.NonPublic)
            .Single(f => f.FieldType == typeof(Nk.IRawProducer))
            .GetValue(producer)!;
        var handle = (SafeHandle)typeof(Nk.Handle)
            .GetProperty("LibrdkafkaHandle", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(raw.Handle)!;
        return handle.DangerousGetHandle();
    }

    private static int TestFatalError(IntPtr rk, int err, IntPtr reason)
    {
        var export = NativeLibrary.GetExport(LoadedLibrdkafka(null), "rd_kafka_test_fatal_error");
        return Marshal.GetDelegateForFunctionPointer<TestFatalErrorFn>(export)(rk, err, reason);
    }

    [UnsafeAccessor(UnsafeAccessorKind.StaticField, Name = "nativeLibraryHandle")]
    private static extern ref IntPtr LoadedLibrdkafka([UnsafeAccessorType("Nanov.Kafka.Impl.Librdkafka, Nanov.Kafka")] object? type);

    private delegate int TestFatalErrorFn(IntPtr rk, int err, IntPtr reason);

    private sealed class CapturingLoggerFactory : ILoggerFactory, ILogger
    {
        private readonly List<(int Id, Dictionary<string, object?> State)> events = [];

        public IReadOnlyList<Dictionary<string, object?>> Events(int id)
        {
            lock (events)
            {
                return [.. events.Where(e => e.Id == id).Select(e => e.State)];
            }
        }

        public ILogger CreateLogger(string categoryName) => this;

        public void AddProvider(ILoggerProvider provider)
        {
        }

        public IDisposable? BeginScope<TState>(TState state) where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(LogLevel logLevel, EventId eventId, TState state, Exception? exception, Func<TState, Exception?, string> formatter)
        {
            var fields = state is IEnumerable<KeyValuePair<string, object?>> pairs ? pairs.ToDictionary(p => p.Key, p => p.Value) : [];
            lock (events)
            {
                events.Add((eventId.Id, fields));
            }
        }

        public void Dispose()
        {
        }
    }
}
