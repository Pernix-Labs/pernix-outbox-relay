using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Kafka;
using Confluent.Kafka;
using Xunit;

namespace Pernix.OutboxRelay.Unit.Tests.Kafka;

public sealed class KafkaClientSettingsTests
{
    private static KafkaClusterOptions Cluster(Dictionary<string, string>? producer = null) => new()
    {
        BootstrapServers = "kafka-1:9092,kafka-2:9092",
        Producer = producer ?? [],
    };

    [Theory]
    [InlineData(OrderingMode.None, "5", "30000")]
    [InlineData(OrderingMode.PerKey, "5", "0")]
    [InlineData(OrderingMode.Strict, "0", "0")]
    public void Forced_keys_and_bootstrap_servers_win_over_producer_entries(OrderingMode mode, string lingerMs, string messageTimeoutMs)
    {
        var settings = KafkaClientSettings.Build(
            Cluster(new()
            {
                ["enable.idempotence"] = "false",
                ["acks"] = "1",
                ["partitioner"] = "consistent",
                ["linger.ms"] = "100",
                ["message.timeout.ms"] = "1000",
                ["bootstrap.servers"] = "other:9092",
            }),
            mode);

        Assert.Equal("true", settings.Producer.Get("enable.idempotence"));
        Assert.Equal("all", settings.Producer.Get("acks"));
        Assert.Equal("murmur2_random", settings.Producer.Get("partitioner"));
        Assert.Equal(lingerMs, settings.Producer.Get("linger.ms"));
        Assert.Equal(messageTimeoutMs, settings.Producer.Get("message.timeout.ms"));
        Assert.Equal("kafka-1:9092,kafka-2:9092", settings.Producer.Get("bootstrap.servers"));
        Assert.Equal("kafka-1:9092,kafka-2:9092", settings.Admin.Get("bootstrap.servers"));
    }

    [Theory]
    [InlineData(OrderingMode.None, "linger.ms", "queue.buffering.max.ms", "5", false)]
    [InlineData(OrderingMode.None, "linger.ms", "queue.buffering.max.ms", "5", true)]
    [InlineData(OrderingMode.None, "message.timeout.ms", "delivery.timeout.ms", "30000", false)]
    [InlineData(OrderingMode.None, "message.timeout.ms", "delivery.timeout.ms", "30000", true)]
    [InlineData(OrderingMode.None, "acks", "request.required.acks", "all", false)]
    [InlineData(OrderingMode.None, "acks", "request.required.acks", "all", true)]
    [InlineData(OrderingMode.None, "bootstrap.servers", "metadata.broker.list", "kafka-1:9092,kafka-2:9092", false)]
    [InlineData(OrderingMode.None, "bootstrap.servers", "metadata.broker.list", "kafka-1:9092,kafka-2:9092", true)]
    [InlineData(OrderingMode.PerKey, "linger.ms", "queue.buffering.max.ms", "5", false)]
    [InlineData(OrderingMode.PerKey, "linger.ms", "queue.buffering.max.ms", "5", true)]
    [InlineData(OrderingMode.PerKey, "message.timeout.ms", "delivery.timeout.ms", "0", false)]
    [InlineData(OrderingMode.PerKey, "message.timeout.ms", "delivery.timeout.ms", "0", true)]
    [InlineData(OrderingMode.PerKey, "acks", "request.required.acks", "all", false)]
    [InlineData(OrderingMode.PerKey, "acks", "request.required.acks", "all", true)]
    [InlineData(OrderingMode.PerKey, "bootstrap.servers", "metadata.broker.list", "kafka-1:9092,kafka-2:9092", false)]
    [InlineData(OrderingMode.PerKey, "bootstrap.servers", "metadata.broker.list", "kafka-1:9092,kafka-2:9092", true)]
    [InlineData(OrderingMode.Strict, "linger.ms", "queue.buffering.max.ms", "0", false)]
    [InlineData(OrderingMode.Strict, "linger.ms", "queue.buffering.max.ms", "0", true)]
    [InlineData(OrderingMode.Strict, "message.timeout.ms", "delivery.timeout.ms", "0", false)]
    [InlineData(OrderingMode.Strict, "message.timeout.ms", "delivery.timeout.ms", "0", true)]
    [InlineData(OrderingMode.Strict, "acks", "request.required.acks", "all", false)]
    [InlineData(OrderingMode.Strict, "acks", "request.required.acks", "all", true)]
    [InlineData(OrderingMode.Strict, "bootstrap.servers", "metadata.broker.list", "kafka-1:9092,kafka-2:9092", false)]
    [InlineData(OrderingMode.Strict, "bootstrap.servers", "metadata.broker.list", "kafka-1:9092,kafka-2:9092", true)]
    public void Forced_keys_win_over_librdkafka_aliases_in_either_order(OrderingMode mode, string forced, string alias, string expected, bool aliasFirst)
    {
        Dictionary<string, string> producer = aliasFirst
            ? new() { [alias] = "1", [forced] = "2" }
            : new() { [forced] = "2", [alias] = "1" };

        var settings = KafkaClientSettings.Build(Cluster(producer), mode);

        Assert.Equal(expected, settings.Producer.Get(forced));
        Assert.Null(settings.Producer.Get(alias));
        Assert.Null(settings.Admin.Get(alias));
    }

    [Fact]
    public void Other_producer_entries_pass_through_to_producer_and_admin()
    {
        var settings = KafkaClientSettings.Build(
            Cluster(new() { ["compression.type"] = "lz4", ["client.id"] = "orders-relay", ["security.protocol"] = "plaintext" }),
            OrderingMode.None);

        foreach (var config in new Confluent.Kafka.ClientConfig[] { settings.Producer, settings.Admin })
        {
            Assert.Equal("lz4", config.Get("compression.type"));
            Assert.Equal("orders-relay", config.Get("client.id"));
            Assert.Equal("plaintext", config.Get("security.protocol"));
        }
    }

    [Fact]
    public void Admin_config_has_no_forced_producer_keys()
    {
        var settings = KafkaClientSettings.Build(Cluster(), OrderingMode.None);

        Assert.Equal(
            [new KeyValuePair<string, string>("bootstrap.servers", "kafka-1:9092,kafka-2:9092")],
            settings.Admin.ToList());
    }

    [Theory]
    [InlineData(OrderingMode.None)]
    [InlineData(OrderingMode.PerKey)]
    [InlineData(OrderingMode.Strict)]
    public void Producer_config_is_bootstrap_plus_forced_keys_only(OrderingMode mode)
    {
        var settings = KafkaClientSettings.Build(Cluster(), mode);

        Assert.Equal(
            new[]
            {
                "acks", "bootstrap.servers", "dotnet.producer.delivery.report.fields", "dotnet.producer.enable.delivery.reports",
                "enable.idempotence", "linger.ms", "message.timeout.ms", "partitioner",
            },
            settings.Producer.Select(e => e.Key).Order(StringComparer.Ordinal));
    }

    [Theory]
    [InlineData(OrderingMode.None)]
    [InlineData(OrderingMode.PerKey)]
    [InlineData(OrderingMode.Strict)]
    public void Delivery_reports_are_forced_on_with_status_only(OrderingMode mode)
    {
        var settings = KafkaClientSettings.Build(Cluster(), mode);

        Assert.Equal("status", settings.Producer.Get("dotnet.producer.delivery.report.fields"));
        Assert.Equal("true", settings.Producer.Get("dotnet.producer.enable.delivery.reports"));
        Assert.Equal("status", settings.Producer.DeliveryReportFields);
        Assert.True(settings.Producer.EnableDeliveryReports);
    }

    [Theory]
    [InlineData(OrderingMode.None, "none", "false")]
    [InlineData(OrderingMode.None, "all", "false")]
    [InlineData(OrderingMode.PerKey, "none", "false")]
    [InlineData(OrderingMode.PerKey, "key,value,timestamp,headers,status", "false")]
    [InlineData(OrderingMode.Strict, "none", "false")]
    [InlineData(OrderingMode.Strict, "all", "false")]
    public void Producer_delivery_report_entries_are_overridden(OrderingMode mode, string fields, string enabled)
    {
        var settings = KafkaClientSettings.Build(
            Cluster(new()
            {
                ["dotnet.producer.delivery.report.fields"] = fields,
                ["dotnet.producer.enable.delivery.reports"] = enabled,
            }),
            mode);

        Assert.Equal("status", settings.Producer.Get("dotnet.producer.delivery.report.fields"));
        Assert.Equal("true", settings.Producer.Get("dotnet.producer.enable.delivery.reports"));
    }

    [Theory]
    [InlineData(OrderingMode.None)]
    [InlineData(OrderingMode.PerKey)]
    [InlineData(OrderingMode.Strict)]
    public void Producer_delivery_report_entries_stay_out_of_admin_config(OrderingMode mode)
    {
        var settings = KafkaClientSettings.Build(
            Cluster(new()
            {
                ["dotnet.producer.delivery.report.fields"] = "all",
                ["dotnet.producer.enable.delivery.reports"] = "false",
            }),
            mode);

        Assert.Null(settings.Admin.Get("dotnet.producer.delivery.report.fields"));
        Assert.Null(settings.Admin.Get("dotnet.producer.enable.delivery.reports"));
        Assert.Equal("status", settings.Producer.Get("dotnet.producer.delivery.report.fields"));
        Assert.Equal("true", settings.Producer.Get("dotnet.producer.enable.delivery.reports"));
    }

    [Fact]
    public void Admin_client_builds_when_producer_delivery_report_entries_are_set()
    {
        var settings = KafkaClientSettings.Build(
            new KafkaClusterOptions
            {
                BootstrapServers = "127.0.0.1:1",
                Producer = new()
                {
                    ["dotnet.producer.delivery.report.fields"] = "all",
                    ["dotnet.producer.enable.delivery.reports"] = "false",
                },
            },
            OrderingMode.Strict);

        using var admin = new AdminClientBuilder(settings.Admin).Build();

        Assert.NotNull(admin);
    }

    [Fact]
    public void Message_max_bytes_defaults_to_client_default()
    {
        Assert.Equal(1_000_000, KafkaClientSettings.Build(Cluster(), OrderingMode.None).MessageMaxBytes);
    }

    [Fact]
    public void Message_max_bytes_comes_from_producer_entry()
    {
        Assert.Equal(2_000_000, KafkaClientSettings.Build(Cluster(new() { ["message.max.bytes"] = "2000000" }), OrderingMode.None).MessageMaxBytes);
    }

    [Fact]
    public void Undefined_mode_throws()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => KafkaClientSettings.Build(Cluster(), (OrderingMode)99));
    }
}
