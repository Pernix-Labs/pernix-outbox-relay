using System.Collections.Concurrent;
using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Confluent.Kafka;
using Microsoft.Extensions.Logging;
using LogLevel = Microsoft.Extensions.Logging.LogLevel;
using Nk = Nanov.Kafka;

namespace Pernix.OutboxRelay.Kafka;

public sealed class KafkaDestinationFactory : IDestinationFactory
{
    private const string OwnSetting = "Kafka";

    private readonly ConcurrentDictionary<string, ResolvedDestination> resolved = new(StringComparer.OrdinalIgnoreCase);
    private readonly Func<ProducerConfig, ILogger, Action<Error>, KafkaReportHandler, IKafkaRawProducer> producerFactory;
    private readonly ILogger logger;

    public KafkaDestinationFactory(ILoggerFactory loggerFactory)
        : this(loggerFactory, BuildProducer)
    {
    }

    internal KafkaDestinationFactory(ILoggerFactory loggerFactory, Func<ProducerConfig, ILogger, Action<Error>, KafkaReportHandler, IKafkaRawProducer> producerFactory)
    {
        ArgumentNullException.ThrowIfNull(loggerFactory);
        ArgumentNullException.ThrowIfNull(producerFactory);

        this.producerFactory = producerFactory;
        logger = loggerFactory.CreateLogger<KafkaDestinationFactory>();

        // Loads librdkafka on every start, so a missing native library fails the image smoke test.
        logger.KafkaClientLoaded(Library.VersionString);
    }

    public string Transport => "Kafka";

    public IReadOnlyList<string> Validate(DestinationOptions destination, string path, RelayOptions relay)
    {
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(path);
        ArgumentNullException.ThrowIfNull(relay);

        var failures = new List<string>();
        var kafka = new KafkaDestinationOptions();
        if (destination.Settings is { } settings)
        {
            foreach (var child in settings.GetChildren())
            {
                if (!string.Equals(child.Key, OwnSetting, StringComparison.OrdinalIgnoreCase)
                    && !DestinationOptions.CoreKeys.Contains(child.Key, StringComparer.OrdinalIgnoreCase))
                {
                    failures.Add($"{path}: unknown setting '{child.Key}'");
                }
            }

            kafka = RelayOptionsBinder.BindSection<KafkaDestinationOptions>(settings.GetSection(OwnSetting), $"{path}.{OwnSetting}", failures);
        }

        KafkaClusterOptions? cluster = null;
        if (HasValue(kafka.Cluster) && !relay.KafkaClusters.TryGetValue(kafka.Cluster!, out cluster))
        {
            failures.Add($"{path}.Kafka.Cluster: Kafka cluster '{kafka.Cluster}' is not defined in KafkaClusters");
        }

        Required(failures, $"{path}.Kafka.Cluster", kafka.Cluster);

        var hasTopic = HasValue(kafka.Topic);
        var hasRoutes = kafka.Routes is { Count: > 0 };
        if (hasTopic && hasRoutes)
        {
            failures.Add($"{path}.Kafka: Topic and Routes are both set; set exactly one");
        }
        else if (!hasTopic && !hasRoutes)
        {
            failures.Add($"{path}.Kafka: Topic or Routes is required when Transport is Kafka");
        }

        if (hasRoutes)
        {
            foreach (var (route, topic) in kafka.Routes!)
            {
                if (!HasValue(topic))
                {
                    failures.Add($"{path}.Kafka.Routes.{route}: topic is required");
                }
            }
        }

        if (failures.Count == 0 && cluster is not null && HasValue(destination.Name))
        {
            var deadLetterOf = relay.Outboxes
                .Where(o => string.Equals(o.DeadLetterDestinationName, destination.Name, StringComparison.OrdinalIgnoreCase))
                .Select(o => o.Name ?? string.Empty)
                .ToArray();
            var addresses = hasRoutes ? KafkaAddressMap.ForRoutes(kafka.Routes!) : KafkaAddressMap.ForTopic(kafka.Topic!);
            resolved[destination.Name!] = new ResolvedDestination(kafka.Cluster!, addresses, cluster, deadLetterOf);
        }

        return failures;
    }

    public IDestination Create(DestinationOptions destination, DestinationContext context)
    {
        ArgumentNullException.ThrowIfNull(destination);
        ArgumentNullException.ThrowIfNull(context);

        if (destination.Name is not { } name || !resolved.TryGetValue(name, out var target))
        {
            throw new InvalidOperationException($"Destination '{destination.Name}' was not validated by the Kafka transport.");
        }

        return new KafkaDestination(
            name,
            target.Addresses,
            target.ClusterName,
            KafkaClientSettings.Build(target.Cluster, context.Mode),
            probe: context.IsProbe,
            target.DeadLetterOf,
            producerFactory,
            logger);
    }

    // Test seam: the address table shared by every instance of the destination.
    internal KafkaAddressMap AddressesOf(string destination) => resolved[destination].Addresses;

    internal static IKafkaRawProducer BuildProducer(ProducerConfig config, ILogger logger, Action<Error> onFatal, KafkaReportHandler onReport)
    {
        var builder = new Nk.RawProducerBuilder(config);
        builder.SetDeliveryReportHandler(KafkaRawProducer.ReportHandler(onReport));
        builder.SetLogHandler((_, message) => OnClientLog(logger, message));
        builder.SetErrorHandler((client, raw) =>
        {
            var error = KafkaRawProducer.ToStock(raw);
            OnClientError(logger, client.Name, error);
            if (error.IsFatal)
            {
                onFatal(error);
            }
        });

        try
        {
            return new KafkaRawProducer(builder.BuildRaw());
        }
        catch (Nk.KafkaException ex)
        {
            throw new KafkaException(KafkaRawProducer.ToStock(ex.Error));
        }
    }

    internal static void OnClientLog(ILogger logger, LogMessage message) =>
        logger.KafkaClientLog(LevelOf(message.Level), message.Name, message.Facility, message.Message);

    internal static void OnClientLog(ILogger logger, Nk.LogMessage message) =>
        logger.KafkaClientLog(LevelOf((SyslogLevel)(int)message.Level), message.Name, message.Facility, message.Message);

    internal static void OnClientError(ILogger logger, string client, Error error) =>
        logger.KafkaClientError(client, error.Code.ToString(), error.Reason);

    internal static LogLevel LevelOf(SyslogLevel level) => level switch
    {
        SyslogLevel.Emergency or SyslogLevel.Alert or SyslogLevel.Critical => LogLevel.Critical,
        SyslogLevel.Error => LogLevel.Error,
        SyslogLevel.Warning => LogLevel.Warning,
        SyslogLevel.Notice or SyslogLevel.Info => LogLevel.Information,
        _ => LogLevel.Debug,
    };

    private static bool HasValue(string? value) => !string.IsNullOrWhiteSpace(value);

    private static void Required(List<string> failures, string path, string? value)
    {
        if (!HasValue(value))
        {
            failures.Add($"{path}: required when Transport is Kafka");
        }
    }

    private sealed record ResolvedDestination(string ClusterName, KafkaAddressMap Addresses, KafkaClusterOptions Cluster, IReadOnlyList<string> DeadLetterOf);
}
