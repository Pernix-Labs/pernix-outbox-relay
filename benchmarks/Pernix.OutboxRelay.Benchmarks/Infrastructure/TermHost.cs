using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;
using Pernix.OutboxRelay.Core.Delivery;
using Pernix.OutboxRelay.Core.Leadership;
using Microsoft.Extensions.Logging.Abstractions;

namespace Pernix.OutboxRelay.Benchmarks.Infrastructure;

public enum TermDestination
{
    Null,
    Kafka,
}

internal static class TermHost
{
    public const int BatchSize = 500;
    public static readonly TimeSpan SteadyInterval = TimeSpan.FromMilliseconds(5);
    public static readonly TimeSpan IdleInterval = new PollingOptions().Interval;

    public static DeliveryTerm Create(
        OrderingMode mode, Func<IDestination> createDestination, TimeSpan interval, int batchSize = BatchSize, DeadLetterTarget? deadLetter = null)
    {
        var options = new OutboxOptions
        {
            Name = Workload.Outbox,
            Destination = Workload.Destination,
            Polling = new PollingOptions { Interval = interval, BatchSize = batchSize },
            Ordering = new OrderingOptions { Mode = mode },
        };

        if (deadLetter is not null)
        {
            options.DeadLetter = new DeadLetterOptions { Destination = deadLetter.DestinationName };
        }

        IDeliveryOrdering ordering = mode switch
        {
            OrderingMode.None => new UnorderedDelivery(batchSize),
            OrderingMode.PerKey or OrderingMode.Strict => new ScopedDelivery(mode, batchSize),
            _ => throw new ArgumentOutOfRangeException(nameof(mode), mode, "no delivery term for mode"),
        };

        return new DeliveryTerm(
            Workload.Outbox,
            options,
            Workload.Destination,
            createDestination,
            ordering,
            new OutboxRuntimeStatus(Workload.Outbox),
            TimeProvider.System,
            NullLogger.Instance,
            deadLetter);
    }
}
