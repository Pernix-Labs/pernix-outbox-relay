using System.Diagnostics.Metrics;

namespace Pernix.OutboxRelay.Core.Diagnostics;

internal static class DeliveryMetrics
{
    public const string DeliveredName = "outbox.delivered";
    public const string MessageFaultsName = "outbox.message_faults";
    public const string DestinationFaultsName = "outbox.destination_faults";
    public const string DeadLetteredName = "outbox.dead_lettered";

    private static readonly Counter<long> Delivered =
        RelayDiagnostics.Meter.CreateCounter<long>(DeliveredName, "{message}", "Messages the destination confirmed delivered");

    private static readonly Counter<long> MessageFaults =
        RelayDiagnostics.Meter.CreateCounter<long>(MessageFaultsName, "{message}", "Sends that returned a message fault");

    private static readonly Counter<long> DestinationFaults =
        RelayDiagnostics.Meter.CreateCounter<long>(DestinationFaultsName, "{message}", "Sends that returned a destination fault");

    private static readonly Counter<long> DeadLettered =
        RelayDiagnostics.Meter.CreateCounter<long>(DeadLetteredName, "{message}", "Dead letters the dead-letter destination confirmed delivered");

    public static void Record(string outbox) => Delivered.Add(1, new KeyValuePair<string, object?>("outbox", outbox));

    public static void RecordMessageFault(string outbox, string destination) => Add(MessageFaults, outbox, destination);

    public static void RecordDestinationFault(string outbox, string destination) => Add(DestinationFaults, outbox, destination);

    public static void RecordDeadLettered(string outbox, string destination) => Add(DeadLettered, outbox, destination);

    private static void Add(Counter<long> counter, string outbox, string destination) =>
        counter.Add(1, new KeyValuePair<string, object?>("outbox", outbox), new KeyValuePair<string, object?>("destination", destination));
}
