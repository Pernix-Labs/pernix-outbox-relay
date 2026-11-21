using Pernix.OutboxRelay.Core.Configuration;

namespace Pernix.OutboxRelay.Core.Abstractions;

public sealed record OutgoingMessage
{
    public required MessageSource Source { get; init; }

    public byte[]? Key { get; init; }

    public required byte[] Payload { get; init; }

    public required MessageHeaders Headers { get; init; }

    public DeadLetterInfo? DeadLetter { get; init; }

    // Main send: the row's route, accepted by the destination. Dead letter: the original route, data only.
    public string? Route { get; init; }
}

public sealed record DeadLetterInfo(string Reason, int Attempts, DateTimeOffset FailedAt);

public sealed record MessageSource(string Outbox, long Id, string Destination);

public sealed record DestinationContext(OrderingMode Mode)
{
    public static DestinationContext Probe { get; } = new(OrderingMode.None) { IsProbe = true };

    public bool IsProbe { get; private init; }
}

// Closed: only the nested results exist, so Core handles every case.
public abstract record DeliveryResult
{
    private DeliveryResult()
    {
    }

    public sealed record Delivered : DeliveryResult
    {
        public static Delivered Instance { get; } = new();
    }

    public sealed record MessageFault(string Error, bool Permanent) : DeliveryResult;

    public sealed record DestinationFault(string Error) : DeliveryResult;
}
