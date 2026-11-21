namespace Pernix.OutboxRelay.Core.Abstractions;

// Dispose discards unsent messages.
public interface IDestination : IAsyncDisposable
{
    // Longest a SendAsync call stays incomplete; the transport returns a result by then. Null: unbounded, liveness does not track the send.
    TimeSpan? SendBound { get; }

    // Null: single address, sends rows without a route. Otherwise the ordinal, non-empty set of routes it maps;
    // fixed for the instance's lifetime. Core holds every row it does not accept.
    IReadOnlySet<string>? Routes { get; }

    Task ValidateAsync(CancellationToken ct);

    // Never throws for delivery failures. DeadLetter null: Core calls this only for a row it accepts; send to the
    // address of Route (the single address when Routes is null). DeadLetter set: Routes is null (startup-checked);
    // send to the single address, Route is data only.
    ValueTask<DeliveryResult> SendAsync(OutgoingMessage message, CancellationToken ct);
}
