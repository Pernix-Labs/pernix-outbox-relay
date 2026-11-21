namespace Pernix.OutboxRelay.Core.Abstractions;

public interface IValidatedOutboxStore
{
    Task<IOutboxSession> OpenSessionAsync(CancellationToken ct);

    // Off the session: own connection, never an IOutboxSession's. Bounded by the provider's deadline, honours ct at once.
    // Throws OutboxStatsUnavailableException (sanitized) when unreachable, past the deadline, refused, or on a standby.
    Task<OutboxStats> GetStatsAsync(CancellationToken ct);
}
