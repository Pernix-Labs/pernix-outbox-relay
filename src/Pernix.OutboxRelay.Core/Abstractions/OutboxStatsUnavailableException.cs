namespace Pernix.OutboxRelay.Core.Abstractions;

// Sanitized: outbox name and reason only. Never a connection string, key, payload or header value.
public sealed class OutboxStatsUnavailableException(string outbox, string reason)
    : Exception($"Outboxes[{outbox}]: stats unavailable ({reason})")
{
    public string Reason { get; } = reason;
}
