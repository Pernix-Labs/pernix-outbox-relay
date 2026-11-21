namespace Pernix.OutboxRelay.Core.Abstractions;

// Exactly one of Value and UnavailableReason is set. default is Of(TimeSpan.Zero), a valid state.
public readonly record struct GateLag
{
    private readonly TimeSpan lag;

    private GateLag(TimeSpan lag, string? unavailableReason)
    {
        this.lag = lag;
        UnavailableReason = unavailableReason;
    }

    // Null exactly when unavailable.
    public TimeSpan? Value => UnavailableReason is null ? lag : null;

    // Sanitized: names the missing grant. Null when Value is set.
    public string? UnavailableReason { get; }

    public static GateLag Of(TimeSpan lag)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(lag, TimeSpan.Zero);
        return new GateLag(lag, null);
    }

    public static GateLag Unavailable(string reason)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(reason);
        return new GateLag(TimeSpan.Zero, reason);
    }
}
