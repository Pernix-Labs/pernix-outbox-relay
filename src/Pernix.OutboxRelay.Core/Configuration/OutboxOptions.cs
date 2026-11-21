namespace Pernix.OutboxRelay.Core.Configuration;

public sealed class OutboxOptions
{
    public string? Name { get; set; }
    public string? Destination { get; set; }
    public DatabaseOptions? Database { get; set; }
    public PollingOptions Polling { get; set; } = new();
    public RetryOptions Retry { get; set; } = new();
    public DeadLetterOptions? DeadLetter { get; set; }
    public OrderingOptions? Ordering { get; set; }

    // Null when dead-lettering is off. An empty DeadLetter.Destination switches it off, so an env var can override an SSM value.
    public string? DeadLetterDestinationName => string.IsNullOrWhiteSpace(DeadLetter?.Destination) ? null : DeadLetter.Destination;
}

public sealed class PollingOptions
{
    public TimeSpan Interval { get; set; } = TimeSpan.FromSeconds(1);
    public int BatchSize { get; set; } = 500;
}

public sealed class RetryOptions
{
    public int MaxRetryAttempts { get; set; } = 3;
    public TimeSpan InitialDelay { get; set; } = TimeSpan.FromSeconds(10);
    public double BackoffMultiplier { get; set; } = 2.0;
    public TimeSpan MaxDelay { get; set; } = TimeSpan.FromMinutes(5);
}

public sealed class DeadLetterOptions
{
    public string? Destination { get; set; }
}

public sealed class OrderingOptions
{
    public OrderingMode? Mode { get; set; }
    public OnPermanentFailure OnPermanentFailure { get; set; } = OnPermanentFailure.Continue;
}
