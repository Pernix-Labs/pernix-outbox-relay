using Pernix.OutboxRelay.Core.Configuration;

namespace Pernix.OutboxRelay.Core.Delivery;

internal static class Backoff
{
    public static TimeSpan Delay(RetryOptions options, int n)
    {
        var max = options.MaxDelay;
        var ticks = options.InitialDelay.Ticks * Math.Pow(options.BackoffMultiplier, n - 1.0);
        return double.IsNaN(ticks) || ticks >= max.Ticks ? max : TimeSpan.FromTicks((long)ticks);
    }
}
