using Pernix.OutboxRelay.Core.Configuration;

namespace Pernix.OutboxRelay.Core.Delivery;

internal sealed class PauseSchedule(RetryOptions options)
{
    private int count;

    public TimeSpan Next()
    {
        if (count < int.MaxValue)
        {
            count++;
        }

        return Backoff.Delay(options, count);
    }

    public void Reset() => count = 0;
}
