namespace Pernix.OutboxRelay.Core.Delivery;

internal abstract record DeliveryTermResult
{
    private DeliveryTermResult()
    {
    }

    internal sealed record Lost(string Reason) : DeliveryTermResult;

    internal sealed record Stopped(DateTimeOffset StopSignalAt) : DeliveryTermResult;
}
