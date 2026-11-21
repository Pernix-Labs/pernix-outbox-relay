using Pernix.OutboxRelay.Core.Abstractions;

namespace Pernix.OutboxRelay.Core.Delivery;

// Ordering.Mode None: read from head excluding held rows, send every row at once.
internal sealed class UnorderedDelivery : IDeliveryOrdering
{
    private readonly int batchSize;
    private readonly Queue<OutboxMessage> accepted = new();

    public UnorderedDelivery(int batchSize)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);
        this.batchSize = batchSize;
        Capacity = batchSize * IDeliveryOrdering.CapacityFactor;
    }

    public int Capacity { get; }

    public bool UsesSummary => false;

    public bool ExcludesHeld => true;

    public bool Halted => false;

    public bool AwaitingMoveBack => false;

    public void Tick(DateTimeOffset now)
    {
    }

    public ReadRequest? NextRead(int room, IReadOnlyCollection<long> held) =>
        room > 0 ? new ReadRequest(Math.Min(batchSize, room), null, held) : null;

    public void Accept(IReadOnlyList<OutboxMessage> rows, Func<long, bool> isHeld, ICollection<OutboxMessage> queued)
    {
        foreach (var row in rows)
        {
            if (isHeld(row.Id))
            {
                continue;
            }

            accepted.Enqueue(row);
            queued.Add(row);
        }
    }

    public void Sendable(ICollection<OutboxMessage> output)
    {
        while (accepted.TryDequeue(out var row))
        {
            output.Add(row);
        }
    }

    public void Release(long id)
    {
    }

    public void Fail(long id, ICollection<long> evicted)
    {
    }

    public void Unpark(IReadOnlyCollection<long> ids, ICollection<long> evicted)
    {
    }

    public void Left(long id)
    {
    }

    public void Summary(FailedRowsSummary summary)
    {
    }

    public void DrainRewinds(ICollection<string> reasons)
    {
    }
}
