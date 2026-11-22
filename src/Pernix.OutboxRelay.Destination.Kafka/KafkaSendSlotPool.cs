namespace Pernix.OutboxRelay.Destination.Kafka;

internal sealed class KafkaSendSlotPool<TSlot>(Func<int, TSlot> create) : IDisposable
    where TSlot : class
{
    internal const int MaxSlots = 1 << IndexBits;
    internal const long MaxGeneration = (1L << 40) - 1;
    private const int IndexBits = 24;

    private readonly Stack<TSlot> idle = new();
    private TSlot?[] slots = new TSlot?[4];
    private bool disposed;
    private int created;

    internal int Created => Volatile.Read(ref created);

    internal int IdleCount
    {
        get
        {
            lock (idle)
            {
                return idle.Count;
            }
        }
    }

    public static nint Handle(int index, long generation) => (nint)((generation << IndexBits) | (uint)index);

    internal static long NextGeneration(long generation) => generation >= MaxGeneration ? 1 : generation + 1;

    public TSlot Rent()
    {
        lock (idle)
        {
            if (idle.TryPop(out var slot))
            {
                return slot;
            }

            var index = created;
            if (index == MaxSlots)
            {
                throw new InvalidOperationException($"Send slot pool is full at {MaxSlots} slots.");
            }

            var fresh = create(index);
            if (index == slots.Length)
            {
                var grown = new TSlot?[slots.Length * 2];
                Array.Copy(slots, grown, slots.Length);
                grown[index] = fresh;
                Volatile.Write(ref slots, grown);
            }
            else
            {
                Volatile.Write(ref slots[index], fresh);
            }

            Volatile.Write(ref created, index + 1);
            return fresh;
        }
    }

    public void Return(TSlot slot)
    {
        lock (idle)
        {
            if (!disposed)
            {
                idle.Push(slot);
            }
        }
    }

    public TSlot? Find(nint opaque, out long generation)
    {
        var handle = (ulong)opaque;
        generation = (long)(handle >> IndexBits);
        var index = (int)(handle & (MaxSlots - 1));
        var current = Volatile.Read(ref slots);
        return index < current.Length ? Volatile.Read(ref current[index]) : null;
    }

    // No drain: slots hold no unmanaged state.
    public void Dispose()
    {
        lock (idle)
        {
            disposed = true;
        }
    }
}
