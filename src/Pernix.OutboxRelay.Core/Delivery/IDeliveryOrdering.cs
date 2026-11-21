using Pernix.OutboxRelay.Core.Abstractions;

namespace Pernix.OutboxRelay.Core.Delivery;

// What may be read and sent next. The term owns I/O, held rows, outcomes and policy.
internal interface IDeliveryOrdering
{
    internal const int CapacityFactor = 10;

    int Capacity { get; }

    bool UsesSummary { get; }

    // True when NextRead excludes held rows from the read, so the term must pass the held ids.
    bool ExcludesHeld { get; }

    // True when the last read stopped at a not-due row (Strict halt), so the read was not full.
    bool Halted { get; }

    // True when every row of the last read overflowed a full scope and no scope moved the cursor back since, so the read was not full.
    bool AwaitingMoveBack { get; }

    void Tick(DateTimeOffset now);

    // Pure. The term also calls it with an empty held list as a yes/no test, so the answer must not depend on held.
    ReadRequest? NextRead(int room, IReadOnlyCollection<long> held);

    void Accept(IReadOnlyList<OutboxMessage> rows, Func<long, bool> isHeld, ICollection<OutboxMessage> queued);

    void Sendable(ICollection<OutboxMessage> output);

    void Release(long id);

    void Fail(long id, ICollection<long> evicted);

    void Unpark(IReadOnlyCollection<long> ids, ICollection<long> evicted);

    void Left(long id);

    void Summary(FailedRowsSummary summary);

    // Adds the reason of every applied rewind since the last call, in order, so each is logged once.
    void DrainRewinds(ICollection<string> reasons);
}
