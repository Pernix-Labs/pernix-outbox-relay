using Pernix.OutboxRelay.Core.Abstractions;
using Pernix.OutboxRelay.Core.Configuration;

namespace Pernix.OutboxRelay.Core.Delivery;

// Ordering.Mode PerKey and Strict: scope queues, cursor, overflow, Strict halt and rewind. No I/O.
internal sealed class ScopedDelivery : IDeliveryOrdering
{
    internal const string SummaryReason = "summary";
    internal const string FailureReason = "failure";
    internal const string UnparkReason = "unpark";
    internal const string SafetyReason = "safety";

    internal static readonly TimeSpan SafetyRewindInterval = TimeSpan.FromSeconds(60);

    private readonly OrderingMode mode;
    private readonly int batchSize;
    private readonly Scope? strict;
    private readonly Dictionary<ScopeKey, Scope> scopes = [];

    // Scopes removed from scopes, reset and kept for the next new key. Bounded by peak concurrent scopes.
    private readonly Stack<Scope> freeScopes = new();

    // Queued rows, scope heads and PerKey NULL-key rows not yet released. NULL-key rows map to null.
    private readonly Dictionary<long, Scope?> index = [];
    private readonly Queue<OutboxMessage> unscoped = new();
    private readonly Queue<Scope> ready = new();
    private readonly List<string> rewound = [];

    // PerKey: first position per key at which the cursor passed a row not due since the last applied rewind.
    private readonly Dictionary<ScopeKey, OutboxPosition> passed = [];

    private FailedRowsSummary? baseline;
    private DateTimeOffset? lastTick;
    private DateTimeOffset? lastRewindAt;

    // PerKey: walking = a full read since the last applied rewind and no short read since. A summary,
    // failure or safety rewind asked for meanwhile waits for the short read or a stall (no progress for SafetyRewindInterval).
    // Progress = a full read ending past reached (furthest read position this walk) that did not all overflow. Unpark: at once.
    private bool walking;
    private string? deferred;
    private OutboxPosition reached;
    private DateTimeOffset progressAt;

    public ScopedDelivery(OrderingMode mode, int batchSize)
    {
        if (mode is not (OrderingMode.PerKey or OrderingMode.Strict))
        {
            throw new ArgumentOutOfRangeException(nameof(mode), mode, "ScopedDelivery serves PerKey and Strict only.");
        }

        ArgumentOutOfRangeException.ThrowIfLessThan(batchSize, 1);
        this.mode = mode;
        this.batchSize = batchSize;
        if (mode == OrderingMode.Strict)
        {
            strict = new Scope(default);
        }

        Capacity = mode == OrderingMode.Strict ? batchSize : batchSize * IDeliveryOrdering.CapacityFactor;
    }

    public int Capacity { get; }

    // Tests only: never logged or exported.
    internal int ScopesCreated { get; private set; }

    public bool UsesSummary => true;

    public bool ExcludesHeld => false;

    public OutboxPosition? After { get; private set; }

    public bool Halted { get; private set; }

    public bool AwaitingMoveBack { get; private set; }

    public void Tick(DateTimeOffset now)
    {
        lastTick = now;
        lastRewindAt ??= now;

        // With the PostgreSQL provider a retry_count = 0 row is !IsDue only behind a failed row of its key, and the
        // failed-row summary already covers that case. This safety rewind stays as a backstop
        // with no known trigger in this provider. Like a summary or failure rewind it waits while the cursor is walking and has
        // not stalled.
        if (deferred is not null && Stalled())
        {
            Apply(deferred);
        }
        else if (mode == OrderingMode.PerKey && now - lastRewindAt >= SafetyRewindInterval)
        {
            Rewind(SafetyReason);
        }
    }

    public ReadRequest? NextRead(int room, IReadOnlyCollection<long> held)
    {
        if (room < batchSize)
        {
            return null;
        }

        return new ReadRequest(Halted ? 1 : batchSize, After, []);
    }

    public void Accept(IReadOnlyList<OutboxMessage> rows, Func<long, bool> isHeld, ICollection<OutboxMessage> queued)
    {
        ArgumentNullException.ThrowIfNull(rows);
        ArgumentNullException.ThrowIfNull(isHeld);
        ArgumentNullException.ThrowIfNull(queued);
        Halted = false;
        var previous = After;
        var overflowed = 0;
        foreach (var row in rows)
        {
            if (mode == OrderingMode.Strict && !row.IsDue)
            {
                Halted = true;
                break;
            }

            var keyPassed = PassedForKey(row);
            if (row.IsDue && !keyPassed && !isHeld(row.Id) && !index.ContainsKey(row.Id) && !Queue(row, previous, queued))
            {
                overflowed++;
            }

            previous = row.Position;
        }

        After = previous;
        AwaitingMoveBack = overflowed > 0 && overflowed == rows.Count;
        if (mode == OrderingMode.PerKey)
        {
            // An all-overflow read extends reached but is no progress, because its rows come back after the move-back.
            if (rows.Count >= batchSize)
            {
                var end = rows[rows.Count - 1].Position;
                if (!walking || (!AwaitingMoveBack && end > reached))
                {
                    progressAt = lastTick ?? DateTimeOffset.MinValue;
                }

                if (!walking || end > reached)
                {
                    reached = end;
                }
            }

            walking = rows.Count >= batchSize;
            if (!walking && deferred is not null)
            {
                Apply(deferred);
            }
        }
    }

    public void DrainRewinds(ICollection<string> reasons)
    {
        ArgumentNullException.ThrowIfNull(reasons);
        foreach (var reason in rewound)
        {
            reasons.Add(reason);
        }

        rewound.Clear();
    }

    public void Sendable(ICollection<OutboxMessage> output)
    {
        ArgumentNullException.ThrowIfNull(output);
        while (unscoped.TryDequeue(out var row))
        {
            output.Add(row);
        }

        while (ready.TryDequeue(out var scope))
        {
            scope.Ready = false;
            if (scope.Head is null && scope.Queued.TryDequeue(out var head))
            {
                scope.Head = head;
                output.Add(head);
            }
        }
    }

    public void Release(long id)
    {
        if (!index.TryGetValue(id, out var scope) || (scope is not null && scope.Head?.Id != id))
        {
            return;
        }

        index.Remove(id);
        if (scope is not null)
        {
            ClearHead(scope);
        }
    }

    public void Fail(long id, ICollection<long> evicted)
    {
        ArgumentNullException.ThrowIfNull(evicted);

        // A PerKey NULL-key row holds nothing back. It is re-read when due through the summary.
        if (!index.TryGetValue(id, out var scope) || scope is null || scope.Head?.Id != id)
        {
            return;
        }

        EvictQueued(scope, evicted);
        HoldKey(scope);
        Rewind(FailureReason);
    }

    public void Unpark(IReadOnlyCollection<long> ids, ICollection<long> evicted)
    {
        ArgumentNullException.ThrowIfNull(ids);
        ArgumentNullException.ThrowIfNull(evicted);
        if (ids.Count == 0)
        {
            return;
        }

        foreach (var id in ids)
        {
            if (!index.TryGetValue(id, out var scope))
            {
                continue;
            }

            if (scope is null)
            {
                index.Remove(id);
            }
            else if (scope.Head?.Id == id)
            {
                Untrack(scope, evicted);
            }
        }

        Apply(UnparkReason);
    }

    public void Left(long id)
    {
        if (!index.TryGetValue(id, out var scope))
        {
            return;
        }

        if (scope is null)
        {
            index.Remove(id);
            return;
        }

        if (scope.Head?.Id != id)
        {
            return;
        }

        index.Remove(id);
        ClearHead(scope);
    }

    public void Summary(FailedRowsSummary summary)
    {
        if (baseline is { } seen && seen != summary)
        {
            Rewind(SummaryReason);
        }

        baseline = summary;
    }

    // A later row of a key whose not-due row the cursor passed is passed too, so it cannot overtake that row once it is due.
    // Both come back in order on the next applied rewind. NULL keys hold nothing.
    private bool PassedForKey(OutboxMessage row)
    {
        if (mode != OrderingMode.PerKey || row.Key is null)
        {
            return false;
        }

        var key = new ScopeKey(row.Key);
        if (passed.TryGetValue(key, out var first))
        {
            if (!row.IsDue && row.Position < first)
            {
                passed[key] = row.Position;
            }

            return row.Position > first;
        }

        if (!row.IsDue)
        {
            passed.Add(key, row.Position);
        }

        return false;
    }

    private bool Queue(OutboxMessage row, OutboxPosition? previous, ICollection<OutboxMessage> queued)
    {
        var scope = ScopeOf(row);
        if (scope is null)
        {
            index[row.Id] = null;
            unscoped.Enqueue(row);
            queued.Add(row);
            return true;
        }

        // An overflowed scope keeps overflowing until the cursor moves back, so no later row passes an unread earlier one.
        if (scope.Overflowed || scope.Count >= batchSize)
        {
            scope.OverflowAfter = scope.Overflowed ? Min(scope.OverflowAfter, previous) : previous;
            scope.Overflowed = true;
            return false;
        }

        scope.Queued.Enqueue(row);
        index[row.Id] = scope;
        queued.Add(row);
        MarkReady(scope);
        return true;
    }

    private Scope? ScopeOf(OutboxMessage row)
    {
        if (strict is not null)
        {
            return strict;
        }

        if (row.Key is null)
        {
            return null;
        }

        var key = new ScopeKey(row.Key);
        if (!scopes.TryGetValue(key, out var scope))
        {
            scope = freeScopes.TryPop(out var free) ? free.Reuse(key) : NewScope(key);
            scopes.Add(key, scope);
        }

        return scope;
    }

    private Scope NewScope(ScopeKey key)
    {
        ScopesCreated++;
        return new Scope(key);
    }

    // Only a scope no ready entry, index value or caller local references.
    private void Recycle(Scope scope)
    {
        scope.Reset();
        freeScopes.Push(scope);
    }

    private void ClearHead(Scope scope)
    {
        scope.Head = null;

        // Hysteresis: the cursor moves back only once the scope has half its cap free.
        if (scope.Overflowed && scope.Count <= batchSize / 2)
        {
            After = Min(After, scope.OverflowAfter);
            AwaitingMoveBack = false;
            scope.Overflowed = false;
            scope.OverflowAfter = null;
        }

        if (scope.Queued.Count > 0)
        {
            MarkReady(scope);
        }
        else if (scope != strict)
        {
            scopes.Remove(scope.Key);
            Recycle(scope);
        }
    }

    private void Untrack(Scope scope, ICollection<long> evicted)
    {
        EvictQueued(scope, evicted);
        index.Remove(scope.Head!.Id);
        scope.Head = null;
        if (scope != strict)
        {
            scopes.Remove(scope.Key);
            Recycle(scope);
        }
    }

    private void EvictQueued(Scope scope, ICollection<long> evicted)
    {
        foreach (var row in scope.Queued)
        {
            index.Remove(row.Id);
            evicted.Add(row.Id);
        }

        scope.Queued.Clear();
    }

    private void MarkReady(Scope scope)
    {
        if (scope.Head is null && !scope.Ready)
        {
            scope.Ready = true;
            ready.Enqueue(scope);
        }
    }

    // A later row of a failed head's key must not pass it while the rewind waits (operator reset racing the guarded commit).
    private void HoldKey(Scope scope)
    {
        if (mode != OrderingMode.PerKey || scope.Head is not { } head)
        {
            return;
        }

        if (!passed.TryGetValue(scope.Key, out var first) || head.Position < first)
        {
            passed[scope.Key] = head.Position;
        }
    }

    // PerKey: no progress for SafetyRewindInterval on the term clock. A held-room step is no progress.
    private bool Stalled() => walking && lastTick is { } now && now - progressAt >= SafetyRewindInterval;

    private void Rewind(string reason)
    {
        if (walking && !Stalled())
        {
            deferred ??= reason;
            return;
        }

        Apply(reason);
    }

    private void Apply(string reason)
    {
        walking = false;
        deferred = null;
        After = null;
        Halted = false;
        AwaitingMoveBack = false;
        strict?.ClearOverflow();
        passed.Clear();
        foreach (var scope in scopes.Values)
        {
            scope.ClearOverflow();
        }

        rewound.Add(reason);
        lastRewindAt = lastTick;
    }

    // Null is the head, before every position.
    private static OutboxPosition? Min(OutboxPosition? a, OutboxPosition? b) =>
        a is null || b is null ? null : a.Value <= b.Value ? a : b;

    private sealed class Scope(ScopeKey key)
    {
        public ScopeKey Key { get; private set; } = key;

        public OutboxMessage? Head { get; set; }

        public Queue<OutboxMessage> Queued { get; } = new();

        public bool Ready { get; set; }

        public bool Overflowed { get; set; }

        public OutboxPosition? OverflowAfter { get; set; }

        public int Count => (Head is null ? 0 : 1) + Queued.Count;

        public void ClearOverflow()
        {
            Overflowed = false;
            OverflowAfter = null;
        }

        public Scope Reuse(ScopeKey newKey)
        {
            Key = newKey;
            return this;
        }

        // Key = default drops the old key bytes.
        public void Reset()
        {
            Head = null;
            Queued.Clear();
            Ready = false;
            ClearOverflow();
            Key = default;
        }
    }

    private readonly struct ScopeKey(byte[] bytes) : IEquatable<ScopeKey>
    {
        private readonly byte[] bytes = bytes;

        public bool Equals(ScopeKey other) => bytes.AsSpan().SequenceEqual(other.bytes);

        public override bool Equals(object? obj) => obj is ScopeKey other && Equals(other);

        public override int GetHashCode()
        {
            var hash = new HashCode();
            hash.AddBytes(bytes);
            return hash.ToHashCode();
        }
    }
}
