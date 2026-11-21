namespace Pernix.OutboxRelay.Core.Abstractions;

// One stats read of one outbox, from the database clock.
// Pending: estimate of rows not failed. Retrying: failed rows not blocked (dead-letter pending and due-again included).
// Blocked: blocked rows, or blocked keys on PerKey (a blocked NULL-key row counts as one). 0 on None.
// Rows on Strict, keys on PerKey: do not sum Blocked across modes.
// HeadAge: age of the first row in read order, any state, Zero when empty.
// GateLag: age of the oldest transaction holding the gate, Zero when none, or unavailable with the missing grant.
/// <param name="Pending">
/// Estimate from the database's live-tuple statistics minus failed rows, not a count. Inserts appear
/// up to ~10 s after a burst (longer while a producer connection stays busy), so it can read low or 0
/// while rows exist. Drifts ~1-2% after an autovacuum or auto-analyze re-estimate. Alert on a sustained trend.
/// </param>
public readonly record struct OutboxStats(
    long Pending,
    long Retrying,
    long Blocked,
    TimeSpan HeadAge,
    GateLag GateLag);
