# 0006. Poll outbox stats on the leader only, on a per-outbox stats pool

**Status:** Accepted
**Date:** 2026-11-05
**Deciders:** Apostolos Stephanou

## Context

The relay exports five gauges per outbox: `outbox.pending`, `outbox.retrying`, `outbox.blocked`, `outbox.head_age` and `outbox.gate_lag`. Collecting them must never slow the leader session (1 s lock check, 5 s deadline), never affect delivery, leadership or health, and stay allocation-conscious ([ADR 0005](./0005-allocation-conscious-hot-path.md)). Producers write only `message_key`, `payload` and `headers` through SQL, so the relay cannot rely on producer-side cooperation.

The leader already holds one unpooled session connection per outbox ([ADR 0003](./0003-leadership-by-postgresql-session-advisory-lock.md)). Any stats work on that connection competes with the lock check and the delivery reads.

An independent design study measured the proposal (A) and five alternatives (B to F) on PostgreSQL 17.11.

## Decision

The leader of each outbox reads its stats with one statement every 15 s on its own pooled stats connection, never on the session.

- **Leader only (D-a).** A loop per outbox reads `OutboxRuntimeStatus.IsLeader` each 1 s tick. Non-leaders run no statement and report no series. The result caches for the gauges and is stale after 30 s.
- **Fixed 15 s cadence (D-b).** Not configurable and not tied to the OTLP export interval.
- **Per-outbox pool (D-c).** `PostgreSqlConnectionStrings.ForStats` gives each outbox its own Npgsql pool: `Pooling=true`, `MinPoolSize=0`, `MaxPoolSize=1`, `ConnectionIdleLifetime=60`, `ApplicationName=outbox-relay-stats:<name>`. The application name is part of the pool key, so outboxes never share a stats connection, even with the same connection string.

Per gauge, the statement uses the cheapest exact source: live-tuple statistics minus failed rows for `pending` (estimate), the partial failed-row index for `retrying` and `blocked`, the read index with `LIMIT 1` for `head_age`, and `pg_stat_activity` plus `pg_prepared_xacts` for `gate_lag`.

Measured cost of A: 0.28 ms server and 0.39 ms client per tick normally, 4.0 ms server at 100k failed rows, about 3 KB allocated per tick.

## Alternatives considered

- **B. In-process derived counters** from delivery outcomes. Cannot produce `pending`, `head_age` under a gate stall, or `gate_lag`, so it still needs A's statement to reconcile. Adds `Interlocked` work per outcome on the hot path and stats code in `DeliveryTerm`.
- **C. Piggyback on the session's failed-rows summary.** Runs on the leader session every 1 s, with no isolation. Measured: adding `PerKey` blocked keys to the summary makes it 2.7 times slower (8.5 to 23.2 ms p50, +14.7 ms per summary) at 100k failed rows, and changes `IOutboxSession` and `FailedRowsSummary`.
- **D. PostgreSQL-native statistics only.** 0.13 ms, no outbox pages and no outbox lock, but covers only 2 of 5 gauges. Kept per gauge inside A for `pending` and `gate_lag`.
- **E. Database-maintained counters or `NOTIFY`.** Measured producer cost: a row-level counter trigger −75% to −97.5% tps, a statement-level delta table +1 row per statement, statement-level `pg_notify` −52% tps. Relay delete +45% with a delete trigger. Needs DDL per domain, startup checks and a compaction job, and is bypassed by `TRUNCATE`, replica role and disabled triggers. Still no `PerKey` keys, head or gate lag.
- **F. Scrape-driven reads** inside the metrics export callback. One statement per export (60 s), but sync-over-async on the export thread, so database latency stalls all telemetry export.
- **Adaptive or idle cadence** instead of a fixed 15 s. Saves at most 67 ms of database time per hour per outbox, in the case where the statement is cheapest.
- **One shared stats pool per database** (study option A1). Saves N − 1 backends per database, but a stats statement stuck behind DDL on one table would fail the other outboxes' stats ticks.

No alternative saves more than about 15 ms of database time per minute per outbox, and only during a large failed backlog. Accuracy, isolation and contract cost decided, not efficiency.

## Consequences

- Stats never touch the leader session. A failed, hung or slow read only makes the five gauges absent (event 130). It never affects the lock, delivery, `/livez`, `/readyz` or the stop budget.
- Core abstractions gain `IValidatedOutboxStore.GetStatsAsync`, `OutboxStats` and `OutboxStatsUnavailableException`. `IOutboxSession` and `FailedRowsSummary` stay unchanged.
- One extra backend per led outbox stays open on the leader (60 s idle lifetime > 15 s cadence), +50% backends at 2 replicas. A former leader's stats connection closes within about 70 s of losing the lock.
- The stats connection runs no `SET` batch, so it takes the server keepalives as startup `Options`.
- At the default 60 s OTLP export, 3 of 4 reads are never exported, about 50 ms of database time per hour per outbox. Accepted for a fresh value at shorter export intervals.
- Exported values are 0 to 15 s old. `pending` is an estimate with documented error bounds.
