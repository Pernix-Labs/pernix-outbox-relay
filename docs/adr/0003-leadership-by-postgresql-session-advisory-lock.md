# 0003. Elect one leader per outbox with a PostgreSQL session advisory lock

**Status:** Accepted
**Date:** 2026-11-03
**Deciders:** Apostolos Stephanou

## Context

Every replica of the relay reads the same outbox tables. Rows must be sent at least once and in order, so exactly one replica may process each outbox at a time, and a replica that dies or loses its database must hand over within seconds. The outbox table already lives in the domain's PostgreSQL database, and the relay must not run DDL there.

## Decision

Each replica competes per outbox for the session advisory lock `pg_try_advisory_lock(hashtext('outbox-relay'), hashtext(<schema-qualified table>))`, and the replica whose session holds it leads that outbox.

- The lock lives on one dedicated, non-pooled connection per outbox per replica. Closing the connection, or the server dropping it, releases the lock.
- The key depends on the schema-qualified table PostgreSQL resolves, not on the outbox `Name`, so two names for one table share one lock. Startup refuses two outboxes on one table or one lock key.
- The leader checks every 1 s that its own backend still holds the lock (`pg_locks`, `pid = pg_backend_pid()`), under a 5 s wall-clock deadline, and stops leading on any failure.
- Sessions run on a primary only. Startup refuses a standby, and a session that lands on one counts as an unreachable database.

## Alternatives considered

- **Lease table with heartbeat updates.** Needs DDL the relay does not own, write traffic on every heartbeat, and clock-based expiry that can briefly allow two leaders.
- **Kubernetes `Lease` objects.** Ties leadership to the cluster rather than the database the rows live in, so a replica cut off from the database can keep leading. Adds RBAC to every deployment.
- **Transaction-scoped advisory locks (`pg_try_advisory_xact_lock`).** Need one long-open transaction per outbox, which holds back vacuum and conflicts with `idle_in_transaction_session_timeout`.

## Consequences

- No extra infrastructure and no DDL. Takeover is bounded by keepalives and the 5 s deadline: the old leader stops within 6 s, a graceful stop or crash frees the lock at once, and PostgreSQL frees it after network loss within about 11 s.
- Direct connection only. RDS Proxy and PgBouncer in transaction or statement mode move the session between backends, so the lock would stay on a backend the relay no longer owns. The `pg_locks` check turns that into a lost session, but leadership cannot work behind them.
- Primary only. Advisory locks are per server, so a lock on a standby does not exclude a leader on the primary. Read endpoints and load-balanced host lists are refused or forced to a read-write host.
- One connection per outbox per replica, held for the life of the process.
- `hashtext` is 32-bit, so distinct tables can collide. Startup detects it within one database and the domain renames a table.
