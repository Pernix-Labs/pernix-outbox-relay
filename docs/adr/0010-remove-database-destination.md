# 0010. Remove the Database destination transport

**Status:** Accepted
**Date:** 2026-11-11
**Deciders:** Apostolos Stephanou
**Supersedes:** [0007](./0007-database-transport-resolution-send-bound-and-pool.md) and the "Amendment 2026-11-09: Database Routes" of [0009](./0009-explicit-per-row-routing.md). Also supersedes two clauses of [0008](./0008-producer-client-packages.md): the route amendment's "Later schema changes return to additive `Vnnn` scripts" and the D13 rule that `OutboxColumns.All` equals the columns V001 creates.
**Superseded in part by:** [0014](./0014-clean-slate-schema-and-client-1-0-0.md) (V002 and client 2.0.0: V001 holds the final table, client starts at 1.0.0).

## Context

The `Database` destination works, but its design is rejected (human ruling 2026-11-11). It forces six destination-only columns (`source_outbox`, `source_id`, `source_destination`, `dead_lettered_at`, `dead_letter_reason`, `dead_letter_attempts`), the `<prefix>_source_complete` CHECK and the `<prefix>_source_uq` unique index into every producer outbox table, including tables that only ever feed Kafka. It couples "outbox" and "target table" into one schema.

It also shaped Core. `IDestination.MaxInFlight` and `IDestinationFactory.SupportsDeadLetters` (`231f326`), the send caps and dead-letter backlog in `DeliveryTerm` (`bd4bbb6`), destination pools in `PostgreSqlConnectionStrings` (`9e1fc89`), the self-loop query columns (`1f82e90`) and `DestinationContext.Outbox` exist only for it. With Kafka alone, `MaxInFlight` is `null` (cap `int.MaxValue`, never bound) and `SupportsDeadLetters` is constant `true`.

## Decision

Remove the `Database` destination transport completely.

- **Transport.** No Core abstraction, option type, resolver branch, PostgreSQL engine, registration, startup check, log event, metric or query that exists only for it. A `Transport: Database` config fails as any unknown transport: `transport 'Database' is not implemented in this build`.
- **`MaxInFlight`.** `IDestination.MaxInFlight` and all cap logic go (D3): main and dead-letter caps, the dead-letter backlog and its waiting state. Main and dead-letter in-flight counts collapse to one. Per-kind `SendBound` deadlines stay.
- **`SupportsDeadLetters`.** `IDestinationFactory.SupportsDeadLetters` and its refusal go (D4).
- **`DestinationContext.Outbox`.** Removed (D16). No production reader remains.
- **V002.** The six columns, the CHECK and the unique index leave producer tables through a new destructive script <!-- [adr-honest-edit] 2026-11-11: was "additive" (PR #33 review NAME-1) --> `sql/postgresql/V002__drop_destination_columns.sql`, one `ALTER TABLE ... DROP COLUMN IF EXISTS` per column (D9). PostgreSQL drops the index and CHECK with their columns. `V001__create_outbox.sql` stays byte-identical, since producers applied it. V002 is the one exception to 0008's additive-only `Vnnn` rule, and `OutboxColumns.All` lists the columns left after V001 and V002.
- **Client 2.0.0.** `OutboxColumns` loses the six constants. Client packages ship as `2.0.0` with a release note and V002.

Transport-neutral members Kafka uses stay: `IDestination.Routes`, `SendBound`, `OutgoingMessage.Source`, outbox-side `Outboxes[].Database`, `OutboxDatabaseUnreachableException` and the outbox readiness check.

## Alternatives considered

- **Keep the transport dormant** (undocumented, unregistered, or behind a flag). The schema cost stays on every producer table, and `MaxInFlight`, `SupportsDeadLetters` and the caps stay as dead contract in Core that every change to the delivery term must still carry.
- **Remove the transport, keep the schema.** Producers keep six columns, a CHECK and a unique index the relay never reads or writes. Relay outbox validation already ignores them, so they would only cost writes and confuse readers.

## Consequences

- No Kafka dead-letter replay. Replay worked only through a `Database` destination feeding a second outbox. It stays out until a new database destination design ships.
- Producer tables shrink to the columns the relay uses. Producers apply V002 from client `2.0.0`. A producer view on a dropped column fails the migration instead of being silently dropped.
- Client `2.0.0` is a breaking change for code that referenced the six `OutboxColumns` constants.
- Core delivery loses the cap and backlog paths. Kafka behaviour is unchanged.
- A `Database` destination may return only under a new spec and a new design. Nothing here constrains that design.
- 0007 is superseded. 0009's Database Routes amendment is superseded. 0005's 2026-11-09 amendment no longer applies to Database destination sends, and the rest of it stands.
- 0008's "later schema changes are additive `Vnnn` scripts" clause has one exception, V002, which is destructive. 0008's D13 rule now reads against V001 and V002: `OutboxColumns.All` lists the 10 columns left after V002. The rest of 0008 stands.
