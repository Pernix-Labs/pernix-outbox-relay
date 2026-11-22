# Product Overview

## Product Purpose
Services that change their database and then publish an event lose that event when the publish fails after the commit. The consumers then work from stale data until the next change. For example, a service that publishes configuration snapshots on a best-effort basis after commit leaves its consumers with a stale cache when a publish fails.

The Outbox Relay fixes this with the transactional outbox pattern, delivered as a container image or as the `Pernix.OutboxRelay.Hosting` NuGet package for embedding in a host. A producer inserts a row into an outbox table in the same transaction as its business change, so the message exists if and only if the change committed. The relay polls the outbox tables and delivers each message to a Kafka topic, at least once, in the configured order. Producers integrate with plain SQL or the producer client packages, published as NuGet from this repo; the relay itself ships as an image and as the `Pernix.OutboxRelay.Hosting` NuGet package.

## Target Users
- **Producing teams (domains).** Need durable event publishing without a framework or language lock-in. They write outbox rows in SQL or through the producer client packages.
- **Consuming teams.** Need messages that are never lost and that arrive in a predictable order. They deduplicate on the producer's idempotency key.
- **Operators and on-call.** Need to see lag, stalls and blocked messages, and to skip, retry or unblock a message with documented SQL.

## Key Features
1. Transactional outbox over SQL: PostgreSQL in version 1. SQL Server is on hold until an adopter needs it.
2. Kafka destinations, with transports added as modules.
3. Three ordering modes per outbox: `None`, `PerKey` and `Strict`.
4. Retries with backoff, dead-lettering, and an option to block a key or a whole outbox until an operator acts.
5. Separate handling of message faults, which spend retries, and destination faults, which pause the outbox without spending retries.
6. One leader per outbox, chosen by a database lock. A second replica takes over within seconds.
7. Configured only: one image or one package reference.
8. Metrics for head age, pending rows, gate lag, retrying and blocked rows, deliveries, faults and leadership.

## Business Objectives
- Remove lost-event failures between a commit and its publish.
- Give every domain one reusable outbox service instead of each team building its own. *(Needs product-owner confirmation.)*
- Keep onboarding a new domain to configuration and DDL, with no code changes.

## Success Metrics
- End-to-end lag (`outbox.head_age`) stays within each domain's latency target. Targets: TBD per adopter.
- Failover to the second replica takes seconds, as the design's failover table states for crash and graceful shutdown.
- Number of domains onboarded. Target: TBD with the product owner.

## Product Principles
1. **Configured only.** Domains adopt the relay through SQL and configuration. They never change the relay's code.
2. **At least once, never lost.** Consumers deduplicate. Exactly-once delivery is a non-goal.
3. **Ordering is explicit.** Every outbox must name its ordering mode. There is no default.
4. **The domain owns its schema.** The relay ships versioned DDL scripts that only ever add columns, and it never runs DDL.
5. **Keep it simple.** One leader per outbox, no row claims, no Kafka transactions, no change data capture.

## Monitoring & Visibility
Each outbox emits OpenTelemetry metrics over OTLP:
- `outbox.head_age`, `outbox.pending`, `outbox.gate_lag`;
- `outbox.retrying`, `outbox.blocked`;
- `outbox.delivered`, `outbox.message_faults`, `outbox.destination_faults`, `outbox.dead_lettered`;
- `outbox.leader`.

Liveness fails when a worker loop stops or stalls past its progress deadline. Readiness fails while the outbox database is unreachable. Operators act on failed messages with the documented SQL actions. The relay never logs keys or payloads.

## Future Vision
These items come from the design's open questions and are not committed. Each needs roadmap confirmation.

### Potential Enhancements
- A SQL Server provider, on hold until an adopter needs it.
- librdkafka's gapless producer mode for fast `Strict` outboxes (Q3).
- Kafka `Routes` (route → topic) shipped, so a row's `route` picks its topic (ADR 0009).
- The `Database` destination was removed on 2026-11-11 ([ADR 0010](adr/0010-remove-database-destination.md)).
- Splitting an outbox's keys into fixed shards if one replica ever stops keeping up.
- MySQL support when a MySQL domain asks for it (Q4).
- Kafka authentication (MSK IAM), not supported in version 1.
