# 0007. Resolve database destinations by transport and provider, bound sends by wall clock, pool per outbox and destination

**Status:** Superseded by 0010
**Date:** 2026-11-06
**Deciders:** Apostolos Stephanou

## Context

The `Database` transport delivers outbox rows into a table instead of a broker. Three choices it makes constrain every later transport and engine:

- How a destination finds its engine. Before it, one factory served one `Transport`. `Transport: Database` needs one engine per database product, and the store provider and the destination factory for one product must not depend on each other.
- How long a send may take. Core's liveness, stop drain and pause logic need a send bound (`IDestination.SendBound`). A database insert can wait on a pool, a connection open, locks, triggers and a synchronous replica, and a client cancel cannot stop a statement that is already running server-side.
- How many connections a leader opens to a target. Core can dispatch up to 10 × `Polling.BatchSize` rows at once in `None`, and the target's connection budget belongs to the operator.

## Decision

Database destinations resolve by (`Transport`, `Provider`), each send is bounded by one wall clock that never cancels a running command, and each (outbox, destination) gets its own pool sized by `MaxInFlight`.

- **Resolution (D2).** Every engine factory registers as `IDestinationFactory`. `DestinationFactoryResolver` indexes plain factories by `Transport` and `IDatabaseDestinationFactory` by (`Transport`, `Provider`), case-insensitive, once at startup. A duplicate pair, a duplicate plain `Transport`, or one `Transport` with both kinds aborts startup. A destination resolves by its `Database.Provider`. `IOutboxStoreProvider` is unchanged and shares no member with the destination factory.
- **Send bound (D6).** `Database.SendTimeout` (default 5 s, 1 to 20 s) is one wall clock from send start covering pool wait, open, insert and the zero-row confirm `SELECT`. The pool wait and open take a token cancelled at the remaining budget. The insert and confirm run without a token: at the bound the relay abandons the call and reports `DestinationFault("deadline: not confirmed")` (ADR 0004). Server-side `statement_timeout = SendTimeout` ends an abandoned statement. The commit phase is not cut, so an abandoned insert can commit late, and deduplication on (`source_outbox`, `source_id`) absorbs its resend.
- **Pool (D7).** One `NpgsqlDataSource` per destination instance. Instances exist per (outbox, destination, term) and terms of one outbox never overlap, so there is one pool per (outbox, destination), `MaxPoolSize = MaxInFlight`, `MinPoolSize = 0`, idle connections closed after 60 s, `ApplicationName=outbox-relay-dest:<outbox>:<destination>`. Core caps sends in flight at the destination's `MaxInFlight` (D8), so a healthy target never makes a send wait for a connection.

## Alternatives considered

- **Resolve through `IOutboxStoreProvider.DestinationFactory`** — couples the store provider to the destination factory, so a host could not load one without the other, and two engines on `Transport: Database` would need a second lookup anyway.
- **One factory per `Transport` that forwards to engines** — every engine would repeat or depend on the forwarder, and duplicate engine registration would not be caught at startup.
- **Cancel the running command at the bound** — Npgsql's cancel aborts commit-time waits (deferred triggers, synchronous replica) and makes the outcome unknowable. A late commit followed by a deduplicated resend is safe, a half-cancelled commit is not provable either way.
- **Fixed 5 s bound** — fits neither slow targets nor ordered outboxes that want a short gate hold. Superseded at the design gate by a configurable `SendTimeout`.
- **Client bound only, no `statement_timeout`** — the abandoned statement keeps running and holds the target cluster's gate past the bound.
- **One shared pool per target, or a fixed `MaxPoolSize`** — sends of one outbox would queue behind another's, time out and pause a healthy outbox. A fixed size either wastes connections or under-serves `MaxInFlight`.

## Consequences

- A new database engine adds one `IDatabaseDestinationFactory` with a new `Provider`. A new plain transport keeps resolving by `Transport` alone.
- Every `Database` send returns within `SendTimeout`, so liveness counts it at its deadline + 10 s and a stop drain reaches its result inside 20 s.
- An insert can commit after its send returned a fault. Targets must keep the unique index on (`source_outbox`, `source_id`), which startup checks.
- An insert holds a transaction id on the target cluster for at most `SendTimeout` plus commit-time waits, which holds the gate of every `PerKey` and `Strict` outbox there.
- The relay opens up to the sum of `MaxInFlight` over (outbox, destination) pairs on a target cluster. The operator sizes the target's connection budget, and there is no upper limit on `MaxInFlight`.
- Throughput per (outbox, destination) is about `MaxInFlight` / (insert + commit latency).
