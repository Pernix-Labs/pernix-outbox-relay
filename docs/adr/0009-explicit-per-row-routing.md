# 0009. Route each outbox row explicitly inside its destination, and hold every row the destination does not accept

**Status:** Accepted
**Date:** 2026-11-08
**Deciders:** Apostolos Stephanou
**Superseded in part by:** [0014](./0014-clean-slate-schema-and-client-1-0-0.md) (client 1.0.0 and 2.0.0 references).

## Context

V001 carries a producer-written `route text NULL` with CHECK `<prefix>_route_format` (`^[a-z0-9][a-z0-9._-]{0,62}$`). NULL means the row has no route. One outbox table per domain should serve several addresses of one transport without a table per address, while the simple case (one outbox, one destination, no route) keeps its producer code and relay config.

Routing must never misdeliver. A delivered message cannot be taken back, so a producer that ships a new route before the relay config maps it must not reach any address. A mismatch must stay visible and recoverable, and must not lose the row.

## Decision

Each row reaches only an address configured for its exact route, NULL included, and Core holds every other row in place as a retry-free failure of that row.

- **Explicit addressing (D7 = O29, Q16).** A destination is single-address xor a `Routes` map. A single address (`Topic`, `Table`) takes rows with a NULL route only. A `Routes` map takes rows whose route is a map key only. No default, no catch-all.
- **Route resolved inside the destination (D2 = O22, Q3).** One `Outboxes[]` entry, one session and one term per table. The destination maps a route to its address. Ordering scopes (`PerKey`, `Strict`) span routes.
- **One acceptance rule, in Core.** `Routes` null: accept a row if and only if its route is NULL. `Routes` set: accept a row if and only if its route is a key. Transports only declare `Routes`. Dead-letter pending rows go to the dead-letter send without the rule.
- **Hold (D4 = O26, Q12).** Core sends nothing for a row it does not accept. It settles the row as failed: `retry_count = max(r, 1)`, `next_attempt_at = now() + Retry.MaxDelay`, `last_error = 'route not mapped: <route>'` (`(none)` for NULL, routes over 63 characters cut to 63 plus `...(truncated)`), and logs `MessageHeld` (event 125). The hold is scoped like a send failure of that row: `PerKey` holds the key, `None` skips the row, `Strict` holds the outbox. No retry is spent. A held row is never dead-lettered, dropped or blocked. A released row counts retries from 0. No new `Outcome`, `FailureDecision` or `DeliveryResult`.
- **Single-address dead-letter destination (D8 = O36, Q14).** A dead-letter destination that declares `Routes` is refused at startup. The original route travels with the dead letter as data.
- **`route` required (O34, Q19).** The relay refuses an outbox table or a `Database` target table without `route` at startup.
- **Core members (D6, Q9).** `OutboxMessage.Route` (with a positional constructor argument), `OutgoingMessage.Route`, `IDestination.Routes` (`IReadOnlySet<string>?`, null = single address, otherwise an ordinal, non-empty set fixed for the instance's lifetime), and the `SendAsync` contract: Core calls it only for a row it accepts, and with `DeadLetter` set the destination is single-address and `Route` is data only. No other Core abstraction changes.
- **No relay-side broadcast (human ruling).** The relay never copies one row to several routes. A producer that needs broadcast or a subset writes one row per route in the same transaction.

## Alternatives considered

- **O27 `route NOT NULL` without default, every row routed** — earliest detection, but the simple case writes a route that selects nothing, and 1.0.0 inserts fail (`23502`, client 2.0.0).
- **O28 Single address (or a `"*"` key) takes every row** — a producer that ships a route before the config maps it sends those rows to the catch-all address, and wrong-address delivery cannot be undone.
- **O30 `route NOT NULL DEFAULT '<literal>'`** — the fallback address moved into DDL: a forgotten route is delivered to the default address.
- **O31 `Routes` with a reserved key for NULL, mixed with real routes** — single address and map in one destination under another name.
- **O9 Route-filtered `Outboxes[]` entries sharing one table** — not built (Q3): an extra session, stats and route index per route, no order across routes, little gain over one table per destination.
- **O11 Hold by pausing the entry** — one producer deploy ahead of the relay config halts every route of the table.
- **O12 Treat a row not accepted as a permanent message fault** — `Continue` without a dead letter loses the message, and with one the message leaves its key's order.
- **O37 Route the dead letter through the dead-letter destination's own `Routes`** — an unmapped route would need a dead letter for the dead letter, or a held dead letter.
- **O39 Pass-through routing (address = the row's route, any route accepted)** — accepts every route, so it is O28 under another name.

## Consequences

- No row reaches an address its route was not mapped to. A config or producer mismatch shows as `MessageHeld`, `last_error` and `outbox.retrying`, and loses nothing.
- **R7.** A destination fault on one address pauses every route of the entry, because routes share one entry, session and pause.
- **R8.** Each routed row read materializes one route string. Route-less rows allocate nothing new.
- **R9.** Held rows are failed rows: they count in `outbox.retrying` and the failed-row summary, and on `None` they add to the read cost of failed rows.
- **R13.** A forgotten `WithRoute` on a routed table is detected in the relay (the row is held), not at insert. A domain may add `SET NOT NULL` on `route` itself.
- Routes are read once per destination instance. Releasing held rows after a config or producer fix needs a relay restart, after which they deliver within `Retry.MaxDelay`, or at once after "retry from scratch".
- `Strict` keeps total order across routes only within one-partition topics. Routes to different topics or tables have no common order for consumers.
- Kafka and `Database` destinations stay single-address until their `Routes` maps ship, so a routed row on them is held.

## Amendment 2026-11-09: Kafka `Routes` (spec 2026-11-09-kafka-routes)

No decision above changes, so this is an append and not a superseding ADR.

- **Kafka is `Topic` xor `Routes`.** `Destinations[].Kafka.Routes` maps route → topic. A `Kafka` destination with `Routes` declares the map's keys as `IDestination.Routes` and sends each accepted row to the topic mapped for its route, through the one producer of the outbox and destination. A `Kafka` destination with `Topic` stays single-address and holds routed rows.
- **Dead letters.** A Kafka dead letter writes the original route as header `x-original-route`, absent when the route is NULL. `Database` dead letters still do not write it.
- **`Database` stays single-address.** A `Database` destination still holds every routed row until its `Routes` map ships.

## Amendment 2026-11-09: Database Routes (spec 2026-11-09-database-routes)

No decision above changes, so this is an append and not a superseding ADR.

- **`Database` is `Table` xor `Routes`.** `Destinations[].Database.Routes` maps route → target table. A `Database` destination with `Routes` declares the map's keys as `IDestination.Routes`. A `Database` destination with `Table` stays single-address and holds routed rows.
- **One insert per message.** Each accepted row is one autocommit insert into the table mapped for its route, through the one connection pool of the outbox and destination. No transaction spans tables.
- **Every insert writes `route`.** A main send writes the row's route, NULL on a `Table` destination. A dead letter writes the original route, NULL when the row had none, so a replay outbox can route it again.
- **Startup per mapped table.** The probe and the self-loop check run for every mapped table.
- **`Routes` edits (Q15, R10).** A `Routes` edit that moves a route to another table can leave one copy of an in-flight row in the old table and one in the new table. Delivery stays at least once, deduplicated per table only.
- The Kafka amendment bullets "`Database` dead letters still do not write it" and "`Database` stays single-address" no longer hold.

> *(Not part of the approved amendment text.)* Update 2026-11-11: the Database Routes amendment is superseded by [0010](./0010-remove-database-destination.md), which removes the `Database` destination. The core decision and the Kafka `Routes` amendment stand.
