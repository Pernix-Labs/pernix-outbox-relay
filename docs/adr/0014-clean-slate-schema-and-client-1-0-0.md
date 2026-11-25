# 0014. Clean-slate schema and client 1.0.0 before first release

**Status:** Accepted
**Date:** 2026-11-25
**Deciders:** Apostolos Stephanou
**Supersedes:** in [0008](./0008-producer-client-packages.md), the route amendment's client 1.1.0 framing and the Follow-up 2026-11-11 (V002). In [0009](./0009-explicit-per-row-routing.md), the client 1.0.0 and 2.0.0 references. In [0010](./0010-remove-database-destination.md), the V002 and client 2.0.0 decisions. In [0013](./0013-adopt-pernix-ci-and-tag-only-versions.md), D6 first client tag `client/v2.0.0`. The rest of each ADR stands.

## Context

ADRs 0008, 0009 and 0010 described the schema and client packages as a version history: client 1.0.0 shipped V001, client 1.1.0 added `route` to V001 in place, and client 2.0.0 shipped the destructive `V002__drop_destination_columns.sql` to remove the six columns of the removed `Database` destination. ADR 0013 D6 set the first tags to `relay/v1.0.0` and `client/v2.0.0`.

None of these versions was published. No consumer applied V001. The history therefore describes states no producer table or client install can be in, and the code, tests and docs carried compatibility for them: V002, `PostgreSqlOutboxSchema.Version` = 2, tolerance of tables with or without the six columns, the route-less table error row CE15, and release notes 1.1.0 and 2.0.0.

Human ruling 2026-11-25: "This is a new repo. The client was never published. No one used the table script yet. Clean that up, there's no need for a v002 script. Update the V001 script and change the client back to v1.0.0."

## Decision

Before the first release, the schema and the client packages start from a clean slate: one script V001 with the final table, and client 1.0.0.

- **D1. One script.** `sql/postgresql/V001__create_outbox.sql` creates the 10-column table (`id`, `tx_id`, `message_key`, `payload`, `headers`, `route`, `created_at`, `retry_count`, `next_attempt_at`, `last_error`) with `<prefix>_route_format` and no destination-only columns, constraints or indexes. `V002__drop_destination_columns.sql` is deleted. `PostgreSqlOutboxSchema.Version` = 1.
- **D2. No compatibility for unshipped states.** The relay validates against the single V001 shape. The client and docs carry no 1.0.0, 1.1.0 or 2.0.0 distinctions, no route-less table handling and no V002 guidance.
- **D3. First tags.** `relay/v1.0.0` and `client/v1.0.0`. SemVer rules for later client versions are unchanged: a new additive `Vnnn` is a minor, a destructive one a major.

## Alternatives considered

- **Keep V002 and client 2.0.0** — accurate to the commit history, but every producer would apply a destructive script to undo columns it never needed, and the docs would explain versions nobody installed.
- **Keep V002 and restart the client at 1.0.0** — the script would still exist only to fix a table no one created.

## Consequences

- A producer applies one script. `OutboxColumns.All` equals the columns V001 creates.
- The ADR bodies of 0008, 0009, 0010 and 0013 still describe the superseded version history. Read them as the record of how the design got here, not as shipped versions.
- The rule "Later versions only add columns" in V001 holds from the first release on.
