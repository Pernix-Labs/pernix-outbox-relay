# Architecture Decision Records

An ADR captures **one decision** that closed off plausible alternatives, with the context and consequences. ADRs are immutable once accepted — to change a decision, write a new ADR that supersedes the old one.

## When to write one

Write an ADR whenever a design decision:

- Closes off alternatives that a reasonable engineer would otherwise pick (e.g. chose Postgres over Spanner, Confluent Cloud over Aiven, REST over gRPC).
- Constrains future work in a load-bearing way (e.g. "all tenant data is scoped by RLS, not application-layer filtering").
- Is the answer to a "why is it this way?" question we expect to come up more than once.

If a decision is local to one module and easily revisited, prefer notes in the spec's `design.md`. ADRs are for the choices we want to remember.

## File format

- Filename: `NNNN-<kebab-slug>.md`, four-digit sequential ID starting at `0001`.
- Use [`_template.md`](./_template.md).
- Status lifecycle: `Proposed` → `Accepted` → optionally `Superseded by NNNN`. Once `Accepted`, the body does not change.
- To revise: write a new ADR that supersedes the old one, and update the old one's status (the only allowed edit) to `Superseded by NNNN`.

## Enforcement

The `block-adr-edits.sh` PreToolUse hook, shipped with the spec-docs plugin, enforces immutability on `NNNN-*.md` files:

- **A `Proposed` ADR is still a draft** — any edit passes, including body revisions and the `Proposed` → `Accepted` status flip. Enforcement begins at `Accepted`.
- **Append-only edits are allowed** — anything where the `old_string` in the Edit call survives intact inside `new_string`. Use this for adding late-arriving alternatives, links to related ADRs, or follow-up context.
- **Replacing existing text is blocked.** The agent should write a new ADR that supersedes this one instead. One exception: the `Accepted` → `Superseded by NNNN` status flip is allowed when `old_string`/`new_string` are exactly the status line.
- **Bypass for genuine corrections** (typo, broken link, factual error in the original): include the literal marker `[adr-honest-edit]` anywhere in `new_string`. The marker stays in the file as the audit trail. Use sparingly.

The hook does not apply to `README.md` or `_template.md`.

## Index

- [`0001-record-architecture-decisions.md`](./0001-record-architecture-decisions.md) — we use ADRs.
- [`0002-load-ssm-config-once-below-env-vars.md`](./0002-load-ssm-config-once-below-env-vars.md) — SSM config loads once at startup, below env vars and command line.
- [`0003-leadership-by-postgresql-session-advisory-lock.md`](./0003-leadership-by-postgresql-session-advisory-lock.md) — one leader per outbox by PostgreSQL session advisory lock, direct connection to a primary only.
- [`0004-unconfirmed-send-is-a-destination-fault.md`](./0004-unconfirmed-send-is-a-destination-fault.md) — a send the broker did not confirm is a destination fault, no retry spent, duplicates accepted.
- [`0005-allocation-conscious-hot-path.md`](./0005-allocation-conscious-hot-path.md) — the read -> send -> commit path avoids avoidable allocations, headers are UTF-8 `MessageHeaders`. Amended 2026-11-09: raw prepared Npgsql commands on the processing path (session read, summary, commits and check), Dapper on cold paths. Amended 2026-11-10: process-wide dead-letter name bytes, allocation guard (not a CI gate).
- [`0006-leader-only-stats-polling-on-a-per-outbox-pool.md`](./0006-leader-only-stats-polling-on-a-per-outbox-pool.md) — outbox stats are polled by the leader only, every 15 s, on a per-outbox stats pool, never on the session.
- [`0007-database-transport-resolution-send-bound-and-pool.md`](./0007-database-transport-resolution-send-bound-and-pool.md) — superseded by 0010. Transport resolution, send bound and pool of the removed `Database` transport.
- [`0008-producer-client-packages.md`](./0008-producer-client-packages.md) — relay ships as an image only, producer client packages ship as NuGet from this repo, publish needs a transaction and uses raw Npgsql, DDL comes from the embedded V001. Follow-up 2026-11-11: additive-only `Vnnn` and `OutboxColumns.All` = V001 superseded by 0010 (V002 drops six columns). Follow-up 2026-11-21: "relay ships as an image only" superseded by 0011. Version statements (`VersionPrefix` lockstep, GitHub Packages publish workflows) superseded by 0013.
- [`0009-explicit-per-row-routing.md`](./0009-explicit-per-row-routing.md) — each row reaches only an address configured for its exact route (single address xor `Routes`, no default), resolved inside the destination, and Core holds every row the destination does not accept. Amended 2026-11-09: Kafka `Routes`.
- [`0010-remove-database-destination.md`](./0010-remove-database-destination.md) — the `Database` destination transport is removed with `MaxInFlight`, `SupportsDeadLetters` and `DestinationContext.Outbox`, V002 drops its six producer columns, client packages 2.0.0.
- [`0011-relay-config-section-and-hosting-package.md`](./0011-relay-config-section-and-hosting-package.md) — relay config lives only under `OutboxRelay` through `RelayConfiguration` (all ADR 0002 sources move), a second `AddOutboxRelay` throws, startup requires at least one outbox in the image and the package, and the relay ships as the image and four lockstep packages (Core, PostgreSql, Kafka, Hosting) on a version line separate from the client packages. Supersedes 0008's "relay ships as an image only". D7 version statements (`.version`, `client/PackageVersion.props`, publish job follow-up) superseded by 0013.
- [`0012-lean-hosting-and-role-named-provider-packages.md`](./0012-lean-hosting-and-role-named-provider-packages.md) — `AddOutboxRelay()` registers Core and health checks only and Hosting depends on Core only, provider packages are named by role (`Outbox.PostgreSql`, `Destination.Kafka`) and register through idempotent chained `AddPostgreSqlOutbox`/`AddKafkaDestination`, and `KafkaClusters` config moves from Core to `Destination.Kafka` behind `IRelaySectionValidator`. Supersedes 0011's D7 composition clause and package names.
- [`0013-adopt-pernix-ci-and-tag-only-versions.md`](./0013-adopt-pernix-ci-and-tag-only-versions.md) — CI/CD runs on pernix-ci `v1.0.0` templates with units `relay` (image and 4 packages) and `client` (3 packages), every release version comes only from a `<unit>/vX.Y.Z[-pre]` tag, the image goes to `ghcr.io/pernix-labs/pernix-outbox-relay` with daily retention, packages go to nuget.org by Trusted Publishing from `cd.yml` environment `release`, and the build treats every warning as an error with `NuGetAuditMode=all`, `NuGetAuditLevel=high`. Supersedes the version statements of 0008 and 0011 D7.
