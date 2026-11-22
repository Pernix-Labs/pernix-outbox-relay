# 0008. Ship producer client packages as NuGet from this repo, with a transaction-only publish API on raw Npgsql

**Status:** Accepted
**Date:** 2026-11-07
**Deciders:** Apostolos Stephanou

## Context

The standing ruling was "image-only delivery, no NuGet", and `docs/product.md` said "Plain SQL is the only integration. There is no client library." Producers wrote the outbox INSERT by hand and copied `sql/postgresql/V001__create_outbox.sql` into their migrations. That invites three mistakes the relay can only detect after the fact:

- Write order. `tx_id DEFAULT pg_current_xact_id()` takes the xid at the transaction's first write, so an outbox INSERT that runs before the business writes inverts order on `PerKey` and `Strict`.
- Header format. The relay's `HeadersParser` dead-letters headers that are not a JSON object of strings, carry lone surrogates or arrive with duplicate names collapsed by `jsonb`. `jsonb` refuses U+0000 at INSERT.
- DDL drift. Every copied V001 can diverge from the one the relay validates against.

The target producers and consumers run net8.0 to net10.0 with Npgsql 8 to 10.

## Decision

The relay ships as an image only, and producer client packages ship as NuGet from this repo (amended ruling, Q1). Plain SQL stays a supported integration.

- **Package split and ids (D1, D2).** Core `Pernix.OutboxRelay.Client` (no package dependencies) plus one package per engine, starting with `Pernix.OutboxRelay.Client.PostgreSql` (Npgsql and `Microsoft.Extensions.DependencyInjection.Abstractions` only). ORM add-ons are separate packages (EF Core in a later spec). No client package depends on a `Pernix.*` package. Client code lives under `client/`, versioned in lockstep by one `VersionPrefix`.
- **Publish API without autocommit (D3 = O9, Q6).** `IOutboxPublisher.PublishAsync(DbTransaction, ...)` for an open transaction and `PublishEnlistedAsync(DbConnection, ...)` for an ambient `TransactionScope` only, each with a single and a batch overload. No overload publishes without a transaction. `PublishEnlistedAsync` throws when `Transaction.Current` is null.
- **Raw Npgsql in the client, not Dapper (Q3).** The provider uses `NpgsqlCommand` and `NpgsqlBatch` with typed parameters and SQL text built once per publisher. The "Dapper for data access" ruling of ADR 0005 covers relay reads and commits and stays unchanged.
- **DDL accessor from embedded V001 (D11 = O20, D12 = O21).** The provider embeds `sql/postgresql/V001__create_outbox.sql` by link and `PostgreSqlOutboxSchema.Scripts(table, namePrefix)` renders it by token replacement for any table and index/constraint name prefix. It refuses a prefix whose rendered names exceed 63 bytes. Neither the package nor the relay runs DDL. The producer's migration tool or DBA applies the text.
- **Single `net8.0` TFM (D14 = O26, Q11).** Every API the packages use exists in net8, and the net8 asset runs on net9 and net10 runtimes. Move to a newer single TFM only when a needed API or dependency major requires it.
- **`OutboxColumns` (D13).** Core exposes engine-neutral column names: one constant per V001 column, `ProducerWritten` and `All`. Tests assert `All` equals the columns V001 creates and relay `RequiredColumns ⊆ OutboxColumns.All` (test-only reference, no product coupling).
- **Shared header vectors as the drift guard.** One file, `tests/contract-vectors/headers.json` (vectors H1-H16), drives client unit tests, relay `HeadersParser` tests and contract tests. A meta-test fails if any vector is accepted by the client and rejected by the relay. Client header rules may be stricter than the relay, never looser.
- **ADR 0005 scope extended to the client publish path.** Publish runs inside the producer's business transaction, so the allocation-conscious rules apply to it: no LINQ, no reflection, cached SQL text and header JSON, no copy of key and payload bytes where Npgsql allows it.
- **Npgsql floor `8.0.3` (DV1, approved).** The study set `8.0.0`. Npgsql below `8.0.3` carries GHSA-x9vc-6hfv-hg8c (CVE-2024-32655), which fails restore under `TreatWarningsAsErrors` and would ship a vulnerable floor. `Microsoft.Extensions.DependencyInjection.Abstractions` keeps floor `8.0.0`.
- **Never call `NpgsqlConnection.EnlistTransaction` (DD1, approved Q14).** The call changes behaviour only for a connection opened before the scope, and then it splits the outbox row from business writes that already autocommitted on that connection (lost message on scope rollback). Publish runs in whatever transaction the connection is in. XML docs require opening the connection inside the scope.

## Alternatives considered

- **O1 Core plus one provider package that bundles EF, Dapper and ADO.NET support** — every Npgsql user would pull EF.
- **O3 One package per (ORM × engine)** — matrix explosion, no gain while the EF part is engine-neutral.
- **O7 `PublishAsync(DbConnection, DbTransaction?, ...)`** — redundant pair, mismatched-pair bug class, and `null` cannot tell an open transaction from autocommit.
- **O8 `PublishAsync(DbTransaction)` plus `PublishAsync(DbConnection)`** — the `DbConnection` overload has the same open-transaction versus autocommit ambiguity. Withdrawn.
- **O18 Docs only, producers copy V001** — drift by copy.
- **O19 EF `ModelBuilder` configuration** — a second copy of the schema, Npgsql-EF-only, and must model the `xid8` default, partial indexes, `jsonb_path_exists` CHECK and storage parameters.
- **O22 Separate template file with placeholders, V001 generated or checked equal** — two files for the same DDL. The 63-byte limit is the same as under O21, so it buys only explicit placeholders.
- **O23 Placeholders in V001 itself** — breaks plain `psql -f` on the file.
- **O25 Multi-target `net8.0;net10.0`** — a net10 asset buys nothing: no needed API is missing in net8, and runtime gains come from the consumer's runtime.
- **O27 `net10.0` only** — cuts net8 and net9 consumers until they upgrade.

## Consequences

- Producers publish through a typed API that validates headers before the INSERT and cannot publish outside a transaction. Plain SQL remains available.
- One V001 source feeds both the relay and producer migrations, and column or header-rule drift between client and relay fails a test in PR CI.
- The write-order rule is still the caller's: `PublishAsync` runs immediately and cannot detect a call made before the business writes. XML docs carry the rule.
- `PublishEnlistedAsync` cannot detect a connection opened before the scope or with `Enlist=false`. Such a publish follows the connection's own autocommit state, and docs require opening the connection inside the scope.
- This repo now owns a public package surface with SemVer: a producer contract change or breaking API is a client major. A NuGet publish workflow and a consumer guide follow in a later spec.
- Tests run on the CPM Npgsql version, not the `8.0.3` floor. Floor-version testing is a known gap.
- The client publish path is held to ADR 0005's allocation rules, and a benchmark baseline is recorded for it.

## Amendment 2026-11-08: EF Core add-on (spec 2026-11-08-client-ef)

No decision above changes, so this is an append and not a superseding ADR.

- **Package and floor (D6 = O15 + O24, Q16, DV2).** `Pernix.OutboxRelay.Client.EntityFrameworkCore` depends on `Microsoft.EntityFrameworkCore.Relational` only, floor `8.0.10`. Below it EF pulls GHSA-qj66-m88j-hmgj.
- **Queue per context instance.** The queue is an EF internal service registered by an options extension, one per `DbContext` instance, reset per pooled lease through `IResettableService`. The interceptor is stateless, bound to the publisher, and finds the queue through `eventData.Context`.
- **Flush at commit (Q14).** The queue flushes at `TransactionCommittingAsync` as the transaction's last writes, in one batch. The `SavingChanges` guard refuses a non-empty queue without an EF transaction. Sync `Commit` with a non-empty queue refuses and names `CommitAsync`.
- **Queue cleared on every end without commit (RF20, RF25, RF27, RF32).** Rollback, failed publish (transaction poisoned) and dispose without commit (detected at next sight, since EF has no dispose event) clear the queue. The discard Warning `OutboxMessagesDiscarded` (event id 1) logs on dispose, pool return, undetected ends, and flushed messages whose transaction ended without commit (reported when the context works again). Dispose never throws. Under a retrying execution strategy, an enqueue with no current EF transaction (inside or outside `strategy.ExecuteAsync`) throws E8 instead of being dropped or duplicated by a retry, so L7 stays the only drop case.
- **One registration line.** `UseOutbox(publisher)` covers `AddDbContext`, `AddDbContextPool`, `AddDbContextFactory` and `AddPooledDbContextFactory`. `IOutboxQueue<TContext>` is the DI facade.
- **Requirement 13: transactions begun outside EF.** The queue refuses `UseTransaction` (a, b, d joined context) and a `SaveChanges` inside a `TransactionScope` (c). **O15** supports (a), (b) and (d) and refuses (c) in favour of `PublishEnlistedAsync`. The remaining loss case L7 (writes committed outside EF's view) is covered by **O15** and the Warning.
- **Rejected.** **O16** outbox entity (EF reorders commands). **O17** commit flush alone (silent drop on plain `SaveChanges`). DI-scoped queue (leaks across pooled leases and contexts). `ConditionalWeakTable` (same pooled instance across leases, no reset hook). Refuse pooling at startup. Docs-only "no pooling" (silent leak). Throw on dispose (masks the original exception, breaks pool return).
- **Consequences.** The EF behaviours the add-on relies on are pinned by tests on EF 10 and on EF `8.0.10` (floor project). Empty-queue callbacks allocate 0 B.

## Follow-up 2026-11-08: publish workflow and consumer guide (spec 2026-11-08-client-publish)

The "NuGet publish workflow and a consumer guide follow in a later spec" consequence is done. No decision above changes.

- `.github/workflows/publish-client-nuget.yml` (manual dispatch) publishes all three packages at one version to GitHub Packages. `publish-client-nuget-dry-run.yml` packs and checks them on pull requests and never pushes.
- Consumer guide: [`docs/client-packages.md`](../client-packages.md).

## Amendment 2026-11-08: route column and route-scoped services (spec 2026-11-08-client-route)

Client 1.1.0. No decision above changes except the one-time V001 exception below, so this is an append and not a superseding ADR.

- **V001 edited in place once (Q18).** `route` and its CHECK were added to V001 before any consumer applied it. `PostgreSqlOutboxSchema.Version` stays 1. Later schema changes return to additive `Vnnn` scripts.
- **Route column.** `route text NULL`, checked by `<prefix>_route_format` (`^[a-z0-9][a-z0-9._-]{0,62}$`). NULL means the row has no route. The relay ignores `route` until the relay routing spec ships.
- **Route-scoped keyed services (Q21).** `AddKeyedPostgreSqlOutboxPublisher(key, table, route)` and `AddKeyedOutboxQueue<TContext>(key, route)` stamp the route on route-less messages, pass a message with an equal route, and throw `ArgumentException` on a different route before anything is written or queued. They never override a message's route.
- **`UseOutbox` with a route-scoped publisher (Q22).** Allowed. Queued messages are stamped at commit. A different route fails `CommitAsync` through the existing flush failure path. The guide recommends the keyed queue for per-route EF enqueue.
- **Route rules shared by source link (DD3).** `client/src/Shared/RouteRules.cs` is compiled into each package. No package reads another package's internals, because package dependencies are floors and internals can move.

## Follow-up 2026-11-08: relay reads `route` (spec 2026-11-08-core-routing)

The route amendment above says the relay does not read `route` yet. That no longer holds. The relay now reads and requires `route` and holds every row its destination does not accept. See [ADR 0009](./0009-explicit-per-row-routing.md). No decision above changes.

## Follow-up 2026-11-10: relay reads and commits use raw Npgsql (spec 2026-11-10-alloc-pg-session)

The Q3 bullet above says the "Dapper for data access" ruling of ADR 0005 covers relay reads and commits. That no longer holds. ADR 0005 was amended 2026-11-09, and relay session read, summary, commits and check now run raw prepared Npgsql commands. Dapper stays on relay cold paths only. No decision above changes.

## Follow-up 2026-11-11: V002 drops columns (spec 2026-11-11-remove-database-destination)

Two clauses above no longer hold. They are superseded by [ADR 0010](./0010-remove-database-destination.md). The route amendment's "Later schema changes return to additive `Vnnn` scripts" has one exception: V002 is destructive and drops the six destination-only columns (client 2.0.0). The D13 rule that `OutboxColumns` has one constant per V001 column and `All` equals the columns V001 creates now reads against V001 and V002: `All` lists the 10 columns left after V002. The rest of this ADR stands.

## Follow-up 2026-11-21: relay ships as image and packages (spec 2026-11-21-embeddable-relay)

The Decision's "The relay ships as an image only" no longer holds. It is superseded by [ADR 0011](./0011-relay-config-section-and-hosting-package.md): the relay ships as the image and as four lockstep packages (`Pernix.OutboxRelay.Core`, `Pernix.OutboxRelay.PostgreSql`, `Pernix.OutboxRelay.Kafka`, `Pernix.OutboxRelay.Hosting`) on a version line separate from the client packages. The rest of this ADR stands.
