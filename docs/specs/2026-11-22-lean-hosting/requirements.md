---
status: approved
createdAt: 2026-11-22T09:13:13Z
updatedAt: 2026-11-22T09:15:30Z
approvedAt: 2026-11-22T09:15:30Z
---

# Requirements: Lean Hosting package and role-named provider packages

**Spec ID:** 2026-11-22-lean-hosting
**Author:** Architect (Orca task task_fbae9a02ab9a)

## Introduction

Base: `origin/main` at `be8df1f` (PR #2, embeddable relay, ADR 0011). No relay package published yet. This spec reshapes the package set before first publish, so no consumer migrates.

Current state, relevant facts:
- `AddOutboxRelay` (`src/Pernix.OutboxRelay.Hosting/OutboxRelayServiceCollectionExtensions.cs`) calls `AddOutboxRelayCore().AddPostgreSqlOutboxProvider().AddKafkaTransport()` plus health checks. Hosting csproj has `ProjectReference` to Core, PostgreSql, Kafka. Every embedder gets PostgreSQL + Kafka + `librdkafka.redist`.
- Projects/packages/namespaces `Pernix.OutboxRelay.PostgreSql`, `Pernix.OutboxRelay.Kafka`. Registration methods `AddPostgreSqlOutboxProvider`, `AddKafkaTransport`.
- Kafka-specific types in Core: `KafkaClusterOptions` and `RelayOptions.KafkaClusters` (`src/Pernix.OutboxRelay.Core/Configuration/RelayOptions.cs`), `KafkaClusters` binding in `RelayOptionsBinder.Bind`. `KafkaDestinationFactory.Validate` reads `RelayOptions.KafkaClusters` through `IDestinationFactory.Validate(..., RelayOptions relay)`.
- Unregistered outbox provider fails startup: `OutboxRelay.Outboxes[<name>].Database.Provider: provider '<value>' is not registered`. Unregistered transport fails: `OutboxRelay.Destinations[<name>].Transport: transport '<value>' is not implemented in this build`.
- `PostgreSqlServiceCollectionExtensions` uses `TryAddEnumerable` (repeat call safe). `AddKafkaTransport` uses `AddSingleton`; repeat call makes `DestinationFactoryResolver` throw `Destination transport 'Kafka' is registered more than once.`
- Kafka logger category `Pernix.OutboxRelay.Kafka.KafkaDestinationFactory`. `docs/configuration.md` tells operators to set `Logging:LogLevel:Pernix.OutboxRelay.Kafka` for librdkafka debug output.
- `Dockerfile`, `OutboxRelay.slnx`, test projects, benchmarks, `InternalsVisibleTo` (`src/Pernix.OutboxRelay.Kafka/AssemblyInfo.cs`) reference current project names/paths.

## Alignment with Product Vision

`docs/product.md` "Configured only" kept: image needs config only; embedder adds package references + one registration call per provider, no relay code. Embedders pay only for the provider/transport they use (no forced Kafka native dependency for a host that never sends to Kafka). Role naming (`Outbox.*`, `Destination.*`) leaves room for later providers without implying them now.

## Goals

- **G1.** `Pernix.OutboxRelay.Hosting` `AddOutboxRelay()` registers Core relay + health checks only. Hosting package has no dependency on any outbox provider or destination package.
- **G2.** Packages named by role: `Pernix.OutboxRelay.Outbox.PostgreSql`, `Pernix.OutboxRelay.Destination.Kafka` (project, assembly, root namespace, package id).
- **G3.** Embedder composes relay explicitly, chained from `AddOutboxRelay()`, e.g. `builder.AddOutboxRelay().AddPostgreSqlOutbox().AddKafkaDestination()` (final names: design).
- **G4.** Core is provider-agnostic: no Kafka- or PostgreSQL-specific type, property or config binding in Core.
- **G5.** Image registers Hosting + every provider package explicitly. Image behavior and `OutboxRelay` config shape unchanged.
- **G6.** Config naming an unregistered provider/transport fails startup, naming the value.
- **G7.** `dotnet pack` yields exactly Core, Hosting, Outbox.PostgreSql, Destination.Kafka. Existing tests green, plus new lean-registration test.
- **G8.** README, `docs/embedding.md`, architecture doc, ADR updated.

## Non-goals

- **NG1.** New providers or transports, and empty packages (no `Destination.PostgreSql`, no `Outbox.SqlServer`). Human ruling.
- **NG2.** Compatibility shims for old names: no type forwarders, no `[Obsolete]` aliases of `AddPostgreSqlOutboxProvider`/`AddKafkaTransport`, no old package ids, no "legacy"/migration guard code. Nothing published yet. Human ruling.
- **NG3.** Change to `OutboxRelay` config section or any key under it (`Outboxes`, `Destinations`, `KafkaClusters`, `ConnectionStrings`), or to `Database.Provider`/`Transport` values (`PostgreSql`, `Kafka`). Human ruling.
- **NG4.** Plugin discovery (assembly scanning, config-driven loading). Explicit registration only. Human ruling.
- **NG5.** Package publish pipeline (feed, CI job). Still follow-up of 2026-11-21-embeddable-relay NG8.
- **NG6.** Delivery, ordering, leadership, health, metric, trace semantics. Unchanged.
- **NG7.** Client packages (`client/`). Separate product, unaffected.
- **NG8.** Moving health checks, `LeaderGauge`, or opt-in helpers (`MapOutboxRelayHealth`, `AddOutboxRelayInstrumentation`). Stay where ADR 0011 placed them.

## Requirements

### Requirement 1: Lean Hosting registration

**User Story:** As .NET service owner, I want `AddOutboxRelay()` to register only the relay core, so that my host carries only the provider and transport packages I choose.

#### Acceptance Criteria

1. WHEN host calls `AddOutboxRelay()` on `IServiceCollection` or `IHostApplicationBuilder` THEN Hosting SHALL register Core relay services (`AddOutboxRelayCore` equivalent: `RelayConfiguration`, `ValidatedOutboxes`, `OutboxRuntimeState`, hosted services `RelayStartupValidator`, `OutboxWorkerService`, `OutboxStatsService`, `LeaderGaugeRegistration`) and health checks `host-lifetime`, `outbox-database`, `worker-loop` with current tags.
2. WHEN `AddOutboxRelay()` runs THEN Hosting SHALL register no `IOutboxStoreProvider` and no `IDestinationFactory`.
3. WHEN Hosting project builds THEN it SHALL reference Core only among relay projects, and SHALL contain no `using`/type reference to any provider or destination assembly.
4. IF `AddOutboxRelay()` called twice on same service collection THEN Hosting SHALL keep current D3 behavior (`InvalidOperationException`, nothing duplicated).
5. WHEN `AddOutboxRelay()` returns THEN return value SHALL let caller chain provider/destination registrations on both `IServiceCollection` and `IHostApplicationBuilder` entry points (G3 shape; type is design choice).

### Requirement 2: Explicit provider and destination registration

**User Story:** As .NET service owner, I want one registration call per provider package, so that composition is explicit and visible in my `Program.cs`.

#### Acceptance Criteria

1. WHEN host calls PostgreSQL outbox registration (from `Pernix.OutboxRelay.Outbox.PostgreSql`) THEN package SHALL register `PostgreSqlOutboxStoreProvider` as `IOutboxStoreProvider` with `Name` `PostgreSql`.
2. WHEN host calls Kafka destination registration (from `Pernix.OutboxRelay.Destination.Kafka`) THEN package SHALL register `KafkaDestinationFactory` as `IDestinationFactory` with `Transport` `Kafka`, plus Kafka cluster config binding/validation (Req4).
3. WHEN host registers relay + both packages THEN DI graph SHALL equal today's `AddOutboxRelay()` graph (same service types, lifetimes, hosted service order, health checks).
4. IF provider/destination registration called more than once on same service collection THEN registration SHALL NOT register duplicates and startup SHALL NOT fail because of it (see Q2).
5. WHEN provider/destination registration called without `AddOutboxRelay()` THEN behavior per Q3.
6. WHEN registration methods named THEN names SHALL follow role (`...Outbox`, `...Destination`), not `Provider`/`Transport` (final names: design).

### Requirement 3: Startup fails clearly for unregistered provider or destination

**User Story:** As operator or service owner, I want startup to name the provider or transport my config needs but my host lacks, so that I fix composition without reading code.

#### Acceptance Criteria

1. IF outbox `Database.Provider` names provider with no registered `IOutboxStoreProvider` THEN startup validation SHALL fail with `RelayStartupValidationException` containing `OutboxRelay.Outboxes[<name>].Database.Provider: provider '<value>' is not registered`.
2. IF destination `Transport` names transport with no registered `IDestinationFactory` THEN startup validation SHALL fail with `RelayStartupValidationException` naming path `OutboxRelay.Destinations[<name>].Transport` and value `<value>` (exact text per Q4).
3. WHEN validation fails per AC1/AC2 THEN failure SHALL be reported in configuration stage with all other config failures, before table/database and destination probe stages, and no worker SHALL start.
4. WHEN image hits AC1/AC2 THEN image SHALL log `StartupFailed` and exit `1` (current behavior).
5. WHEN validation runs THEN relay SHALL NOT inspect assemblies, package lists or config keys to guess missing packages. Plain registered-name lookup only. Human ruling.
6. IF `OutboxRelay:KafkaClusters` present and Kafka destination package not registered THEN relay SHALL ignore the section (no failure from that alone).

### Requirement 4: Core provider-agnostic, Kafka config owned by Kafka package

**User Story:** As maintainer, I want Core free of Kafka types, so that Core does not change when a destination is added or removed.

#### Acceptance Criteria

1. WHEN Core builds THEN Core SHALL contain no `KafkaClusterOptions`, no `RelayOptions.KafkaClusters`, no `KafkaClusters` binding in `RelayOptionsBinder`, and no other type/member/string specific to Kafka or PostgreSQL.
2. WHEN Kafka destination package registered THEN it SHALL bind `OutboxRelay:KafkaClusters` (same keys: `<cluster>:BootstrapServers`, `<cluster>:Producer:<key>`, cluster names case-insensitive).
3. WHEN `OutboxRelay:KafkaClusters` has binding failures THEN startup SHALL fail in configuration stage with same messages as today, including `OutboxRelay.` prefix (e.g. `OutboxRelay.KafkaClusters[Main]: unknown setting 'BootstrapServer'`).
4. WHEN Kafka destination names undefined cluster THEN failure SHALL stay `OutboxRelay.Destinations[<name>].Kafka.Cluster: Kafka cluster '<value>' is not defined in KafkaClusters`.
5. WHEN Core's `IDestinationFactory` contract changes to drop Kafka-specific inputs THEN contract SHALL still give a destination factory what it needs for validation today (destination options, path, outboxes for dead-letter mapping, relay config section). Shape: design.
6. WHEN config failures reported THEN secrecy SHALL hold: no connection string, `Producer` value or secret in any message (`LogSecrecyTests` green).

### Requirement 5: Role-named packages

**User Story:** As .NET service owner, I want package names that state their role, so that I pick the right package for my outbox and my destination.

#### Acceptance Criteria

1. WHEN repo builds THEN project `src/Pernix.OutboxRelay.PostgreSql` SHALL be `src/Pernix.OutboxRelay.Outbox.PostgreSql` with assembly name, root namespace and `PackageId` `Pernix.OutboxRelay.Outbox.PostgreSql`.
2. WHEN repo builds THEN project `src/Pernix.OutboxRelay.Kafka` SHALL be `src/Pernix.OutboxRelay.Destination.Kafka` with assembly name, root namespace and `PackageId` `Pernix.OutboxRelay.Destination.Kafka`.
3. WHEN renamed THEN every reference SHALL follow: `OutboxRelay.slnx`, `Dockerfile` COPY/restore lines, test + benchmark project references and `using`s, `InternalsVisibleTo`, `src/RelayPackage.props` comment, docs. No old name left outside `docs/specs/`, `docs/adr/` history and `KafkaDeadLetterHeadersLegacy` (untouched).
4. WHEN `dotnet pack` runs THEN repo SHALL NOT produce any `Pernix.OutboxRelay.PostgreSql`, `Pernix.OutboxRelay.Kafka`, or empty-role package.

### Requirement 6: Image registers every provider explicitly

**User Story:** As operator, I want the image to behave exactly as today, so that this refactor needs no deployment change.

#### Acceptance Criteria

1. WHEN Host project builds THEN `Pernix.OutboxRelay.Host.csproj` SHALL reference Hosting, `Outbox.PostgreSql` and `Destination.Kafka` explicitly.
2. WHEN `Program.cs` composes relay THEN it SHALL call `AddOutboxRelay()` then PostgreSQL outbox and Kafka destination registrations explicitly.
3. WHEN image runs with current config THEN external behavior SHALL be unchanged: config section and keys, validation messages, port 5612, `/livez` `/readyz`, log event ids/names, metric names/tags, trace sources, exit codes. Exception: logger category names that embed renamed namespace (Q1).
4. WHEN image built (`Dockerfile`) THEN build SHALL succeed with renamed projects and container smoke test SHALL pass (librdkafka loads, `KafkaClientLoaded` logged).

### Requirement 7: Packaging

**User Story:** As .NET service owner, I want a package graph matching roles, so that restore pulls only what I reference.

#### Acceptance Criteria

1. WHEN `dotnet pack` runs on relay projects THEN output SHALL be exactly `Pernix.OutboxRelay.Core`, `Pernix.OutboxRelay.Hosting`, `Pernix.OutboxRelay.Outbox.PostgreSql`, `Pernix.OutboxRelay.Destination.Kafka`, all net10.0, one lockstep version (`src/RelayPackage.props`).
2. WHEN Hosting `.nuspec` inspected THEN it SHALL depend on `Pernix.OutboxRelay.Core` and SHALL NOT depend on `Pernix.OutboxRelay.Outbox.PostgreSql`, `Pernix.OutboxRelay.Destination.Kafka`, `Npgsql`, `Confluent.Kafka`, `Nanov.Kafka` or `librdkafka.redist`.
3. WHEN `Outbox.PostgreSql` / `Destination.Kafka` `.nuspec` inspected THEN each SHALL depend on `Pernix.OutboxRelay.Core` and its own third-party dependencies, and SHALL NOT depend on Hosting or each other.
4. WHEN consumer restores `Destination.Kafka` THEN `librdkafka.redist` native assets SHALL flow transitively (current Kafka behavior).
5. WHEN `PackageGraphTests` run THEN they SHALL stay green.
6. WHEN packages built THEN `PackageTags` SHALL fit each package's role (no `kafka` tag on Hosting/Core/Outbox.PostgreSql, no `postgresql` tag on Hosting/Core/Destination.Kafka).

### Requirement 8: Tests

**User Story:** As maintainer, I want tests to prove lean composition, so that a provider dependency cannot creep back into Hosting.

#### Acceptance Criteria

1. WHEN unit + integration suites run after change THEN all existing tests SHALL pass (renamed `using`s/references only where needed, assertions unchanged except Q1/Q4 outcomes).
2. WHEN embedded tests (`tests/Pernix.OutboxRelay.Integration.Tests/Embedded/`) compose relay THEN they SHALL register PostgreSQL outbox and Kafka destination explicitly after `AddOutboxRelay()`.
3. WHEN new test calls `AddOutboxRelay()` alone THEN it SHALL assert service collection contains no `IOutboxStoreProvider` and no `IDestinationFactory`.
4. WHEN new test starts host with `AddOutboxRelay()` alone and valid config (PostgreSql outbox, Kafka destination) THEN host start SHALL throw `RelayStartupValidationException` whose message names provider `PostgreSql` (Req3 AC1) and transport `Kafka` (Req3 AC2).
5. WHEN new test inspects Hosting assembly references THEN it SHALL assert no reference to `Pernix.OutboxRelay.Outbox.*` or `Pernix.OutboxRelay.Destination.*` assemblies.
6. WHEN Kafka cluster binding tests move from Core binder tests THEN they SHALL keep same cases and messages against Kafka package binding.
7. WHEN `RunbookExampleTests` run THEN they SHALL pass against `docs/runbook-appsettings.example.json` with explicit provider registration.

### Requirement 9: Documentation

**User Story:** As service owner or operator, I want docs that show explicit composition, so that I embed correctly on first try.

#### Acceptance Criteria

1. WHEN `README.md` updated THEN embedding example SHALL show `AddOutboxRelay()` chained with PostgreSQL outbox and Kafka destination registration and list the four packages by role.
2. WHEN `docs/embedding.md` updated THEN it SHALL cover: which packages to reference, explicit registration chain, that `AddOutboxRelay()` alone registers no provider/transport, the startup failure messages for an unregistered provider/transport, and native `librdkafka` + `Nanov.Kafka` pin moving to `Destination.Kafka`.
3. WHEN `docs/architecture/outbox-relay-host.md` updated THEN it SHALL show new project/package graph and Kafka cluster config owned by Kafka destination package.
4. WHEN ADR written THEN new ADR (0012) SHALL record lean Hosting, role naming, Kafka config move and explicit registration, superseding ADR 0011 D7 composition clause. ADR 0011 SHALL gain "superseded in part by 0012" annotation only.
5. WHEN `docs/configuration.md` / `docs/runbook.md` reference project, namespace or logger category names THEN they SHALL use new names (incl. Q1 outcome for `Logging:LogLevel:Pernix.OutboxRelay.Kafka`).
6. WHEN `docs/configuration.md` describes `Database.Provider`/`Transport` values THEN it SHALL state each requires its package registered.

## Non-Functional Requirements

### Code Architecture and Modularity
- Dependency direction: Hosting -> Core. Outbox.PostgreSql -> Core. Destination.Kafka -> Core. Host -> Hosting + both providers. No provider -> Hosting, no provider -> provider.
- Core names no concrete provider/transport. Provider-specific config lives with its provider.
- Registration stays explicit. No reflection/assembly scanning.

### Performance
- No hot-path change. `AllocationGuardTests` and benchmarks unaffected (benchmark project references renamed only).

### Security
- Message secrecy unchanged (`LogSecrecyTests` green). Kafka cluster binding keeps value-free failure text.

### Reliability
- Startup atomic: missing provider/transport fails before any worker starts or Kestrel listens.

### Usability
- Embedder writes one chained statement. Config copies unchanged between image and embedded host.

## Open questions

1. [ ] **Q1. Logger category rename.** Namespace rename changes categories (`Pernix.OutboxRelay.Kafka.KafkaDestinationFactory` -> `Pernix.OutboxRelay.Destination.Kafka.KafkaDestinationFactory`). Operators following `docs/configuration.md` `Logging:LogLevel:Pernix.OutboxRelay.Kafka` lose librdkafka debug output. Conflicts with "image behaviour unchanged". Recommend: accept category change (ruling 2 mandates namespace), document new key in `docs/configuration.md`. Alternative: pin old category string in Kafka logger (keeps old name forever).
2. [ ] **Q2. Repeat provider registration.** Recommend idempotent (`TryAddEnumerable`, as PostgreSQL today). Alternative: throw like `AddOutboxRelay` D3.
3. [ ] **Q3. Provider registration without `AddOutboxRelay()`.** Recommend: allowed, no check. Provider services sit unused when no relay is registered. Alternative: throw `InvalidOperationException` at composition. Chained API (Req1 AC5) makes misuse unlikely either way.
4. [ ] **Q4. Unregistered transport message.** Current `transport '<value>' is not implemented in this build` is misleading for embedders. Recommend: `transport '<value>' is not registered`, matching provider message. Changes image failure text for that misconfiguration only (Req6 AC3 exception). Alternative: keep text unchanged.
