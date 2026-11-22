---
status: approved
createdAt: 2026-11-21T20:05:00Z
updatedAt: 2026-11-22T08:39:36Z
approvedAt: 2026-11-22T08:11:01Z
---

# Requirements: Embeddable relay hosting package

**Spec ID:** 2026-11-21-embeddable-relay
**Author:** Architect (Orca task task_e6069b747ceb)

## Introduction

Relay ships only as docker image (`src/Pernix.OutboxRelay.Host`). Teams with existing .NET host want relay in-process as hosted service. New NuGet package `Pernix.OutboxRelay.Hosting` exposes same registration entry points image uses. Image becomes thin app over package, so embedded and image behavior identical by construction.

Current state, relevant facts:
- Relay config bound from configuration root: `Outboxes`, `Destinations`, `KafkaClusters` (`RelayOptionsBinder.Bind`), connection strings via root `ConnectionStrings` (`RelayStartupValidator`, `PostgreSqlOutboxStoreProvider` call `IConfiguration.GetConnectionString`).
- Health checks (`host-lifetime`, `outbox-database`, `worker-loop`), tags `live`/`ready`, `/livez` `/readyz` mapping live in Host (`HealthRegistration`).
- `LeaderGauge` + `LeaderGaugeRegistration` live in Host (`Telemetry/`), registered inside `AddRelayTelemetry`.
- Core/PostgreSql/Kafka libraries `IsPackable=false` (root `Directory.Build.props`), net10.0, `FrameworkReference Microsoft.AspNetCore.App`.
- `RelayStartupValidator` refuses `HostOptions.ShutdownTimeout < 00:00:28`.

## Alignment with Product Vision

`docs/product.md` principle "Configured only" kept: embedding needs package reference + config, no relay code change. Product statement "relay itself ships only as an image" changes: relay ships as image and as hosting package. Same delivery guarantees, ordering, leadership, metrics in both.

## Goals

- **G1.** `Pernix.OutboxRelay.Hosting` NuGet package (net10.0) registers relay + health checks into any generic/web host with one call.
- **G2.** Image (`Pernix.OutboxRelay.Host`) calls same package entry points. No relay registration logic left in Host.
- **G3.** All relay config under one `OutboxRelay` section, same shape for package and image.
- **G4.** Embedded relay passes same startup validation, delivers same, reports same health as image.
- **G5.** Breaking image config change documented with migration steps.
- **G6.** Embedding documented in README + docs/.
- **G7.** Existing tests green. New integration test proves embedded delivery + health parity.

## Non-goals

- **NG1.** New transports, providers, delivery semantics. Out of scope; parity only.
- **NG2.** Backward-compat read or key-by-key detection of root-level `Outboxes`/`Destinations`/`KafkaClusters`/`ConnectionStrings`, in image or package. Human ruling: breaking change, migrate. Un-migrated config fails through Req3 AC6.
- **NG3.** Package configuring logging providers, OTel exporters/resource/service name, Kestrel endpoints/ports. Human ruling: host-owned.
- **NG4.** Target frameworks other than net10.0, or hosts without ASP.NET Core shared framework. Libraries already require `Microsoft.AspNetCore.App`.
- **NG5.** Runtime config reload. Existing load-once semantics kept.
- **NG6.** Changes to client packages (`client/`). Separate product.
- **NG7.** Changes to Dockerfile runtime contract other than what thin-app refactor needs (port 5612, env vars, entrypoint, user stay).
- **NG8.** Package publish pipeline (feed, CI publish job). Follow-up. This PR only makes `dotnet pack` produce packages.
- **NG9.** Custom config section name or registration overload taking an `IConfigurationSection`. Ruling: section name fixed `OutboxRelay`.

## Requirements

### Requirement 1: Hosting package registration

**User Story:** As .NET service owner, I want one call to add relay to my host, so that I run outbox delivery in-process without a separate deployment.

#### Acceptance Criteria

1. WHEN host app calls package registration entry point on `IServiceCollection` / `IHostApplicationBuilder` THEN package SHALL register Core relay services, PostgreSql provider, Kafka transport and relay hosted services (`RelayStartupValidator`, `OutboxWorkerService`, `OutboxStatsService`) exactly as image registers them.
2. WHEN registration runs THEN package SHALL register health checks `host-lifetime` (tag `ready`), `outbox-database` (tag `ready`), `worker-loop` (tag `live`) with current check implementations.
3. WHEN registration runs THEN package SHALL register `LeaderGauge` state registration (`LeaderGaugeRegistration` equivalent) so `outbox.leader` instrument reports without opt-in telemetry helper.
4. WHEN registration runs THEN package SHALL NOT add/remove/configure logging providers, OTel `TracerProvider`/`MeterProvider` exporters, OTel resource or service name, Kestrel endpoints, URLs or ports.
5. WHEN registration runs THEN package SHALL NOT map any HTTP endpoint.
6. IF registration entry point called more than once on same `IServiceCollection` THEN package SHALL throw `InvalidOperationException` and SHALL NOT register duplicate services, hosted services or health checks.

### Requirement 2: Opt-in helper extensions

**User Story:** As .NET service owner, I want opt-in helpers for health endpoints and relay telemetry, so that I expose image-equivalent probes and metrics only when I choose.

#### Acceptance Criteria

1. WHEN host app calls health-mapping helper on `IEndpointRouteBuilder` THEN package SHALL map `/livez` (checks tagged `live`) and `/readyz` (checks tagged `ready`) with same predicates and response behavior as current `MapRelayHealth`.
2. WHEN host app does not call health-mapping helper THEN package SHALL map no endpoint, and relay health checks SHALL still run under host's own health endpoints.
3. WHEN host app calls tracing helper on `TracerProviderBuilder` THEN package SHALL add source `RelayDiagnostics.Name` and nothing else (no exporter, no instrumentation, no resource).
4. WHEN host app calls metrics helper on `MeterProviderBuilder` THEN package SHALL add meter `RelayDiagnostics.Name` and nothing else.
5. WHEN host app adds relay meter THEN all relay instruments (`outbox.head_age`, `outbox.pending`, `outbox.gate_lag`, `outbox.retrying`, `outbox.blocked`, `outbox.delivered`, `outbox.message_faults`, `outbox.destination_faults`, `outbox.dead_lettered`, `outbox.leader`) SHALL report as in image.

### Requirement 3: Single `OutboxRelay` config section

**User Story:** As operator, I want all relay settings under one section, so that relay config does not collide with host app config.

#### Acceptance Criteria

1. WHEN relay binds config THEN relay SHALL read from fixed section name `OutboxRelay` only, and SHALL read `Outboxes`, `Destinations`, `KafkaClusters` from `OutboxRelay:Outboxes`, `OutboxRelay:Destinations`, `OutboxRelay:KafkaClusters`.
2. WHEN outbox `Database.Connection` names connection THEN relay SHALL resolve it from `OutboxRelay:ConnectionStrings:<name>`, in startup validation and in `PostgreSqlOutboxStoreProvider`.
3. IF `OutboxRelay:ConnectionStrings:<name>` missing or empty THEN startup SHALL fail with current "must exist and be non-empty" failure. Relay SHALL NOT fall back to root `ConnectionStrings:<name>`.
4. WHEN relay reports config failure THEN message SHALL name setting path including `OutboxRelay` prefix, and SHALL still never contain connection string values, `Producer` values or secrets.
5. WHEN env var override used THEN `OutboxRelay__Outboxes__0__Polling__BatchSize` style keys SHALL override single settings as root-level keys do today.
6. IF `OutboxRelay:Outboxes` has no entries THEN startup validation SHALL fail in image and package with `OutboxRelay.Outboxes: at least one outbox is required` (`RelayStartupValidationException`; image: `StartupFailed`, exit `1`). Relay SHALL NOT start with zero outboxes, and the `NoOutboxesConfigured` warning path SHALL be removed. Existing rule kept: every outbox `Destination` SHALL name a destination defined in `OutboxRelay:Destinations`, and that destination SHALL be probed at startup.
7. WHEN relay binds config THEN neither image nor package SHALL inspect root-level `Outboxes`/`Destinations`/`KafkaClusters`/`ConnectionStrings` keys. Root keys are ignored. An un-migrated image config has an empty `OutboxRelay:Outboxes` and fails through AC6.
8. WHEN `HostOptions` read THEN relay SHALL keep reading host's `HostOptions` (root, host-owned), not `OutboxRelay:HostOptions`.

### Requirement 4: Startup validation semantics in embedded host

**User Story:** As .NET service owner, I want same startup safety checks as image, so that misconfigured relay never runs in my process.

#### Acceptance Criteria

1. WHEN embedded host starts THEN relay SHALL run all current validation stages (configuration, table/database, destination probe, bounds/routes) in `StartingAsync`, before any worker runs and before Kestrel listens.
2. IF any validation stage fails THEN relay SHALL throw `RelayStartupValidationException` and host start SHALL abort.
3. IF host `HostOptions.ShutdownTimeout < 00:00:28` THEN startup SHALL fail with current `HostOptions.ShutdownTimeout` failure.
4. WHEN host uses .NET default `HostOptions.ShutdownTimeout` (30 s) THEN validation SHALL pass that check.
5. WHEN image starts THEN image SHALL keep `HostOptions.ShutdownTimeout` `00:00:28`, `StartupFailed` log entry, exit code `1` on failure.

### Requirement 5: Image as thin app over package

**User Story:** As maintainer, I want image to call package entry points, so that embedded/image parity is structural, not tested-by-hope.

#### Acceptance Criteria

1. WHEN Host project builds THEN `Program.cs` SHALL register relay only via package registration entry point, health endpoints only via package health-mapping helper, relay source/meter only via package telemetry helpers.
2. WHEN Host builds THEN Host SHALL keep only host-owned concerns: JSON console logging, `RelayIdentity`, `RelayStarting` startup log, OTel resource/exporters, ASP.NET Core + runtime instrumentation, health-path trace filter, Kestrel `Diagnostics` endpoint, `HostOptions` binding.
3. WHEN `LeaderGauge`, `LeaderGaugeRegistration` and health check types move THEN they SHALL live in package or Core, not Host.
4. WHEN image runs with migrated config THEN external behavior SHALL be unchanged: port 5612, `/livez` `/readyz` responses, log events, metric names/tags, trace sources, exit codes.
5. WHEN `Pernix.OutboxRelay.Host` built THEN project SHALL stay non-packable docker image app.

### Requirement 6: Packaging

**User Story:** As .NET service owner, I want to install relay from NuGet, so that I add it with standard package restore.

#### Acceptance Criteria

1. WHEN `dotnet pack` runs THEN repo SHALL produce `Pernix.OutboxRelay.Hosting` package targeting net10.0.
2. WHEN Hosting depends on Core/PostgreSql/Kafka THEN those SHALL be packable net10.0 packages (or included in Hosting package) so consumer restore resolves full graph.
3. WHEN consumer restores Hosting THEN `librdkafka.redist` native assets SHALL flow transitively and load at runtime on linux-x64 (image platform) and consumer dev platforms covered by `librdkafka.redist`.
4. WHEN consumer project lacks `Microsoft.AspNetCore.App` framework reference THEN package SHALL declare it so restore/build surfaces requirement (via `FrameworkReference` in package).
5. WHEN packages built THEN every relay package version SHALL equal image version (lockstep with `.version` / `BUILD_VERSION`).
6. WHEN `PackageGraphTests` run THEN they SHALL stay green (no unlisted `Pernix.*` package references).

### Requirement 7: Embedded parity integration test

**User Story:** As maintainer, I want test proof that embedded relay matches image, so that parity regressions fail CI.

#### Acceptance Criteria

1. WHEN integration suite runs THEN new test SHALL start relay embedded in sample host (generic or web host, not `Pernix.OutboxRelay.Host` `Program`) via package entry point, against PostgreSQL + Kafka testcontainers.
2. WHEN row inserted into configured outbox table THEN test SHALL assert delivery to Kafka topic with same key, payload, headers as image host delivers.
3. WHEN sample host maps package health helper THEN test SHALL assert `/livez` and `/readyz` status codes match image host for same states (healthy; outbox database unreachable).
4. WHEN embedded host config invalid THEN test SHALL assert host start fails with `RelayStartupValidationException`.
5. WHEN existing unit + integration suites run after refactor THEN all SHALL pass, with test config moved to `OutboxRelay` section where needed.

### Requirement 8: Documentation and migration

**User Story:** As operator or service owner, I want docs for embedding and for image config migration, so that I adopt or upgrade without reading code.

#### Acceptance Criteria

1. WHEN docs updated THEN `README.md` SHALL state relay ships as image and as `Pernix.OutboxRelay.Hosting` package, with minimal embedding example.
2. WHEN docs updated THEN docs/ SHALL contain embedding guide covering: registration call, opt-in health mapping, opt-in telemetry helpers, what package does not touch (logging, OTel exporters/resource/service name, Kestrel).
3. WHEN embedding guide written THEN it SHALL document host-app implications: `HostOptions.ShutdownTimeout >= 00:00:28`, relay startup failure aborts whole host, shared process resources (thread pool, memory, native librdkafka, process-wide `LeaderGauge`, DB connections per outbox), net10.0 + ASP.NET Core shared framework requirement, multi-replica leadership unchanged (one leader per outbox across all host replicas).
4. WHEN `docs/configuration.md` updated THEN all relay settings, examples, env var override examples SHALL use `OutboxRelay` prefix, including `OutboxRelay:ConnectionStrings`.
5. WHEN migration documented THEN `docs/configuration.md` and `docs/runbook.md` SHALL describe image breaking change with before/after mapping (root `Outboxes`/`Destinations`/`KafkaClusters`/`ConnectionStrings` -> `OutboxRelay:*`, env vars `ConnectionStrings__Main` -> `OutboxRelay__ConnectionStrings__Main`), and SHALL state that an un-migrated image fails startup with `OutboxRelay.Outboxes: at least one outbox is required`. Relay does no key-by-key root-level detection.
6. WHEN `docs/runbook-appsettings.example.json` updated THEN it SHALL use `OutboxRelay` section and `RunbookExampleTests` SHALL pass against it.
7. WHEN `docs/product.md` / `docs/architecture/outbox-relay-host.md` reference "image only" THEN they SHALL reflect package delivery (architecture doc update at closeout per spec workflow).

## Non-Functional Requirements

### Code Architecture and Modularity
- Registration logic in one place (package). Host holds no relay DI registrations.
- Package depends on OpenTelemetry API surface only for helper extensions (no exporter/instrumentation packages).
- Health check, `LeaderGauge` types keep current behavior; move is namespace/assembly change only.

### Performance
- No hot-path change. Allocation guard tests (`AllocationGuardTests`) and benchmarks baseline unaffected.

### Security
- Error and log messages keep current secrecy (`LogSecrecyTests` green): no connection string values, `Producer` values, keys, payloads.
- Package reads only `OutboxRelay` section and host `HostOptions`. No other host config read.

### Reliability
- Shutdown: leader closes session within `OutboxWorker.StopCloseDeadline` (27 s) in embedded host as in image.
- Startup failure atomic: no worker starts if any validation stage fails.

### Usability
- One call to embed. Two optional helper calls for probes and telemetry.
- Config shape identical between embedded and image, so config copies across.
