---
status: approved
createdAt: 2026-11-21T21:15:00Z
updatedAt: 2026-11-21T21:42:26Z
approvedAt: 2026-11-21T20:07:20Z
completedAt: 2026-11-21T21:42:26Z
---

# Tasks: Embeddable relay hosting package

**Spec ID:** 2026-11-21-embeddable-relay
**Requirements:** [requirements.md](./requirements.md)
**Design:** [design.md](./design.md)

Shape: 1 -> 2 -> 3 -> {4, 5, 6} -> {7.1, 7.2, 7.3} -> 8 -> 9. Every code task leaves `dotnet build OutboxRelay.slnx` and `dotnet test` green. Integration tests need Docker (Testcontainers).

- [ ] 1 Scope relay config to `OutboxRelay` section (Core + PostgreSql + test harness)
  - Files: src/Pernix.OutboxRelay.Core/Configuration/RelayConfiguration.cs, src/Pernix.OutboxRelay.Core/Configuration/RelayStartupValidator.cs, src/Pernix.OutboxRelay.Core/Configuration/RelayConfigurationValidator.cs, src/Pernix.OutboxRelay.Core/OutboxRelayCoreServiceCollectionExtensions.cs, src/Pernix.OutboxRelay.PostgreSql/PostgreSqlOutboxStoreProvider.cs, benchmarks/Pernix.OutboxRelay.Benchmarks/Infrastructure/PgEnvironment.cs, tests/Pernix.OutboxRelay.Unit.Tests/Configuration/RelayStartupValidatorTests.cs, tests/Pernix.OutboxRelay.Unit.Tests/Configuration/RelayConfigurationValidatorTests.cs, tests/Pernix.OutboxRelay.Unit.Tests/Configuration/RunbookExampleTests.cs, tests/Pernix.OutboxRelay.Unit.Tests/PostgreSql/PostgreSqlOutboxStoreProviderTests.cs, tests/Pernix.OutboxRelay.Integration.Tests/RelayFactory.cs, tests/Pernix.OutboxRelay.Integration.Tests/Startup/StartupValidationTests.cs, tests/Pernix.OutboxRelay.Integration.Tests/PostgreSql/PostgreSqlOutboxStoreTests.cs
  - Verify: dotnet build OutboxRelay.slnx -c Release && dotnet test tests/Pernix.OutboxRelay.Unit.Tests && dotnet test tests/Pernix.OutboxRelay.Integration.Tests --filter-class "*StartupValidationTests" (or full integration suite); all green
  - Purpose: relay reads `OutboxRelay:*` only; startup failures carry `OutboxRelay.` prefix. Refs: requirements.md Req3 AC1-5, AC8; design.md D1, D2, C1.
  - Add `public sealed class RelayConfiguration(IConfiguration section)` in `Pernix.OutboxRelay.Core.Configuration`: `public const string SectionName = "OutboxRelay"`, `public IConfiguration Section { get; }`.
  - `AddOutboxRelayCore`: `services.TryAddSingleton(sp => new RelayConfiguration(sp.GetRequiredService<IConfiguration>().GetSection(RelayConfiguration.SectionName)))`.
  - `RelayStartupValidator`: ctor takes `RelayConfiguration` instead of `IConfiguration`; `RelayOptionsBinder.Bind(configuration.Section)`; connection lookup `configuration.Section.GetConnectionString`. Every failure list thrown (config, store, destination probe, bounds/routes stages) gets `"OutboxRelay." + failure` before `new RelayStartupValidationException(...)`. `HostOptions.ShutdownTimeout` failure line stays unprefixed. Runtime exception texts elsewhere unchanged.
  - `RelayConfigurationValidator`: connection failure text `... is not defined or empty in ConnectionStrings` -> `... is not defined or empty in OutboxRelay.ConnectionStrings`. Paths otherwise section-relative (no prefix inside validator/binder).
  - `PostgreSqlOutboxStoreProvider`: ctor takes `RelayConfiguration`; `configuration.Section.GetConnectionString(connectionName)`. No root fallback.
  - `PgEnvironment.cs` (benchmarks) + unit tests: construct `new RelayConfiguration(config)` with section-relative keys; update expected texts (prefix in `RelayStartupValidatorTests`; new `OutboxRelay.ConnectionStrings` text). Add tests: prefix on each stage, `HostOptions` line unprefixed, root `ConnectionStrings:<name>` alone does not satisfy lookup.
  - `RelayFactory`: `WithRelaySettings` prefixes keys whose first segment is `Outboxes`, `Destinations`, `KafkaClusters` or `ConnectionStrings` with `OutboxRelay:`; other keys unchanged. Add `WithRootSettings(IDictionary<string,string?>)` adding keys unprefixed (same source position). `OutboxSettings` helper unchanged (prefix applied by `WithRelaySettings`).
  - `StartupValidationTests`: expected failure lines gain `OutboxRelay.` prefix (not `HostOptions` lines).
  - `PostgreSqlOutboxStoreTests.Provider(...)` (internal static helper, also used by `PostgreSqlPreparedSessionTests` and `PostgreSqlOutboxSessionTests`): wrap config in `new RelayConfiguration(...)` (section-relative `ConnectionStrings:<name>` keys). Call sites unchanged.
  - Call-site sweep (grep `new PostgreSqlOutboxStoreProvider(` / target-typed `new(` returning `PostgreSqlOutboxStoreProvider`, `new RelayStartupValidator(`, `GetConnectionString(`): only the files listed here construct either type; other `GetConnectionString(` hits are Testcontainers `container.GetConnectionString()` (unaffected). `RunbookExampleTests` line `configuration.GetConnectionString("Main")` asserts null on example root; keep valid against section after task 7.2.
  - Acceptance: full unit + integration suites green; no `IConfiguration` root reads of relay keys remain in `src/Pernix.OutboxRelay.Core` or `src/Pernix.OutboxRelay.PostgreSql`.

- [ ] 2 Move `LeaderGauge` + `LeaderGaugeRegistration` to Core
  - Deps: 1
  - Files: src/Pernix.OutboxRelay.Core/Diagnostics/LeaderGauge.cs, src/Pernix.OutboxRelay.Core/Diagnostics/LeaderGaugeRegistration.cs, src/Pernix.OutboxRelay.Host/Telemetry/LeaderGauge.cs, src/Pernix.OutboxRelay.Host/Telemetry/LeaderGaugeRegistration.cs, src/Pernix.OutboxRelay.Host/Telemetry/TelemetryRegistration.cs, src/Pernix.OutboxRelay.Core/OutboxRelayCoreServiceCollectionExtensions.cs, tests/Pernix.OutboxRelay.Unit.Tests/Telemetry/LeaderGaugeTests.cs, tests/Pernix.OutboxRelay.Integration.Tests/Delivery/StatsMetricsTests.cs
  - Verify: dotnet build OutboxRelay.slnx -c Release && dotnet test tests/Pernix.OutboxRelay.Unit.Tests && dotnet test tests/Pernix.OutboxRelay.Integration.Tests; green; `outbox.leader` still asserted in StatsMetricsTests
  - Purpose: Core owns `outbox.leader` gauge registration. Refs: requirements.md Req1 AC3, Req5 AC3; design.md D4, C2.
  - `git mv` both files Host/Telemetry -> Core/Diagnostics; namespace `Pernix.OutboxRelay.Core.Diagnostics`; bodies unchanged. Core already has `Microsoft.AspNetCore.App` framework ref (hosting abstractions available).
  - `AddOutboxRelayCore`: add `services.AddHostedService<LeaderGaugeRegistration>()` after existing hosted services.
  - `TelemetryRegistration.AddRelayTelemetry`: remove `AddHostedService<LeaderGaugeRegistration>()` (no double registration).
  - Tests: `using` updates only.

- [ ] 3 Create `Pernix.OutboxRelay.Hosting` project; thin Host over it
  - Deps: 2
  - Files: src/Pernix.OutboxRelay.Hosting/Pernix.OutboxRelay.Hosting.csproj, src/Pernix.OutboxRelay.Hosting/OutboxRelayServiceCollectionExtensions.cs, src/Pernix.OutboxRelay.Hosting/OutboxRelayEndpointRouteBuilderExtensions.cs, src/Pernix.OutboxRelay.Hosting/OutboxRelayTelemetryExtensions.cs, src/Pernix.OutboxRelay.Hosting/Health/OutboxRelayHealthChecks.cs, src/Pernix.OutboxRelay.Hosting/Health/HostLifetimeReadinessCheck.cs, src/Pernix.OutboxRelay.Hosting/Health/OutboxDatabaseReadinessCheck.cs, src/Pernix.OutboxRelay.Hosting/Health/WorkerLoopLivenessCheck.cs, src/Pernix.OutboxRelay.Host/Health/HealthRegistration.cs, src/Pernix.OutboxRelay.Host/Health/HostLifetimeReadinessCheck.cs, src/Pernix.OutboxRelay.Host/Health/OutboxDatabaseReadinessCheck.cs, src/Pernix.OutboxRelay.Host/Health/WorkerLoopLivenessCheck.cs, src/Pernix.OutboxRelay.Host/Program.cs, src/Pernix.OutboxRelay.Host/Pernix.OutboxRelay.Host.csproj, src/Pernix.OutboxRelay.Host/Telemetry/TelemetryRegistration.cs, Directory.Packages.props, OutboxRelay.slnx, Dockerfile, tests/Pernix.OutboxRelay.Unit.Tests/Pernix.OutboxRelay.Unit.Tests.csproj, tests/Pernix.OutboxRelay.Unit.Tests/Health/HostLifetimeReadinessCheckTests.cs, tests/Pernix.OutboxRelay.Unit.Tests/Health/OutboxDatabaseReadinessCheckTests.cs, tests/Pernix.OutboxRelay.Unit.Tests/Health/WorkerLoopLivenessCheckTests.cs, tests/Pernix.OutboxRelay.Unit.Tests/Hosting/OutboxRelayRegistrationTests.cs, tests/Pernix.OutboxRelay.Unit.Tests/Hosting/OutboxRelayTelemetryExtensionsTests.cs
  - Verify: dotnet build OutboxRelay.slnx -c Release && dotnet test tests/Pernix.OutboxRelay.Unit.Tests && dotnet test tests/Pernix.OutboxRelay.Integration.Tests && docker build . ; all green; `HealthEndpointTests`, `ReadinessTests`, `TracingTests` unchanged and passing
  - Purpose: one registration entry point used by embedders and image. Refs: requirements.md Req1, Req2, Req5; design.md D3, D6, C3, C4, C5, C7.
  - New project `src/Pernix.OutboxRelay.Hosting` (`Microsoft.NET.Sdk`, `RootNamespace` `Pernix.OutboxRelay.Hosting`, `FrameworkReference Microsoft.AspNetCore.App`, `PackageReference OpenTelemetry.Api`, `ProjectReference` Core, PostgreSql, Kafka). Add `<PackageVersion Include="OpenTelemetry.Api" Version="$(OpenTelemetryVersion)" />` to `Directory.Packages.props`. Add project to `OutboxRelay.slnx` under `/src/`. Not packable yet (task 4).
  - `OutboxRelayServiceCollectionExtensions`: `AddOutboxRelay(this IServiceCollection)` and `AddOutboxRelay<TBuilder>(this TBuilder) where TBuilder : IHostApplicationBuilder`. Body: null check; if `services.Any(d => d.ServiceType == typeof(ValidatedOutboxes))` throw `InvalidOperationException("AddOutboxRelay was already called on this service collection.")` before registering anything; `AddOutboxRelayCore().AddPostgreSqlOutboxProvider().AddKafkaTransport()`; `AddHealthChecks()` with `host-lifetime` (ready), `outbox-database` (ready), `worker-loop` (live). No logging, OTel, Kestrel, endpoint, `HostOptions` registration.
  - `Health/OutboxRelayHealthChecks`: consts `LiveTag`, `ReadyTag`, `HostLifetime`, `OutboxDatabase`, `WorkerLoop`, `LivePath` `/livez`, `ReadyPath` `/readyz`, `Paths` (OrdinalIgnoreCase set). `git mv` three check classes Host/Health -> Hosting/Health, namespace `Pernix.OutboxRelay.Hosting.Health`, bodies unchanged. Delete `Host/Health/HealthRegistration.cs`.
  - `OutboxRelayEndpointRouteBuilderExtensions.MapOutboxRelayHealth(this IEndpointRouteBuilder)`: body of old `MapRelayHealth` (two `MapHealthChecks`, tag predicates, default writer); returns `endpoints`.
  - `OutboxRelayTelemetryExtensions.AddOutboxRelayInstrumentation` on `TracerProviderBuilder` (`AddSource(RelayDiagnostics.Name)`) and `MeterProviderBuilder` (`AddMeter(RelayDiagnostics.Name)`); nothing else.
  - Host: csproj `ProjectReference` Hosting only (drop Core/PostgreSql/Kafka direct refs). `Program.cs` per design.md C7 (`builder.AddOutboxRelay()`, `app.MapOutboxRelayHealth()`); keep logging, `RelayIdentity`, `RelayStarting`, `HostOptions` binding, catch/exit path. `TelemetryRegistration`: replace `.AddSource`/`.AddMeter(RelayDiagnostics.Name)` with `.AddOutboxRelayInstrumentation()`, trace filter uses `OutboxRelayHealthChecks.Paths`.
  - Dockerfile: add `COPY ["src/Pernix.OutboxRelay.Hosting/Pernix.OutboxRelay.Hosting.csproj", "src/Pernix.OutboxRelay.Hosting/"]` before restore. Runtime stage unchanged.
  - Unit tests: test csproj adds Hosting `ProjectReference`; health tests `using` updates. New `Hosting/OutboxRelayRegistrationTests`: expected hosted services (`RelayStartupValidator`, `OutboxWorkerService`, `OutboxStatsService`, `LeaderGaugeRegistration`), PostgreSql provider, Kafka factory, three checks with names/tags via `IOptions<HealthCheckServiceOptions>`; second `AddOutboxRelay` throws and descriptor count unchanged; `AddOutboxRelayCore` then `AddOutboxRelay` throws; no added `ILoggerProvider`, `TracerProvider`, `MeterProvider`, `IConfigureOptions<KestrelServerOptions>`, `IConfigureOptions<LoggerFilterOptions>` descriptors. New `Hosting/OutboxRelayTelemetryExtensionsTests`: tracer helper captures `RelayDiagnostics.ActivitySource` activity; meter helper exports a relay instrument via in-memory exporter (add `OpenTelemetry.Exporter.InMemory` to unit test csproj if needed).
  - Acceptance: Host contains no relay DI registration; image behavior unchanged (existing integration suite green).

- [ ] 4 Make relay libraries packable, lockstep version
  - Deps: 3
  - Files: src/RelayPackage.props, src/Pernix.OutboxRelay.Core/Pernix.OutboxRelay.Core.csproj, src/Pernix.OutboxRelay.PostgreSql/Pernix.OutboxRelay.PostgreSql.csproj, src/Pernix.OutboxRelay.Kafka/Pernix.OutboxRelay.Kafka.csproj, src/Pernix.OutboxRelay.Hosting/Pernix.OutboxRelay.Hosting.csproj, Dockerfile
  - Verify: dotnet pack OutboxRelay.slnx -c Release -o <tmp>; exactly 4 relay nupkgs (Core, PostgreSql, Kafka, Hosting) + existing client nupkgs, no Host nupkg; relay versions = `.version` normalized (1.0.0); `dotnet pack ... -p:Version=9.9.9` yields 9.9.9 on all four; Hosting nuspec has `frameworkReference Microsoft.AspNetCore.App`, deps on the 3 libraries + `OpenTelemetry.Api`; Kafka nuspec deps `Confluent.Kafka`, `Nanov.Kafka`; throwaway net10.0 consumer project restoring Hosting from the pack dir resolves `librdkafka.redist`; `docker build .` succeeds; unit tests incl. `PackageGraphTests` green
  - Purpose: `dotnet pack` produces relay packages at image version. Refs: requirements.md Req6; design.md D7, D8, C8. Publish pipeline out of scope (NG8).
  - `src/RelayPackage.props`: `IsPackable=true`, `VersionPrefix` = `$([System.IO.File]::ReadAllText('$(MSBuildThisFileDirectory)../.version').Trim())`, `Authors`/`Company` Pernix, `RepositoryUrl` `https://github.com/Pernix-Labs/pernix-outbox-relay`, `RepositoryType` git, `PackageTags` `outbox;transactional-outbox;postgresql;kafka`. Mirror `client/PackageVersion.props` + `client/Directory.Build.props` metadata style.
  - Import it in the four csproj. Host stays `IsPackable=false`.
  - Dockerfile restore stage: also `COPY` `src/RelayPackage.props` and `.version` before `dotnet restore`.
  - Do not touch client packages or CI publish config.

- [ ] 5 Image-only `LegacyConfigurationGuard`
  - Deps: 1, 3
  - Files: src/Pernix.OutboxRelay.Host/Configuration/LegacyConfigurationGuard.cs, src/Pernix.OutboxRelay.Host/Program.cs, tests/Pernix.OutboxRelay.Unit.Tests/Configuration/LegacyConfigurationGuardTests.cs, tests/Pernix.OutboxRelay.Integration.Tests/Startup/LegacyConfigurationTests.cs
  - Verify: dotnet test tests/Pernix.OutboxRelay.Unit.Tests && dotnet test tests/Pernix.OutboxRelay.Integration.Tests; green; new tests prove guard fires before `RelayStartupValidator` and never prints connection string values
  - Purpose: pre-migration image config fails loudly instead of starting with zero outboxes. Refs: requirements.md Req3 AC6-7; design.md D5, C6, Error scenarios 2-3.
  - `LegacyConfigurationGuard(IConfiguration configuration) : IHostedLifecycleService`, `internal static IReadOnlyList<string> Find(IConfiguration root)` per design.md C6: empty when `root.GetSection("OutboxRelay").Exists()`; one line per existing root `Outboxes` / `Destinations` / `KafkaClusters` (`"<key>: relay settings moved under OutboxRelay; move to OutboxRelay:<key> (env OutboxRelay__<key>__...)"`); one line per distinct root `Outboxes:<i>:Database:Connection` name whose root `ConnectionStrings:<name>` exists (`"ConnectionStrings.<name>: move to OutboxRelay:ConnectionStrings:<name> (env OutboxRelay__ConnectionStrings__<name>)"`). Names only, never values. `StartingAsync` throws `RelayStartupValidationException(failures)` when non-empty; other lifecycle methods no-op.
  - `Program.cs`: `builder.Services.AddHostedService<LegacyConfigurationGuard>()` immediately before `builder.AddOutboxRelay()`.
  - Unit tests on `Find`: each legacy key alone, connection name referenced and present, section present -> empty, secret value absent from output.
  - Integration: `new RelayFactory().WithRootSettings(<root-level Outboxes/Destinations/KafkaClusters/ConnectionStrings>)` -> host start fails; captured message contains `OutboxRelay` and each legacy key; does not contain the connection string value. Root keys + `OutboxRelay` section present -> starts.
  - Package (Hosting) gets no guard.

- [ ] 6 Embedded-host parity integration tests
  - Deps: 3
  - Files: tests/Pernix.OutboxRelay.Integration.Tests/Embedded/EmbeddedHostParityTests.cs, tests/Pernix.OutboxRelay.Integration.Tests/Embedded/EmbeddedRelayHost.cs, tests/Pernix.OutboxRelay.Integration.Tests/Embedded/GenericHostDeliveryTests.cs
  - Verify: dotnet test tests/Pernix.OutboxRelay.Integration.Tests; all green including new Embedded tests
  - Purpose: prove embedded relay = image in delivery and health. Refs: requirements.md Req7; design.md Testing Strategy § Integration.
  - `EmbeddedRelayHost` (test helper): `WebApplication.CreateBuilder()`, `builder.WebHost.UseTestServer()`, in-memory `OutboxRelay:*` settings (same shape as `RelayFactory.OutboxSettings`, prefixed), `builder.AddOutboxRelay()`, `app.MapOutboxRelayHealth()`; exposes `HttpClient`, `Services`, async dispose. Never uses `Program`.
  - `EmbeddedHostParityTests` (reuse `PostgreSqlFixture`, `KafkaFixture`, `RelayFactory`):
    - Delivery: insert same row shape (key, payload, headers) into one outbox per host (separate schemas); consume both from Kafka; assert key, payload, headers equal across hosts.
    - Health: both hosts `/livez` 200 and `/readyz` 200 once leader; own PostgreSQL container paused (technique from `Leadership/ReadinessTests`) -> both `/readyz` 503, `/livez` 200; recovers after unpause.
    - Invalid config: embedded host start throws `RelayStartupValidationException` whose failures include an `OutboxRelay.`-prefixed line.
  - `GenericHostDeliveryTests`: `Host.CreateApplicationBuilder()` + `AddOutboxRelay()`, no endpoints; one row delivered.

- [ ] 7.1 Embedding guide + README
  - Deps: 4, 5, 6
  - Files: docs/embedding.md, README.md, docs/README.md
  - Verify: links resolve (relative paths exist); code snippets use exact API names from `src/Pernix.OutboxRelay.Hosting`; `dotnet test` green (client docs `ReadmeLinkTests` if it covers README)
  - Purpose: embedders adopt without reading code. Refs: requirements.md Req8 AC1-3; design.md C9.
  - `docs/embedding.md`: install `Pernix.OutboxRelay.Hosting`; `builder.AddOutboxRelay()`; opt-in `app.MapOutboxRelayHealth()`; opt-in `.WithTracing(t => t.AddOutboxRelayInstrumentation())` / `.WithMetrics(m => m.AddOutboxRelayInstrumentation())`; config under `OutboxRelay` (link `docs/configuration.md`); what package does not touch (logging providers, OTel exporters/resource/service name, Kestrel); host implications: `HostOptions.ShutdownTimeout >= 00:00:28` (default 30 s ok), relay startup failure aborts host start, shared process resources (thread pool, memory, native librdkafka, process-wide `LeaderGauge`, DB connections per outbox), net10.0 + ASP.NET Core shared framework, one leader per outbox across all replicas, `AddOutboxRelay` once per service collection (throws otherwise), relay health checks appear in host's own health endpoints.
  - `README.md`: relay ships as image and as `Pernix.OutboxRelay.Hosting`; minimal snippet; link guide. `docs/README.md` index entry.

- [ ] 7.2 Configuration + runbook migration docs
  - Deps: 4, 5, 6
  - Files: docs/configuration.md, docs/runbook.md, docs/runbook-appsettings.example.json, tests/Pernix.OutboxRelay.Unit.Tests/Configuration/RunbookExampleTests.cs
  - Verify: dotnet test tests/Pernix.OutboxRelay.Unit.Tests --filter-class "*RunbookExampleTests" green (binds `OutboxRelay` section of example); grep shows no root-level relay key examples left in docs/configuration.md / docs/runbook.md outside the "before" column of migration
  - Purpose: document `OutboxRelay` section and image breaking change. Refs: requirements.md Req8 AC4-6; design.md C9, Migration mapping table.
  - `docs/configuration.md`: every relay key, example, env override under `OutboxRelay` (`OutboxRelay:ConnectionStrings`, `OutboxRelay__Outboxes__0__Polling__BatchSize`); startup failure messages shown with `OutboxRelay.` prefix and `OutboxRelay.ConnectionStrings` text; new section "Migrating from root-level settings": full design.md mapping table (appsettings, env, SSM paths `.../OutboxRelay/...`, Helm env keys `OutboxRelay__*`, command line), legacy-guard failure text, note that with `OutboxRelay` present root keys are ignored, no root `ConnectionStrings` fallback.
  - `docs/runbook.md`: `OutboxRelay__ConnectionStrings__Main` from secret; upgrade note for SSM paths + Helm values.
  - `docs/runbook-appsettings.example.json`: relay keys nested under `OutboxRelay`; `HostOptions`/`Kestrel`/`Logging` stay root. `RunbookExampleTests` binds `GetSection("OutboxRelay")` / `new RelayConfiguration(...)`.

- [ ] 7.3 Product doc delivery statement
  - Deps: 4
  - Files: docs/product.md
  - Verify: none
  - Purpose: product doc says relay ships as image and package. Refs: requirements.md Alignment, Req8 AC7.
  - Replace "the relay itself ships only as an image" with image + `Pernix.OutboxRelay.Hosting` NuGet package; principle "Configured only" unchanged.

- [ ] 8 Closeout: architecture doc + ADRs
  - Deps: 7.1, 7.2, 7.3
  - Files: docs/architecture/outbox-relay-host.md, docs/adr/0011-relay-config-section-and-hosting-package.md, docs/adr/README.md
  - Verify: ADR follows docs/adr/_template.md; ADR records D1, D3, D5, D7 each with Status Accepted; ADR cross-references docs/adr/0002-load-ssm-config-once-below-env-vars.md; ADR index lists 0011; architecture doc component table names Hosting project and moved types with correct paths
  - Purpose: record design decisions. Refs: design.md Links, D1, D3, D5, D7; docs/specs/README.md § When a spec is "done".
  - `docs/architecture/outbox-relay-host.md`: add Hosting component (`AddOutboxRelay`, `MapOutboxRelayHealth`, `AddOutboxRelayInstrumentation`, health checks); move `LeaderGauge*` to Core diagnostics row; `Program` row reflects thin composition + `LegacyConfigurationGuard`; Core configuration row: `RelayConfiguration`, `OutboxRelay` section, `OutboxRelay.` failure prefix.
  - ADR 0011 (one ADR, four decisions): D1 `RelayConfiguration` section scoping under `OutboxRelay` (cross-ref ADR 0002: all layered sources move under `OutboxRelay`); D3 duplicate registration throws; D5 legacy guard image-only (cross-ref ADR 0002); D7 four packable packages, relay ships as image and package, version lockstep with image (cross-ref ADR 0008, separate version line from client packages). Alternatives per design.md.
  - Index entry in `docs/adr/README.md`.

- [ ] 9 Full verification
  - Deps: 8
  - Files: none
  - Verify: dotnet build OutboxRelay.slnx -c Release (TreatWarningsAsErrors); dotnet test OutboxRelay.slnx all green; dotnet pack OutboxRelay.slnx -c Release produces 4 relay nupkgs; docker build . succeeds and container with `OutboxRelay`-shaped config serves `/livez` on 5612; container with root-level legacy config exits 1 with `StartupFailed` naming `OutboxRelay`
  - Purpose: prove requirements.md G1-G7 end to end. No code changes; report any failure with output to coordinator.
