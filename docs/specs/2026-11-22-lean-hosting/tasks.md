---
status: approved
createdAt: 2026-11-22T10:03:41Z
updatedAt: 2026-11-22T11:11:07Z
approvedAt: 2026-11-22T10:06:22Z
completedAt: 2026-11-22T11:11:07Z
---

# Tasks: Lean Hosting package and role-named provider packages

**Spec ID:** 2026-11-22-lean-hosting
**Requirements:** [requirements.md](./requirements.md)
**Design:** [design.md](./design.md)

<!--
DAG grammar — parsed by scripts/dag/*.sh in the orca-spec-workflow skill. One task:

  - [ ] <coord> <title>          coord = 1, 2.1, 4.1.2 … unique, dotted numeric
  - Deps / Files / Agent / Verify / Isolation — see skill template.

Checkboxes = plan notation, never ticked. Doc FROZEN once approved; runtime state lives in
orca (task-list) and docs/specs/<id>/journal.md.

Shape: 1 (rename) -> 2 (Core hook + Q4) -> 3 (Kafka config move) -> 4 (lean Hosting + registration API) -> 5 (pack/image/full-suite gate).
6 (user docs) after 1, 7 (ADRs) root, both parallel with code chain, both feed 5.
Code chain is serial: every step changes compile-coupled files (Core contract, Kafka factory ctor, registration methods) across src + tests.
Files tokens: directory tokens end with `/` and mean every file under it. Same token used consistently across tasks.
Common rules for every task: base = this branch; build must stay green at task end (`dotnet build OutboxRelay.slnx -c Release` 0 warnings-as-errors failures);
no compatibility shims, no `[Obsolete]` aliases, no type forwarders, no "legacy"/migration guard code (NG2);
config section `OutboxRelay` and every key under it unchanged (NG3); do not edit `docs/specs/`, `client/`, `tests/Pernix.OutboxRelay.Unit.Tests/Kafka/KafkaDeadLetterHeadersLegacy.cs`.
-->

- [ ] 1 Rename provider projects to role names (mechanical, no behavior change)
  - Files: src/Pernix.OutboxRelay.PostgreSql/, src/Pernix.OutboxRelay.Kafka/, src/Pernix.OutboxRelay.Outbox.PostgreSql/, src/Pernix.OutboxRelay.Destination.Kafka/, src/Pernix.OutboxRelay.Hosting/Pernix.OutboxRelay.Hosting.csproj, src/Pernix.OutboxRelay.Hosting/OutboxRelayServiceCollectionExtensions.cs, src/RelayPackage.props, OutboxRelay.slnx, Dockerfile, tests/Pernix.OutboxRelay.Unit.Tests/, tests/Pernix.OutboxRelay.Integration.Tests/, benchmarks/Pernix.OutboxRelay.Benchmarks/
  - Verify: `dotnet build OutboxRelay.slnx -c Release` green; `dotnet test tests/Pernix.OutboxRelay.Unit.Tests -c Release` green; `git grep -nE 'Pernix\.OutboxRelay\.(Kafka|PostgreSql)\b' -- src tests benchmarks OutboxRelay.slnx Dockerfile ':!benchmarks/Pernix.OutboxRelay.Benchmarks/BASELINE.md'` returns nothing (client package names `Pernix.OutboxRelay.Client.PostgreSql` are not matches)
  - Purpose: Req5 AC1-AC3. Design C6/C7 rename part, C9 plumbing. Pure rename: method/class names, DI registrations, behavior unchanged in this task.
  - Steps:
    - `git mv src/Pernix.OutboxRelay.PostgreSql src/Pernix.OutboxRelay.Outbox.PostgreSql`; `git mv` csproj to `Pernix.OutboxRelay.Outbox.PostgreSql.csproj`; `<RootNamespace>Pernix.OutboxRelay.Outbox.PostgreSql</RootNamespace>`. Assembly name and package id follow project file name (no explicit `PackageId` today; keep it that way unless pack proves otherwise).
    - Same for `src/Pernix.OutboxRelay.Kafka` -> `src/Pernix.OutboxRelay.Destination.Kafka`, csproj `Pernix.OutboxRelay.Destination.Kafka.csproj`, `<RootNamespace>Pernix.OutboxRelay.Destination.Kafka</RootNamespace>`. `AssemblyInfo.cs` moves with it, content unchanged.
    - Namespaces: `Pernix.OutboxRelay.PostgreSql` -> `Pernix.OutboxRelay.Outbox.PostgreSql`, `Pernix.OutboxRelay.Kafka` -> `Pernix.OutboxRelay.Destination.Kafka` in every `namespace`/`using` under src, tests, benchmarks. Test-project namespaces like `Pernix.OutboxRelay.Unit.Tests.Kafka` are not provider namespaces: leave them.
    - `ProjectReference` paths: `src/Pernix.OutboxRelay.Hosting/Pernix.OutboxRelay.Hosting.csproj` (still references both providers in this task; task 4 removes), `tests/Pernix.OutboxRelay.Unit.Tests/Pernix.OutboxRelay.Unit.Tests.csproj`, `benchmarks/Pernix.OutboxRelay.Benchmarks/Pernix.OutboxRelay.Benchmarks.csproj`.
    - `OutboxRelay.slnx` 2 project paths. `Dockerfile` 2 `COPY` csproj lines (lines ~12-13).
    - `src/RelayPackage.props`: comment only -> "Imported by Core, Outbox.PostgreSql, Destination.Kafka, Hosting only." Tags untouched here (task 5).
    - Logger category becomes `Pernix.OutboxRelay.Destination.Kafka.KafkaDestinationFactory` via namespace (accepted, design Q1). Do not pin old string. If any test asserts the old category string, update it to the new one.
    - Fix any CS0118/ambiguity from new `Outbox`/`Destination` namespace segments (design R1) with minimal qualification.
  - Acceptance: build + unit tests green, grep clean, no file content change beyond names/paths.

- [ ] 2 Core: `IRelaySectionValidator` hook and "is not registered" transport message
  - Deps: 1
  - Files: src/Pernix.OutboxRelay.Core/Abstractions/IRelaySectionValidator.cs, src/Pernix.OutboxRelay.Core/Configuration/RelayStartupValidator.cs, src/Pernix.OutboxRelay.Core/Configuration/DestinationFactoryResolver.cs, tests/Pernix.OutboxRelay.Unit.Tests/Configuration/RelayStartupValidatorTests.cs, tests/Pernix.OutboxRelay.Unit.Tests/Configuration/DestinationFactoryResolverTests.cs, tests/Pernix.OutboxRelay.Unit.Tests/Configuration/RelayConfigurationValidatorTests.cs, tests/Pernix.OutboxRelay.Integration.Tests/Startup/StartupValidationTests.cs
  - Verify: `dotnet build OutboxRelay.slnx -c Release` green; `dotnet test tests/Pernix.OutboxRelay.Unit.Tests -c Release` green; `git grep -n 'not implemented in this build' -- src tests` returns nothing
  - Purpose: design C1, C2, C4, Q4. Additive Core hook; Kafka still reads `RelayOptions.KafkaClusters` until task 3.
  - Steps:
    - New `src/Pernix.OutboxRelay.Core/Abstractions/IRelaySectionValidator.cs`, namespace `Pernix.OutboxRelay.Core.Abstractions`, exactly: `public interface IRelaySectionValidator { IReadOnlyList<string> Validate(IConfiguration section); }` with the comment from design C1 (binds/checks a provider-owned key under `OutboxRelay` in configuration stage, before any `IDestinationFactory.Validate`; failure paths section-relative, never carry values).
    - `RelayStartupValidator`: primary ctor gains `IEnumerable<IRelaySectionValidator> sectionValidators`. In configuration stage, after `var options = RelayOptionsBinder.Bind(configuration.Section);`, compute `sectionFailures = sectionValidators.SelectMany(v => v.Validate(configuration.Section)).ToList()` and build `failures = sectionFailures.Concat(RelayConfigurationValidator.Validate(...)).Select(Prefixed).ToList()`. Section failures first, then Core. Rest unchanged. DI resolves `IEnumerable<>` empty when none registered, so no registration change in Core.
    - `DestinationFactoryResolver.Resolve` failure text: `$"{path}.Transport: transport '{destination.Transport}' is not registered"`.
    - Tests: update every `is not implemented in this build` expectation to `is not registered` (RelayStartupValidatorTests ~132, RelayConfigurationValidatorTests ~316/~380/~422, DestinationFactoryResolverTests ~57, integration StartupValidationTests ~23/~191). Update `RelayStartupValidator` ctor call sites in `RelayStartupValidatorTests` to pass section validators (empty by default). Add `RelayStartupValidatorTests` case: a fake `IRelaySectionValidator` returning `["KafkaClusters[Main]: x"]` plus a Core failure -> exception message lists `OutboxRelay.KafkaClusters[Main]: x` (prefixed) before the Core failure.
  - Acceptance: Req3 AC2 text, Req4 AC5 hook present; unit suite green. Integration suite runs in task 5.

- [ ] 3 Move Kafka cluster config out of Core into `Destination.Kafka`
  - Deps: 2
  - Files: src/Pernix.OutboxRelay.Core/Configuration/RelayOptions.cs, src/Pernix.OutboxRelay.Core/Configuration/RelayOptionsBinder.cs, src/Pernix.OutboxRelay.Destination.Kafka/, tests/Pernix.OutboxRelay.Unit.Tests/Kafka/, tests/Pernix.OutboxRelay.Unit.Tests/Configuration/RelayOptionsBinderTests.cs, tests/Pernix.OutboxRelay.Unit.Tests/Configuration/RelayConfigurationValidatorTests.cs, tests/Pernix.OutboxRelay.Unit.Tests/Configuration/RelayStartupValidatorTests.cs, tests/Pernix.OutboxRelay.Unit.Tests/Configuration/RunbookExampleTests.cs, tests/Pernix.OutboxRelay.Integration.Tests/Kafka/, tests/Pernix.OutboxRelay.Integration.Tests/Startup/StartupValidationTests.cs, benchmarks/Pernix.OutboxRelay.Benchmarks/Infrastructure/KafkaMock.cs
  - Verify: `dotnet build OutboxRelay.slnx -c Release` green; `dotnet test tests/Pernix.OutboxRelay.Unit.Tests -c Release` green; `git grep -niE 'kafka|postgres' -- src/Pernix.OutboxRelay.Core` shows only the comment-only PostgreSQL rationale lines design "Modular Design Principles" allows (no type, member or string literal)
  - Purpose: Req4 AC1-AC6, design D4, D5, D6 (DI body), C3, C7 Kafka config part, Testing Strategy Kafka harness items.
  - Steps:
    - Core: remove `KafkaClusterOptions` class and `RelayOptions.KafkaClusters` (`RelayOptions.cs`); remove `KafkaClusters` loop in `RelayOptionsBinder.Bind`. `RelayOptionsBinder.BindSection<T>` unchanged and public.
    - New `src/Pernix.OutboxRelay.Destination.Kafka/KafkaClusterOptions.cs`: same members (`BootstrapServers`, `Producer` `Dictionary<string,string>` Ordinal), `internal sealed`.
    - New `src/Pernix.OutboxRelay.Destination.Kafka/KafkaClusterSection.cs`: exactly design C7 code block. Immutable: binds `configuration.Section.GetSection("KafkaClusters").GetChildren()` once in ctor via `RelayOptionsBinder.BindSection<KafkaClusterOptions>(cluster, $"KafkaClusters[{cluster.Key}]", bindFailures)`, clusters dict `StringComparer.OrdinalIgnoreCase`, failures `Distinct(StringComparer.Ordinal)`; `Validate(IConfiguration)` returns stored failures, ignores arg; `internal bool TryGet(string, [NotNullWhen(true)] out KafkaClusterOptions?)`.
    - `KafkaDestinationFactory`: both ctors `internal`, take `KafkaClusterSection clusters`; `Validate` cluster lookup via `clusters.TryGet(kafka.Cluster!, out var cluster)`, message unchanged (`...Kafka.Cluster: Kafka cluster '<value>' is not defined in KafkaClusters`); still reads `relay.Outboxes` for dead-letter mapping. `KafkaClientSettings` adapts to moved type.
    - Kafka registration (still named `AddKafkaTransport` in `KafkaServiceCollectionExtensions`; task 4 renames): body per design D6 — `TryAddSingleton(sp => new KafkaClusterSection(sp.GetRequiredService<RelayConfiguration>()))`, `TryAddEnumerable(ServiceDescriptor.Singleton<IRelaySectionValidator, KafkaClusterSection>(sp => sp.GetRequiredService<KafkaClusterSection>()))`, `TryAddEnumerable(ServiceDescriptor.Singleton<IDestinationFactory, KafkaDestinationFactory>(sp => new KafkaDestinationFactory(sp.GetRequiredService<ILoggerFactory>(), sp.GetRequiredService<KafkaClusterSection>())))`.
    - Tests:
      - Move `KafkaClusters` cases out of `RelayOptionsBinderTests` (~66, ~104-120, `[InlineData("KafkaClusters:Main:BootstrapServer", ...)]`) into new `tests/Pernix.OutboxRelay.Unit.Tests/Kafka/KafkaClusterSectionTests.cs`, same inputs/messages, each building `new KafkaClusterSection(new RelayConfiguration(config))` and asserting `Validate(...)` failures and `TryGet`. Add: repeat `Validate` returns same failures; `TryGet` works with no `Validate` call. Keep one Core case: `KafkaClusters` key is ignored by `RelayOptionsBinder.Bind`.
      - `KafkaHarness` (used by `KafkaDestinationTests`, `KafkaDestinationFactoryTests`, `KafkaRecordSizeTests`, `KafkaSendSlotPoolTests`): `Factory` lazy, built on first access from current `settings`; `Bind()` reuses that one `IConfiguration`; `Set`/`Remove` of a `KafkaClusters` key after `Factory` built throws `InvalidOperationException`. No test reorder needed.
      - `KafkaRawProducerTests`: hoist constant config to a static field, build factory from it.
      - Integration `KafkaCoexistenceTests`, `KafkaHeaderParityTests`, `KafkaDestinationIntegrationTests`, `KafkaCoexistenceGateTests` (+ any other compile breaks under `tests/Pernix.OutboxRelay.Integration.Tests/Kafka/`): `Bind` helpers return config/section with `RelayOptions`; `KafkaCoexistenceGateTests` uses `section.TryGet("Main", out var cluster)`.
      - `RelayConfigurationValidatorTests`, `RelayStartupValidatorTests`, `StartupValidationTests`: drop `relay.KafkaClusters` usage; where a Kafka factory is built, build `KafkaClusterSection` and register/pass it as section validator so `KafkaClusters[...]` failure messages and order stay byte-identical.
      - `RunbookExampleTests`: `var kafka = new KafkaClusterSection(relay)`; assert `kafka.Validate(relay.Section)` and core failures empty; factory from `kafka`.
      - `benchmarks/.../KafkaMock.cs`: construct factory via section. Benchmark project must build (`InternalsVisibleTo` covers it if needed; if not, add benchmark assembly to `AssemblyInfo.cs`).
  - Acceptance: Core has no Kafka type/member/binding; all moved cases green with unchanged messages; `LogSecrecyTests` unaffected (runs in task 5).

- [ ] 4 Lean `AddOutboxRelay()`, role-named registration methods, explicit image composition, new tests
  - Deps: 3
  - Files: src/Pernix.OutboxRelay.Hosting/OutboxRelayServiceCollectionExtensions.cs, src/Pernix.OutboxRelay.Hosting/Pernix.OutboxRelay.Hosting.csproj, src/Pernix.OutboxRelay.Outbox.PostgreSql/, src/Pernix.OutboxRelay.Destination.Kafka/, src/Pernix.OutboxRelay.Host/Pernix.OutboxRelay.Host.csproj, src/Pernix.OutboxRelay.Host/Program.cs, tests/Pernix.OutboxRelay.Unit.Tests/Hosting/OutboxRelayRegistrationTests.cs, tests/Pernix.OutboxRelay.Unit.Tests/Registration/ProviderRegistrationTests.cs, tests/Pernix.OutboxRelay.Unit.Tests/PostgreSql/, tests/Pernix.OutboxRelay.Integration.Tests/Embedded/
  - Verify: `dotnet build OutboxRelay.slnx -c Release` green; `dotnet test tests/Pernix.OutboxRelay.Unit.Tests -c Release` green; `git grep -nE 'AddKafkaTransport|AddPostgreSqlOutboxProvider' -- src tests benchmarks` returns nothing; `grep -c ProjectReference src/Pernix.OutboxRelay.Hosting/Pernix.OutboxRelay.Hosting.csproj` = 1 (Core)
  - Purpose: Req1, Req2, Req3 AC1/AC3, Req6 AC1-AC2, Req8 AC2-AC5. Design D1, D2, D3, D6, D9, C5, C6, C7 registration part, C8.
  - Steps:
    - Hosting: `OutboxRelayServiceCollectionExtensions` drops `using Pernix.OutboxRelay.Destination.Kafka;`/`using Pernix.OutboxRelay.Outbox.PostgreSql;`; body calls `services.AddOutboxRelayCore();` instead of the 3-call chain. D3 duplicate guard (first statement), health checks (`host-lifetime`, `outbox-database`, `worker-loop`, tags), both overloads and return types (`IServiceCollection`, `TBuilder`) unchanged. Comment: registers relay core + health checks, no outbox provider or destination. Hosting csproj: remove both provider `ProjectReference`s.
    - `Outbox.PostgreSql`: rename `PostgreSqlServiceCollectionExtensions.cs` -> `PostgreSqlOutboxServiceCollectionExtensions.cs`, class `PostgreSqlOutboxServiceCollectionExtensions`, namespace `Pernix.OutboxRelay.Outbox.PostgreSql`. Methods `public static IServiceCollection AddPostgreSqlOutbox(this IServiceCollection services)` (`ArgumentNullException.ThrowIfNull`, `TryAddEnumerable(ServiceDescriptor.Singleton<IOutboxStoreProvider, PostgreSqlOutboxStoreProvider>())`) and `public static TBuilder AddPostgreSqlOutbox<TBuilder>(this TBuilder builder) where TBuilder : IHostApplicationBuilder` (registers on `builder.Services`, returns `builder`). No new package reference: both provider csprojs already have `FrameworkReference Microsoft.AspNetCore.App`, which supplies `IHostApplicationBuilder`.
    - `Destination.Kafka`: rename `KafkaServiceCollectionExtensions.cs` -> `KafkaDestinationServiceCollectionExtensions.cs`, class `KafkaDestinationServiceCollectionExtensions`. `AddKafkaDestination(this IServiceCollection)` (task 3 D6 body) + `AddKafkaDestination<TBuilder>(this TBuilder) where TBuilder : IHostApplicationBuilder`. Old names deleted, no alias.
    - Host: csproj adds `ProjectReference` to `..\Pernix.OutboxRelay.Outbox.PostgreSql\Pernix.OutboxRelay.Outbox.PostgreSql.csproj` and `..\Pernix.OutboxRelay.Destination.Kafka\Pernix.OutboxRelay.Destination.Kafka.csproj`. `Program.cs`: `builder.AddOutboxRelay().AddPostgreSqlOutbox().AddKafkaDestination();` + 2 `using`s.
    - Call sites: `PostgreSqlOutboxStoreProviderTests` and any other src/tests/benchmarks caller of old names -> new names. Embedded integration tests (`EmbeddedRelayHost`, `GenericHostDeliveryTests`): `builder.AddOutboxRelay().AddPostgreSqlOutbox().AddKafkaDestination()`.
    - `OutboxRelayRegistrationTests`: replace `Registers_postgresql_provider_and_kafka_factory` with `Registers_no_outbox_provider_or_destination_factory` (no `IOutboxStoreProvider`, `IDestinationFactory`, `IRelaySectionValidator` descriptor). Add `Startup_without_providers_fails_naming_provider_and_transport`: `Host.CreateEmptyApplicationBuilder`, in-memory valid config (PostgreSql outbox, Kafka destination + `KafkaClusters`, `HostOptions:ShutdownTimeout` >= relay minimum), `AddOutboxRelay()` only; `StartAsync` throws `RelayStartupValidationException` containing `provider 'PostgreSql' is not registered` and `transport 'Kafka' is not registered`, and nothing else from `KafkaClusters` (Req3 AC6). Add `Hosting_references_no_provider_assembly` (`GetReferencedAssemblies()` none starting `Pernix.OutboxRelay.Outbox.` / `Pernix.OutboxRelay.Destination.`). Add `Hosting_project_depends_on_core_only` (unit-test `deps.json` `targets` entry `Pernix.OutboxRelay.Hosting/*`: `Pernix.*` deps = `Pernix.OutboxRelay.Core` only).
    - New `tests/Pernix.OutboxRelay.Unit.Tests/Registration/ProviderRegistrationTests.cs`: `AddPostgreSqlOutbox` registers `PostgreSqlOutboxStoreProvider` (`Name` `PostgreSql`); `AddKafkaDestination` resolves one `IDestinationFactory` (`Transport` `Kafka`) and one `IRelaySectionValidator` that is the same `KafkaClusterSection` instance the factory uses (resolution needs `RelayConfiguration`: use `AddOutboxRelayCore()` or `TryAddSingleton` over in-memory section); each called twice -> descriptor count unchanged, and full chain twice + valid config passes `RelayStartupValidator` configuration stage; both callable without `AddOutboxRelay()` without throw; builder overloads `Assert.Same` builder; full-chain DI parity: service types + lifetimes + hosted-service order equal a pinned list matching pre-change `AddOutboxRelay()` graph (design C8 notes the only descriptor differences: lambda Kafka factory, `KafkaClusterSection`, its `IRelaySectionValidator` entry).
    - Design R2 fallback if lambda `TryAddEnumerable` does not dedupe: make `KafkaClusterSection` public with internal members, factory type-activated. Record choice in task report.
  - Acceptance: unit suite green incl. new tests; image composition explicit; Hosting has Core as only relay reference.

- [ ] 5 Package tags, pack, image and full-suite gate
  - Deps: 4, 6, 7
  - Files: src/RelayPackage.props, src/Pernix.OutboxRelay.Outbox.PostgreSql/Pernix.OutboxRelay.Outbox.PostgreSql.csproj, src/Pernix.OutboxRelay.Destination.Kafka/Pernix.OutboxRelay.Destination.Kafka.csproj
  - Verify: Re-run every command in this task's Steps and confirm output matches; inspect nuspec dependency groups yourself
  - Purpose: Req7 AC1-AC6, Req6 AC3-AC4, Req8 AC1, design D8 + End-to-End Testing. Final evidence for the PR.
  - Steps:
    - Tags (D8): `src/RelayPackage.props` `<PackageTags>outbox;transactional-outbox</PackageTags>`. `Outbox.PostgreSql` csproj `<PackageTags>$(PackageTags);postgresql</PackageTags>`, `Destination.Kafka` csproj `<PackageTags>$(PackageTags);kafka</PackageTags>`. Core/Hosting untouched.
    - `dotnet build OutboxRelay.slnx -c Release` green.
    - `dotnet test tests/Pernix.OutboxRelay.Unit.Tests -c Release` and `dotnet test tests/Pernix.OutboxRelay.Integration.Tests -c Release` (needs Docker for Testcontainers) green. `LogSecrecyTests`, `PackageGraphTests`, `RunbookExampleTests`, `AllocationGuardTests`, `StartupValidationTests`, embedded tests all pass. If Docker unavailable, escalate; do not report success without integration run.
    - `dotnet pack` Core, Hosting, Outbox.PostgreSql, Destination.Kafka to a scratch dir outside repo: exactly 4 `.nupkg` named `Pernix.OutboxRelay.Core`, `.Hosting`, `.Outbox.PostgreSql`, `.Destination.Kafka`, same version, net10.0. Unzip nuspecs: Hosting deps = `Pernix.OutboxRelay.Core` + `OpenTelemetry.Api` (+ framework ref), none of `Pernix.OutboxRelay.Outbox.PostgreSql`, `Pernix.OutboxRelay.Destination.Kafka`, `Npgsql`, `Dapper`, `Confluent.Kafka`, `Nanov.Kafka`, `librdkafka.redist`. `Outbox.PostgreSql` deps = Core + `Npgsql` + `Dapper`. `Destination.Kafka` deps = Core + `Confluent.Kafka` + `Nanov.Kafka`. Neither provider depends on Hosting or the other. Tags per D8. No pack output for old names.
    - `docker build .` succeeds; run container with Development/minimal config (as `docs/development.md` smoke test describes) and confirm `KafkaClientLoaded` logged. If Docker unavailable, escalate.
    - `git grep -nE 'Pernix\.OutboxRelay\.(Kafka|PostgreSql)\b|AddKafkaTransport|AddPostgreSqlOutboxProvider|not implemented in this build' -- . ':!docs/specs' ':!docs/adr/0005-*' ':!docs/adr/0008-*' ':!docs/adr/0010-*' ':!docs/adr/0011-*' ':!client'` returns only ADR 0012's description of the old names (if any).
    - Report pack listing, nuspec dependency lists and test counts in worker_done.
  - Acceptance: G7, Req6 AC4, Req7 all ACs evidenced.

- [ ] 6 Update user-facing docs for lean Hosting and role-named packages
  - Deps: 1
  - Files: README.md, docs/embedding.md, docs/architecture/outbox-relay-host.md, docs/configuration.md, docs/runbook.md, docs/development.md, benchmarks/Pernix.OutboxRelay.Benchmarks/BASELINE.md
  - Verify: `git grep -nE 'Pernix\.OutboxRelay\.(Kafka|PostgreSql)\b|AddKafkaTransport|AddPostgreSqlOutboxProvider|not implemented in this build' -- README.md docs/embedding.md docs/architecture docs/configuration.md docs/runbook.md docs/development.md benchmarks/Pernix.OutboxRelay.Benchmarks/BASELINE.md` returns nothing; every relative link resolves (incl. to `docs/adr/0012-lean-hosting-and-role-named-provider-packages.md`, written by task 7)
  - Purpose: Req9 AC1-AC3, AC5-AC6, design Documentation section. Doc-only; names come from design (code may still be in flight, do not read code for names).
  - Steps:
    - `README.md`: list 4 packages by role (`Pernix.OutboxRelay.Core`, `Pernix.OutboxRelay.Hosting`, `Pernix.OutboxRelay.Outbox.PostgreSql`, `Pernix.OutboxRelay.Destination.Kafka`); embedding snippet `builder.AddOutboxRelay().AddPostgreSqlOutbox().AddKafkaDestination();` with `using Pernix.OutboxRelay.Hosting; using Pernix.OutboxRelay.Outbox.PostgreSql; using Pernix.OutboxRelay.Destination.Kafka;`.
    - `docs/embedding.md`: packages to reference; chain on both `IHostApplicationBuilder` and `IServiceCollection`; `AddOutboxRelay()` alone registers no provider/transport; repeat provider registration is a no-op; failure messages `OutboxRelay.Outboxes[<name>].Database.Provider: provider '<value>' is not registered` and `OutboxRelay.Destinations[<name>].Transport: transport '<value>' is not registered`; native `librdkafka` + `Nanov.Kafka` `[2.16.0.3]` pin now on `Pernix.OutboxRelay.Destination.Kafka` only (scope any "loads native librdkafka" text to the Kafka destination).
    - `docs/architecture/outbox-relay-host.md`: new package graph (design Mermaid "after": Hosting -> Core, Outbox.PostgreSql -> Core, Destination.Kafka -> Core, Host image -> all three); `OutboxRelay:KafkaClusters` bound/validated by `Destination.Kafka` via Core `IRelaySectionValidator` in configuration stage; Core provider-agnostic.
    - `docs/configuration.md`: logging key `Logging:LogLevel:Pernix.OutboxRelay.Destination.Kafka` replaces `...Pernix.OutboxRelay.Kafka` (Q1); `Database.Provider` `PostgreSql` requires `Pernix.OutboxRelay.Outbox.PostgreSql` registered, `Transport` `Kafka` requires `Pernix.OutboxRelay.Destination.Kafka` registered (image registers both); unregistered-transport message text (Q4). Config keys unchanged.
    - `docs/runbook.md`, `docs/development.md`, `benchmarks/Pernix.OutboxRelay.Benchmarks/BASELINE.md`: old project/namespace/path names -> new. No other edits.
  - Acceptance: grep clean, Req9 content present.

- [ ] 7 ADR 0012 and ADR 0011 partial-supersede annotation
  - Files: docs/adr/0012-lean-hosting-and-role-named-provider-packages.md, docs/adr/0011-relay-config-section-and-hosting-package.md, docs/adr/README.md
  - Verify: ADR 0012 follows `docs/adr/_template.md` sections; ADR 0011 diff is status-line annotation only; `docs/adr/README.md` index row resolves
  - Purpose: Req9 AC4, design Documentation (ADR items).
  - Steps:
    - New `docs/adr/0012-lean-hosting-and-role-named-provider-packages.md` from `docs/adr/_template.md`, status Accepted, date 2026-11-22. Record: lean Hosting (`AddOutboxRelay()` = Core + health checks, Hosting depends on Core only); role naming `Pernix.OutboxRelay.Outbox.<Impl>` / `Pernix.OutboxRelay.Destination.<Impl>`, no empty packages; explicit registration chain via extension methods on `IServiceCollection` and `TBuilder : IHostApplicationBuilder`, names `AddPostgreSqlOutbox`/`AddKafkaDestination`, idempotent (`TryAddEnumerable`), allowed without `AddOutboxRelay()` (design D1, D2, Q2, Q3; rejected `IOutboxRelayBuilder` and why); `IRelaySectionValidator` + `KafkaClusters` config moved to `Destination.Kafka`, immutable `KafkaClusterSection` (D4, D5; rejected per-`Validate` binding and why); per-package tags (D8); consequences: Kafka logger category now `Pernix.OutboxRelay.Destination.Kafka.KafkaDestinationFactory`, new logging key (Q1); unregistered transport message now `is not registered` (Q4); no plugin discovery (NG4); no shims (NG2). Supersedes ADR 0011 D7 composition clause and D7 package names. Link spec `docs/specs/2026-11-22-lean-hosting/`.
    - ADR 0011: status-line annotation only: "Superseded in part (D7) by [0012](./0012-lean-hosting-and-role-named-provider-packages.md)". No other edit.
    - `docs/adr/README.md`: index row for 0012 matching existing row format.
  - Acceptance: Req9 AC4.
