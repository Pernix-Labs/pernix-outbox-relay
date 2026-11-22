# 0011. Scope relay configuration under `OutboxRelay` and ship the relay as an image and a Hosting package

**Status:** Accepted
**Date:** 2026-11-21
**Deciders:** Apostolos Stephanou
**Related specs:** [2026-11-21-embeddable-relay](../specs/2026-11-21-embeddable-relay/design.md)
**Supersedes:** the clause "The relay ships as an image only" of [0008](./0008-producer-client-packages.md). The rest of 0008 stands.

## Context

The relay ran only as container image `outbox-relay`. Its composition (Core, PostgreSQL provider, Kafka transport, health checks, leader gauge) lived in `Pernix.OutboxRelay.Host`, and it read `Outboxes`, `Destinations`, `KafkaClusters` and `ConnectionStrings` from the root of `IConfiguration`. .NET service owners want to run delivery in-process in their own generic or web host, without a separate deployment.

Embedding exposes three problems. Root-level relay keys collide with the host's own configuration, `ConnectionStrings` above all. A host can compose the relay twice, which would duplicate workers, locks and hosted services. Existing image deployments carry root-level keys in every layered source of [ADR 0002](./0002-load-ssm-config-once-below-env-vars.md) (appsettings, SSM Parameter Store, Helm env vars, command line), and a moved section would make the image start with zero outboxes and deliver nothing, silently.

Human rulings on the spec: no backward-compatible read of root-level keys in the image (breaking change, migrate), section name fixed as `OutboxRelay`, `OutboxRelay:ConnectionStrings` only with no root fallback, the relay does no root-level key detection, at least one outbox is required in the image and the package (2026-11-22, replacing an image-only root-key guard), and relay packages version in lockstep with the image.

## Decision

The relay reads its configuration only from the `OutboxRelay` section, through one Core type, and ships as both the image and four NuGet packages that share the image's version. Four decisions make this up.

### D1. Section scoping through `RelayConfiguration`

- `Pernix.OutboxRelay.Core.Configuration.RelayConfiguration` carries the `OutboxRelay` section (`SectionName = "OutboxRelay"`). `AddOutboxRelayCore` registers it with `TryAddSingleton` from `IConfiguration.GetSection("OutboxRelay")`.
- `RelayStartupValidator` and `PostgreSqlOutboxStoreProvider` take `RelayConfiguration` instead of the root `IConfiguration`. `RelayOptionsBinder` and `GetConnectionString` run unchanged on the section, so `ConnectionStrings` resolves as `OutboxRelay:ConnectionStrings:<name>` with no root fallback.
- `RelayStartupValidator` prefixes every relay failure with `OutboxRelay.`. `HostOptions.ShutdownTimeout` stays at the root, host-owned, and unprefixed.
- The section is fixed. No overload takes a custom section or `IConfigurationSection`.
- Every source layered by [ADR 0002](./0002-load-ssm-config-once-below-env-vars.md) moves under `OutboxRelay`: appsettings `OutboxRelay:*`, SSM paths `.../OutboxRelay/...`, env vars and Helm values `OutboxRelay__*`, command line `--OutboxRelay:*`. The layering order, load-once and no-reload rules of ADR 0002 do not change.

### D3. A second relay registration throws

`AddOutboxRelay` throws `InvalidOperationException("AddOutboxRelay was already called on this service collection.")` when the service collection already has a `ValidatedOutboxes` descriptor. Only `AddOutboxRelayCore` registers it, so the check catches `AddOutboxRelay` twice and `AddOutboxRelayCore` followed by `AddOutboxRelay`. It runs before any registration, so nothing is duplicated. One relay per service collection.

### D5. At least one outbox is required, in the image and the package

`RelayConfigurationValidator.Validate` (Core) fails with `Outboxes: at least one outbox is required` when no outbox is configured. `RelayStartupValidator` prefixes it to `OutboxRelay.Outboxes: at least one outbox is required` and reports it with the other configuration failures, before the table and destination checks. The image logs `StartupFailed` and exits `1`. An embedding host's start throws `RelayStartupValidationException`. The relay never inspects root-level keys: an unmigrated image binds an empty `OutboxRelay` section and fails on this rule. The `NoOutboxesConfigured` warning (event 100) and the zero-outbox start are removed. The rule that every outbox names a defined destination, probed at startup, is unchanged.

### D7. Four packable relay packages, version in lockstep with the image

- `Pernix.OutboxRelay.Core`, `Pernix.OutboxRelay.PostgreSql`, `Pernix.OutboxRelay.Kafka` and `Pernix.OutboxRelay.Hosting` each pack as a separate package, mirroring the project graph. Kafka's native dependencies (`Confluent.Kafka`, `Nanov.Kafka`, `librdkafka.redist`) flow from the Kafka package. `Pernix.OutboxRelay.Host` is not packed.
- The relay ships as the image and as the Hosting package, which composes Core, PostgreSQL and Kafka (`AddOutboxRelay`) and offers opt-in `MapOutboxRelayHealth` and `AddOutboxRelayInstrumentation`. Logging, OpenTelemetry exporters and resource, Kestrel, endpoints and `HostOptions` stay with the host.
- `src/RelayPackage.props` sets one `VersionPrefix` from the repo-root `.version` for all four. Lockstep with the image holds once the publish job (follow-up, NG8) passes `-p:Version=$BUILD_VERSION` to pack. No such job exists yet, and the Dockerfile `BUILD_VERSION` default (`1.0.0`) is not tied to `.version`.
- This is a version line separate from the producer client packages of [ADR 0008](./0008-producer-client-packages.md) (`client/PackageVersion.props`). The relay packages follow 0008's packaging precedent (lockstep packages from this repo) but neither line follows the other's version. The publish pipeline is a follow-up.

## Alternatives considered

- **D1: keyed or replaced `IConfiguration` instead of a typed wrapper.** Ambiguous with the host's root `IConfiguration` and harder to substitute in unit tests. `RelayConfiguration` is explicit and tests build it from a plain `ConfigurationBuilder`.
- **D1: keep relay keys at the root, or read the section with a root fallback.** Root keys collide with the embedding host, `ConnectionStrings` above all, and a fallback makes the effective source of a connection string unclear. Rejected by ruling.
- **D1: configurable section name.** No requirement for it, and a fixed name keeps docs and messages simple. Rejected by ruling (NG9).
- **D3: make a second call a no-op (`TryAdd` everywhere).** Hides a composition mistake and still leaves a mixed `AddOutboxRelayCore` + `AddOutboxRelay` collection half-registered. Throwing at composition fails before the host builds.
- **D5: start with zero outboxes and log a warning.** An unmigrated image would start healthy and deliver nothing.
- **D5: detect root-level relay keys and name each one to move.** Rejected by ruling. Root keys in an embedding host may be the host's own, and the required-outbox rule already stops an unmigrated image.
- **D7: one bundled package.** Hides the native Kafka dependency behind a package that does not match the project graph and forces every transport and provider on every consumer.
- **D7: independent package versions, or one line shared with the client packages.** Independent versions let package and image drift apart for the same code. Sharing the client line couples two products with different release cadences.

## Consequences

- A host embeds the relay with `AddOutboxRelay`, and gets the same startup validation, delivery and health checks as the image. The image composes through the same entry points, so no relay registration logic remains in `Host`.
- Breaking change for every image deployment: all relay keys in appsettings, SSM, Helm values, env vars and command line move under `OutboxRelay`. An unmigrated image fails startup with `OutboxRelay.Outboxes: at least one outbox is required`. The message does not name the root keys to move. Migration steps are in [`docs/configuration.md`](../configuration.md#migrating-from-root-level-settings).
- Startup failure text changes: relay lines start with `OutboxRelay.`, and the missing connection string message names `OutboxRelay.ConnectionStrings`. Runtime messages and logs stay unprefixed.
- Root relay keys left behind after migration are ignored. Operators remove them from every source.
- A host or image with no outbox configured does not start.
- One relay per service collection. Running two relays in one process needs two hosts.
- Four packages to keep compatible, version and eventually publish. Each relay release is an image and four packages at one version.
- An embedding host owns telemetry export, log output, endpoint mapping and `HostOptions.ShutdownTimeout` (≥ `00:00:28`). The relay's leadership ([ADR 0003](./0003-leadership-by-postgresql-session-advisory-lock.md)) is unchanged: embedded replicas compete for the same lock as image replicas.
