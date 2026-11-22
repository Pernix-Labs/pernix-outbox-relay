# 0012. Keep Hosting lean and name provider packages by role, with explicit registration

**Status:** Accepted
**Date:** 2026-11-22
**Deciders:** Apostolos Stephanou
**Related specs:** [2026-11-22-lean-hosting](../specs/2026-11-22-lean-hosting/)
**Supersedes:** the D7 composition clause of [0011](./0011-relay-config-section-and-hosting-package.md) (the Hosting package "composes Core, PostgreSQL and Kafka (`AddOutboxRelay`)") and the D7 package names `Pernix.OutboxRelay.PostgreSql` and `Pernix.OutboxRelay.Kafka`. The rest of 0011 stands.

## Context

ADR 0011 shipped the relay as the image and four packages. `AddOutboxRelay` in `Pernix.OutboxRelay.Hosting` called `AddOutboxRelayCore().AddPostgreSqlOutboxProvider().AddKafkaTransport()`, and the Hosting csproj referenced both providers. Every embedder got PostgreSQL, Kafka and the native `librdkafka.redist`, whether or not it sent to Kafka.

Core also held Kafka-specific configuration: `KafkaClusterOptions`, `RelayOptions.KafkaClusters` and the `KafkaClusters` binding in `RelayOptionsBinder.Bind`. `KafkaDestinationFactory.Validate` read clusters from `RelayOptions`. The package names `Pernix.OutboxRelay.PostgreSql` and `Pernix.OutboxRelay.Kafka` did not say which role each package fills.

No relay package is published yet, so the package set can change before first publish without any consumer migrating.

Human rulings on the spec: no empty packages (NG1), no compatibility shims for old names (NG2), no change to the `OutboxRelay` section or its keys (NG3), and explicit registration only, no plugin discovery (NG4).

## Decision

Hosting registers only Core and health checks, provider packages are named by role and register themselves through chained extension methods, and Kafka configuration moves out of Core behind a provider-agnostic section validator. Five decisions make this up.

### D1. Lean Hosting

- `AddOutboxRelay()` registers `AddOutboxRelayCore()` and the three health checks. It registers no outbox provider and no destination factory.
- `Pernix.OutboxRelay.Hosting` depends on `Pernix.OutboxRelay.Core` only. Its csproj has no provider `ProjectReference`.
- The ADR 0011 D3 guard (a second `AddOutboxRelay` throws), both overloads and their return types are unchanged.
- The image (`Pernix.OutboxRelay.Host`) references Hosting and both providers and calls the full chain. Image behavior and DI graph are unchanged.

### D2. Packages named by role

- `Pernix.OutboxRelay.Outbox.<Impl>` for outbox store providers, `Pernix.OutboxRelay.Destination.<Impl>` for destination transports. The name applies to directory, csproj, assembly, root namespace and package id.
- Today: `Pernix.OutboxRelay.Outbox.PostgreSql` (was `Pernix.OutboxRelay.PostgreSql`) and `Pernix.OutboxRelay.Destination.Kafka` (was `Pernix.OutboxRelay.Kafka`).
- No empty packages. A role package exists only when it has an implementation (no `Destination.PostgreSql`, no `Outbox.SqlServer`).
- Each package sets its own `PackageTags`. `src/RelayPackage.props` sets `outbox;transactional-outbox`. `Outbox.PostgreSql` appends `postgresql`, `Destination.Kafka` appends `kafka`. Core and Hosting keep the base tags.

### D3. Explicit registration chain

- Each provider package adds two extension methods, one on `IServiceCollection` and one on `TBuilder : IHostApplicationBuilder`, returning the same type: `AddPostgreSqlOutbox` (`Pernix.OutboxRelay.Outbox.PostgreSql.PostgreSqlOutboxServiceCollectionExtensions`) and `AddKafkaDestination` (`Pernix.OutboxRelay.Destination.Kafka.KafkaDestinationServiceCollectionExtensions`).
- `builder.AddOutboxRelay().AddPostgreSqlOutbox().AddKafkaDestination()` and the same chain on `services` both compile.
- Registration is idempotent. Both methods use `TryAddEnumerable`, so a repeat call adds nothing and startup is unaffected. The `DestinationFactoryResolver` throw for two distinct factories claiming one transport stays.
- Provider registration without `AddOutboxRelay()` is allowed and not checked. The services stay registered and are never resolved.
- Old methods `AddPostgreSqlOutboxProvider` and `AddKafkaTransport` are deleted with no alias.

### D4. Provider-owned configuration through `IRelaySectionValidator`

- Core gains `Pernix.OutboxRelay.Core.Abstractions.IRelaySectionValidator` with `IReadOnlyList<string> Validate(IConfiguration section)`. A provider binds and checks its own key under `OutboxRelay` in the configuration stage. Failure paths are section-relative and value-free.
- `RelayStartupValidator` takes `IEnumerable<IRelaySectionValidator>`, runs them before `RelayConfigurationValidator.Validate`, prefixes their failures with `OutboxRelay.` and reports them first.
- `KafkaClusterOptions`, `RelayOptions.KafkaClusters` and the `KafkaClusters` binding move from Core to `Destination.Kafka`. `IDestinationFactory.Validate(DestinationOptions, string, RelayOptions)` is unchanged. Core names no concrete provider or transport.
- Config keys, binding paths and failure text for `OutboxRelay:KafkaClusters` are unchanged. Kafka reuses Core's `RelayOptionsBinder.BindSection<T>`.

### D5. `KafkaClusterSection` is immutable

- `KafkaClusterSection` is an internal sealed singleton in `Destination.Kafka` that implements `IRelaySectionValidator`. Its constructor takes `RelayConfiguration`, binds `KafkaClusters` once and stores the cluster dictionary and failure list in `readonly` fields.
- `Validate` returns the stored failures. `KafkaDestinationFactory` takes the section by constructor and reads clusters through `TryGet`. The factory sees the same clusters whether or not, and in which order, `Validate` runs.
- Binding collects failures and never throws, so construction during DI resolution cannot fail outside the configuration-stage report.

## Alternatives considered

- **D1: keep `AddOutboxRelay` composing both providers.** Forces PostgreSQL, Kafka and native `librdkafka` on every embedder and ties Hosting to every future provider.
- **D2: keep `Pernix.OutboxRelay.PostgreSql` / `Pernix.OutboxRelay.Kafka`.** Names do not say whether PostgreSQL is an outbox store or a destination, which matters once a second implementation of either role exists.
- **D2: ship empty role packages for symmetry.** Rejected by ruling (NG1). A package with no implementation is a published promise with nothing behind it.
- **D3: an `IOutboxRelayBuilder` returned from `AddOutboxRelay` (ASP.NET `IHealthChecksBuilder` style).** Adds a public Core type, breaks `builder.AddOutboxRelay()` returning `TBuilder`, and makes provider registration reachable only through the relay builder, which contradicts allowing it without `AddOutboxRelay()`.
- **D3: plugin discovery by assembly scanning or config-driven loading.** Rejected by ruling (NG4). Composition stays visible in code and trimming-safe.
- **D3: `[Obsolete]` aliases or type forwarders for the old names.** Rejected by ruling (NG2). Nothing is published, so no consumer needs them.
- **D4/D5: `KafkaDestinationFactory` binds `KafkaClusters` inside each `Validate` call.** Failures for one cluster would repeat once per Kafka destination, and failures for clusters no destination names would vanish, changing the reported message set.

## Consequences

- An embedder references Hosting plus only the provider packages it uses, and adds one registration call per provider. A host that never sends to Kafka no longer carries `Confluent.Kafka`, `Nanov.Kafka` or `librdkafka.redist`.
- `AddOutboxRelay()` alone with an outbox or destination configured fails startup in the configuration stage, before any database or broker call: `OutboxRelay.Outboxes[<name>].Database.Provider: provider '<value>' is not registered` and `OutboxRelay.Destinations[<name>].Transport: transport '<value>' is not registered`. Both report together.
- The unregistered transport message changes from `is not implemented in this build` to `is not registered`.
- The Kafka logger category changes from `Pernix.OutboxRelay.Kafka.KafkaDestinationFactory` to `Pernix.OutboxRelay.Destination.Kafka.KafkaDestinationFactory`. Operators set `Logging:LogLevel:Pernix.OutboxRelay.Destination.Kafka` for librdkafka debug output. The old key no longer matches.
- Project, namespace and package id renames touch the solution, Dockerfile, test and benchmark projects and `InternalsVisibleTo`. No shims remain for the old names.
- A new provider package adds its own registration methods and, if it owns config under `OutboxRelay`, its own `IRelaySectionValidator`. Core does not change for it.
- `KafkaClusters` present with Kafka not registered is ignored. Usually the unregistered transport failure fires as well.
- ADR 0005, 0008 and older specs keep the old names as history.
