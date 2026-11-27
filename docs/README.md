# docs/

Outbox Relay: a generic transactional outbox service that delivers outbox rows to a Kafka topic or an HTTP endpoint, at least once and in order. This build holds the walking skeleton (host, config, health, telemetry, image), the outbox configuration model, startup validation, one leader per outbox on PostgreSQL, and delivery to Kafka in all three `Ordering.Mode` values (`None`, `PerKey`, `Strict`), delivery to Http endpoints, retries, dead letters to Kafka or Http and, for `PerKey` and `Strict`, `OnPermanentFailure` `Block`. Product summary: [`docs/product.md`](product.md).

Startup refuses features this build lacks, see [Refused in this build](configuration.md#refused-in-this-build).

The relay ships as a container image and as the `Pernix.OutboxRelay.Hosting` package: [`docs/embedding.md`](embedding.md).

Deploying and operating the relay (config examples, database prerequisites, alerts, incidents): [`docs/runbook.md`](runbook.md).

## Reference

| Page | For | Contents |
|---|---|---|
| [`configuration.md`](./configuration.md) | Deployers, operators | Configuration chain, environment variables, outbox configuration, refused settings, startup validation, PostgreSQL prerequisites, Kafka, Http, health, metrics. |
| [`delivery.md`](./delivery.md) | Producers, consumers, operators | Ordering modes, write-order rule, failure policy, dead letters, graceful stop, routing, operator actions, broker-outage test. |
| [`embedding.md`](./embedding.md) | Service owners | Run the relay in your own .NET host with `Pernix.OutboxRelay.Hosting`: install, registration, health mapping, telemetry helpers, configuration, host implications. |
| [`client-packages.md`](./client-packages.md) | Producers | Producer client packages: install, outbox table, grants, publish paths, contracts, first publish. |
| [`runbook.md`](./runbook.md) | Operators | Deploy, database prerequisites, health, alerts, incidents, duplicates. |
| [`development.md`](./development.md) | Relay developers | Local run, local docker build, CI/CD and releasing, measuring allocations. |
| [`product.md`](./product.md) | Everyone | Product purpose, users and features. |
| [`BASELINE.md`](../benchmarks/Pernix.OutboxRelay.Benchmarks/BASELINE.md) | Relay developers | Relay allocation baseline and the dated sections later allocation changes append. |

## Kinds of documentation

Three kinds of documentation live here. Pick the right one for what you're writing.

| Folder | Kind | Time-bound? | When to write |
|---|---|---|---|
| [`architecture/`](./architecture/) | Evergreen architecture docs | No — kept in sync with the code as the system evolves | After (or alongside) the spec that introduced the system component. |
| [`adr/`](./adr/) | Architecture Decision Records | No — but **immutable** once accepted; supersede with a new ADR instead of editing | Whenever a decision closes off a plausible alternative. See [`adr/README.md`](./adr/README.md). |
| [`runbook.md`](./runbook.md) | Operator runbook (Kafka destination, PostgreSQL outbox) | No — kept in sync with the code: settings, event ids and metric names match the release | When deployment, alerts or incidents change. |

The spec workflow is the entry point for every change. ADRs and architecture docs are the durable artifacts that outlive any individual spec.
