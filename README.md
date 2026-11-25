# Pernix Outbox Relay

**Publish an event if and only if your transaction commits.**

Outbox Relay is a transactional outbox for .NET and PostgreSQL. The producer inserts the event as a row in an outbox table, in the same transaction as the business change. The relay polls the table and delivers each row to a Kafka topic, at least once and in the configured order.

[![CI](https://github.com/Pernix-Labs/pernix-outbox-relay/actions/workflows/ci.yml/badge.svg)](https://github.com/Pernix-Labs/pernix-outbox-relay/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![.NET](https://img.shields.io/badge/.NET-8.0%20client%20%7C%2010.0%20relay-purple.svg)](https://dotnet.microsoft.com/)

The relay runs from configuration alone. One outbox table to one Kafka topic:

<!-- config: readme-minimal -->
```json
{
  "OutboxRelay": {
    "KafkaClusters": {
      "Main": {
        "BootstrapServers": "kafka:9092"
      }
    },
    "Destinations": [
      {
        "Name": "RiskEvents",
        "Transport": "Kafka",
        "Kafka": {
          "Cluster": "Main",
          "Topic": "risk.events"
        }
      }
    ],
    "Outboxes": [
      {
        "Name": "Risk",
        "Destination": "RiskEvents",
        "Database": {
          "Provider": "PostgreSql",
          "Connection": "RiskDb",
          "Table": "risk.outbox"
        },
        "Ordering": {
          "Mode": "PerKey"
        }
      }
    ]
  }
}
```

Mount the file into the image and pass the connection string as an environment variable:

```shell
docker run \
  -v "$PWD/appsettings.json:/app/appsettings.Production.json:ro" \
  -e OutboxRelay__ConnectionStrings__RiskDb="Host=risk-db;Database=risk;Username=outbox_relay;Password=<secret>" \
  ghcr.io/pernix-labs/pernix-outbox-relay:<version>
```

The producer enqueues the event in the same EF Core transaction as the business change:

<!-- snippet: readme-ef-uow-handler -->
```csharp
public sealed class UpdateRuleSetHandler(IUnitOfWork unitOfWork, IRuleSetRepository ruleSets, IOutboxQueue<RiskDb> outbox)
{
    public async Task HandleAsync(Guid ruleSetId, string definition, Guid eventId, CancellationToken ct)
    {
        var ruleSet = await ruleSets.GetAsync(ruleSetId, ct);
        ruleSet.Definition = definition;

        var headers = OutboxHeaders.CreateBuilder(1)
            .Add("idempotency-key", eventId.ToString())
            .Build();
        var changed = new RuleSetChanged(eventId, ruleSetId);
        outbox.Enqueue(OutboxMessage.Json(ruleSetId.ToString(), changed, RiskJson.Default.RuleSetChanged, headers));

        // The queued row is written at commit, after SaveChanges, in the same transaction.
        await unitOfWork.CommitAsync(cancellationToken: ct);
    }
}
```

`Enqueue` costs no round trip. The row is written at commit, as the transaction's last write. The relay does the rest.

## At a glance

Business write and outbox row commit together → the relay leader polls the table → each row goes to its Kafka topic → the row is deleted once the broker confirms it.

![Flow](docs/flow.svg)

## Why

A service that changes its database and then publishes an event loses the event when the publish fails after the commit. Publishing first is no better: the event goes out and the transaction rolls back.

- **Atomic.** The outbox row commits with the business change or not at all.
- **At least once, in order.** Three ordering modes per outbox: `None`, `PerKey`, `Strict`. Retries with backoff, then dead letter, drop or block.
- **One leader per outbox.** Replicas elect a leader through a PostgreSQL session advisory lock. No extra coordinator.
- **Allocation-conscious hot path.** Measured, not estimated: see [Performance](#performance).
- **Two ways to run.** A container image, or the same relay embedded in your own .NET host.

## Packages & installation

Producer side, `net8.0`:

```shell
dotnet add package Pernix.OutboxRelay.Client.PostgreSql
dotnet add package Pernix.OutboxRelay.Client.EntityFrameworkCore   # EF Core producers only
```

Relay side, `net10.0`, embedded in your host:

```shell
dotnet add package Pernix.OutboxRelay.Hosting
dotnet add package Pernix.OutboxRelay.Outbox.PostgreSql
dotnet add package Pernix.OutboxRelay.Destination.Kafka
```

Or run the image: `ghcr.io/pernix-labs/pernix-outbox-relay:<version>`.

| Package | Side | What it is |
|---|---|---|
| `Pernix.OutboxRelay.Client` | Producer | Contracts: `OutboxMessage`, `OutboxHeaders`, `IOutboxPublisher`. |
| `Pernix.OutboxRelay.Client.PostgreSql` | Producer | PostgreSQL publisher (`AddPostgreSqlOutboxPublisher`) and the outbox DDL scripts (`PostgreSqlOutboxSchema`). |
| `Pernix.OutboxRelay.Client.EntityFrameworkCore` | Producer | EF Core queue (`UseOutbox`, `AddOutboxQueue`, `IOutboxQueue<TContext>`). |
| `Pernix.OutboxRelay.Core` | Relay | Relay engine and provider abstractions. Referenced by the other three. |
| `Pernix.OutboxRelay.Hosting` | Relay | `AddOutboxRelay`, health checks, telemetry helpers. Registers no outbox provider or destination. |
| `Pernix.OutboxRelay.Outbox.PostgreSql` | Relay | PostgreSQL outbox provider (`AddPostgreSqlOutbox`). |
| `Pernix.OutboxRelay.Destination.Kafka` | Relay | Kafka destination (`AddKafkaDestination`). |

Releases are cut from tags: `client/vX.Y.Z` publishes the three client packages, `relay/vX.Y.Z` publishes the image and the four relay packages at one version. Install every client package at one version. No version is released yet. The first tags will be `client/v2.0.0` and `relay/v1.0.0` ([Release](docs/development.md#release)).

## Quick start

The examples publish a `RuleSetChanged` event for the outbox table `risk.outbox`. `RiskDb` is the `DbContext`, `RuleSet` the entity, `IUnitOfWork` and `IRuleSetRepository` your unit of work and repository, and `RiskJson` the source-generated `JsonSerializerContext`. Full guide: [`docs/client-packages.md`](docs/client-packages.md).

### 1. Create the outbox table

<!-- snippet: readme-ddl-ef-migration -->
```csharp
[DbContext(typeof(RiskDb))]
[Migration("20261108000000_CreateRiskOutbox")]
public sealed class CreateRiskOutbox : Migration
{
    protected override void Up(MigrationBuilder migrationBuilder)
    {
        // One migration per outbox script. Scripts come in Version order: [0] is V001.
        var v001 = PostgreSqlOutboxSchema.Scripts(OutboxTable.Parse("risk.outbox"))[0];
        migrationBuilder.Sql(v001.Sql);
    }
}
```

The package renders the relay's DDL for your table. Your migration applies it. The relay never runs DDL. The schema must exist before the first script runs: the scripts create no schema. The producer role needs `USAGE` on the schema and `INSERT` on the table. DbUp, Flyway and grants: [Create the outbox table](docs/client-packages.md#create-the-outbox-table).

### 2. Register

<!-- snippet: readme-register -->
```csharp
services.AddPostgreSqlOutboxPublisher("risk.outbox");
services.AddDbContext<RiskDb>((sp, options) => options
    .UseNpgsql(connectionString)
    .UseOutbox(sp.GetRequiredService<IOutboxPublisher>()));
services.AddOutboxQueue<RiskDb>();
```

One publisher per outbox table. Only the EF Core queue needs `UseOutbox` and `AddOutboxQueue`. Dapper, ADO.NET and the EF Core explicit-transaction path need only `AddPostgreSqlOutboxPublisher`. Keyed publishers and pooled contexts: [Register the publisher](docs/client-packages.md#register-the-publisher).

### 3. Publish

**EF Core.** The handler at the top of this page: `outbox.Enqueue(...)`, then commit. The unit of work must begin an EF transaction with `Database.BeginTransactionAsync`, save, and commit with `CommitAsync`, as `EfUnitOfWork` does. Its commit writes the row as the transaction's last write, so the write-order rule holds by construction. Under a retrying execution strategy (`EnableRetryOnFailure` or a custom `IExecutionStrategy`), enqueue only after `BeginTransactionAsync`, inside the operation passed to `strategy.ExecuteAsync`. Otherwise `Enqueue` throws E8 and queues nothing. `EfUnitOfWork` begins its transaction outside any strategy operation, so it cannot run under a retrying strategy. Details: [EF Core queue](docs/client-packages.md#ef-core-queue).

**Dapper or ADO.NET.**

<!-- snippet: readme-dapper-publish -->
```csharp
await using var connection = await dataSource.OpenConnectionAsync(ct);
await using var transaction = await connection.BeginTransactionAsync(ct);

// Business write first.
var updated = await connection.ExecuteAsync(new CommandDefinition(
    "UPDATE risk.rule_sets SET definition = @Definition, version = @Version WHERE id = @Id",
    ruleSet,
    transaction,
    cancellationToken: ct));

// No row changed: publish no event.
if (updated != 1)
{
    await transaction.RollbackAsync(ct);
    return;
}

var headers = OutboxHeaders.CreateBuilder(1)
    .Add("idempotency-key", eventId.ToString())
    .Build();
var changed = new RuleSetChanged(eventId, ruleSet.Id);
var payload = MessagePackSerializer.Serialize(changed, ContractlessStandardResolver.Options, ct);
var message = OutboxMessage.Create(ruleSet.Id.ToString(), payload, headers);

// Outbox row last, on the same transaction.
await publisher.PublishAsync(transaction, message, ct);
await transaction.CommitAsync(ct);
```

`PublishAsync` inserts on the transaction's connection and never commits it. Publish after the business writes. There is no autocommit publish. Details: [ADO.NET and Dapper](docs/client-packages.md#adonet-and-dapper), [TransactionScope](docs/client-packages.md#transactionscope).

### 4. Run the relay

Embedded in your own host:

<!-- snippet: readme-embed-usings -->
```csharp
using Pernix.OutboxRelay.Hosting;
using Pernix.OutboxRelay.Outbox.PostgreSql;
using Pernix.OutboxRelay.Destination.Kafka;
```

<!-- snippet: readme-embed-register -->
```csharp
builder.AddOutboxRelay()
    .AddPostgreSqlOutbox()
    .AddKafkaDestination();

var app = builder.Build();
app.MapOutboxRelayHealth();   // optional: /livez and /readyz
```

The Hosting package uses the ASP.NET Core shared framework. It configures no logging, OpenTelemetry exporters or Kestrel. Telemetry helpers, configuration and host implications: [`docs/embedding.md`](docs/embedding.md).

Or as a container, see [Run the image](#run-the-image).

## How it works

The outbox row commits with the business change or not at all. Per outbox, one relay replica holds a PostgreSQL session advisory lock and leads: it polls the table, sends each row to its destination (a Kafka topic), and deletes the row once the destination confirms it. A row whose `route` the destination does not accept is held, not sent ([Routing](docs/delivery.md#routing)). A failing row retries with backoff, then is dead-lettered, dropped or blocked, as configured. Delivery is at least once, so consumers deduplicate on the `idempotency-key` header, which the producer sets to the business event id ([Idempotency key and dedupe](docs/client-packages.md#idempotency-key-and-dedupe)).

Each outbox has one `Ordering.Mode`:

| Mode | Order |
|---|---|
| `None` | No order between rows. |
| `PerKey` | Order per `message_key`. A failing row holds back only later rows of its key. |
| `Strict` | Order across the whole outbox, one send at a time. Needs a one-partition topic. |

Delivery, failure policy, dead letters and operator actions: [`docs/delivery.md`](docs/delivery.md).

## Configuration

Image and embedded relay read the same `OutboxRelay` configuration section, from appsettings or environment variables. An excerpt of the worked example in [runbook section 2](docs/runbook.md#2-deploy):

```json
{
  "OutboxRelay": {
    "KafkaClusters": {
      "Main": {
        "BootstrapServers": "b-1.msk:9092,b-2.msk:9092"
      }
    },
    "Destinations": [
      {
        "Name": "Snapshots",
        "Transport": "Kafka",
        "Kafka": {
          "Cluster": "Main",
          "Topic": "riskbackoffice.snapshots"
        }
      }
    ],
    "Outboxes": [
      {
        "Name": "ConfigSnapshots",
        "Destination": "Snapshots",
        "Database": {
          "Provider": "PostgreSql",
          "Connection": "Main",
          "Table": "public.outbox"
        },
        "Polling": {
          "Interval": "00:00:01",
          "BatchSize": 500
        },
        "Ordering": {
          "Mode": "PerKey"
        }
      }
    ]
  }
}
```

The excerpt leaves out the connection string, retry, dead-letter and failure-policy settings. Every setting, its default and the startup checks: [`docs/configuration.md`](docs/configuration.md).

### Run the image

A minimal Dockerfile copies the config file into the image. To mount it at run time instead, use the `docker run` at the [top of this page](#pernix-outbox-relay).

```dockerfile
FROM ghcr.io/pernix-labs/pernix-outbox-relay:<version>
COPY appsettings.json /app/appsettings.Production.json
```

The connection string is a secret, so pass it as an environment variable at run time and keep it out of the image:

```shell
docker build -t my-relay .
docker run \
  -e OutboxRelay__ConnectionStrings__RiskDb="Host=risk-db;Database=risk;Username=outbox_relay;Password=<secret>" \
  my-relay
```

- **Configuration.** appsettings (mounted as `/app/appsettings.Production.json`) or the same settings as environment variables. All sources load once at startup: a change needs a restart.
- **Database.** The relay role needs `SELECT, UPDATE, DELETE` on the outbox table, plus `pg_monitor` for the `outbox.gate_lag` gauge. Connect directly to the primary endpoint: no RDS Proxy, no PgBouncer transaction pooling, because leadership uses session advisory locks. [Database prerequisites](docs/runbook.md#3-database-prerequisites).
- **Kafka.** Topics exist before the first start. The dead-letter topic is not compacted. `PLAINTEXT` only. [Kafka](docs/configuration.md#kafka).
- **Shutdown.** `HostOptions:ShutdownTimeout` at least `00:00:28` (the image default) and `terminationGracePeriodSeconds` at least `30`. [Graceful stop](docs/delivery.md#graceful-stop).
- **Health.** `GET :5612/livez` and `GET :5612/readyz`. Port 5612 is the only port. [Health](docs/configuration.md#health).
- **Metrics.** Exported over OTLP to `OTEL_EXPORTER_OTLP_ENDPOINT`. There is no scrape endpoint. [Metrics](docs/configuration.md#metrics), [alerts](docs/runbook.md#5-alerts).

Startup validates configuration, tables, grants and topics, and exits `1` on any failure (`StartupFailed`). [Startup validation](docs/configuration.md#startup-validation).

## Performance

Allocated bytes per delivered message on the relay's hot path. Median of 3 runs, batch 500, 1 header per message.

| Path | `None` | `PerKey` | `Strict` |
|---|---:|---:|---:|
| Kafka send alone (B/msg) | **0.0** | **0.0** | **0.0** |
| Delivery loop, Kafka destination (B/msg) | 201.6 | 191.6 | 248.3 |
| Delivery loop, dead-letter send (B/dlq-msg) | 895.4 | 930.3 | 992.2 |
| PostgreSQL read + commit (B/row) | 319.5 | 319.3 | 319.8 |

- **0 B per Kafka send**, with 0, 1 or 4 headers, in every ordering mode.
- **57-132 B/s** for an idle leader polling an empty outbox every second.
- **Flat heap** over 1M delivered messages and 1M rows read.

> _Measured on `main` at `ecc4861`, 2026-11-24: Apple M6, .NET 10.0.15, `postgres:17.11` in a local container, librdkafka mock cluster. These are allocation figures, not throughput. Absolute numbers vary by hardware and runtime. Every table, the method, the limits of the run and how to reproduce it: [`BENCHMARKS.md`](BENCHMARKS.md)._

## Documentation

| Page | Contents |
|---|---|
| [`docs/client-packages.md`](docs/client-packages.md) | Producer guide for the client packages. |
| [`docs/embedding.md`](docs/embedding.md) | Run the relay inside your own .NET host. |
| [`docs/configuration.md`](docs/configuration.md) | Configuration, environment variables, startup validation, PostgreSQL prerequisites, Kafka, health, metrics. |
| [`docs/delivery.md`](docs/delivery.md) | Ordering modes, write-order rule, failure policy, dead letters, operator actions. |
| [`docs/runbook.md`](docs/runbook.md) | Deploy, alerts, incidents. |
| [`BENCHMARKS.md`](BENCHMARKS.md) | What is measured, latest results, how to reproduce. |
| [`docs/product.md`](docs/product.md) | Product summary. |
| [`docs/architecture/`](docs/architecture/), [`docs/adr/`](docs/adr/) | Architecture and decisions. |

## Development

Local run, docker build, CI/CD and releasing, and measuring allocations: [`docs/development.md`](docs/development.md).

## License

[MIT](LICENSE)
