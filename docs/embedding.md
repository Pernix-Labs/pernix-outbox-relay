# Embedding the Outbox Relay

The relay ships as a container image and as NuGet packages. The packages run the relay as hosted services inside your own .NET host. The image is a thin app over the same packages, so startup validation, delivery, leadership, health checks and metrics are the same in both.

## 1. Install

```shell
dotnet add package Pernix.OutboxRelay.Hosting
dotnet add package Pernix.OutboxRelay.Outbox.PostgreSql
dotnet add package Pernix.OutboxRelay.Destination.Kafka
```

| Package | Reference when |
|---|---|
| `Pernix.OutboxRelay.Hosting` | Always. Relay services, health checks, telemetry helpers. Brings `Pernix.OutboxRelay.Core`. |
| `Pernix.OutboxRelay.Outbox.PostgreSql` | An outbox uses `Database.Provider` `PostgreSql`. |
| `Pernix.OutboxRelay.Destination.Kafka` | A destination uses `Transport` `Kafka`. |

`Pernix.OutboxRelay.Hosting` targets `net10.0` and needs the ASP.NET Core shared framework (`Microsoft.AspNetCore.App`). It declares that framework reference itself, so a project on `Microsoft.NET.Sdk` gets it on restore. It references no outbox provider and no destination. Use the same version for every `Pernix.OutboxRelay.*` relay package. Relay packages and image share one version per `relay/vX.Y.Z` tag ([ADR 0013](adr/0013-adopt-pernix-ci-and-tag-only-versions.md)).

Only `Pernix.OutboxRelay.Destination.Kafka` brings the native `librdkafka` assets. It pins `Nanov.Kafka` `[2.16.0.3]` (with `Confluent.Kafka` 2.16.0), so a host that references it together with other `Confluent.Kafka` or `Nanov.Kafka` versions gets NU1605 or NU1107 restore errors until it aligns its versions. A host without the Kafka destination carries neither.

## 2. Register

```csharp
using Pernix.OutboxRelay.Hosting;
using Pernix.OutboxRelay.Outbox.PostgreSql;
using Pernix.OutboxRelay.Destination.Kafka;

var builder = WebApplication.CreateBuilder(args);

builder.AddOutboxRelay().AddPostgreSqlOutbox().AddKafkaDestination();
```

The chain exists on `IHostApplicationBuilder` and on `IServiceCollection` (`builder.Services.AddOutboxRelay().AddPostgreSqlOutbox().AddKafkaDestination()`), so a generic host without ASP.NET Core endpoints works too. Each call returns the builder or service collection it was called on.

`AddOutboxRelay` registers the relay services, the hosted services (startup validator, outbox workers, stats, `outbox.leader` gauge) and three health checks:

| Check | Tag |
|---|---|
| `host-lifetime` | `ready` |
| `outbox-database` | `ready` |
| `worker-loop` | `live` |

`AddOutboxRelay()` alone registers no outbox provider and no destination. Register each one the configuration names:

- `AddPostgreSqlOutbox()` registers the PostgreSQL outbox provider (`Database.Provider` `PostgreSql`).
- `AddKafkaDestination()` registers the Kafka destination (`Transport` `Kafka`) and binds and validates `OutboxRelay:KafkaClusters`.

A configured provider or transport that is not registered fails startup in the configuration stage, before any database or broker call:

- `OutboxRelay.Outboxes[<name>].Database.Provider: provider '<value>' is not registered`
- `OutboxRelay.Destinations[<name>].Transport: transport '<value>' is not registered`

Call `AddOutboxRelay` once per service collection. A second call throws `InvalidOperationException` and registers nothing. A repeat `AddPostgreSqlOutbox()` or `AddKafkaDestination()` call is a no-op.

## 3. Map health endpoints (opt-in)

```csharp
var app = builder.Build();

app.MapOutboxRelayHealth();
```

`MapOutboxRelayHealth` maps `GET /livez` (checks tagged `live`) and `GET /readyz` (checks tagged `ready`) with the same behavior as the image. Meaning of each status: [Health](configuration.md#health).

Without this call the package maps no endpoint. The relay checks still run under the host's own health endpoints, for example `app.MapHealthChecks("/health")`, because they are registered with the host's `AddHealthChecks()` builder. Filter on the `live` and `ready` tags to include or exclude them.

## 4. Add relay telemetry (opt-in)

```csharp
using OpenTelemetry.Metrics;
using OpenTelemetry.Trace;

builder.Services.AddOpenTelemetry()
    .WithTracing(t => t.AddOutboxRelayInstrumentation())
    .WithMetrics(m => m.AddOutboxRelayInstrumentation());
```

`AddOutboxRelayInstrumentation` adds the relay activity source or meter (`RelayDiagnostics.Name`) and nothing else. Exporters, resource and other instrumentation stay your choice. With the meter added, every relay instrument reports as in the image, see [Metrics](configuration.md#metrics). `AddOpenTelemetry()` comes from your own `OpenTelemetry.Extensions.Hosting` reference. The relay package depends only on `OpenTelemetry.Api`.

## 5. Configure

All relay settings live under the `OutboxRelay` section. The section name is fixed. Connection strings are read from `OutboxRelay:ConnectionStrings`, never from the root `ConnectionStrings`, so the relay does not collide with your app's own settings.

```json
{
  "OutboxRelay": {
    "ConnectionStrings": {
      "MainDb": "Host=orders-db;Port=5432;Database=orders;Username=outbox_relay"
    },
    "KafkaClusters": {
      "Main": { "BootstrapServers": "b-1.msk:9092,b-2.msk:9092" }
    },
    "Destinations": [
      { "Name": "OrdersTopic", "Transport": "Kafka", "Kafka": { "Cluster": "Main", "Topic": "orders.events" } }
    ],
    "Outboxes": [
      {
        "Name": "Orders",
        "Destination": "OrdersTopic",
        "Database": { "Provider": "PostgreSql", "Connection": "MainDb", "Table": "outbox" },
        "Ordering": { "Mode": "PerKey" }
      }
    ]
  }
}
```

Environment variables override single settings with the `OutboxRelay__` prefix, for example `OutboxRelay__ConnectionStrings__MainDb` or `OutboxRelay__Outboxes__0__Polling__BatchSize=1000`. Every setting, default and rule: [Outbox configuration](configuration.md#outbox-configuration). Database grants and Kafka requirements are the same as for the image: [PostgreSQL prerequisites](configuration.md#postgresql-prerequisites), [Kafka](configuration.md#kafka).

## 6. What the package does not touch

The package does not add, remove or configure:

- logging providers or log filters,
- OpenTelemetry exporters, resource or service name,
- Kestrel endpoints, URLs or ports,
- HTTP endpoints, unless you call `MapOutboxRelayHealth`,
- `HostOptions`.

## 7. Host implications

- **Shutdown timeout.** `HostOptions.ShutdownTimeout` must be at least `00:00:28` to cover the relay's 27 s stop budget. The .NET default of 30 s passes. A lower value fails startup with `HostOptions.ShutdownTimeout: must be at least 00:00:28 to cover the 27 s stop budget`. The relay reads the host's root `HostOptions`. Stop budget: [Graceful stop](delivery.md#graceful-stop).
- **Startup failure aborts the host.** The relay validates configuration, tables, grants and topics before any worker runs and before Kestrel listens. Any failure throws `RelayStartupValidationException` from host start, so your whole app does not start. Messages name the setting path with the `OutboxRelay.` prefix: [Startup validation](configuration.md#startup-validation).
- **Shared process resources.** The relay shares the thread pool, memory and process with your app. With `Pernix.OutboxRelay.Destination.Kafka` registered it loads native `librdkafka` into the process. Each outbox holds its own database connections, including one session for its leadership advisory lock. The `outbox.leader` gauge is process-wide and reports every outbox of every relay host in the process.
- **Runtime.** `net10.0` with the ASP.NET Core shared framework only.
- **Leadership.** Unchanged by embedding. Per outbox one replica leads across all replicas of your service, whether they run the image or the package. Other replicas stay standby.
- **One registration.** `AddOutboxRelay` once per service collection. It throws `InvalidOperationException` on a second call.
- **Health.** Relay checks appear in the host's own health endpoints, see [section 3](#3-map-health-endpoints-opt-in).
