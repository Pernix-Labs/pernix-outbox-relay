# Outbox Relay configuration

## Configuration

Sources, last wins:

1. `appsettings.json` (+ `appsettings.{Environment}.json`)
2. Environment variables
3. Command line

All sources are loaded once at startup. There is no reload. A config change needs a restart.

## Environment variables

| Variable | Default | Purpose |
|---|---|---|
| `SERVICE_NAME` | `outbox-relay` | Service name. Sets OTel `service.name`. |
| `EnvironmentName` | none | Logged at startup. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | SDK default | OTLP collector endpoint. |
| `OTEL_EXPORTER_OTLP_PROTOCOL` | SDK default | OTLP protocol (`grpc` or `http/protobuf`). |
| `OTEL_SERVICE_NAME` | `SERVICE_NAME` | Overrides OTel `service.name`. |
| `OTEL_RESOURCE_ATTRIBUTES` | none | Extra OTel resource attributes. Overrides `service.name` if set there. |
| `KESTREL__ENDPOINTS__DIAGNOSTICS__URL` | `http://+:5612` | Diagnostics (health) listener. |

An unreachable OTLP collector does not affect startup.

## Outbox configuration

All relay settings live under the `OutboxRelay` section, bound once at startup from the configuration chain above. The image and a host that embeds `Pernix.OutboxRelay.Hosting` read the same section, so the settings copy across unchanged. `HostOptions`, `Kestrel` and `Logging` stay at the root.

- `OutboxRelay:ConnectionStrings`: name → connection string.
- `OutboxRelay:KafkaClusters`: name → `BootstrapServers` plus `Producer` (librdkafka producer properties).
- `OutboxRelay:Destinations[]`: where an outbox delivers. This build registers transport `Kafka` (cluster, and topic or routes).
- `OutboxRelay:Outboxes[]`: one entry per outbox table.

The relay reads connection strings only from `OutboxRelay:ConnectionStrings`. A root `ConnectionStrings` entry is never used as a fallback.

Example, the design page § Configuration example reduced to one `Kafka` destination and no `DeadLetter`. To dead-letter, add a second entry to `OutboxRelay:Destinations[]` and name it in `OutboxRelay:Outboxes[].DeadLetter.Destination`:

```json
{
  "OutboxRelay": {
    "ConnectionStrings": {
      "MainDb": "Host=orders-db;Port=5432;Database=orders;Username=outbox_relay"
    },
    "KafkaClusters": {
      "Main": {
        "BootstrapServers": "b-1.msk:9092,b-2.msk:9092",
        "Producer": {
          "security.protocol": "PLAINTEXT"
        }
      }
    },
    "Destinations": [
      {
        "Name": "OrdersTopic",
        "Transport": "Kafka",
        "Kafka": {
          "Cluster": "Main",
          "Topic": "orders.events"
        }
      }
    ],
    "Outboxes": [
      {
        "Name": "Orders",
        "Destination": "OrdersTopic",
        "Database": {
          "Provider": "PostgreSql",
          "Connection": "MainDb",
          "Table": "outbox"
        },
        "Polling": {
          "Interval": "00:00:01",
          "BatchSize": 500
        },
        "Retry": {
          "MaxRetryAttempts": 3,
          "InitialDelay": "00:00:10",
          "BackoffMultiplier": 2.0,
          "MaxDelay": "00:05:00"
        },
        "Ordering": {
          "Mode": "PerKey",
          "OnPermanentFailure": "Continue"
        }
      }
    ]
  }
}
```

| Setting | Default | Rule |
|---|---|---|
| `OutboxRelay.ConnectionStrings.<name>` | none | Must exist and be non-empty when a `Database.Connection` names it. A root `ConnectionStrings.<name>` does not satisfy it. |
| `OutboxRelay.KafkaClusters.<name>` | none | Must exist when a `Kafka.Cluster` names it. `Producer` entries pass to librdkafka unchecked by the relay. A property librdkafka rejects fails startup, see [Startup validation](#startup-validation). |
| `OutboxRelay.Destinations[].Name` | required | Unique. |
| `OutboxRelay.Destinations[].Transport` | required | A registered transport: `Kafka`. Any other value is refused. |
| `OutboxRelay.Destinations[].Kafka.Cluster` | required for `Kafka` | Names an `OutboxRelay.KafkaClusters` entry. A `Kafka` destination accepts only `Name`, `Transport` and `Kafka`. Any other block is an unknown setting. |
| `OutboxRelay.Destinations[].Kafka.Topic`, `.Kafka.Routes` | exactly one for `Kafka` | `Topic` is one topic and takes rows with no route. `Routes` maps route → topic and takes rows whose route is a key, see [Routing](delivery.md#routing). Both set or neither set is refused. An empty `Routes` object counts as not set. Every `Routes` value must be non-blank. Two routes may map to the same topic. A main send writes no route header, so consumers of a shared topic cannot tell its routes apart, and on a compacted shared topic same-key rows of different routes replace each other. Route keys are checked as described in [Startup validation](#startup-validation). |
| `OutboxRelay.Outboxes[].Name` | required | Unique. |
| `OutboxRelay.Outboxes[].Destination` | required | Names an `OutboxRelay.Destinations[]` entry. |
| `OutboxRelay.Outboxes[].Database.Provider` | required | `PostgreSql`. `SqlServer` is not registered and is refused. |
| `OutboxRelay.Outboxes[].Database.Connection` | required | Names an `OutboxRelay.ConnectionStrings` entry. |
| `OutboxRelay.Outboxes[].Database.Table` | required | Plain (`outbox`) or schema-qualified (`public.outbox`). |
| `OutboxRelay.Outboxes[].Polling.Interval` | `00:00:01` | Greater than `00:00:00`. Wait before the next read when the last read was short or the worker is at its 10 × `BatchSize` hold limit. |
| `OutboxRelay.Outboxes[].Polling.BatchSize` | `500` | At least `1`, at most `1000`. Rows per read. The worker holds at most 10 × `BatchSize` rows. |
| `OutboxRelay.Outboxes[].Retry.MaxRetryAttempts` | `3` | At least `0`. Retries after a transient message fault. `0` makes the first failure final. |
| `OutboxRelay.Outboxes[].Retry.InitialDelay` | `00:00:10` | Greater than `00:00:00`. First retry delay, first dead-letter retry delay and first pause. |
| `OutboxRelay.Outboxes[].Retry.BackoffMultiplier` | `2.0` | At least `1.0`. Delay growth per retry and per consecutive pause. |
| `OutboxRelay.Outboxes[].Retry.MaxDelay` | `00:05:00` | At least `Retry.InitialDelay`. Longest retry delay or pause, and the delay before a held row is checked again. |
| `OutboxRelay.Outboxes[].DeadLetter.Destination` | none (off) | Absent or empty = dead-lettering off. Names an `OutboxRelay.Destinations[]` entry other than the outbox's own destination. |
| `OutboxRelay.Outboxes[].Ordering.Mode` | required | `None`, `PerKey` or `Strict`. Must be in the provider's supported modes (`PostgreSql`: all three). See [Delivery (`PerKey` and `Strict`)](delivery.md#delivery-perkey-and-strict). |
| `OutboxRelay.Outboxes[].Ordering.OnPermanentFailure` | `Continue` | `Continue` or `Block`. `Block` is refused with `Mode` `None`: `OutboxRelay.Outboxes[<name>].Ordering.OnPermanentFailure: Block is not allowed when Ordering.Mode is None`. See [Block](delivery.md#block). |

A `Kafka` destination with `Routes` instead of `Topic`. Rows with route `alerts` go to `risk.alerts`, rows with route `audit` go to `risk.audit`:

```json
{ "Name": "RiskEvents", "Transport": "Kafka",
  "Kafka": { "Cluster": "Main", "Routes": { "alerts": "risk.alerts", "audit": "risk.audit" } } }
```

Durations use `hh:mm:ss`. Enum values and all names are case-insensitive. Unknown settings, bare-number durations (`"Interval": "5"`) and numeric enum values (`"Mode": "1"`) are refused at startup.

Environment variables override single settings with `__` as separator and the array index as a segment, starting with `OutboxRelay__`:

```sh
OutboxRelay__Outboxes__0__Polling__BatchSize=1000
OutboxRelay__Outboxes__0__DeadLetter__Destination=   # empty: dead-lettering off
OutboxRelay__ConnectionStrings__MainDb=<from secret>
```

The command line uses the same paths with `:`, for example `--OutboxRelay:Outboxes:0:Polling:BatchSize=1000`.

With no `OutboxRelay:Outboxes` the relay starts, logs one `NoOutboxesConfigured` warning and runs no workers.

## Refused in this build

The relay refuses to start on a configuration that names a feature this build lacks, so no setting reads as active when it is not. Each refusal is a configuration failure, collected with every other configuration failure into the one `StartupFailed` entry, exit `1`.

| Configuration | Message |
|---|---|
| `OutboxRelay.Destinations[].Transport` that names no registered transport | `OutboxRelay.Destinations[<name>].Transport: transport '<value>' is not implemented in this build` |

A `DeadLetter.Destination` that names no `OutboxRelay.Destinations[]` entry fails with `OutboxRelay.Outboxes[<name>].DeadLetter.Destination: destination '<value>' is not defined in Destinations`, and one that names the outbox's own destination with `OutboxRelay.Outboxes[<name>].DeadLetter.Destination: names the outbox's own destination '<destination>'`. A missing `Transport` is reported as `OutboxRelay.Destinations[<name>].Transport: required`. A destination without `Name` is named by its index (`OutboxRelay.Destinations[0]`).

## Startup validation

Before Kestrel listens and before any worker starts, the relay checks the configuration against the rules above, then checks every outbox table in its database, then checks every configured destination, then checks the bounds and routes each outbox's destinations declare. Any failure logs one `StartupFailed` entry and exits `1`. The entry lists the failures of the first stage that failed: every configuration failure, or every table and database failure, or every destination failure, or every bounds and routes failure, so a fix can surface the next group on the following start. The generic host also logs its own start-failure entry for the same exception. Expect `CrashLoopBackOff`.

Every failure of a relay setting starts with `OutboxRelay.`, the section it came from. A `HostOptions.ShutdownTimeout` failure is not a relay setting and has no prefix. A missing connection string reads `OutboxRelay.Outboxes[<outbox>].Database.Connection: connection string '<name>' is not defined or empty in OutboxRelay.ConnectionStrings`.

Configuration failures of a `Kafka` destination's `Topic` and `Routes`:

- `OutboxRelay.Destinations[<name>].Kafka: Topic and Routes are both set; set exactly one`
- `OutboxRelay.Destinations[<name>].Kafka: Topic or Routes is required when Transport is Kafka`
- `OutboxRelay.Destinations[<name>].Kafka.Routes.<route>: topic is required`

Database checks per outbox:

- The outbox database is reachable. An unreachable database (connect failure or timeout) is a startup failure, not a retry. A server error, such as a rejected password or a table name PostgreSQL refuses, is reported as `table '<table>' could not be validated (<SqlState>)`.
- The database is a primary. A read-only standby is refused.
- The table exists, resolved to the schema-qualified name PostgreSQL resolves for the connection's role.
- The table has columns `id`, `tx_id`, `message_key`, `payload`, `headers`, `route`, `created_at`, `retry_count`, `next_attempt_at`, `last_error`. Each missing column is named.
- Each of those columns has its V001 type and nullability, exactly as [`V001__create_outbox.sql`](../sql/postgresql/V001__create_outbox.sql) declares them. `character varying` for `text`, `json` for `jsonb`, `timestamp` for `timestamptz`, a type modifier such as `timestamptz(3)`, a domain, a nullable column that V001 declares `NOT NULL` and a `NOT NULL` `message_key`, `route` or `last_error` are all refused. Every mismatched column is named in V001 order: `table '<table>' columns differ from V001: tx_id xid8 NULL (expected xid8 NOT NULL)`.
- The connection's role has `SELECT`, `UPDATE` and `DELETE` on the table. Each missing privilege is named: `table '<table>' role lacks privileges: UPDATE`.
- No two outboxes resolve to the same table on the same database (for example `outbox` and `public.outbox`, one table through two connection names, or a connection string with and without `Database`). The database is identified by the server's own address, port and database name.
- No two outbox tables on one database share an advisory lock key. A collision names both outboxes.

Destination checks, per `OutboxRelay.Destinations[]` entry, in parallel, reported in configuration order. A `Kafka` destination runs these:

- The topic's configuration is described through the cluster (`max.message.bytes`, `cleanup.policy`, cached for the process per destination and topic). A `Routes` destination describes every distinct mapped topic in one request. Failures: `OutboxRelay.Destinations[<name>]: topic '<topic>' does not exist`, `... cannot be described (<ErrorCode>)`, `... describe timed out` (15 s). When several mapped topics fail, one message names every failing topic, joined with `; `.
- A destination named in any `OutboxRelay.Outboxes[].DeadLetter.Destination` must not be compacted: `OutboxRelay.Destinations[<name>]: topic '<topic>' is compacted and cannot hold dead letters of outbox '<outbox>'`.
- One producer is built and disposed from the merged configuration, so a property librdkafka rejects fails here: `OutboxRelay.Destinations[<name>]: invalid producer configuration for cluster '<cluster>'`. The property value is not quoted. No message is sent.

Destination bounds and routes, per outbox, for its destination and its `DeadLetter.Destination`. The relay creates each destination as a leader does for that outbox's `Ordering.Mode`, reads the send bound and the routes it declares, and disposes it. Nothing is sent and no connection is opened. A `null` send bound means unbounded and is accepted. Kafka declares a send bound of 30 s on `None` and none on `PerKey` and `Strict`. This stage refuses only a transport that declares an unusable value:

- `OutboxRelay.Outboxes[<outbox>]: destination '<destination>' declares SendBound <bound>; must be greater than zero`

Routes (`null` = one address, see [Routing](delivery.md#routing)). A `Kafka` destination declares the keys of its `Routes`, or `null` when it has `Topic`. This stage refuses:

- `OutboxRelay.Outboxes[<outbox>].DeadLetter.Destination: destination '<destination>' declares Routes. A dead-letter destination has one address.`
- `OutboxRelay.Outboxes[<outbox>]: destination '<destination>' declares an empty Routes set; Routes is null (one address) or names at least one route`
- `OutboxRelay.Outboxes[<outbox>]: destination '<destination>' declares route key '<key>'; must match ^[a-z0-9][a-z0-9._-]{0,62}$` (`route key (null)` for a null key)

Messages name setting paths, outbox, destination, connection and cluster names, and the configured table. They never contain connection string values, `Producer` values or secrets.

## PostgreSQL prerequisites

- **Direct connection only.** No RDS Proxy, no PgBouncer transaction pooling. Leadership uses session advisory locks, which do not survive them. See [ADR 0003](adr/0003-leadership-by-postgresql-session-advisory-lock.md). The leader's 1 s check proves its own backend still holds the lock, so a lock stranded on a pooled backend reads as lost.
- **Primary only.** Sessions need a read-write primary. A standby is refused at startup, and a session that lands on one is treated as an unreachable database. With a host list (`Host=a,b`) the relay forces `Target Session Attributes=read-write` and `Load Balance Hosts=false`.
- **Lock key collisions.** The lock key is `hashtext()` of the schema-qualified table name, a 32-bit hash. Two outbox tables in one database whose keys collide are refused at startup. Rename one table.
- **DDL.** [`sql/postgresql/V001__create_outbox.sql`](../sql/postgresql/V001__create_outbox.sql) creates the outbox table, its indexes and vacuum thresholds. [`sql/postgresql/V002__drop_destination_columns.sql`](../sql/postgresql/V002__drop_destination_columns.sql) drops six columns the relay never reads, and the relay accepts the table before and after it. The domain applies both scripts in order with its own migration tool. The relay never runs DDL. Index and constraint names are unique per schema, so a copy for a second outbox table in the same schema renames them together with the table.
- **Grants** (design page § Deployment): `SELECT, UPDATE, DELETE` on outbox tables, and `pg_monitor` (or `pg_read_all_stats`) for `outbox.gate_lag`. This version reads the catalog (`pg_class`, `pg_namespace`, `pg_attribute`, `pg_type`, `pg_constraint`, `pg_locks`) and statistics (`pg_stat_get_live_tuples`, `pg_partition_tree`, `pg_stat_activity`, `pg_prepared_xacts`), takes advisory locks, and runs `SELECT`, `UPDATE` and `DELETE` on outbox tables. `UPDATE` writes only `retry_count`, `next_attempt_at` and `last_error`. This release requires the `UPDATE` grant, which earlier releases did not use. A role without it is refused at startup. A role without `pg_monitor` is not refused: the relay logs `GateLagUnavailable` (event 132, Warning) once per outbox and reports no `outbox.gate_lag`. The other gauges and delivery are unaffected.
- **Domain settings.** Set `idle_in_transaction_session_timeout` for application roles, because a forgotten transaction stalls every `PerKey` and `Strict` outbox on the cluster (see [Gate](delivery.md#gate)). The relay does not set it.
- **Session settings.** The relay forces 5 s connect and command timeouts and TCP keepalives on every outbox connection (session, validation and stats). It forces `Pooling=false` on session and validation connections, and runs `SET tcp_keepalives_idle = 5`, `tcp_keepalives_interval = 2`, `tcp_keepalives_count = 3`, `tcp_user_timeout = 11000` on each session. The configured connection string cannot disable them. `ApplicationName` defaults to `outbox-relay`. The stats connection is pooled, at most 1 per outbox, with `ApplicationName=outbox-relay-stats:<outbox>`, and closed after 60 s idle, so a former leader's stats connection closes within about 70 s of losing the lock. It runs no `SET`: it takes `tcp_keepalives_idle` 5, `tcp_keepalives_interval` 2 and `tcp_keepalives_count` 3 as startup `Options`, appended to any configured `Options`.

## Kafka

- **Security.** v1 supports `PLAINTEXT` only. MSK IAM and SASL are not supported. The relay does not check `security.protocol`.
- **Client.** The send path uses `Nanov.Kafka` (raw producer), the admin client (topic describe) uses stock `Confluent.Kafka`. Both run over one bundled librdkafka 2.16.0. The version is logged at every start as `KafkaClientLoaded` (event 200).
- **One producer per outbox and destination.** No producer is shared between outboxes. It is built on the first send of a leadership term. On lock loss it is disposed without flushing, so messages still queued in the client are discarded and cannot land after the next leader's messages. On a `Routes` destination the one producer serves every mapped topic: each record goes to the topic mapped for its route.

Producer configuration, in merge order, last wins:

1. Every `OutboxRelay.KafkaClusters.<name>.Producer` entry, unchanged, except that a librdkafka alias of a forced property or of `bootstrap.servers` is renamed to the canonical key.
2. `bootstrap.servers` = `OutboxRelay.KafkaClusters.<name>.BootstrapServers`.
3. Forced per `Ordering.Mode` of the outbox. The dead-letter producer gets the same set as the outbox's own producer.

| Property | Alias rewritten to it | `None` | `PerKey` | `Strict` | Why |
|---|---|---|---|---|---|
| `enable.idempotence` | | `true` | `true` | `true` | No duplicates or reordering from client retries within one producer session. |
| `acks` | `request.required.acks` | `all` | `all` | `all` | Confirmed means written to all in-sync replicas. |
| `partitioner` | | `murmur2_random` | `murmur2_random` | `murmur2_random` | Same partition per key as Java producers. |
| `linger.ms` | `queue.buffering.max.ms` | `5` | `5` | `0` | Small batching delay. `Strict` sends one message at a time, so it waits for nothing. |
| `message.timeout.ms` | `delivery.timeout.ms` | `30000` | `0` (infinite) | `0` (infinite) | `None`: every send reports within 30 s. `PerKey`, `Strict`: a send waits out a broker outage instead of timing out and landing later, out of order. |
| `dotnet.producer.delivery.report.fields` | | `status` | `status` | `status` | No effect on the `Nanov.Kafka` raw send path. Its report is a view over the native message, and the relay reads only error, status and opaque. Kept in the forced set. |
| `dotnet.producer.enable.delivery.reports` | | `true` | `true` | `true` | The delivery report is the only delivery signal. Without it sends never complete. |

A `Producer` entry for `bootstrap.servers` or any forced property, or for one of the four aliases the relay rewrites (`metadata.broker.list` to `bootstrap.servers`, `request.required.acks` to `acks`, `queue.buffering.max.ms` to `linger.ms`, `delivery.timeout.ms` to `message.timeout.ms`), is overridden without a warning. For example `delivery.timeout.ms: 120000` still times `None` sends out at 30 s. The admin client that describes topics gets the `Producer` entries except `dotnet.producer.*` keys, with the four aliases rewritten, plus `bootstrap.servers`. The relay does not apply its forced values to the admin client.

Pre-send checks:

Both checks use the configuration of the record's own topic: the `Topic`, or on a `Routes` destination the topic mapped for the record's route.

- **Size.** The relay computes an upper bound of the encoded record (key, value, header names and values, record and batch overhead) and refuses the record when it exceeds the smaller of the topic's `max.message.bytes` and the producer's `message.max.bytes` (default `1000000`). The check ignores compression, so with `compression.type` set a record that would fit compressed is still refused.
- **Compacted topics.** A null key is refused when `cleanup.policy` contains `compact`.

Kafka client errors and librdkafka log lines go to the relay log (`KafkaClientLog`, event 201, `KafkaClientError`, event 202). The relay never logs producer configuration, keys, payloads or header values. librdkafka debug lines are logged at `Debug`, and the default `Logging:LogLevel:Default` is `Information`, so a domain that sets the `debug` producer property sees that output only after it also sets `Logging:LogLevel:Pernix.OutboxRelay.Kafka` to `Debug`. The relay does not redact that output. That choice is the domain's.

## Migrating from root-level settings

Breaking change for the image. Earlier images read relay settings from the root (`Outboxes`, `Destinations`, `KafkaClusters`, `ConnectionStrings`). This image reads them only from the `OutboxRelay` section. Move every relay setting in every configuration source: appsettings, SSM Parameter Store, Helm env values and the command line.

| Source | Before | After |
|---|---|---|
| appsettings | `Outboxes`, `Destinations`, `KafkaClusters` | `OutboxRelay:Outboxes`, `OutboxRelay:Destinations`, `OutboxRelay:KafkaClusters` |
| appsettings | `ConnectionStrings:<name>` (relay) | `OutboxRelay:ConnectionStrings:<name>` |
| env | `Outboxes__0__Polling__BatchSize` | `OutboxRelay__Outboxes__0__Polling__BatchSize` |
| env | `ConnectionStrings__Main` | `OutboxRelay__ConnectionStrings__Main` |
| SSM Parameter Store | `.../Outboxes/0/...`, `.../Destinations/...`, `.../KafkaClusters/...`, `.../ConnectionStrings/<name>` | `.../OutboxRelay/Outboxes/0/...`, `.../OutboxRelay/Destinations/...`, `.../OutboxRelay/KafkaClusters/...`, `.../OutboxRelay/ConnectionStrings/<name>` |
| Helm values env keys | `Outboxes__*`, `Destinations__*`, `KafkaClusters__*`, `ConnectionStrings__*` | `OutboxRelay__Outboxes__*`, `OutboxRelay__Destinations__*`, `OutboxRelay__KafkaClusters__*`, `OutboxRelay__ConnectionStrings__*` |
| command line | `--Outboxes:0:...` | `--OutboxRelay:Outboxes:0:...` |
| all | `HostOptions`, `Kestrel`, `Logging`, `SERVICE_*`, `OTEL_*` | unchanged |

When no source sets any `OutboxRelay` key, the image refuses root-level relay settings at startup instead of starting with zero outboxes. It logs `StartupFailed` with one line per legacy key and exits `1`:

- `<key>: relay settings moved under OutboxRelay; move to OutboxRelay:<key> (env OutboxRelay__<key>__...)` for each of `Outboxes`, `Destinations`, `KafkaClusters` present at the root.
- `ConnectionStrings.<name>: move to OutboxRelay:ConnectionStrings:<name> (env OutboxRelay__ConnectionStrings__<name>)` for each root connection string an outbox names. The line names the connection, never its value.

When any `OutboxRelay` key is set, this check does not run and root-level relay keys are ignored. Only `OutboxRelay` settings apply. Remove the root keys after the move. A root `ConnectionStrings:<name>` is never a fallback: an outbox whose connection is missing under `OutboxRelay:ConnectionStrings` fails with `OutboxRelay.Outboxes[<outbox>].Database.Connection: connection string '<name>' is not defined or empty in OutboxRelay.ConnectionStrings`.

## Health

| Endpoint | Meaning |
|---|---|
| `GET :5612/livez` | 200 while the process is up and every outbox worker loop runs and makes progress. 503 if a worker loop stops, or a worker is past its progress due time (stuck on an await), while the host is not stopping. Database and broker outages, gate stalls and delivery pauses do not affect it. A Kafka `None` send counts as stuck 30 s + 10 s after it started. A Kafka `PerKey` or `Strict` send never does, because it has no timeout. |
| `GET :5612/readyz` | 200 while the host has started, is not stopping and every outbox database is reachable. 503 while any outbox database is unreachable. A lock held by another replica does not affect it. |

## Metrics

| Metric | Meaning |
|---|---|
| `outbox.leader` | Gauge, unit `{leader}`, attribute `outbox` = outbox `Name`. `1` while this replica leads the outbox, `0` otherwise. Alert when the sum across replicas is not `1`. |
| `outbox.delivered` | Counter, unit `{message}`, attribute `outbox` = outbox `Name`. Messages the original destination confirmed. Dead letters do not count. A message sent again after a lost delete counts again. |
| `outbox.message_faults` | Counter, unit `{message}`, attributes `outbox` and `destination` (original or dead-letter destination `Name`). `+1` per message fault, malformed `headers` included. |
| `outbox.destination_faults` | Counter, unit `{message}`, attributes `outbox` and `destination`. `+1` per destination fault. Alert when it keeps rising. |
| `outbox.dead_lettered` | Counter, unit `{message}`, attributes `outbox` and `destination` (dead-letter destination `Name`). `+1` per dead letter confirmed. |
| `outbox.pending` | Gauge, unit `{message}`, attribute `outbox`. Estimate of rows not failed (`retry_count = 0`), see [`outbox.pending` error bounds](#outboxpending-error-bounds). Alert when it grows steadily. |
| `outbox.retrying` | Gauge, unit `{message}`, attribute `outbox`. Failed rows not blocked: waiting to retry, dead-letter pending and held. On `None` it also shows the read cost, see [Delivery (`Ordering.Mode` `None`)](delivery.md#delivery-orderingmode-none). |
| `outbox.blocked` | Gauge, unit `1`, attribute `outbox`. `Strict`: blocked rows. `PerKey`: blocked keys, each blocked NULL-key row counts as one. `None`: `0`. Do not sum across modes: it counts rows on `Strict` and keys on `PerKey`. Alert on any value above `0`. |
| `outbox.head_age` | Gauge, unit `s`, attribute `outbox`. Age of the first row in read order (`id` on `None`, `(tx_id, id)` on `PerKey` and `Strict`), in any state. `0` when the table is empty. Alert when it is above the outbox's latency target. See [`outbox.head_age` under long transactions](#outboxhead_age-under-long-transactions). |
| `outbox.gate_lag` | Gauge, unit `s`, attribute `outbox`. Age of the oldest transaction anywhere on the PostgreSQL cluster that holds the [gate](delivery.md#gate): running transactions with an assigned xid and prepared transactions. `0` when none. Reported for every `Ordering.Mode`. Needs `pg_monitor`, see [PostgreSQL prerequisites](#postgresql-prerequisites). Alert when it is above a few seconds while rows are pending. |

The five outbox stats gauges are reported by the leader only. Non-leader replicas report no series for the outbox. The leader reads the stats with one statement on its own stats connection, never the session, within 1 s of acquiring the lock and then every 15 s. The cadence is fixed and not configurable. A read is bounded by a 5 s deadline. A gauge reports nothing while the last read failed, while the last successful read is older than 30 s, and on a replica that does not lead. `outbox.gate_lag` also reports nothing when the role lacks `pg_monitor`. Stats failures never affect delivery, leadership, `/livez` or `/readyz`.

| Event | Name | Level | When |
|---|---|---|---|
| 130 | `StatsUnavailable` | Warning | `Outbox stats unavailable for outbox {Outbox}: {Reason}`. Once when reads start failing, at start or after a success. Reasons include the provider's reason (unreachable, deadline, server error, `read-only standby`), `stats read exceeded 10 s`, and `stats loop faulted: <exception type name>`. |
| 131 | `StatsAvailable` | Information | `Outbox stats available again for outbox {Outbox}`. First successful read after a failure. |
| 132 | `GateLagUnavailable` | Warning | `Outbox gate lag unavailable for outbox {Outbox}: {Reason}`. Once per outbox per process, when the role cannot see gate lag. Reason `role lacks pg_monitor (pg_read_all_stats)`. |

### `outbox.pending` error bounds

`outbox.pending` is an estimate from PostgreSQL live-tuple statistics minus failed rows, not a count.

- Inserts appear up to about 10 s after a burst, longer while a producer connection stays busy, so it reads low and can read `0` while rows exist.
- It drifts about 1–2% after an autovacuum or auto-analyze re-estimate. It is exact after `VACUUM` or `ANALYZE`.
- A partitioned outbox sums its leaf partitions.

Alert on sustained growth, never on one sample.

### `outbox.head_age` under long transactions

A long-running producer transaction that has written (it holds an xid and the gate) affects `outbox.head_age`:

- Its uncommitted rows are not counted. `outbox.head_age` shows the first committed row in read order only.
- On commit its rows appear with `created_at` = its start time. On `PerKey` and `Strict` they sort first, so `outbox.head_age` can jump to the transaction's age in one read.
- It keeps the relay's deleted rows from being vacuumed, so the head probe walks their dead index entries before the first live row (measured 20 ms at 200k dead entries). The relay's own read walks the same entries. A probe past the 5 s deadline fails the read (event 130, gauges absent). Delivery is not affected.

Read `outbox.head_age` together with `outbox.gate_lag`.
