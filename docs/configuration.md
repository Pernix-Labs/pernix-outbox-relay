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
- `OutboxRelay:Destinations[]`: where an outbox delivers. This build registers transports `Kafka` (cluster, and topic or routes) and `Http` (URL or routes, see [Http](#http)).
- `OutboxRelay:HttpCredentials`: name → secrets for `Http` destinations, see [`HttpCredentials`](#httpcredentials).
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
| `OutboxRelay.Destinations[].Transport` | required | A registered transport: `Kafka`, which requires `Pernix.OutboxRelay.Destination.Kafka` registered (`AddKafkaDestination`), or `Http`, which requires `Pernix.OutboxRelay.Destination.Http` registered (`AddHttpDestination`). The image registers both. Any other value is refused. |
| `OutboxRelay.Destinations[].Kafka.Cluster` | required for `Kafka` | Names an `OutboxRelay.KafkaClusters` entry. A `Kafka` destination accepts only `Name`, `Transport` and `Kafka`. Any other block is an unknown setting. |
| `OutboxRelay.Destinations[].Kafka.Topic`, `.Kafka.Routes` | exactly one for `Kafka` | `Topic` is one topic and takes rows with no route. `Routes` maps route → topic and takes rows whose route is a key, see [Routing](delivery.md#routing). Both set or neither set is refused. An empty `Routes` object counts as not set. Every `Routes` value must be non-blank. Two routes may map to the same topic. A main send writes no route header, so consumers of a shared topic cannot tell its routes apart, and on a compacted shared topic same-key rows of different routes replace each other. Route keys are checked as described in [Startup validation](#startup-validation). |
| `OutboxRelay.Outboxes[].Name` | required | Unique. |
| `OutboxRelay.Outboxes[].Destination` | required | Names an `OutboxRelay.Destinations[]` entry. |
| `OutboxRelay.Outboxes[].Database.Provider` | required | `PostgreSql`, which requires `Pernix.OutboxRelay.Outbox.PostgreSql` registered (`AddPostgreSqlOutbox`). The image registers it. `SqlServer` is not registered and is refused. |
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

At least one outbox is required. With no `OutboxRelay:Outboxes` entry, startup fails with `OutboxRelay.Outboxes: at least one outbox is required`. The rule is the same in the image and in a host that embeds the relay.

## Refused in this build

The relay refuses to start on a configuration that names a feature this build lacks, so no setting reads as active when it is not. Each refusal is a configuration failure, collected with every other configuration failure into the one `StartupFailed` entry, exit `1`.

| Configuration | Message |
|---|---|
| `OutboxRelay.Destinations[].Transport` that names no registered transport | `OutboxRelay.Destinations[<name>].Transport: transport '<value>' is not registered` |

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

Destination bounds and routes, per outbox, for its destination and its `DeadLetter.Destination`. The relay creates each destination as a leader does for that outbox's `Ordering.Mode`, reads the send bound and the routes it declares, and disposes it. Nothing is sent and no connection is opened. A `null` send bound means unbounded and is accepted. Kafka declares a send bound of 30 s on `None` and none on `PerKey` and `Strict`. Http declares its `SendTimeout` in every mode. This stage refuses only a transport that declares an unusable value:

- `OutboxRelay.Outboxes[<outbox>]: destination '<destination>' declares SendBound <bound>; must be greater than zero`

Routes (`null` = one address, see [Routing](delivery.md#routing)). A `Kafka` destination declares the keys of its `Routes`, or `null` when it has `Topic`. An `Http` destination declares the keys of its `Routes`, or `null` when it has `Url`. This stage refuses:

- `OutboxRelay.Outboxes[<outbox>].DeadLetter.Destination: destination '<destination>' declares Routes. A dead-letter destination has one address.`
- `OutboxRelay.Outboxes[<outbox>]: destination '<destination>' declares an empty Routes set; Routes is null (one address) or names at least one route`
- `OutboxRelay.Outboxes[<outbox>]: destination '<destination>' declares route key '<key>'; must match ^[a-z0-9][a-z0-9._-]{0,62}$` (`route key (null)` for a null key)

Messages name setting paths, outbox, destination, connection and cluster names, and the configured table. They never contain connection string values, `Producer` values or secrets.

## PostgreSQL prerequisites

- **Direct connection only.** No RDS Proxy, no PgBouncer transaction pooling. Leadership uses session advisory locks, which do not survive them. See [ADR 0003](adr/0003-leadership-by-postgresql-session-advisory-lock.md). The leader's 1 s check proves its own backend still holds the lock, so a lock stranded on a pooled backend reads as lost.
- **Primary only.** Sessions need a read-write primary. A standby is refused at startup, and a session that lands on one is treated as an unreachable database. With a host list (`Host=a,b`) the relay forces `Target Session Attributes=read-write` and `Load Balance Hosts=false`.
- **Lock key collisions.** The lock key is `hashtext()` of the schema-qualified table name, a 32-bit hash. Two outbox tables in one database whose keys collide are refused at startup. Rename one table.
- **DDL.** [`sql/postgresql/V001__create_outbox.sql`](../sql/postgresql/V001__create_outbox.sql) creates the outbox table, its indexes and vacuum thresholds. The domain applies it with its own migration tool. The relay never runs DDL. Index and constraint names are unique per schema, so a copy for a second outbox table in the same schema renames them together with the table.
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

Kafka client errors and librdkafka log lines go to the relay log (`KafkaClientLog`, event 201, `KafkaClientError`, event 202). The relay never logs producer configuration, keys, payloads or header values. librdkafka debug lines are logged at `Debug`, and the default `Logging:LogLevel:Default` is `Information`, so a domain that sets the `debug` producer property sees that output only after it also sets `Logging:LogLevel:Pernix.OutboxRelay.Destination.Kafka` to `Debug`. The relay does not redact that output. That choice is the domain's.

## Http

An `Http` destination sends one request per row. The body is the row's `payload` bytes, unchanged: no envelope, no re-encoding. The relay owns one `SocketsHttpHandler` per outbox and destination, built on the first send of a leadership term and disposed with the term, like the Kafka producer. Redirects are not followed, cookies are not kept and responses are not decompressed. The response body is drained up to 64 KB for connection reuse and is never read, logged or stored.

### Http settings

Every key sits under `OutboxRelay.Destinations[].Http`. An `Http` destination accepts only `Name`, `Transport` and `Http`. Unknown keys are refused. Enum values (`Method`, `Mode`, `Scheme`, `Encoding`, `ClientCredentialsIn`) are case-insensitive.

| Setting | Default | Rule |
|---|---|---|
| `Url` | none | Exactly one of `Url` and `Routes`. Absolute `http` or `https` URL. Scheme, host and port are literal. Placeholders only in path and query. No user info, no fragment. Literal path and query must be canonical: percent-encode reserved and non-ASCII characters, no dot segments. |
| `Routes.<route>` | none | Route → `{ "Url", "Method", "ContentType", "Headers" }`. Non-empty. Route keys match `^[a-z0-9][a-z0-9._-]{0,62}$`. `Url` is required, the other three override the destination value. Route `Headers` merge with destination `Headers`, the route entry wins on the same name. See [Routing](delivery.md#routing). |
| `Method` | `POST` | `POST`, `PUT` or `PATCH`. |
| `ContentType` | `application/octet-stream` | A media type. A forwarded row `content-type` header wins over it. |
| `Headers.<name>` | none | Request headers. Name is an RFC 9110 token. Value takes the [placeholders](#placeholders) without URL escaping. A value without placeholders is built once at startup. Refused: `Content-Type` (use `ContentType`), `Authorization` (use `Authentication`), the hop-by-hop and framing headers listed under [Header forwarding](#header-forwarding), and names the relay sets itself (the `Authentication` header, `Signing` headers, dead-letter headers). |
| `ForwardHeaders.Mode` | `All` | `All`, `None` or `Listed`. Which row headers become request headers. |
| `ForwardHeaders.Names` | none | Required when `Mode` is `Listed`, refused otherwise. RFC 9110 tokens. |
| `ForwardHeaders.Exclude` | none | Row headers never forwarded. RFC 9110 tokens. |
| `ForwardHeaders.NamePrefix` | none | Prepended to each forwarded header name, for example `x-outbox-`. RFC 9110 token characters. |
| `StatusCodes.Delivered` | `["2xx"]` | Codes that mean delivered. Entries are a code (`"409"`), a class (`"2xx"`) or a range (`"500-504"`), within 100-599. |
| `StatusCodes.Rejected` | `["400", "413", "414", "415", "422"]` | Permanent message fault: dead letter, drop or block per the outbox's policy. |
| `StatusCodes.Retried` | `[]` | Transient message fault: spends one retry. |
| `StatusCodes.Paused` | every other code | Destination fault: the outbox pauses with backoff, no retry spent. |
| `SendTimeout` | `00:00:30` | Greater than `00:00:00`. Covers the whole send: slot wait, token fetch, request, response headers. It is the destination's send bound in every ordering mode. |
| `ConnectTimeout` | `00:00:10` | Greater than `00:00:00` and at most `SendTimeout`. |
| `MaxRequestsInFlight` | `64` | At least `1`. Requests in flight per outbox and destination. A send waits for a free slot within `SendTimeout`, then fails with `timeout waiting for a request slot`. |
| `HttpVersion` | `"1.1"` | `"1.1"`, `"2"` or `"2-cleartext"`. See [`HttpVersion`](#httpversion). |
| `Authentication.Scheme` | `None` | `None`, `Basic`, `Bearer`, `ApiKey` or `OAuth2ClientCredentials`. Combines with `Tls.ClientCertificate`. |
| `Authentication.Credential` | none | Required for every scheme but `None`. Names an [`HttpCredentials`](#httpcredentials) entry that has the scheme's fields. |
| `Authentication.HeaderName` | none | `ApiKey` only, required. RFC 9110 token, not a reserved or content header. |
| `Authentication.ValuePrefix` | none | `ApiKey` only. Prepended to the key. |
| `Authentication.TokenUrl` | none | `OAuth2ClientCredentials` only, required. Absolute `https` URL, `http` only for a loopback host. Always sent over HTTP/1.1. |
| `Authentication.Scope`, `.Audience` | none | `OAuth2ClientCredentials` only. Sent as `scope` and `audience` form fields. |
| `Authentication.ClientCredentialsIn` | `Header` | `OAuth2ClientCredentials` only. `Header` (`Authorization: Basic`) or `Body` (`client_id`, `client_secret` form fields). |
| `Signing.Scheme` | none | Required when `Signing` is set. `StandardWebhooks` or `HmacSha256`. |
| `Signing.Credential` | none | Required. Names an `HttpCredentials` entry with `SigningSecret`. |
| `Signing.HeaderName` | none | `HmacSha256` only, required. RFC 9110 token, not a reserved or content header, not the `Authentication` header. |
| `Signing.Encoding` | `Hex` | `HmacSha256` only. `Hex` or `Base64`. |
| `Signing.ValuePrefix` | none | `HmacSha256` only. Prepended to the signature, for example `sha256=`. |
| `Signing.WebhookId` | `{outbox}-{id}` | `StandardWebhooks`. The `webhook-id` value. Takes the placeholders. |
| `Tls.CaCertificatePath` | none | PEM bundle of private CAs. The server chain must end in one of them. No setting disables certificate validation. |
| `Tls.ClientCertificate.CertificatePath`, `.KeyPath` | none | PEM client certificate and key, both required together. |
| `Tls.ClientCertificate.PfxPath` | none | PFX client certificate, instead of the PEM pair. |
| `Tls.ClientCertificate.Credential` | none | `PfxPath` only. Names an `HttpCredentials` entry with `CertificatePassword`. |
| `HealthProbe.Url` | none | Required when `HealthProbe` is set. Literal URL, no placeholders. Sent once at startup. |
| `HealthProbe.Method` | `HEAD` | `HEAD` or `GET`. |
| `HealthProbe.Expected` | `["2xx"]` | Status entries that pass, same format as `StatusCodes`. |
| `RequestMapper` | none | Name of an `IHttpRequestMapper` the host registered. Embedded hosts only, see [Hooks](embedding.md#http-hooks). The image registers none. |
| `ResponseClassifier` | none | Name of an `IHttpResponseClassifier` the host registered. Replaces `StatusCodes`. Embedded hosts only. |

A listed `StatusCodes` class replaces that class's default set. For example `"Rejected": ["400"]` makes `413`, `414`, `415` and `422` paused. A code in two classes is refused.

Array settings take an index in environment variables and on the command line:

```sh
OutboxRelay__Destinations__1__Http__StatusCodes__Retried__0=500
OutboxRelay__Destinations__1__Http__ForwardHeaders__Names__0=traceparent
```

### Placeholders

`Url`, `Routes.<route>.Url`, `Headers` values and `Signing.WebhookId` take a fixed set of placeholders. There are no expressions, defaults or unescaped variants.

| Placeholder | Value | Bad row data |
|---|---|---|
| `{outbox}`, `{destination}` | Configured names. Folded in at startup. | |
| `{route}` | The row's route. `Routes` destinations only. | |
| `{id}` | Row id digits. | |
| `{key}` | Key as UTF-8. | Null or invalid UTF-8 key: permanent message fault. |
| `{key:hex}`, `{key:base64url}` | Key bytes encoded. | Null key: permanent message fault. |
| `{header:<name>}` | Row header value. | Missing header: permanent message fault. |

In a URL every rendered value is percent-encoded (RFC 3986 unreserved characters kept). A rendered value that forms a whole path segment of exactly `.` or `..` is a permanent message fault. In a header value no escaping applies, and a CR, LF or NUL is a permanent message fault. Fault text names the placeholder or header, never the value.

### `HttpVersion`

| Value | Protocol | URL scheme |
|---|---|---|
| `"1.1"` | HTTP/1.1 keep-alive. | `http` or `https` |
| `"2"` | HTTP/2 negotiated by ALPN. A server without h2 fails the request as a destination fault. No downgrade. | `https` only |
| `"2-cleartext"` | HTTP/2 with prior knowledge (h2c), no upgrade. For sidecars and meshes. | `http` only |

The rule applies to every URL of the destination: `Url`, each `Routes` entry and `HealthProbe.Url`. `"2"` with an `http` URL fails startup with `Http.HttpVersion: "2" requires https URLs; use "2-cleartext" for prior-knowledge h2c`, so a scheme typo never turns into cleartext HTTP/2. `TokenUrl` always uses HTTP/1.1. Under HTTP/2 the relay opens another connection when `MaxRequestsInFlight` exceeds the server's stream limit.

### Header forwarding

Request headers come from three sources. On an equal name (case-insensitive) the later source wins:

1. Forwarded row headers, per `ForwardHeaders`.
2. `Headers`.
3. Relay-owned headers: `Authentication`, `Signing`, `Content-Type`, `Content-Length`, dead-letter headers.

Never forwarded: `Connection`, `Keep-Alive`, `Proxy-Connection`, `Transfer-Encoding`, `TE`, `Trailer`, `Upgrade`, `Host`, `Content-Length`, `Expect`, `Authorization`, `Proxy-Authorization`, `Cookie`. A forwarded `content-type` sets the request `Content-Type`. Forwarded `Allow`, `Content-Disposition`, `Content-Encoding`, `Content-Language`, `Content-Location`, `Content-MD5`, `Content-Range`, `Expires` and `Last-Modified` go on the content headers. A row header whose name is not an RFC 9110 token, whose value has CR, LF or NUL, or that differs from another row header only by case is a permanent message fault. It is never dropped silently. Request header values are sent as UTF-8. A row `traceparent` reaches the endpoint unchanged: the relay adds no trace headers of its own.

`All` is the default, so every row header reaches the endpoint. For an endpoint outside your trust boundary, set `Mode` `Listed` with the headers its contract needs, or list internal headers in `Exclude`.

### `HttpCredentials`

`OutboxRelay:HttpCredentials:<name>` holds secrets apart from destinations, so they survive reordering of `Destinations[]`. Unknown fields are refused. Startup failures name the path, never a value.

| Field | Used by |
|---|---|
| `Username`, `Password` | `Authentication.Scheme` `Basic` |
| `Token` | `Bearer` |
| `ApiKey` | `ApiKey` |
| `ClientId`, `ClientSecret` | `OAuth2ClientCredentials` |
| `SigningSecret` | `Signing`. `StandardWebhooks`: `whsec_` + base64. `HmacSha256`: raw UTF-8. |
| `CertificatePassword` | `Tls.ClientCertificate.Credential` (PFX) |

Set secrets as environment variables or from a secret store, never in a committed file:

```sh
OutboxRelay__HttpCredentials__<name>__<field>=<secret>
OutboxRelay__HttpCredentials__Ledger__ApiKey=<from secret>
OutboxRelay__HttpCredentials__Billing__ClientSecret=<from secret>
```

OAuth2 tokens are fetched with `grant_type=client_credentials`, cached until `expires_in` minus 60 s, refreshed by one caller at a time, and shared by every outbox that uses the destination. A token response without `expires_in` is cached until the endpoint answers `401`. A `401` drops the cached token and pauses the outbox. The next send fetches a new one.

### Examples

Minimal. One URL, `POST`, raw payload, every row header forwarded:

```json
{ "Name": "OrdersHook", "Transport": "Http",
  "Http": { "Url": "https://orders-sink.internal.example/events" } }
```

Routed, OAuth2 and CloudEvents binary mode. The producer writes `ce-type` and `content-type` row headers. `Headers` adds the constant attributes and the event id:

```json
{
  "OutboxRelay": {
    "HttpCredentials": {
      "Billing": { "ClientId": "outbox-relay", "ClientSecret": "<from secret>" }
    },
    "Destinations": [
      {
        "Name": "BillingEvents",
        "Transport": "Http",
        "Http": {
          "Routes": {
            "invoices": { "Url": "https://billing.example.com/events/invoices" },
            "payments": { "Url": "https://billing.example.com/events/payments" }
          },
          "Headers": {
            "ce-id": "{outbox}-{id}",
            "ce-source": "/orders",
            "ce-specversion": "1.0"
          },
          "ForwardHeaders": { "Mode": "Listed", "Names": ["ce-type", "content-type", "traceparent"] },
          "Authentication": {
            "Scheme": "OAuth2ClientCredentials",
            "Credential": "Billing",
            "TokenUrl": "https://auth.example.com/oauth2/token",
            "Scope": "events.write"
          }
        }
      }
    ]
  }
}
```

Fully custom. Templated URL, `PUT`, own status contract, HTTP/2, API key, Standard Webhooks signing, private CA with mTLS, health probe and a host request mapper:

```json
{
  "Name": "LedgerSink",
  "Transport": "Http",
  "Http": {
    "Url": "https://ledger.internal.example/accounts/{key:base64url}/entries?source={outbox}",
    "Method": "PUT",
    "ContentType": "application/vnd.ledger.entry+json",
    "Headers": { "Idempotency-Key": "{outbox}-{id}", "X-Tenant": "{header:tenant-id}" },
    "ForwardHeaders": { "Mode": "Listed", "Names": ["traceparent", "tenant-id"], "NamePrefix": "x-outbox-" },
    "StatusCodes": { "Delivered": ["2xx", "409"], "Rejected": ["400", "422"], "Retried": ["423"] },
    "SendTimeout": "00:00:10",
    "ConnectTimeout": "00:00:03",
    "MaxRequestsInFlight": 16,
    "HttpVersion": "2",
    "Authentication": { "Scheme": "ApiKey", "HeaderName": "X-Api-Key", "Credential": "Ledger" },
    "Signing": { "Scheme": "StandardWebhooks", "Credential": "LedgerSigning" },
    "Tls": {
      "CaCertificatePath": "/etc/relay/tls/internal-ca.pem",
      "ClientCertificate": { "CertificatePath": "/etc/relay/tls/client.crt", "KeyPath": "/etc/relay/tls/client.key" }
    },
    "HealthProbe": { "Url": "https://ledger.internal.example/healthz", "Method": "GET" },
    "RequestMapper": "ledger-envelope"
  }
}
```

`RequestMapper` fails startup in the image, which registers no hook. Drop it there.

### CloudEvents binary mode

There is no CloudEvents mode. Binary mode is plain header forwarding:

1. The producer writes the payload as the event data and sets row headers `ce-type` and `content-type` (and any other `ce-*` attribute that varies per event).
2. `Headers` sets the constant attributes, `"ce-source": "/orders"` and `"ce-specversion": "1.0"`, and `"ce-id": "{outbox}-{id}"`.
3. Default forwarding (`All`) or a `Listed` set that names the `ce-*` and `content-type` headers carries the rest.

The request is a conformant CloudEvents HTTP binary-mode request. Structured mode (a JSON envelope) is not built in. An embedded host can build it with a request mapper.

### Delivery behavior

- **Status classes.** `Delivered` deletes the row. `Rejected` is a permanent message fault. `Retried` spends one retry. `Paused` is a destination fault: the outbox pauses with backoff and spends no retry. Fault text is `HTTP 503`, or `route 'alerts': HTTP 503` on a `Routes` destination. It never holds the URL, key, header values or response text.
- **Transport failures pause.** Connect, DNS, TLS, reset and `SendTimeout` failures are destination faults, for example `HttpRequestException: ConnectionError` or `timeout after 00:00:30`. A send never throws for a delivery failure.
- **`Retry-After` is ignored.** The outbox pause backoff applies (`Retry.InitialDelay`, growing by `Retry.BackoffMultiplier` to `Retry.MaxDelay`).
- **`500` pauses by default, and a poison row stalls the outbox.** A row the endpoint always answers with `500` pauses the outbox again on every attempt, and no later row is delivered. Alert on a steadily rising `outbox.destination_faults`. When the endpoint's contract says `500` means a bad message, move it to `Retried` (`"Retried": ["500"]`), so the row spends retries and then dead-letters, drops or blocks.
- **One dead host pauses all routes.** A destination fault on any route pauses the whole outbox, so a `Routes` destination whose routes point at different hosts stalls every route when one host is down. Use one destination, and one outbox, per host when the hosts fail independently.
- **Order across timeouts.** On `PerKey` and `Strict` a request that timed out may still reach the endpoint after its resend, so the endpoint can see A, B, then the late A. HTTP has no unbounded send that would prevent it. Send an idempotency key (for example `"Idempotency-Key": "{outbox}-{id}"`) and deduplicate on it. An endpoint that needs strict order must check its own sequence in the payload. The relay provides none.
- **Dead letters.** An `Http` destination can be a dead-letter destination when it has a single `Url`. It sends the same body with `x-dead-letter-reason`, `x-retry-attempts`, `x-original-destination`, `x-original-outbox`, `x-original-id`, `x-failed-at` and `x-original-route`, the same names and values as Kafka dead letters, replacing producer headers with those names. `Http` and `Kafka` dead-letter into each other. See [Dead letters](delivery.md#dead-letters).
- **Liveness.** A send counts as stuck `SendTimeout` + 10 s after it started, in every ordering mode.

### Proxy

The relay uses a proxy only when `HTTPS_PROXY`, `HTTP_PROXY` or `ALL_PROXY` is set and non-empty, in exact upper or lower case (`https_proxy`). `NO_PROXY` then applies as .NET reads it. With none of them set, no proxy is used and the system proxy settings are not consulted (that lookup cost about 539 B per request, measured on macOS).

### Startup checks

The configuration stage checks every rule above, with no I/O. The destination stage then runs, per `Http` destination:

- With `OAuth2ClientCredentials`: one token fetch.
- With `HealthProbe`: one probe request, which must answer a status in `Expected`.

No request goes to `Url` or a `Routes` URL. Failures name the destination and host only: `OutboxRelay.Destinations[<name>]: ...`.

### Logs and telemetry

| Event | Name | Level | When |
|---|---|---|---|
| 300 | `HttpClientCreated` | Debug | Handler built: destination, host, max connections. |
| 301 | `HttpTokenRefreshFailed` | Warning | OAuth2 token fetch failed: destination, token host, status code or exception type. |
| 302 | `HttpProbeFailed` | Warning | Startup probe failed: destination, host, method, status code or exception type. |

The relay never logs a URL path or query, header values, body, response body or credentials. Delivery outcomes use the Core metrics and events, see [Metrics](#metrics).

Request duration comes from .NET's own `System.Net.Http` meter (`http.client.request.duration`), not a relay instrument. Add it in an embedded host:

```csharp
builder.Services.AddOpenTelemetry()
    .WithMetrics(m => m.AddOutboxRelayInstrumentation().AddMeter("System.Net.Http"));
```

OpenTelemetry HttpClient tracing instrumentation records `url.full` on each span. A templated URL then puts keys and header values in your traces. Leave that instrumentation off, or redact `url.full`, when URLs carry row data.

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

The image ignores root-level relay settings. An image whose outboxes are still at the root finds no `OutboxRelay:Outboxes` entry, logs `StartupFailed` with `OutboxRelay.Outboxes: at least one outbox is required` and exits `1`. The relay does not check root keys one by one, so the failure does not name the keys to move. Use the table above. Root keys left behind after the move are ignored. Remove them from every source (appsettings, SSM Parameter Store paths, Helm env values and the command line) so no reader mistakes them for live settings. A root `ConnectionStrings:<name>` is never a fallback: an outbox whose connection is missing under `OutboxRelay:ConnectionStrings` fails with `OutboxRelay.Outboxes[<outbox>].Database.Connection: connection string '<name>' is not defined or empty in OutboxRelay.ConnectionStrings`.

## Health

| Endpoint | Meaning |
|---|---|
| `GET :5612/livez` | 200 while the process is up and every outbox worker loop runs and makes progress. 503 if a worker loop stops, or a worker is past its progress due time (stuck on an await), while the host is not stopping. Database and broker outages, gate stalls and delivery pauses do not affect it. A Kafka `None` send counts as stuck 30 s + 10 s after it started. A Kafka `PerKey` or `Strict` send never does, because it has no timeout. An Http send counts as stuck `SendTimeout` + 10 s after it started, in every mode. |
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
