# Outbox Relay runbook

Deploying and running the relay with a PostgreSQL outbox and a Kafka destination. Settings, defaults, log events and metric meanings are in [`configuration.md`](configuration.md) and [`delivery.md`](delivery.md). This runbook links to them and repeats only the alert table.

## 1. Scope

- One relay deployment per domain, 2 replicas. One replica leads each outbox and delivers. The other takes over when the leader stops or loses its PostgreSQL session.
- Outbox: a PostgreSQL table created from [`sql/postgresql/V001__create_outbox.sql`](../sql/postgresql/V001__create_outbox.sql), in `Ordering.Mode` `PerKey`.
- Destinations: one Kafka topic, plus a second Kafka topic for dead letters.
- Delivery is at least once. Consumers deduplicate (section 7).
- Reference: [Outbox configuration](configuration.md#outbox-configuration), [Startup validation](configuration.md#startup-validation), [PostgreSQL prerequisites](configuration.md#postgresql-prerequisites), [Graceful stop](delivery.md#graceful-stop), [Operator actions](delivery.md#operator-actions), [Health](configuration.md#health), [Metrics](configuration.md#metrics).

## 2. Deploy

- Image: `ghcr.io/pernix-labs/pernix-outbox-relay:<version>`.
- Set env `SERVICE_VERSION=<version>` in the deployment. Without it, telemetry `service.version` is `0.0.0-ci.g<sha7>`, the CI build version baked into the image.
- `SERVICE_COMMIT` comes from the image and needs no setting.

### 2a. appsettings

One `PerKey` outbox on table `public.outbox`, delivering to Kafka destination `Snapshots`, with dead letters to Kafka destination `SnapshotsDlq`. Relay settings sit under the `OutboxRelay` section. The file is [`docs/runbook-appsettings.example.json`](runbook-appsettings.example.json). `RunbookExampleTests` binds and validates it, and checks that this section embeds it verbatim.

```json
{
  "OutboxRelay": {
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
        "Name": "Snapshots",
        "Transport": "Kafka",
        "Kafka": {
          "Cluster": "Main",
          "Topic": "riskbackoffice.snapshots"
        }
      },
      {
        "Name": "SnapshotsDlq",
        "Transport": "Kafka",
        "Kafka": {
          "Cluster": "Main",
          "Topic": "riskbackoffice.snapshots.dlq"
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
        "Retry": {
          "MaxRetryAttempts": 3,
          "InitialDelay": "00:00:10",
          "BackoffMultiplier": 2.0,
          "MaxDelay": "00:05:00"
        },
        "DeadLetter": {
          "Destination": "SnapshotsDlq"
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

- `OutboxRelay:ConnectionStrings:Main` is not in the file. It holds a password, so it comes from a Kubernetes secret as env var `OutboxRelay__ConnectionStrings__Main`. A root `ConnectionStrings__Main` is not read. Startup refuses an empty or missing value.
- The image ships `appsettings.json` with the diagnostics listener and `HostOptions:ShutdownTimeout` `00:00:28`. `HostOptions`, `Kestrel` and `Logging` stay at the root. Mount the domain settings as `/app/appsettings.Production.json` (the ASP.NET Core default environment is `Production`), or set them as env vars (2b).
- The topics must exist before the first start. `SnapshotsDlq` must not be compacted. Startup describes both and refuses a missing topic or a compacted dead-letter topic.
- `Ordering.OnPermanentFailure` `Continue` dead-letters a permanently failed row and moves on. `Block` holds its key until an operator acts (section 6c).

### 2b. The same settings as environment variables

```sh
OutboxRelay__ConnectionStrings__Main=<from secret>
OutboxRelay__KafkaClusters__Main__BootstrapServers=b-1.msk:9092,b-2.msk:9092
OutboxRelay__KafkaClusters__Main__Producer__security.protocol=PLAINTEXT
OutboxRelay__Destinations__0__Name=Snapshots
OutboxRelay__Destinations__0__Transport=Kafka
OutboxRelay__Destinations__0__Kafka__Cluster=Main
OutboxRelay__Destinations__0__Kafka__Topic=riskbackoffice.snapshots
OutboxRelay__Destinations__1__Name=SnapshotsDlq
OutboxRelay__Destinations__1__Transport=Kafka
OutboxRelay__Destinations__1__Kafka__Cluster=Main
OutboxRelay__Destinations__1__Kafka__Topic=riskbackoffice.snapshots.dlq
OutboxRelay__Outboxes__0__Name=ConfigSnapshots
OutboxRelay__Outboxes__0__Destination=Snapshots
OutboxRelay__Outboxes__0__Database__Provider=PostgreSql
OutboxRelay__Outboxes__0__Database__Connection=Main
OutboxRelay__Outboxes__0__Database__Table=public.outbox
OutboxRelay__Outboxes__0__Polling__Interval=00:00:01
OutboxRelay__Outboxes__0__Polling__BatchSize=500
OutboxRelay__Outboxes__0__Retry__MaxRetryAttempts=3
OutboxRelay__Outboxes__0__Retry__InitialDelay=00:00:10
OutboxRelay__Outboxes__0__Retry__BackoffMultiplier=2.0
OutboxRelay__Outboxes__0__Retry__MaxDelay=00:05:00
OutboxRelay__Outboxes__0__DeadLetter__Destination=SnapshotsDlq
OutboxRelay__Outboxes__0__Ordering__Mode=PerKey
OutboxRelay__Outboxes__0__Ordering__OnPermanentFailure=Continue
```

The librdkafka key `security.protocol` keeps its dot in the variable name. Durations use `hh:mm:ss`. A bare number (`"Interval": "5"`) is refused.

### 2c. Upgrading from root-level settings

This image reads relay settings only under `OutboxRelay`. Before rolling it out, move every relay key in each source the deployment uses ([Migrating from root-level settings](configuration.md#migrating-from-root-level-settings) has the full mapping):

- **SSM Parameter Store.** Move parameters `.../Outboxes/...`, `.../Destinations/...`, `.../KafkaClusters/...` and `.../ConnectionStrings/<name>` to `.../OutboxRelay/Outboxes/...`, `.../OutboxRelay/Destinations/...`, `.../OutboxRelay/KafkaClusters/...` and `.../OutboxRelay/ConnectionStrings/<name>`.
- **Helm values.** Rename env keys `Outboxes__*`, `Destinations__*`, `KafkaClusters__*` and `ConnectionStrings__*` to `OutboxRelay__Outboxes__*`, `OutboxRelay__Destinations__*`, `OutboxRelay__KafkaClusters__*` and `OutboxRelay__ConnectionStrings__*`. The secret reference for the connection string becomes `OutboxRelay__ConnectionStrings__Main`.
- **Mounted appsettings and command line.** Nest the relay sections under `"OutboxRelay"` and prefix command-line keys with `OutboxRelay:`.
- `HostOptions`, `Kestrel`, `Logging`, `SERVICE_*` and `OTEL_*` do not change.

A pod started with only root-level relay settings finds no outbox, logs `StartupFailed` with `OutboxRelay.Outboxes: at least one outbox is required` and exits `1` (section 6e). The relay does not name root keys one by one, so use the mapping above. Root keys left behind are ignored. After the migration, remove them from appsettings, SSM parameter paths, Helm env values and the command line.

## 3. Database prerequisites

- **DDL.** The domain applies [`V001__create_outbox.sql`](../sql/postgresql/V001__create_outbox.sql) with its own migration tool. The relay never runs DDL. The `route` column is required: startup refuses an outbox table without it. Startup also refuses an outbox table whose relay columns differ from V001 in type or nullability, so apply the script's column definitions unchanged.
- **Grants** for the relay's role, the minimum set:

  ```sql
  GRANT SELECT, UPDATE, DELETE ON public.outbox TO outbox_relay;
  GRANT pg_monitor TO outbox_relay;  -- or pg_read_all_stats; needed for outbox.gate_lag only
  ```

  The role must also reach the table (`CONNECT` on the database, `USAGE` on its schema). Startup refuses a role without `SELECT`, `UPDATE` or `DELETE` and names the missing privilege. Without `pg_monitor` the relay starts, logs `GateLagUnavailable` (event 132) once and reports no `outbox.gate_lag`.
- **Producer grant** for each publishing application role, for example:

  ```sql
  GRANT USAGE ON SCHEMA public TO riskbackoffice_app;
  GRANT INSERT ON public.outbox TO riskbackoffice_app;
  ```

  No `SELECT`, no sequence grant. Details: [client packages](client-packages.md#grants).
- **`idle_in_transaction_session_timeout`** for the domain's application roles, for example:

  ```sql
  ALTER ROLE riskbackoffice_app SET idle_in_transaction_session_timeout = '60s';
  ```

  A forgotten open transaction anywhere on the cluster stops every `PerKey` and `Strict` outbox there (section 6b). The relay does not set it.
- **Direct connection only.** No RDS Proxy, no PgBouncer transaction pooling. Leadership uses session advisory locks, which do not survive them ([ADR 0003](adr/0003-leadership-by-postgresql-session-advisory-lock.md)).
- **Primary only.** Point the connection string at the primary (writer) endpoint. Startup refuses a read-only standby.

Details: [PostgreSQL prerequisites](configuration.md#postgresql-prerequisites).

## 4. Health

| Endpoint | 200 means | 503 means |
|---|---|---|
| `GET :5612/livez` | The process is up and every outbox worker loop runs and makes progress. | A worker loop stopped, or a worker is stuck on an await past its due time. Kubernetes restarts the pod. |
| `GET :5612/readyz` | The host has started, is not stopping, and every outbox database is reachable. | Starting, stopping, or an outbox database is unreachable. |

Neither fails on a broker outage, a gate stall, a delivery pause or a lock held by the other replica. A Kafka `PerKey` or `Strict` send has no timeout and never counts as stuck. Those conditions show in metrics and alerts (section 5), not in health. Details: [Health](configuration.md#health).

## 5. Alerts

Rules use Prometheus-style names. Map them to how your backend names the OTel metrics (`outbox.head_age` may arrive as `outbox_head_age_seconds`). Stats gauges come from the leader only, so `max by (outbox)` reads the leader's value.

| Alert | Rule (illustrative PromQL) | For |
|---|---|---|
| Leader count | `sum by (outbox) (outbox_leader) != 1` | 2m |
| Head age | `max by (outbox) (outbox_head_age) > 60` (assumed target, Q1) | 5m |
| Gate stall | `max by (outbox) (outbox_gate_lag) > 10 and on (outbox) max by (outbox) (outbox_pending) > 0` | 2m |
| Blocked | `max by (outbox) (outbox_blocked) > 0` (not summed across modes) | 1m |
| Destination faults | `sum by (outbox) (rate(outbox_destination_faults[5m])) > 0` | 10m |
| Pending growth | `deriv(outbox_pending[30m]) > 0` | 30m |
| Failed rows | max by (outbox) (outbox_retrying + outbox_blocked) > 100000 (assumed, Q1) | 15m |

`outbox.blocked` counts rows on `Strict` and keys on `PerKey`. Never sum it across outboxes of different modes.

First actions:

1. **Leader count.** `0`: no replica leads. Check `/readyz` and events 104 (`OutboxDatabaseUnreachable`) and 106 (`WorkerFaulted`). `2`: two replicas report leading, which points at a pooler between the relay and PostgreSQL. Check the connection goes direct (section 3). Flapping: section 6d.
2. **Head age.** Rows wait longer than the target. Read it with `outbox.gate_lag` and `outbox.retrying`. Gate lag high: section 6b. Destination faults rising: section 6a. Neither: check `outbox.pending` growth (a producer burst or a hot key, see [Scopes and sending](delivery.md#scopes-and-sending)).
3. **Gate stall.** A transaction holds the gate. Section 6b.
4. **Blocked.** A `Block` row holds its key (`PerKey`) or the outbox (`Strict`). Find it with the list query in [Operator actions](delivery.md#operator-actions), read `last_error`, fix the cause, then retry it from scratch or skip it. Act promptly: while failed rows keep changing, a blocked `PerKey` key with a large backlog of later rows is re-walked continuously (section 6c).
5. **Destination faults.** The outbox pauses on each fault. Read `DeliveryPaused` (event 110) for the error text. On a Kafka destination with `Routes`, a fault from a send's produce call or delivery report starts `route '<route>' topic '<topic>': `, naming the route and topic of the faulted send. Three faults keep their text without the prefix: `<code>: producer cannot be built`, `Local_Fatal: producer failed fatally before delivery report` and `Local_State: topic not described, ValidateAsync has not run`. On an `Http` destination with `Routes`, a status fault starts `route '<route>': `, for example `route 'alerts': HTTP 503`. Transport faults (connect, DNS, TLS, reset and `SendTimeout`) keep their text without the prefix ([Http delivery behavior](configuration.md#delivery-behavior)). Broker or topic problem: section 6a. Authorization or missing topic: fix the Kafka side. No restart is needed.
6. **Pending growth.** Delivery is slower than inserts. Check Head age, Gate stall and Destination faults first. If none fires, the producer rate exceeds the relay's rate for this mode ([Delivery (`PerKey` and `Strict`)](delivery.md#delivery-perkey-and-strict)).
7. **Failed rows.** Find the cause through the `Blocked` and `Destination faults` entries, then reset or delete failed rows ([Operator actions](delivery.md#operator-actions)). On `PerKey` and `Strict` the sum equals failed rows (one failed row per blocked key). On `None`, `outbox_blocked` is 0. Held rows (section 6c) count as failed rows. The failed-row summary and the stats statement both scan every failed row, see "Keep failed rows well below 500k per outbox" in [Operator actions](delivery.md#operator-actions).

Logs: `StatsUnavailable` (event 130) means the five stats gauges are absent until `StatsAvailable` (event 131). Absent gauges do not fire the rules above. Delivery is not affected.

## 6. Incidents

### 6a. Broker outage

- `PerKey` and `Strict` Kafka sends run with `message.timeout.ms` `0`: they wait for the broker instead of failing. No retry is spent, no pause, no dead letter. The outage shows as rows staying in the table: `outbox.head_age` and `outbox.pending` grow. `/livez` stays 200.
- When Kafka returns, the waiting sends complete and delivery resumes in order without action.
- Do not restart the relay to "unstick" it. A restart or rolling update during the outage abandons waiting sends at the 20 s stop drain. They are sent again by the next leader and can land twice ([Duplicates](delivery.md#duplicates)).
- `None` outboxes time out after 30 s (`Local_MsgTimedOut`), pause (`DeliveryPaused`, event 110) and resume (`DeliveryResumed`, event 112).
- Tested: `BrokerOutageTests` pauses Kafka for 180 s with `PerKey` and `Strict` sends in flight ([Broker-outage test](delivery.md#broker-outage-test-opt-in)).

### 6b. Gate stall

A long or forgotten write transaction anywhere on the PostgreSQL cluster, or a prepared transaction, stops every `PerKey` and `Strict` outbox on the cluster. `outbox.gate_lag` shows its age. `/livez` and `/readyz` stay 200.

Find the holder:

```sql
SELECT pid, usename, datname, application_name, state, xact_start, now() - xact_start AS age
FROM pg_stat_activity
WHERE backend_xid IS NOT NULL
ORDER BY xact_start;

SELECT gid, database, owner, prepared, now() - prepared AS age
FROM pg_prepared_xacts
ORDER BY prepared;
```

End it, after the owning team agrees:

- A running or idle-in-transaction session: `SELECT pg_terminate_backend(<pid>);`
- A prepared transaction: connect to its `database` and run `ROLLBACK PREPARED '<gid>';` (or `COMMIT PREPARED '<gid>';` if its owner says it must commit).

Delivery resumes on the next read. Prevent it with `idle_in_transaction_session_timeout` (section 3). Prepared transactions are not covered by that timeout.

### 6c. Blocked or failed rows

- List, retry from scratch, skip: [Operator actions](delivery.md#operator-actions). On `PerKey` and `Strict` a reset or delete is picked up within about one `Polling.Interval`. On `PerKey`, while the cursor walks a backlog, the rewind waits for the walk's end, or 60 s after the walk's last progress (no read for lack of room, reads of only overflowing rows, or reads that end at or before the furthest row already read in this walk, such as the re-read after an overflow move-back), whichever comes first (Large blocked-key backlog below). No restart.
- A dead-letter pending row (`retry_count` above `Retry.MaxRetryAttempts`, not held: `last_error` not starting `route not mapped: `) keeps retrying its dead letter. Its send error is in `DeadLetterFailed` (event 119).
- **Held rows** (`MessageHeld`, event 125, `last_error` starting `route not mapped: `). Cause: the destination does not accept the row's route. A routed row on a one-address destination (a Kafka destination with `Topic`), a NULL route on a `Routes` destination, or a route the `Routes` map lacks ([Routing](delivery.md#routing)). The row is held in place: no retry spent, never dead-lettered or dropped. `PerKey` holds its key, `Strict` holds the outbox, `None` skips the row. Find them with "List held rows" in [Operator actions](delivery.md#operator-actions). Fix the configuration (map the route) or the producer (write a mapped route), then restart the relay: routes are fixed at startup. Held rows then deliver within `Retry.MaxDelay`, or at once after retry from scratch. Skip a row that must not be delivered.
- `MessageBlocked` (event 123) logs each blocked row once. `MessageDropped` (event 120) means a message was lost because no `DeadLetter.Destination` was set.
- **Large blocked-key backlog.** After each applied rewind (`DeliveryRewound`, event 124, Debug) the `PerKey` cursor re-walks the later rows of blocked keys, ⌈N / `Polling.BatchSize`⌉ reads for N rows. A failure, summary or safety rewind asked for while a walk runs waits until that walk reaches the gate frontier (a read returns fewer than `Polling.BatchSize` rows) while the walk progresses, or 60 s after its last progress (no read for lack of room, reads of only overflowing rows, or reads that end at or before the furthest row already read in this walk, such as the re-read after an overflow move-back), so a walk that keeps progressing completes ([Rewind](delivery.md#scopes-and-sending)). The safety rewind is asked for 60 s after the last applied rewind and then waits the same way. Measured: one blocked key holding 1M later rows of its own, `Polling.BatchSize` 500 and 1000, with no churn, one key failing on every send, or ten. A row released behind the backlog is delivered 1.25–2.56 s after the release. Under churn (one key or ten failing on every send), a stream of 50 msg/s over 100 other keys for 120 s behind the backlog arrives in full (6,000 / 6,000), p95 2.56–2.73 s, max 2.96–4.04 s. Read load while failed rows keep changing: the relay re-walks the backlog continuously, 38–45 walks per minute at 1M, 2,000 reads per walk at `Polling.BatchSize` 500 and 1,000 at 1000. That is about 43,000–84,000 reads and about 31–34 s of PostgreSQL server time per minute (estimated from walk counts and the measured per-read time). Limit: while the walk progresses, a release behind the backlog takes at most two walks plus one `Polling.Interval`, for any N. That stays within 30 s up to about 6.5M later rows (extrapolated from walk times measured at 1M and 3M, not measured). Above that it grows with N, about 4.2 s per million later rows plus one `Polling.Interval` (extrapolated), and delivery continues. This assumes new rows arrive slower than the cursor reads them, because a progressing walk ends only at a short read. The cursor read about 560k–750k rows/s end to end at 1M (measured). If the walk stops progressing, the rewind applies 60 s after its last progress. Still act on the `Blocked` alert: clearing the blocked or failing rows saves that read load. Once the blocked head is cleared, the key's own later rows drain one at a time at about 145 msg/s (measured at 1M), so 1M rows take about 1.9 h (extrapolated).

### 6d. Leadership flapping or `LeadershipReleasePending`

- Events: `LeadershipAcquired` 101, `LeadershipLost` 102 (with a sanitized reason), `LeadershipReleased` 103, `OutboxDatabaseUnreachable` 104, `OutboxDatabaseReachable` 105, `WorkerFaulted` 106, `LeadershipReleasePending` 107.
- **Flapping** (101 and 102 alternating, `outbox.leader` sum dipping to 0): the session keeps failing. Causes: network loss to the database, a database failover, the 5 s session deadline (for example a failed-row summary over very many failed rows, see the Failed rows alert), or a pooler between the relay and PostgreSQL. Fix the database path. Each takeover can resend rows the old leader sent but had not deleted (section 7).
- **`LeadershipReleasePending` (107)** on stop or lock loss: the leader could not release its lock cleanly (abandoned command, failed or timed-out unlock, broken connection, failed close, or "session close abandoned at stop deadline"). The lock stays held until PostgreSQL ends that backend, after about 11 s on network loss (server keepalives). The other replica takes over then. One 107 per rollout is harmless. Repeated 107 points at the database path.
- Check who holds the lock: `SELECT l.pid, a.application_name, a.client_addr FROM pg_locks l JOIN pg_stat_activity a USING (pid) WHERE l.locktype = 'advisory' AND l.granted;`. The relay's sessions use `application_name` `outbox-relay`.

### 6e. `CrashLoopBackOff` on `StartupFailed`

Startup checks are fatal. The pod logs one `StartupFailed` entry (event 2, Critical) and exits 1. The entry lists every failure of the first stage that failed: configuration, then outbox tables and databases, then destinations, then destination bounds and routes ([Startup validation](configuration.md#startup-validation)). Fix every listed failure and restart. A later stage can then report its own failures. Messages name setting paths, outboxes, destinations and tables, never connection string values or secrets. Relay setting paths start with `OutboxRelay.`.

`OutboxRelay.Outboxes: at least one outbox is required` means no outbox is configured under `OutboxRelay`. On an upgraded image this usually means the deployment still uses root-level relay settings. Move them (section 2c).

The `Database` transport is removed. A destination with `Transport` `Database` fails startup with `OutboxRelay.Destinations[<name>].Transport: transport 'Database' is not registered`.

### 6f. Known untested risks

- **Network loss while PostgreSQL sends a reply**. Not tested on PostgreSQL. The relay forces `tcp_user_timeout` 11000 and TCP keepalives on its sessions, and a session command has a 5 s deadline. Watch for 102 and 107 during network incidents.
- **Blocked-key backlog above 1M rows.** Re-walk under churn is measured up to 1M later rows of one blocked key (section 6c). Above about 6.5M later rows a release behind the backlog can take more than 30 s: at most two walks plus one `Polling.Interval` while the walk progresses, about 4.2 s per million later rows plus one `Polling.Interval` (extrapolated, not measured).

## 7. Duplicates

Consumers deduplicate on the producer's idempotency key, which the producer puts in `headers` or `payload` ([Write-order rule](delivery.md#write-order-rule)). Dead-letter consumers deduplicate on `x-original-outbox` and `x-original-id`.

Duplicates happen when:

- the leader stops or loses its lock after Kafka confirmed a send but before the row was deleted;
- the leader stops or loses its lock with sends in flight, and they land after the next leader sent the same rows (for example a rolling update during a broker outage). These can arrive out of order. First deliveries per key (`PerKey`) or per outbox (`Strict`) stay in gate order;
- a `None` send times out after 30 s but lands (Kafka `PerKey` and `Strict` sends have no timeout);
- a dead letter lands but the stop or lock loss comes before the commit.

Bound per loss event on `Strict`: at most `Polling.BatchSize` rows, one contiguous range in gate order. Only rows the old leader read and had not deleted can be sent again, and `Strict` holds at most `Polling.BatchSize` rows. The `Strict` leader-loss tests assert this bound. Details: [Duplicates](delivery.md#duplicates).
