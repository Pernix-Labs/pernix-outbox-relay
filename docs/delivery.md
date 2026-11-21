# Outbox Relay delivery

## Delivery (`Ordering.Mode` `None`)

The leader of a `None` outbox runs one delivery loop on its session:

1. Read due rows: `next_attempt_at <= now()`, ordered by `id`, at most `min(Polling.BatchSize, room)` rows, excluding the ids it already holds. The worker holds at most 10 × `BatchSize` rows (read and not yet committed), dead letters in flight included.
2. Send every read row to the topic at once, without waiting for earlier results. A dead-letter pending row is sent only to the dead-letter topic, see [Failure policy](#failure-policy).
3. Commit outcomes in arrival order, at most 100 per group, one group at a time. An outcome removes the row or writes its retry state. A group of removes only runs `DELETE FROM <table> WHERE id = ANY($1)`. A group with retry state runs the `DELETE` (when the group has removes) and one `UPDATE` in one transaction. The `UPDATE` sets `retry_count`, `next_attempt_at = now() + <delay>` (database clock) and `last_error`, only where `retry_count` still has the value the relay read. A failed or lost transaction applies nothing of its group.
4. If the last read was full and the worker is below its 10 × `BatchSize` hold limit, read again at once. Otherwise wait `Polling.Interval`.
5. Check the lock every 1 s between session commands, as before.

**Read cost with failed rows.** Failed rows that are not due yet stay at the head of the `id` order, and every read walks past them. Measured on PostgreSQL 17 (500-row reads, 10k held ids, median of three quiet-host runs): 14.88 ms p95 per read with 100k such rows and 59.53 ms p95 with 500k (assumed, Q1), about 0.1 ms per 1k rows. Extrapolated linearly, not measured: 100 ms p95 near 700k–1M such rows. Watch `outbox.retrying`.

A row leaves the table only after Kafka confirms it, after its dead letter is confirmed, or when it fails permanently with no `DeadLetter.Destination` set. Delivery is at least once. Duplicates happen when the relay stops or loses its lock between Kafka's confirm and the commit, and when a send times out after 30 s (`Local_MsgTimedOut`) but lands. Consumers deduplicate. `None` guarantees no order between rows. No log line is written per delivered message.

Each Kafka record carries the row's `message_key` bytes as key (null when `message_key` is NULL), `payload` bytes unchanged as value, and one header per `headers` JSON entry with its value encoded UTF-8. The relay adds no headers to a delivered record. A producer `traceparent` header passes through unchanged. Dead letters add headers, see [Dead letters](#dead-letters).

### Failure policy

Kafka reports every failed send as one of three kinds:

| Kind | Kafka cases | Relay action |
|---|---|---|
| Permanent message fault | Checked before sending where possible: the record is larger than the limit, a null key on a topic whose `cleanup.policy` contains `compact`, `headers` that is not a JSON object of string values. From Kafka: `MsgSizeTooLarge`, `InvalidRecord`, `InvalidMsg` (Kafka `CorruptMessage`), `RecordListTooLarge`. | Skip remaining retries. Permanent failure, see below. |
| Transient message fault | None. Kafka has no transient message fault. | Spend one retry. |
| Destination fault | Broker unreachable, topic or partition missing, authorization failure, fatal producer error (the producer is rebuilt before the next send), `Local_MsgTimedOut` (not confirmed within `message.timeout.ms`, 30 s, `None` only), a report that is not `Persisted`, any unclassified Kafka error code. | Spend nothing. Pause the outbox. |

A message fault never pauses the outbox and never stops other sends or reads.

**Retries.** A transient message fault on a row read with retry count `r` writes `retry_count = r + 1`, `next_attempt_at = now() + min(Retry.InitialDelay × Retry.BackoffMultiplier^r, Retry.MaxDelay)` and `last_error`, while `r + 1 <= Retry.MaxRetryAttempts`. The row leaves the worker's held set and is read again when due. With defaults: 10 s, 20 s, 40 s. A fault past the last retry is a permanent failure.

**Destination faults.** The row stays untouched: no retry is spent and no column is written. The worker stops sending, keeps collecting results already in flight, commits their outcomes (message faults and dead letters included), then pauses. Pause `n` (pauses since the last delivered message sent after the previous pause) lasts `min(Retry.InitialDelay × Retry.BackoffMultiplier^(n−1), Retry.MaxDelay)`, so 10 s, 20 s, 40 s, … up to 5 min with defaults. After the pause the failed rows are read and sent again. A destination fault from a send started before the last pause ended starts no new pause and leaves `n` unchanged. Its row is read again on the next read. A pause does not affect `/readyz` or `/livez`.

**Permanent failure.** `Ordering.OnPermanentFailure` `Continue` is the only behaviour for `None`. `PerKey` and `Strict` also take `Block`, see [Block](#block).

- With `DeadLetter.Destination` set, the relay sends a dead letter first and deletes the row once the dead letter is confirmed.
- Without it, the relay deletes the row and logs `MessageDropped`. The message is lost. Set a `DeadLetter.Destination` to keep it.

**Dead-letter pending.** If the dead-letter send fails, with any fault, the row is written with `retry_count = max(r + 1, Retry.MaxRetryAttempts + 1)`, `next_attempt_at = now() + min(Retry.InitialDelay × Retry.BackoffMultiplier^(retry_count − Retry.MaxRetryAttempts − 1), Retry.MaxDelay)`, and `last_error` keeps the original failure reason. The outbox does not pause. A row read with `retry_count > Retry.MaxRetryAttempts` and not held (`last_error` not starting `route not mapped: `) is dead-letter pending: the relay sends only the dead letter, never the original message again. If `DeadLetter.Destination` was removed since, the row is deleted and logged `MessageDropped`. Without a `DeadLetter.Destination`, such a row is removed unsent on its next read, so lowering `Retry.MaxRetryAttempts` drops rows that already spent more retries than the new value. Raising it turns dead-letter pending rows with `retry_count` up to the new value back into rows waiting to retry, so their original message is sent again. A dead letter too large for the dead-letter topic stays pending until an operator acts.

Row states, from `retry_count`:

| `retry_count` | State |
|---|---|
| `0` | Pending. |
| `1` to `Retry.MaxRetryAttempts`, not held | Waiting to retry at `next_attempt_at`. |
| Above `Retry.MaxRetryAttempts`, not held | Dead-letter pending, retried at `next_attempt_at`. |
| At least `1`, with `last_error` starting `route not mapped: ` | Held: the destination does not accept the row's route. Checked again at `next_attempt_at`, see [Routing](#routing). |
| At least `1`, with `next_attempt_at = 'infinity'` | Blocked (`PerKey` and `Strict` with `Block` only). Never sent again by the relay. |

**Rolling back.** A build without dead-lettering refuses any non-empty `DeadLetter.Destination` at startup and treats dead-letter pending rows as ordinary rows. Before rolling back to it, clear `DeadLetter.Destination` with an empty env var override (`Outboxes__<i>__DeadLetter__Destination=`), and delete or dead-letter by hand every row with `retry_count > Retry.MaxRetryAttempts`.

**Leaving `PerKey` or `Strict`.** The `None` read takes only rows with `next_attempt_at <= now()`, so blocked rows (`next_attempt_at = 'infinity'`) are never read again after an outbox switches from `PerKey` or `Strict` to `None`, or after a rollback to a build without `PerKey` and `Strict` (it refuses both at startup, so the outbox must run `None`). They are not sent, dead-lettered or logged. Before either change, list them with `SELECT id, message_key, retry_count, last_error FROM outbox WHERE next_attempt_at = 'infinity' ORDER BY tx_id, id`, then retry each from scratch or delete it, see [Operator actions](#operator-actions).

### Dead letters

A dead letter goes to the `Kafka` destination named by `DeadLetter.Destination`, through its own producer per outbox, with the forced settings of the outbox's `Ordering.Mode` (see [Kafka](configuration.md#kafka)). It keeps the original key, payload and headers. Original headers named like a relay header are replaced. A row with malformed `headers` gets a dead letter with key, payload and the relay headers only. The relay adds, values UTF-8:

| Header | Value |
|---|---|
| `x-dead-letter-reason` | Error text of the failure that made the row final: the transport error (Kafka error code and client reason) or a relay check such as `headers is not a JSON object of string values`. A dead-letter pending row stores this text in `last_error` and its later dead letter reuses it, or `dead letter pending` when `last_error` is NULL. |
| `x-retry-attempts` | Delivery attempts to the original destination: `min(r + 1, Retry.MaxRetryAttempts + 1)`. A row that failed permanently before its retries ran out reports `Retry.MaxRetryAttempts + 1` when its dead letter is sent from a later read. |
| `x-original-destination` | Original destination `Name`. |
| `x-original-outbox` | Outbox `Name`. |
| `x-original-id` | Row `id`. |
| `x-failed-at` | Time of the dead-letter attempt, ISO 8601 UTC, `yyyy-MM-ddTHH:mm:ss.fffZ`. |
| `x-original-route` | Row `route`, absent when the route is NULL. A producer header with this name is always dropped. |

Dead letters get the same pre-send size check as other sends, against the dead-letter topic and with the added headers counted. A compacted dead-letter topic is refused at startup, because dead letters keep the original key and compaction would discard all but the newest dead letter per key. A lost lock or stop after the dead letter landed but before the commit sends it again, so consumers of the dead-letter topic deduplicate on `x-original-outbox` and `x-original-id`. On stop the relay starts no new dead letters: a row that needs one stays untouched and the next leader sends the original once more, then dead-letters it.

`last_error`, dead-letter headers, logs and metrics carry error text from the destination only. They never contain keys, payloads, header values, connection strings or `Producer` values.

### Failure logs

| Event | Name | Level | When |
|---|---|---|---|
| 110 | `DeliveryPaused` | Warning | Once per pause: outbox, destination, result kind, error text, pause length. |
| 111 | `MessageFaulted` | Warning | Each message fault from the original destination: row id, retry count as read, permanent flag, error text. |
| 112 | `DeliveryResumed` | Information | After a pause. |
| 117 | `MessageRetryScheduled` | Debug | A retry of the original message is scheduled: row id, new retry count, delay. |
| 118 | `MessageDeadLettered` | Information | Dead letter confirmed: row id, dead-letter destination, attempts. |
| 119 | `DeadLetterFailed` | Warning | Dead-letter send failed: row id, dead-letter destination, retry count as read, error text. |
| 120 | `MessageDropped` | Warning | Permanent failure without `DeadLetter.Destination`, row deleted: row id, destination, retry count as read, error text. |
| 121 | `DeadLetterDeferredByStop` | Information | Stop left rows that need a dead letter to the next leader: count. |
| 122 | `DeadLetterRetryScheduled` | Debug | A retry of the dead letter is scheduled after a failed dead-letter send: row id, dead-letter destination, delay. |
| 123 | `MessageBlocked` | Warning | Once per blocked row (`Block`): row id, outbox, destination, retry count written, error text. |
| 124 | `DeliveryRewound` | Debug | A `PerKey` or `Strict` leader moved its cursor back to the head: outbox, reason `summary`, `failure`, `unpark` or `safety`. |
| 125 | `MessageHeld` | Warning | Once per hold, see [Routing](#routing): row id, outbox, destination, route (`(none)` for NULL, cut to 63 UTF-16 code units plus `...(truncated)` as in [Routing](#routing)). |

### Graceful stop

On SIGTERM the leader stops reading and starting sends, dead letters included, and waits up to 20 s for sends in flight, committing outcomes as results arrive. Rows left needing a dead letter stay untouched (`DeadLetterDeferredByStop`, event 121). A pause does not delay stop. At 20 s remaining sends are abandoned (`DeliveryStopAbandoned`, event 114) and their rows stay in the table. Remaining commits and the dispose of both producers (no flush, unsent messages discarded) run until 25 s. Commits not done by then are abandoned (`DeliveryStopCommitIncomplete`, event 115). The session closes by 27 s, or is abandoned and logged `LeadershipReleasePending`. Every deadline counts from the stop signal. The same budget applies to every `Ordering.Mode`. A `PerKey` or `Strict` leader sends no queued row after the stop signal.

`HostOptions:ShutdownTimeout` defaults to `00:00:28` and must be at least `00:00:28`, the 27 s stop budget plus 1 s for host teardown. A lower value fails startup with `HostOptions.ShutdownTimeout: must be at least 00:00:28 to cover the 27 s stop budget`. Set the pod `terminationGracePeriodSeconds` to at least `30`.

## Delivery (`PerKey` and `Strict`)

`PerKey` keeps order per `message_key`. `Strict` keeps order across the whole outbox. Both run the same loop as `None` (commit, check, pause, dead letters, [Failure policy](#failure-policy), [Graceful stop](#graceful-stop)) with these differences.

| Mode | Order | A failing row holds back | Unconfirmed sends at once | Kafka rate measured locally (design K2) |
|---|---|---|---|---|
| `None` | None | Nothing | Any number | — |
| `PerKey` | Per key. NULL-key rows are unordered. | Later rows with the same key | One per non-null key | About 135k msg/s across 1,000 keys. One hot key is not at the `Strict` rate: `PerKey` forces `linger.ms` `5` and sends one message per key at a time, so a single key gets at most about one message per (5 ms + broker round trip). Measured locally: one hot key drains at about 135–142 msg/s. Use `Strict` (`linger.ms` `0`) for a single hot key. |
| `Strict` | Whole outbox | Every later row | One | About 2,000 msg/s, one broker round trip per message |

### Write-order rule

Producers write only `message_key`, `payload`, `headers` and `route`. Every other column belongs to the relay. The relay reads `route` to pick the address, see [Routing](#routing). A producer that writes `retry_count` or `next_attempt_at` can let later rows of its key overtake its row. A producer-written `next_attempt_at` on a `retry_count = 0` row no longer delays the row on `PerKey` and `Strict`.

The relay delivers in gate order `(tx_id, id)`. `tx_id` defaults to `pg_current_xact_id()`, which PostgreSQL assigns at the transaction's first write. Make the write that serializes the key (the update or lock of the business row) the transaction's first write and the outbox insert its last:

```sql
BEGIN;
UPDATE account SET balance = balance + 10 WHERE id = 42;               -- first write
INSERT INTO outbox (message_key, payload, headers)
VALUES ('account-42'::bytea, $1, '{"idempotency-key":"8f6c2a1e-..."}');     -- last write
COMMIT;
```

Without the rule the order is still consistent, but a transaction that started writing earlier is delivered first even when its change happened second. Even with the rule, two transactions whose first writes reach the same row at almost the same instant can get ids in the opposite order. A producer that cannot accept this takes `pg_advisory_xact_lock` on the key before its first write. That mitigation is untested and is not relay work. Put a unique idempotency key in `headers` or `payload`, because consumers must deduplicate.

### Gate

A leader reads only rows behind the snapshot gate `tx_id < pg_snapshot_xmin(pg_current_snapshot())`, so no running transaction can still commit a row in front of them. Gate and rows come from one statement. Rows are read forward from a cursor in `(tx_id, id)` order, at most `Polling.BatchSize` per read. Every leadership term starts at the head.

A long or forgotten write transaction anywhere on the PostgreSQL cluster holds the gate, and every `PerKey` and `Strict` outbox on the cluster stops delivering until it ends. Prepared transactions (`pg_prepared_xacts`) do this invisibly. Set `idle_in_transaction_session_timeout` for application roles and resolve forgotten prepared transactions. A gate stall does not affect `/livez` or `/readyz`. `None` reads due rows without the gate.

### Scopes and sending

- **Scopes.** `Strict` has one scope per outbox. `PerKey` has one scope per non-null key, compared byte by byte.
- **One unconfirmed send per scope.** The next row of a scope is sent only after Kafka confirmed the previous one, or after `Continue` removed it. A dead-letter send counts as the scope's one send.
- **NULL keys.** On `PerKey`, a row with a NULL `message_key` has no scope. It is sent on read without waiting for other rows, is unordered, and its failure holds nothing back. On `Strict`, NULL-key rows are ordered like every other row.
- **Held rows.** `PerKey` holds at most 10 × `Polling.BatchSize` rows and at most `Polling.BatchSize` per key. `Strict` holds at most `Polling.BatchSize`. A leader reads only when at least `Polling.BatchSize` rows fit. Rows of a full scope stay in the table and the cursor moves back to them once the scope has drained to half.
- **Hot key backlog.** While one `PerKey` key drains a backlog larger than its per-key queue, each move-back returns the cursor to that key's rows, so rows of other keys behind it wait while it drains. Measured with Kafka (`Polling.BatchSize` 500, 50 msg/s (assumed, Q1) over 100 other keys): the hot key drains at about 137–145 msg/s. With a 1k-row backlog (assumed, Q1) the other keys' p95 delay is 0.96 s, the same as with no hot key, the max is 1.37 s, and no row waited more than 5 s. With a 10k-row backlog the drain takes about 69 s, the other keys' p95 delay is 53.6 s (max 59.6 s), and 2,739 rows waited more than 5 s.
- **Failures.** A fault on a scope's head evicts the rows queued behind it from memory and moves the cursor back to the head, so they are re-read. On `PerKey`, while that move-back waits for a walk to end (see **Rewind**), later rows of the failed key are passed. A `PerKey` read scans at most `Polling.BatchSize` rows from the cursor and passes the rows of every key whose failed row is not due yet, so the cursor moves past waiting and blocked keys. Once it passes a row that is not due, it also passes every later row of that key until the next applied rewind, so a failed row that becomes due between summaries is not overtaken. `Strict` stops at the first row that is not due (waiting to retry, dead-letter pending or blocked). The rows behind it are not read again on every poll.
- **Destination faults.** The row stays untouched and stays its scope's next send after the pause. No later row of its scope is sent first.
- **Rewind.** At most once per `Polling.Interval`, before a read, the leader reads a summary of failed rows (`retry_count > 0`: count, checksum over `id`, `retry_count`, the blocked flag and the due flag, earliest future `next_attempt_at`). When it changes, the cursor moves back to the head. A fault on a scope's head, an unpause and, on `PerKey`, 60 s without a rewind (safety) also move it back. On `PerKey`, while the cursor walks a backlog (its last read returned `Polling.BatchSize` rows), a failure, summary or safety rewind waits until that walk reaches the gate frontier (a read returns fewer than `Polling.BatchSize` rows), or until 60 s after the walk's last progress, whichever comes first. The walk makes no progress while a read is held back for lack of room, while its reads return only rows that overflow, or while its reads end at or before the furthest row already read in this walk, such as the re-read after an overflow move-back. While the walk progresses there is no time limit. A rewind asked for when the walk has not progressed for 60 s applies at once. The safety rewind is asked for 60 s after the last applied rewind. While a walk runs it then waits like any other rewind: until the walk ends, or 60 s after the walk's last progress. Rewinds asked for during one walk become one. Otherwise the cursor moves back at once, as before. An unpause (reason `unpark`) rewinds at once and applies any waiting rewind with it. So an operator delete or reset of a failed row, a failure recorded by a previous leader, and a waiting row becoming due release the scope within about one `Polling.Interval` of the rewind, without a restart. While a walk runs, the rewind and so the release wait for that walk to end, or 60 s after its last progress (see **Clear blocked keys promptly** under [Operator actions](#operator-actions)). `DeliveryRewound` (event 124, Debug) logs one line per applied rewind. It carries the first deferred reason, except after an unpause, which applies any waiting rewind at once and logs `unpark`.

> **Warning:** a row with `retry_count = 0` and `next_attempt_at` in the future breaks the producer contract, and no documented operator action creates one. On `PerKey` and `Strict` such a row is delivered at once, in order, behind its key's failed row if there is one. `None` still waits until it is due.

### Order across topics

A Kafka destination with `Routes` sends the rows of one outbox to several topics through one producer. Order across topics comes from the one unconfirmed send per scope rule (see [Scopes and sending](#scopes-and-sending)): `PerKey` sends a key's next row only after the previous one is confirmed or removed, on whichever topic each row goes to, and `Strict` does the same for the outbox's next row. So a key's rows (`PerKey`) or the outbox's rows (`Strict`) are sent in order across topics. Consumers of several topics see that order only as send order. Kafka keeps no total order across topics.

### Strict needs a one-partition topic

`Strict` keeps total order only on a topic with one partition. On a topic with more partitions, records still keep per-key order through the `murmur2_random` partitioner, but consumers lose order across keys. With `Routes`, every mapped topic of a `Strict` outbox's destination needs one partition. The relay does not check the partition count. Creating each topic with one partition is the configurator's job.

### Block

`Ordering.OnPermanentFailure` `Block` is allowed with `PerKey` and `Strict` only. When a row fails permanently (permanent message fault, or retries exhausted):

1. With `DeadLetter.Destination` set, the relay sends the dead letter first. If it fails, the row becomes dead-letter pending as with `Continue`, and the dead letter is retried. A dead-letter pending row read under `Block` sends only the dead letter.
2. Once the dead letter is confirmed, or at once without `DeadLetter.Destination`, the relay writes `retry_count = r + 1`, `next_attempt_at = 'infinity'` and `last_error`, guarded on the `retry_count` it read, and logs `MessageBlocked` (event 123, Warning) once.

A blocked row is never sent or dead-lettered again. It holds its key (`PerKey`) or the whole outbox (`Strict`) until an operator deletes or resets it, see [Operator actions](#operator-actions). A blocked `PerKey` row with a NULL key holds nothing back. With `Continue` the row is dead-lettered or dropped and its scope moves on in order.

### No send timeout

With a Kafka destination, `PerKey` and `Strict` producers run with `message.timeout.ms` `0` (infinite), the dead-letter producer included. During a broker outage Kafka sends wait instead of failing: no `Local_MsgTimedOut`, no retry spent, no pause, no dead-letter pending. The outage shows only as rows staying in the table. Other destination faults (missing topic, authorization, unconfirmed report) still pause the outbox. A waiting send does not affect `/livez`. A stop abandons waiting sends at 20 s.

### Duplicates

Delivery is at least once, as with `None`. When the leader loses its lock or stops with sends in flight (for example a rolling update during a broker outage), sends that land after the next leader starts are duplicates, and with `PerKey` or `Strict` they can arrive out of order. First deliveries per key (`PerKey`) or per outbox (`Strict`) stay in gate order. Consumers deduplicate on the producer's idempotency key. With a Kafka destination, timed-out sends that land do not happen here, because its sends have no timeout.

## Routing

Each row carries the producer-written `route`. NULL means the row has no route. A destination has one address or a `Routes` map of route to address. A Kafka destination has `Topic` (one address) or `Routes` (route to topic), see [Outbox configuration](configuration.md#outbox-configuration). Core decides per row, with no default and no catch-all:

| Row `route` | One address | `Routes` map |
|---|---|---|
| NULL | Sent | Held |
| A key of `Routes` | Held | Sent |
| Any other value | Held | Held |

The check applies to main sends only. A dead-letter pending row goes to the dead-letter destination unchecked, and its dead letter carries the original route as data. A Kafka dead letter writes it as header `x-original-route` (see [Dead letters](#dead-letters)). A dead-letter destination always has one address, see [Startup validation](configuration.md#startup-validation).

**Hold.** For a row it does not accept, the relay sends nothing. It writes, guarded on the `retry_count` it read (`r`), `retry_count = max(r, 1)`, `next_attempt_at = now() + Retry.MaxDelay` and `last_error = 'route not mapped: <route>'` (`route not mapped: (none)` for a NULL route), and logs `MessageHeld` (event 125) once per hold. A route longer than 63 UTF-16 code units, possible only without the V001 CHECK, is cut to its first 63 code units, or 62 when the 63rd is a high surrogate so no surrogate pair is split, plus `...(truncated)` in `last_error` and the log. A hold acts like a failure of that row: `PerKey` holds the row's key, `None` skips only that row, `Strict` holds the outbox at that row. It does not pause the outbox and records no `outbox.delivered`, `outbox.message_faults` or `outbox.destination_faults`.

**Hold and retries.** A hold spends no retry: `retry_count` stays `max(r, 1)` however often the row is held again. A held row is never dead-lettered, dropped or blocked, for any `Retry.MaxRetryAttempts` (`0` included) and any `OnPermanentFailure`. A row whose `last_error` starts with `route not mapped: ` is not dead-letter pending, even with `retry_count` above `Retry.MaxRetryAttempts`. A fault or failure reason that itself starts with `route not mapped: ` or `fault: ` is written to `last_error` behind one more `fault: `, so it never reads as a hold, and a re-sent dead letter strips that one `fault: ` to carry the original reason.

**Release.** Routes are fixed at startup. Fix the configuration (map the route) or the producer (write a mapped route), then restart the relay. The held row is checked again when due, within `Retry.MaxDelay`, or at once after retry from scratch (see [Operator actions](#operator-actions)). A row still not accepted is held again. A released row counts retries from 0: its first message fault writes `retry_count = 1` with the first backoff step, and `x-retry-attempts` of its dead letter and the `retry_count` of a blocked row count from 0 as well.

**Held rows are failed rows.** They count in `outbox.retrying` and in the failed-row summary, and on `None` they add to the read cost of failed rows. Each hold rewrites the row, a non-HOT update (`next_attempt_at` is indexed), so a held row costs one such update per `Retry.MaxDelay` until it is released or skipped. A Kafka destination with `Routes` delivers the rows whose route it maps. A destination with `Topic` holds every routed row. List them with "List held rows" in [Operator actions](#operator-actions).

## Operator actions

Plain SQL on the outbox database, for every `Ordering.Mode`. Use the outbox's configured table name.

| Action | PostgreSQL |
|---|---|
| Skip a message | `DELETE FROM outbox WHERE id = $1` |
| Retry from scratch | `UPDATE outbox SET retry_count = 0, next_attempt_at = now(), last_error = NULL WHERE id = $1` |
| Unblock | Retry from scratch (the row is sent again, in order) or skip it. |
| List held rows | `SELECT id, message_key, route, retry_count, next_attempt_at, last_error FROM outbox WHERE retry_count > 0 AND last_error LIKE 'route not mapped: %' ORDER BY tx_id, id`. `retry_count > 0` lets it use `outbox_failed_message_key_idx`. Release them as in [Routing](#routing). |
| List what is failing | `SELECT id, message_key, retry_count, next_attempt_at = 'infinity' AS blocked, last_error AS failure_reason FROM outbox WHERE retry_count > 0 ORDER BY tx_id, id`. For a dead-letter pending row, `failure_reason` is the original failure. The dead-letter send error is only in `DeadLetterFailed` (event 119). |

Which change wins:

- **Retry state is guarded on `retry_count` only.** The relay writes retry, dead-letter pending and blocked state only where `retry_count` is still the value it read. An operator change to `retry_count` made while the message is in flight wins over that write. A reset that leaves `retry_count` unchanged, such as retry from scratch on a row read with `retry_count = 0`, is not detected.
- **Deletes are not guarded.** A delete by the relay (delivered, dead-lettered or dropped) removes the row even after an operator reset, because the next row of its scope may already be sent. Keeping the reset row would put it behind a later row.
- **`next_attempt_at` is not guarded.** An edit of only `next_attempt_at` on a row in flight may be overwritten.
- **Queued rows.** Deleting a row that never failed while the leader holds it does not stop its send.

On `PerKey` and `Strict` outboxes a delete or reset of a failed row is picked up within about one `Polling.Interval` and releases its scope. On `PerKey`, while the cursor walks a backlog, it is picked up when that walk ends, or 60 s after the walk's last progress (see **Rewind** under [Scopes and sending](#scopes-and-sending)). `None` never blocks a row, so `blocked` is true on a `None` outbox only for rows left from `PerKey` or `Strict`. Retry them from scratch or delete them, see **Leaving `PerKey` or `Strict`** under [Failure policy](#failure-policy).

- **Clear blocked keys promptly.** On `PerKey`, after each applied rewind the cursor re-walks the later rows of blocked keys in reads of at most `Polling.BatchSize` rows, so N rows walked cost ⌈N / `Polling.BatchSize`⌉ reads. A rewind asked for during a walk waits for that walk to end while the walk progresses, or 60 s after it stops progressing (see **Rewind** under [Scopes and sending](#scopes-and-sending)), so a walk that keeps progressing completes while failed rows keep changing. Measured on PostgreSQL 17, `Polling.BatchSize` 500 and 1000: each read takes at most 1.85 ms p95 (10.30 ms max) with up to 1M later rows of one blocked key. With 1M such rows and no churn, one key failing on every send, or ten, a row released behind them is delivered 1.25–2.56 s after the release. Under churn (one key or ten failing on every send), a stream of 50 msg/s over 100 other keys behind them arrives in full with p95 at most 2.73 s and max at most 4.04 s. With 100k such rows the release takes at most 1.20 s. While failed rows keep changing, the relay re-walks the backlog continuously: 38–45 walks per minute at 1M, about 31–34 s of PostgreSQL server time per minute (estimated). Limit: while the walk progresses, a release behind the backlog takes at most two walks plus one `Polling.Interval`, for any number of later rows. That stays within 30 s up to about 6.5M later rows (extrapolated from walk times measured at 1M and 3M, not measured). Above that it grows with the number of later rows, about 4.2 s per million plus one `Polling.Interval` (extrapolated), and delivery does not stop. This assumes new rows arrive slower than the cursor reads them, because a progressing walk ends only at a read of fewer than `Polling.BatchSize` rows. The cursor read about 560k–750k rows/s end to end at 1M (measured). If the walk stops progressing, the rewind applies 60 s after its last progress. Act on the `Blocked` alert promptly: clearing the key saves that read load, and its own later rows then drain one at a time at about 145 msg/s.
- **Keep failed rows well below 500k per outbox.** The failed-row summary (`PerKey` and `Strict`, once per `Polling.Interval` on the leader session) and the stats statement (every 15 s, all modes) both scan every failed row, so their cost grows linearly. Measured on PostgreSQL 17: at 100k failed rows both stay at or below 50 ms p95. At 500k failed rows (assumed, Q1) updated since the last vacuum, the summary adds up to 115 ms p95 (336 ms max) to every read cycle, and the stats statement takes up to 268 ms p95. Extrapolated linearly from these measurements, not measured: at about 2.8M failed rows the summary would reach the 5 s session deadline and leadership would flap. The `Failed rows` alert fires at 100k (assumed, Q1).
- **Unblock with retry from scratch or skip.** These are the supported actions. An edit that only sets `next_attempt_at` on a blocked `PerKey` row changes its blocked flag in the failed-row summary and is seen on the next summary, within one `Polling.Interval`.

## Broker-outage test (opt-in)

`BrokerOutageTests` pauses Kafka for 180 s, longer than the client's 60 s socket timeout, with `PerKey` and `Strict` sends in flight. It checks that `/livez` stays 200, every row reaches Kafka with no gap, first deliveries per scope keep gate order, and delivery resumes after Kafka returns. It carries trait `Category=BrokerOutage` and skips unless `OUTBOX_RELAY_BROKER_OUTAGE_TEST=1`. It takes several minutes. Run it before the first `PerKey` or `Strict` outbox goes live:

```sh
OUTBOX_RELAY_BROKER_OUTAGE_TEST=1 dotnet test --project tests/Pernix.OutboxRelay.Integration.Tests -- --filter-trait "Category=BrokerOutage"
```

`StrictLeaderKillTests` runs the relay image as the `Strict` leader in a container and kills it with `SIGKILL` mid-batch, while an in-process relay waits as the second replica. It checks that the second replica delivers every row, first deliveries keep gate order with no gap, duplicates form one contiguous range of at most `Polling.BatchSize` rows, and the table ends empty. It carries trait `Category=LeaderKill` and skips unless `OUTBOX_RELAY_LEADER_KILL_TEST=1`. It runs the image `OUTBOX_RELAY_TEST_IMAGE` (default `outbox-relay:local`), so build that tag first, see [Local docker build](development.md#local-docker-build):

```sh
OUTBOX_RELAY_LEADER_KILL_TEST=1 dotnet test --project tests/Pernix.OutboxRelay.Integration.Tests -- --filter-trait "Category=LeaderKill"
```
