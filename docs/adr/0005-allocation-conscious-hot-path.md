# 0005. Keep the hot path allocation-conscious, with Dapper for data access

**Status:** Accepted
**Date:** 2026-11-03
**Deciders:** Apostolos Stephanou

## Context

The relay reads a batch of outbox rows, sends each row to its destination and commits the outcomes, for every message it delivers. Before PR #8 this path allocated about 4.3 KB per delivered message (`None` and `PerKey`, measured with a real PostgreSQL session and Kafka destination). The human asked for the hot path to avoid avoidable heap allocations, and for the rule to hold for future work on it.

Two choices on that path traded allocations against other concerns:

- Data access. Reading rows with a raw `NpgsqlDataReader` saved about 300 B per message more than Dapper, because Dapper boxes value-type columns and allocates a byte array per headers value.
- Header representation. `IReadOnlyDictionary<string, string>` headers allocated a dictionary per row and re-encoded every value to UTF-8 on each send.

## Decision

The hot path, read -> send -> commit per row and per send, avoids avoidable heap allocations, and never trades correctness or clarity for a micro-gain.

- Scope: the outbox session's read and commit, `DeliveryTerm`'s read, dispatch and commit steps, and the destination's send. Startup, validation, configuration and leadership are out of scope.
- On that path: reuse objects and buffers, prefer shared immutable instances, no LINQ, no redundant conversions. Measure allocated bytes when a gain is unclear.
- Dapper stays the data access library for outbox reads and commits. The human rejected the raw `NpgsqlDataReader` read despite its lower allocation.
- Headers are `MessageHeaders`, an immutable list of `MessageHeader` with ordinal-unique names and UTF-8 value bytes decoded once at read and shared between the read row and its sends. The human approved this representation.
- `.claude/orca-spec/standards.md` carries the rule as a one-line worker standard, so every dispatched worker applies it.

## Alternatives considered

- **Raw `NpgsqlDataReader` reads** — about 300 B per message less than Dapper, but hand-written column reads and type conversions for every mode. Rejected by the human: Dapper's clarity outweighs the gain.
- **Keep `IReadOnlyDictionary<string, string>` headers** — a dictionary per row and a UTF-8 encode per header per send, for no behaviour the relay needs.
- **No standing rule, optimise case by case** — later changes to the path would regress allocations without anyone deciding to.

## Consequences

- PR #8 cut allocated bytes per delivered message by about 30% (`None` and `PerKey`).
- Every change to the read, send or commit path is reviewed against the rule, and a gain that costs clarity needs a measurement.
- `MessageHeader.Value` is a shared mutable array. Nothing may write to it, a rule kept by documentation, not by the type.
- Reused buffers make some state per session or per term, so the path stays single-caller per session and term.

## Amendment, 2026-11-09: raw Npgsql on the processing path, Dapper on cold paths

**Status:** Accepted. Replaces the Decision bullet "Dapper stays the data access library for outbox reads and commits" and
the rejection of raw `NpgsqlDataReader` reads under Alternatives.

- Processing path (session read, failed-rows summary, commit including fail and block, Database destination sends including
  dead-letter sends): cached, prepared, positional `NpgsqlCommand`s with typed `NpgsqlParameter<T>`. Rows are read by ordinal
  through one `OutboxRowReader` per session, which checks the column names once per prepared command.
- Dapper stays for cold paths: outbox and destination validation, the stats query, the session open probe and the zero-row
  exists check. By-name mapping of wide, rarely-run rows is where it serves.
- Why: once commands were raw (2026-11-09 D3, D4), Dapper's only processing-path job was row mapping, which boxes every value
  column and copies the headers bytea. Raw mapping measured -200 B per row in every ordering mode, Gen0 7.0 -> 4.5 per 100k rows,
  identical rows on every column. The mapper (~55 lines) replaces `OutboxMessageMapping` (65 lines) and its process-wide
  Dapper type handler and type map.
- Consequences: the read column order is coupled to `OutboxRowReader` ordinals, guarded by the name check. The constructor
  argument order is the read order under `SequentialAccess`. `OutboxMessageMapping` and the session's `EnsureOpen` guard go.

> **Note (not part of the approved amendment text):** Implemented for Database destination sends by spec
> 2026-11-09-database-routes. Session read, summary and commits follow in a later PostgreSQL session spec (A2);
> until then they keep Dapper.

> *(Not part of the approved amendment text.)* Update 2026-11-10: session read, summary, commits and check implemented by spec 2026-11-10-alloc-pg-session.

## Amendment, 2026-11-10: process-wide dead-letter name bytes and the allocation guard

- Header bytes are also shared across messages. Kafka dead letters take their outbox and destination name header values from
  one process-wide cache, `NameBytes` in `KafkaDeadLetterHeaders` (`src/Pernix.OutboxRelay.Kafka/KafkaDeadLetterHeaders.cs`),
  keyed by configured name. Every dead letter of that name carries the same array, so header arrays are never pooled,
  recycled or written after a send or commit. The comment on `NameBytes` states the rule.
- Allocation guard. `AllocationGuardTests` in `tests/Pernix.OutboxRelay.Unit.Tests` runs the benchmark harness and fails when
  a cell is above its ceiling or a heap check fails. Its facts are explicit (`[Fact(Explicit = true)]`). It is not a CI gate:
  PR CI skips it because CI runners are too noisy for its ceilings. Run it on every change to the read, send or commit path.
  How to run it and read a failure: [`docs/development.md`](../development.md#allocation-guard).

> *(Not part of the approved amendment text.)* Update 2026-11-11: Database destination part superseded by [0010](./0010-remove-database-destination.md). The rest of the 2026-11-09 amendment stands for the session read, summary, commits and check.

## Amendment, 2026-11-14: Nanov.Kafka raw producer for KafkaDestination

- Decision. The human approved the fork on 2026-11-09 (spike Q1): "Approved, start this automatically with measurements
  taken after the other items are merged." The O4 ruling (foreman `msg_6071e4c33cd4`) fixed the package and scope.
- Scope: the `KafkaDestination` send path only. Stock `Confluent.Kafka` 2.16.0 stays everywhere else: admin describe and
  metadata checks, test consumers, other assemblies.
- Package: `Nanov.Kafka`, pinned exactly `[2.16.0.3]`. Source `github.com/nanov/confluent-kafka-dotnet` commit `975571c`
  (tag `v2.16.0.3`) = upstream `v2.16.0` + 61 commits. Bundled `librdkafka` 2.16.0, the same as stock.
- Measured gain (base `e545671` vs branch, 3 interleaved pairs, `pr-evidence.md` `## Allocation`):
  - `kafka`: h0 448.0 → 0.0, h1 632.0 → 0.0, h4 840.0 → 0.0 B/msg in every mode.
  - `kafka-dlq`: h0 1,408.0 → 304.0, h1 1,496.0 → 320.0, h4 1,904.1 → 368.0 B/dlq-msg in every mode.
  - `term` kafka: None 886.8 → 213.1, PerKey 823.3 → 191.5, Strict 880.3 → 248.3 B/msg.
  - `term` dlq: None 2,040.7 → 911.4, PerKey 2,102.7 → 929.3, Strict 2,168.4 → 992.3 B/dlq-msg.
  - No regressed cell, no rising heap, memory parity holds (`flat --target term` max: branch median 2,812 KB, limit 2,884 KB).
  - The guard ceilings of the 6 guard `term` kafka and dlq cells moved (`pr-evidence.md` `## Guard ceilings (task 9)`):
    None kafka 1,038.4 → 260.9, None dlq 2,343.7 → 1,010.7, PerKey kafka 905.6 → 210.8, PerKey dlq 2,311.9 → 1,022.8,
    Strict kafka 968.4 → 273.2, Strict dlq 2,385.2 → 1,091.6. The other 18 ceilings are unchanged.
- Costs:
  - Two published versions (2.16.0.2, 2.16.0.3) and a single maintainer. License `Apache-2.0`.
  - Native rebinding: on net8.0+ the fork binds librdkafka through source-generated `[LibraryImport]` and its own
    `DllImportResolver`, next to stock's binding in the same process, over one native librdkafka. Gates QG1-QG4 prove the
    coexistence.
  - Lock-step upgrade rule: bump `Nanov.Kafka` and stock `Confluent.Kafka` only to versions with the same
    `librdkafka.redist` dependency, then re-run QG1-QG4, the parity tests, `KafkaRawProducerTests` and the allocation
    measurement.
  - Two Kafka client APIs in one assembly, `Pernix.OutboxRelay.Kafka`.
  - H2 interim: five internal fork members reached through `UnsafeAccessor`, two of `Nanov.Kafka.IRawProducer`
    (`ProduceRawCore`, `ThrowIfError`) and three of `Nanov.Kafka.Impl.Librdkafka` (`headers_new`, `headers_add`,
    `headers_destroy`), so native calls use the librdkafka variant the fork loaded. `AllowUnsafeBlocks` in
    `Pernix.OutboxRelay.Kafka.csproj` stays for pinning. All in `src/Pernix.OutboxRelay.Kafka/KafkaEmptyHeaderValues.cs`.
    Removal rule below.
  - Empty-span handling and its tests: `NanovZeroLengthTests`, the wire parity tests (`KafkaWireParityTests`,
    `KafkaHeaderParityTests`) and `Rare_path_reaches_same_slot_and_fault_mapping_as_common_path`.
- Q5 outcome. Zero-length key and value are fixed by 2.16.0.3: the destination sends both through
  `KafkaNativeSpan.Present` (`KafkaNativeSpan.Null` for a null key). The `ReadOnlySpan<byte>` overloads are not used, since
  they send every empty span as NULL.
- Q6 outcome. The adapter drops raw `Local_Fatal` reports, and the error handler's fatal callback ends the send with
  `RetiredError`. One race outcome is removed: a send ending with `"<underlying code>: <fatal reason>"` when a
  `Local_Fatal` report wins the race with the error op, and `KafkaProducerFatal` 203 taking its code from that report.
- Q7 outcome, H2 interim rare path (foreman `msg_ab9f484b3610`). 2.16.0.3 still sends a zero-length header value as NULL on
  every public produce API. A message with at least one such value builds its own native header list and calls
  `ProduceRawCore` on the same handle, with the same opaque, delivery report and fault mapping as the public path. Every
  other message uses public `RawProduce`.
- Removal rule. When a `Nanov.Kafka` release sends zero-length header values non-NULL: pin it, delete
  `KafkaEmptyHeaderValues.cs` and the branch that calls it in `KafkaRawProducer.Produce`, drop the zero-length check in
  `KafkaHeaders.Fill` and return `Fill` to `void`, drop `AllowUnsafeBlocks`, remove `Explicit` from
  `NanovZeroLengthTests.Raw_produce_keeps_zero_length_header_value_non_null`, then re-run the gates, parity tests and
  measurement.
