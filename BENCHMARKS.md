# Benchmarks

What the Outbox Relay allocates per delivered message, per database call and per idle second, and whether its heap stays flat. Every number on this page is from one run on `main` at `ecc4861`, 2026-11-24. History and every earlier run: [`benchmarks/Pernix.OutboxRelay.Benchmarks/BASELINE.md`](benchmarks/Pernix.OutboxRelay.Benchmarks/BASELINE.md).

## What is measured and why

The relay is a long-running service that copies rows from PostgreSQL to Kafka. Its cost to a host is mostly garbage: bytes allocated per message decide how often the GC runs. [ADR 0005](docs/adr/0005-allocation-conscious-hot-path.md) makes the read, send and commit path allocation-conscious, and requires a measurement for any change to it. The harness in [`benchmarks/Pernix.OutboxRelay.Benchmarks`](benchmarks/Pernix.OutboxRelay.Benchmarks) is that measurement.

The metric is allocated bytes, not throughput. Bytes per message repeat to the byte across runs. Rates depend on the machine and on what else runs on it, and are context only.

| Verb | Measures | Unit | Needs |
|---|---|---|---|
| `kafka` | The Kafka destination alone: send and delivery report. | B/msg | librdkafka mock cluster |
| `kafka-dlq` | The same, with every send a dead letter. | B/dlq-msg | librdkafka mock cluster |
| `term` | The delivery loop end to end over an in-memory outbox session, with a null, Kafka or dead-letter destination. | B/msg | librdkafka mock cluster |
| `session` | The PostgreSQL outbox session: read a batch, commit it. | B/row | PostgreSQL |
| `fixed` | One PostgreSQL call of each kind: read, commit, check, summary, stats. | B/call | PostgreSQL |
| `idle` | A leader with an empty outbox, polling every second. | B/s | librdkafka mock cluster |
| `flat`, `churn` | Heap after a full GC over 1M messages, 1M rows, and 4,000 sessions. | KB | PostgreSQL for `flat --target session` and `churn` |
| `bdn` | BenchmarkDotNet suites for one isolated operation each. | B/op | PostgreSQL for `SessionBenchmarks` |

Two methods, on purpose. The GC-delta verbs read `GC.GetTotalAllocatedBytes(precise: true)` around a running loop and give the per-message figure. BenchmarkDotNet gives `Allocated` for one isolated operation and shows where the bytes go. Full method: [`docs/development.md#measuring-allocations`](docs/development.md#measuring-allocations).

## Results

Median of 3 runs, `reported` round of each. Spread is max − min across the 3 runs.

### Kafka destination

The same in every ordering mode (`None`, `PerKey`, `Strict`). Spread 0.0 in every cell.

| Headers per message | Send (B/msg) | Dead-letter send (B/dlq-msg) |
|---:|---:|---:|
| 0 | **0.0** | 304.0 |
| 1 | **0.0** | 320.0 |
| 4 | **0.0** | 368.0 |

### Delivery loop

Batch 500, 1 header. The null destination completes synchronously, so its row is the cost of the loop alone.

| Mode | Null (B/msg) | Kafka (B/msg) | Dead letter (B/dlq-msg) |
|---|---:|---:|---:|
| `None` | 192.8 | 201.6 | 895.4 |
| `PerKey` | 192.9 | 191.6 | 930.3 |
| `Strict` | 192.9 | 248.3 | 992.2 |

Largest spread: 10.7 B (`None` dead letter). The other cells: 7.2 B (`None` Kafka), at most 1.7 B elsewhere.

### PostgreSQL outbox session

Read plus commit, 1 header, B/row.

| Mode | Batch 500 | Batch 1 |
|---|---:|---:|
| `None` | 319.5 | 2,678.7 |
| `PerKey` | 319.3 | 2,710.3 |
| `Strict` | 319.8 | 2,719.6 |

Per call, B/call. The same in every mode except `stats`.

| Call | B/call |
|---|---:|
| read, 1 row | 1,385.6 |
| read, empty | 1,064.3 |
| commit, remove 1 row | 1,296.6 |
| commit, remove 100 rows | 2,088.6 |
| check | 1,328.3 |
| summary | 1,216.3 |
| stats (`None` / `PerKey` / `Strict`) | 5,432.4 / 5,930.8 / 5,628.1 |

### Idle

One leader, empty outbox, polling interval 1 s, 30 s window. A noisy cell: spread 37.3-127.2 B/s.

| Mode | Null (B/s) | Kafka (B/s) |
|---|---:|---:|
| `None` | 57.1 | 95.2 |
| `PerKey` | 86.9 | 132.0 |
| `Strict` | 96.5 | 99.2 |

### Heap over time

Heap after a full GC, first / last / max checkpoint (KB), per run.

| Scenario | Run 1 | Run 2 | Run 3 |
|---|---|---|---|
| Delivery loop, `PerKey` Kafka, 1M messages | 2,446 / 829 / 2,446 | 2,424 / 834 / 2,424 | 2,420 / 827 / 2,420 |
| Session, `None`, batch 500, 1M rows | 4,588 / 4,573 / 4,588 | 4,484 / 4,470 / 4,484 | 4,495 / 4,471 / 4,495 |

No rising trend. 4,000 open-and-close sessions (`churn`) left 0 relay backends on PostgreSQL in every run.

### BenchmarkDotNet

`Allocated` per operation.

| Benchmark | Case | Allocated |
|---|---|---:|
| `KafkaSendBenchmarks.Send` | 0, 1, 4 headers | 0 B |
| `DeliveryTermBenchmarks.Deliver` | `None` / `PerKey`, null destination | 193 B / 193 B |
| `DeliveryTermBenchmarks.Deliver` | `None` / `PerKey`, Kafka | 220 B / 194 B |
| `SessionBenchmarks.Read500` | per row, `None` / `PerKey` | 299 B / 299 B |
| `SessionBenchmarks.Commit100` | per row, `None` / `PerKey` | 24 B / 25 B |
| `HeadersParserBenchmarks.Parse` | one / three / escaped | 96 B / 248 B / 152 B |

Every cell, every run and the BDN means: [the 2026-11-24 section of `BASELINE.md`](benchmarks/Pernix.OutboxRelay.Benchmarks/BASELINE.md#2026-11-24--re-measure-on-main).

## Environment

| Item | Value |
|---|---|
| Commit | `ecc4861` (`main`), 2026-11-24 |
| Machine | Apple M6, 12 cores, 32 GB RAM, macOS 27.2.1, arm64 |
| .NET | runtime 10.0.15, SDK 10.0.404, Release build |
| GC | workstation, non-concurrent |
| PostgreSQL | `postgres:17.11` through Testcontainers, Docker 29.9.3 |
| Kafka | `Nanov.Kafka` 2.16.0.3, librdkafka 2.16.0, in-process mock cluster. No broker, no network |
| BenchmarkDotNet | 0.15.8, `[MemoryDiagnoser]` |

Limits of this run:

- The machine was not idle. Other workloads ran during the runs. Allocated bytes did not move with the load (see the spreads above). Throughput and BDN means did, so this page reports none.
- Kafka is the librdkafka mock cluster and PostgreSQL is a local container. The figures are relay allocations, not end-to-end latency or throughput against a real cluster.
- Absolute figures depend on machine and runtime. Changes are gated on interleaved base vs branch runs on one machine, not on these numbers.

## Reproduce

Prerequisites: .NET SDK 10, and Docker running (Testcontainers starts `postgres:17.11`) or `OUTBOX_BENCH_CONNECTION` set to a throwaway database. Kafka needs nothing.

```sh
dotnet build -c Release benchmarks/Pernix.OutboxRelay.Benchmarks
dotnet run -c Release --no-build --project benchmarks/Pernix.OutboxRelay.Benchmarks -- all   # run 3 times
dotnet run -c Release --no-build --project benchmarks/Pernix.OutboxRelay.Benchmarks -- bdn --filter '*'
```

One `all` run took 16-18 minutes here, the BDN run 10 minutes. Run one verb alone, for example the delivery loop with a Kafka destination:

```sh
dotnet run -c Release --project benchmarks/Pernix.OutboxRelay.Benchmarks -- term --dests kafka
```

Every verb, option and default, the base vs branch procedure and the allocation guard: [`docs/development.md#measuring-allocations`](docs/development.md#measuring-allocations).
