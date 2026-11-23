# Outbox Relay development

## Local run

```sh
dotnet run --project src/Pernix.OutboxRelay.Host
```

The launch profile in `src/Pernix.OutboxRelay.Host/Properties/launchSettings.json` sets `EnvironmentName=local` and `OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317`.

```sh
curl -i http://localhost:5612/readyz
```

## Local docker build

```sh
docker build --platform linux/arm64 -t outbox-relay:local .
```

Optional build args: `BUILD_VERSION` (default `1.0.0`) and `BUILD_COMMIT` (default `unknown`). CI builds set `BUILD_VERSION` to `0.0.0-ci.g<sha7>`.

## CI/CD

Workflows call the reusable workflows and actions of `Pernix-Labs/pernix-ci`, pinned at `v1.0.0`. Bump every pernix-ci line in every workflow together to the same ref. Release units are declared in [`.github/release-units.yml`](../.github/release-units.yml). Decision: [ADR 0013](adr/0013-adopt-pernix-ci-and-tag-only-versions.md).

| Unit | Tag | Publishes |
|---|---|---|
| `relay` | `relay/vX.Y.Z` | Image `ghcr.io/pernix-labs/pernix-outbox-relay`, packages `Pernix.OutboxRelay.Core`, `Pernix.OutboxRelay.Hosting`, `Pernix.OutboxRelay.Outbox.PostgreSql`, `Pernix.OutboxRelay.Destination.Kafka` |
| `client` | `client/vX.Y.Z` | Packages `Pernix.OutboxRelay.Client`, `Pernix.OutboxRelay.Client.PostgreSql`, `Pernix.OutboxRelay.Client.EntityFrameworkCore` |

- [`.github/workflows/ci.yml`](../.github/workflows/ci.yml): on every PR and push to `main`. Secret scan, gates, build, test, image build and scan, pack dry check. A PR never pushes an image or package. A push to `main` pushes image `sha-<L>`.
- [`.github/workflows/cd.yml`](../.github/workflows/cd.yml): on a release tag. `ci` (reuses or rebuilds image `sha-<L>` and runs the gates), then `release` (rescans and promotes `sha-<L>` to `X.Y.Z`), then `publish` (packs and pushes to nuget.org through Trusted Publishing, environment `release`).
- [`.github/workflows/retention.yml`](../.github/workflows/retention.yml): daily GHCR image retention. Deletes `sha-*` and prerelease images after 14 days and keeps the 5 newest stable images. A manual run with `dry-run` lists planned deletions and deletes nothing.

### Release

Tag a commit on `main` and push the tag:

```sh
git tag relay/vX.Y.Z && git push origin relay/vX.Y.Z
git tag client/vX.Y.Z && git push origin client/vX.Y.Z
```

- A prerelease suffix is allowed, for example `client/v2.1.0-rc.1`.
- The version comes only from the tag. No file in the repo holds a version.
- Re-running `cd.yml` for the same tag is idempotent. A change that alters the image digest needs a new patch tag. Never move an existing tag.
- First tags: `relay/v1.0.0` and `client/v2.0.0`.

`SERVICE_VERSION`: a released image reports `SERVICE_VERSION` and OTel `service.version` `0.0.0-ci.g<sha7>`, because the image is built once in CI and only promoted by tag. The release version exists only as the image tag. Deployments set env `SERVICE_VERSION=<image tag>`. Env vars override the image `ENV`. `SERVICE_COMMIT` is already the tagged commit.

Prerequisites in repo and nuget.org settings:

- Environment `release`, with deployment tags `relay/v*` and `client/v*`.
- Repository variable `NUGET_USER`: the nuget.org profile that owns the Trusted Publishing policy. Value: `avataraps`.
- nuget.org Trusted Publishing policy: package owner (nuget.org) `avataraps`, repository owner `Pernix-Labs`, repository `pernix-outbox-relay`, workflow `cd.yml`, environment `release`, packages `Pernix.OutboxRelay.*`.

## Producer client packages

`Pernix.OutboxRelay.Client`, `Pernix.OutboxRelay.Client.PostgreSql` and `Pernix.OutboxRelay.Client.EntityFrameworkCore` let a producer write outbox rows in its own transaction. Install, grants and usage: [`docs/client-packages.md`](client-packages.md).

## Kafka client packages

The shipped send path is `KafkaDestination` only: it sends through the fork package `Nanov.Kafka`. The rest of the relay (admin describe) uses stock `Confluent.Kafka` 2.16.0. Tests reference both: stock for consumers and comparison producers, the fork in `NanovZeroLengthTests`, `KafkaCoexistenceGateTests`, `KafkaCoexistenceTests`, `KafkaHeadersTests`, `FakeProducer` and `KafkaRawProducerTests`. Both load in one process over one native librdkafka.

- **Package.** nuget.org id `Nanov.Kafka`, pinned exactly `[2.16.0.3]` in `Directory.Packages.props`. License `Apache-2.0`. One fork maintainer. Two published versions (2.16.0.2, 2.16.0.3).
- **Source.** `github.com/nanov/confluent-kafka-dotnet` commit `975571c` (tag `v2.16.0.3`) = upstream `v2.16.0` + 61 commits.
- **Native library.** Bundled `librdkafka` 2.16.0 (`librdkafka.redist` 2.16.0), the same as stock `Confluent.Kafka` 2.16.0. Central package pinning holds one `librdkafka.redist` version in every project.
- **Version lag.** None, checked 2026-11-13: the latest listed stable `Confluent.Kafka` on nuget.org is 2.16.0.
- **Binding.** On net8.0+ the fork binds librdkafka through source-generated `[LibraryImport]` and its own `DllImportResolver`, next to stock's binding in the same process. One librdkafka is loaded.
- **Raw API used.** `RawProducerBuilder` (`BuildRaw`), `IRawProducer.RawProduce` with `KafkaNativeSpan` key and value (`KafkaNativeSpan.Null` for a null key, else `KafkaNativeSpan.Present`), `RawDeliveryReport` through `RawDeliveryReportHandler`, and `KafkaHeaders` (8 inline headers per set, up to two sets per call). The `ReadOnlySpan<byte>` overloads are not used: they send every empty span as NULL. Files: `src/Pernix.OutboxRelay.Destination.Kafka/KafkaDestinationFactory.cs`, `src/Pernix.OutboxRelay.Destination.Kafka/KafkaRawProducer.cs`, `src/Pernix.OutboxRelay.Destination.Kafka/KafkaHeaders.cs`, `src/Pernix.OutboxRelay.Destination.Kafka/KafkaEmptyHeaderValues.cs`.
- **Zero-length header values (interim).** `Nanov.Kafka` 2.16.0.3 sends a zero-length header value as NULL on every public produce API. A message with at least one such value goes through `src/Pernix.OutboxRelay.Destination.Kafka/KafkaEmptyHeaderValues.cs`: it builds its own native header list and calls five internal fork members through `UnsafeAccessor`: `ProduceRawCore` and `ThrowIfError` of `IRawProducer`, and `headers_new`, `headers_add` and `headers_destroy` of `Nanov.Kafka.Impl.Librdkafka`. Native calls go through the fork's own delegates, so they use the librdkafka variant the fork loaded. It needs `AllowUnsafeBlocks` in `Pernix.OutboxRelay.Destination.Kafka.csproj` to pin buffers. Same opaque handle, delivery report and fault mapping as the public path. Every other message uses public `RawProduce`.
- **Removal rule.** When a `Nanov.Kafka` release sends zero-length header values non-NULL: pin it, delete `KafkaEmptyHeaderValues.cs` and the `!KafkaHeaders.Fill(...)` branch that calls it in `src/Pernix.OutboxRelay.Destination.Kafka/KafkaRawProducer.cs`, drop the zero-length check in `KafkaHeaders.Fill` and return `Fill` to `void`, drop `AllowUnsafeBlocks`, remove `Explicit` from `NanovZeroLengthTests.Raw_produce_keeps_zero_length_header_value_non_null`, then re-run the gates, parity tests and measurement.
- **Upgrade rule.** Bump `Nanov.Kafka` and stock `Confluent.Kafka` only to versions with the same `librdkafka.redist` dependency. Then re-run the coexistence gates (`KafkaCoexistenceGateTests` and `KafkaCoexistenceTests`: stock admin, raw produce and stock consume in one process over one librdkafka), the parity tests, `KafkaRawProducerTests` (binds the internal members above) and the allocation measurement.

## Measuring allocations

`benchmarks/Pernix.OutboxRelay.Benchmarks` measures relay allocations per delivered message, per call and per second, and heap over time. Any spec that claims or risks an allocation change needs before and after figures from it.

### Prerequisites

- PostgreSQL: Docker running (Testcontainers starts `postgres:17.11`), or `OUTBOX_BENCH_CONNECTION` set to a throwaway database. Each run creates its own `bench_<guid>` schema and leaves it in place.
- Kafka: nothing. The harness uses the librdkafka mock cluster inside the producer.
- Release build (`-c Release`) for every figure.

Without PostgreSQL, the Kafka and Core cases still run. PostgreSQL cases fail with a message that names both options.

### Commands

BenchmarkDotNet suites (`Allocated` per op):

```sh
dotnet run -c Release --project benchmarks/Pernix.OutboxRelay.Benchmarks -- bdn --filter '*'
dotnet run -c Release --project benchmarks/Pernix.OutboxRelay.Benchmarks -- bdn --filter '*SessionBenchmarks*'
dotnet run -c Release --project benchmarks/Pernix.OutboxRelay.Benchmarks -- bdn --filter '*KafkaSendBenchmarks*'
dotnet run -c Release --project benchmarks/Pernix.OutboxRelay.Benchmarks -- bdn --filter '*DeliveryTerm*'
dotnet run -c Release --project benchmarks/Pernix.OutboxRelay.Benchmarks -- bdn --filter '*HeadersParserBenchmarks*'
```

GC-delta scenarios (one verb each, `all` runs every verb in one process):

```sh
dotnet run -c Release --project benchmarks/Pernix.OutboxRelay.Benchmarks -- all
dotnet run -c Release --project benchmarks/Pernix.OutboxRelay.Benchmarks -- term
dotnet run -c Release --project benchmarks/Pernix.OutboxRelay.Benchmarks -- term --modes Strict --dests kafka
dotnet run -c Release --project benchmarks/Pernix.OutboxRelay.Benchmarks -- kafka
dotnet run -c Release --project benchmarks/Pernix.OutboxRelay.Benchmarks -- kafka-dlq
dotnet run -c Release --project benchmarks/Pernix.OutboxRelay.Benchmarks -- session
dotnet run -c Release --project benchmarks/Pernix.OutboxRelay.Benchmarks -- fixed
dotnet run -c Release --project benchmarks/Pernix.OutboxRelay.Benchmarks -- idle
dotnet run -c Release --project benchmarks/Pernix.OutboxRelay.Benchmarks -- flat
dotnet run -c Release --project benchmarks/Pernix.OutboxRelay.Benchmarks -- churn
dotnet run -c Release --project benchmarks/Pernix.OutboxRelay.Benchmarks -- term --modes None --dests kafka --profile
dotnet run -c Release --project benchmarks/Pernix.OutboxRelay.Benchmarks -- term --modes None --dests kafka --otel
```

Options: `--modes None,PerKey,Strict`, `--dests null,kafka,dlq`, `--headers` (`term`, `kafka`, `kafka-dlq`, `flat --target term`: 0, 1, 3, 4; `session`: 1, 3; other verbs exit 2), `--batches`, `--n` (count per round), `--warm`, `--rounds` (default 3), `--seconds` (idle window), `--target term|session`, `--sessions`, `--profile`, `--otel`. An unknown option or an unsupported value, such as `session --headers 4`, exits 2 with usage. A failed cell prints no reported row and the run exits 1. Rows already printed stay.

Defaults:

| Verb | Cells | Count per round (warm-up) |
|---|---|---|
| `term` | modes None, PerKey, Strict × dests null, kafka, dlq; batch 500, 1 header | null/kafka 200k (50k); Strict 20k (5k); dlq 50k (10k) |
| `kafka` | modes × headers 0, 1, 4; window 500, Strict 1 | 200k (50k); Strict 20k (5k) |
| `kafka-dlq` | modes × headers 0, 1, 4; window 500, Strict 1; Kafka dead-letter sends on the mock cluster | 200k (50k); Strict 20k (5k) |
| `session` | modes × batches 500, 100, 10, 1 × headers 1, 3; commit groups of `DeliveryTerm.CommitGroupMax` | batch ≥100: 100k (50k); 10: 10k (5k); 1: 5k (2k) |
| `fixed` | per mode: read 1/10/100/500/1000, read empty, commit-remove 1/10/100, commit-fail 10, check, summary, stats, read with 0/500/5000 excluded ids | fixed call count per group |
| `idle` | modes × null, kafka; polling interval 1 s | 30 s window after 5 s warm-up |
| `flat` | target term (PerKey kafka), session (None, batch 500) | term/session 1M, heap every 100k |
| `churn` | PerKey sessions: check, read 1, summary, commit | 4,000 sessions, heap and backends at each quarter |
| `all` | every verb above with defaults, in this order: `kafka`, `kafka-dlq`, `term` null/kafka/dlq, `idle`, `flat` term, `session`, `fixed`, `flat` session, `churn` | — |

### Method

- Metric: precise allocated-bytes delta (`GC.GetTotalAllocatedBytes(precise: true)`) over a measured window, divided by messages, rows, sends, calls or seconds. Gen0/1/2 collections are reported per 100k units.
- The per-unit verbs (`term`, `kafka`, `kafka-dlq`, `session`, `fixed`, `idle`) run a warm-up, then 3 rounds. The last round is tagged `reported` and is the figure. `flat` and `churn` print checkpoints instead. `flat --target term` forces a full GC (`Collect`, `WaitForPendingFinalizers`, `Collect`) before it reads the heap at each checkpoint.
- Workstation, non-concurrent GC. The project sets it, and every run prints it in its header line.
- BDN gives `Allocated` per op for one isolated operation. GC-delta gives bytes per message, call or second over a running term or loop. Use BDN to find where bytes go, GC-delta for the per-message figure.
- `--profile` samples `GCAllocationTick` events by type. Each sample is attributed by its event time to the reported round's measured window, and the run prints `X KB sampled vs Y KB measured in the reported round (Z%)`. It is diagnosis only: it adds bytes, and its rows are tagged `profiled`. Never paste them as figures.
  - The runtime takes one sample per ~100 KB allocated. `idle` and small `fixed` groups give few or no samples. Use the EventPipe method below there.
  - The sampling budget is fixed, so per-type shares among objects allocated together can flip between processes while the total stays the same. Compare runs in the same sampling mode, or use EventPipe stacks. This limit applies equally to `GCAllocationTick` through EventPipe. The known remedy is the .NET 10 randomized `AllocationSampled` event, which the harness does not use.
- EventPipe method for allocation stacks. Switch the runtime's EventPipe file output on through the environment, one process per run. BDN child processes inherit it:

  ```sh
  DOTNET_EnableEventPipe=1 \
  DOTNET_EventPipeConfig='Microsoft-Windows-DotNETRuntime:0x1:5' \
  DOTNET_EventPipeOutputPath="<dir>/trace_{pid}.nettrace" \
  dotnet run -c Release --no-build --project benchmarks/Pernix.OutboxRelay.Benchmarks -- bdn --filter '*DeliveryTerm*'
  ```

  Keyword `0x1` at verbose level gives `GCAllocationTick` events with type name, object size, thread and stack. Group the samples by type and stack with `Microsoft.Diagnostics.Tracing.TraceEvent`. The same environment works for the GC-delta verbs. Diagnosis only.

### Allocation guard

`AllocationGuardTests` runs the harness in child processes and fails when a cell is above its ceiling or a heap check fails. It covers 24 cells (`term` defaults, `session` batch 500 h1, `fixed` read-empty and check, `idle`), plus `flat --target term`, `flat --target session` and `churn`.

```sh
dotnet test --project tests/Pernix.OutboxRelay.Unit.Tests --filter-class "Pernix.OutboxRelay.Unit.Tests.Allocation.AllocationGuardTests" --explicit only --output Detailed
```

`--output Detailed` shows each fact's output for passed and failed facts. `--show-live-output` has no effect under `dotnet test` (Microsoft.Testing.Platform). To run one group, use `--filter-method "Pernix.OutboxRelay.Unit.Tests.Allocation.AllocationGuardTests.<Fact>"` in place of `--filter-class`. Facts: `Term`, `Session`, `Fixed`, `Idle`, `FlatTerm`, `FlatSession`, `Churn`.

- Prerequisites: Docker running or `OUTBOX_BENCH_CONNECTION` set, as for the harness. The guard builds the harness in Release itself. Run nothing else heavy.
- Run time: about 17 minutes for the class on the recorded machine (1,002-1,028 s over 6 runs). `Idle` takes 12.5 minutes of it.
- Ceilings: `tests/Pernix.OutboxRelay.Unit.Tests/Allocation/AllocationCeilings.cs`. Measured on Apple M5, 10 cores, 24 GB RAM, macOS 27.2 (26B123), arm64, 5 runs. The 6 `term` kafka and dlq rows are from commit `835b04a`, 2026-11-14. The other 18 rows are from commit `85e6bf6`, 2026-11-10. Machine, commits and dates sit at the head of the table, and the guard prints them once, in a `Ceilings measured on ...` line above the verdict table.
- When: any change to the read, send or commit path. [ADR 0005](adr/0005-allocation-conscious-hot-path.md) already requires measurement there. It is not a CI gate. It runs on a developer machine.
- Guard figures differ from a plain harness run of the same verb. The guard runs `term` as 3 child processes, which changes which cells share a process. For example, at commit `210edc4` the `term None dlq` median is 928.3 B/dlq-msg in the guard and 911.4 in a plain `term` run. Compare guard figures only with guard figures.
- Cell failure: re-run the cell alone with the command the failure text prints. That plain re-run has another process shape, so compare its figure with the same narrowed command run on base, not with the ceiling. If it repeats, measure base vs branch per "Base vs branch" below.
- Flat failure (`FlatTerm`, `FlatSession`): a single crossing means re-run that fact alone. Two crossings in a row means measure base vs branch per "Base vs branch" below.
- Another machine: a failure is a prompt to run base vs branch, not a verdict.
- Heap and backend checks: `flat --target term` and `flat --target session` fail when any checkpoint after the second is above the second by more than max(64 KB, 1% of the second). `flat --target session` is the asserted session heap check. `churn` fails when relay backends minus the before-count is not 0 at any quarter. `churn` heap is printed, not asserted: `GetTotalMemory` crossed the rule in 2 of 6 forced-GC `churn` runs with no growing live type.

### Base vs branch

Run the same harness on both sides. Put the base in its own worktree and copy the branch's benchmark source into it, without `bin/` and `obj/`:

```sh
git worktree add ../relay-base <base-sha>
rsync -a --delete --exclude bin --exclude obj benchmarks/ ../relay-base/benchmarks/
```

Build both sides in Release. Run on the same machine with nothing else heavy running, one process per run, interleaved: base 1, branch 1, base 2, branch 2, base 3, branch 3. Three runs per side is the minimum. If the branch harness does not compile on base because the product API changed under it, run each side's own harness and record that as a deviation in `pr-evidence.md`.

### Rules

- Per cell and per side, take the median of the runs. Base spread = base max − base min.
- A cell regresses when branch median > base median + base spread + growth the spec accepted in advance.
- A reduction counts as measured when base median − branch median > base spread.

### Evidence

The spec's `pr-evidence.md` records every run, the medians, the spreads, the verdict per cell, the commands and the machine.

### Baseline

[`benchmarks/Pernix.OutboxRelay.Benchmarks/BASELINE.md`](../benchmarks/Pernix.OutboxRelay.Benchmarks/BASELINE.md) holds the initial baseline. Each later allocation change appends a dated section on merge with its commit, machine and the cells it changed, before and after. Gate on interleaved base vs branch runs, not on these numbers.
