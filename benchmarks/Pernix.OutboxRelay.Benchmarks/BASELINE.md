# Relay allocation baseline

Allocation figures for the Outbox Relay, measured with this project. Method and base vs branch procedure: [`docs/development.md#measuring-allocations`](../../docs/development.md#measuring-allocations).

## 2026-11-09 — Initial baseline

### Method

| Item | Value |
|---|---|
| Product commit | `2671eb5` (`git diff 2671eb5 -- src` = the 3 `InternalsVisibleTo` lines only, no runtime effect) |
| Harness commit | `b93999a` |
| Date | 2026-11-09, runs 05:21-06:26 UTC |
| Machine | Apple M5, 10 cores (10 logical / 10 physical), 24 GB RAM, macOS 27.2 (26B123), arm64 |
| .NET | runtime 10.0.2 (Microsoft.NETCore.App 10.0.2), SDK 10.0.102, Release build |
| GC mode | workstation, non-concurrent (header: `gcServer=False latency=Batch`; BDN job `Concurrent=False Server=False`) |
| PostgreSQL | Testcontainers `postgres:17.11` (header `pg=testcontainers postgres:17.11`), Docker 29.6.1 |
| Kafka | Confluent.Kafka 2.16.0 / librdkafka 2.16.0, librdkafka mock cluster (`test.mock.num.brokers=1`) |
| BenchmarkDotNet | 0.15.8, `[MemoryDiagnoser]`, default out-of-process toolchain |
| Other | `otel=False profile=False` on every reported run. Machine otherwise idle, no other benchmark or test run. |

Commands (repo root, run in this order, one process each):

```sh
dotnet build -c Release benchmarks/Pernix.OutboxRelay.Benchmarks
dotnet run -c Release --no-build --project benchmarks/Pernix.OutboxRelay.Benchmarks -- all   # run 1
dotnet run -c Release --no-build --project benchmarks/Pernix.OutboxRelay.Benchmarks -- all   # run 2
dotnet run -c Release --no-build --project benchmarks/Pernix.OutboxRelay.Benchmarks -- all   # run 3
dotnet run -c Release --no-build --project benchmarks/Pernix.OutboxRelay.Benchmarks -- bdn --filter '*'
```

Each `all` run took ~18 min (idle ~570 s of it). `bdn --filter '*'`: 30 cases, 11 min.

### BDN

`Allocated` per operation as BDN reports it (bytes, per op = per row / message / send / call as stated). Mean is context only.

#### Session (`SessionBenchmarks`)

`Read500` and `Commit100` per row, other methods per call.

| Method | Mode | Mean | Allocated |
|---|---|---:|---:|
| Read500 | None | 953.1 ns | 569 B |
| Read1 | None | 157,061.7 ns | 4,568 B |
| ReadEmpty | None | 151,877.2 ns | 3,976 B |
| Commit100 | None | 1,886.5 ns | 33 B |
| Commit1 | None | 135,387.6 ns | 2,502 B |
| Check | None | 158,483.0 ns | 3,104 B |
| Summary | None | 166,091.5 ns | 2,129 B |
| Read500 | PerKey | 1,634.5 ns | 571 B |
| Read1 | PerKey | 191,882.1 ns | 5,968 B |
| ReadEmpty | PerKey | 231,589.2 ns | 5,376 B |
| Commit100 | PerKey | 1,577.0 ns | 33 B |
| Commit1 | PerKey | 169,630.8 ns | 2,502 B |
| Check | PerKey | 179,727.8 ns | 3,208 B |
| Summary | PerKey | 176,375.7 ns | 2,127 B |

#### Database send (`DatabaseSendBenchmarks`)

Per send.

| Method | MaxInFlight | Headers | Mean | Allocated |
|---|---:|---:|---:|---:|
| Send1 | 1 | 1 | 382.13 us | 7,736 B |
| Send1 | 1 | 3 | 286.17 us | 7,845 B |
| Send8 | 8 | 1 | 66.87 us | 6,890 B |
| Send8 | 8 | 3 | 73.83 us | 7,120 B |

#### Kafka send (`KafkaSendBenchmarks`)

Per message, 500 sends then 500 awaits per invocation.

| Method | Headers | Mean | Allocated |
|---|---:|---:|---:|
| Send | 0 | 12.26 us | 800 B |
| Send | 1 | 13.13 us | 1,105 B |
| Send | 4 | 13.92 us | 1,696 B |

#### DeliveryTerm (`DeliveryTermBenchmarks`)

Per delivered message, 5,000 rows released per invocation. Exceptions per message from the BDN log (`// Exceptions:` line).

| Method | Mode | Destination | Mean | Allocated | Exceptions/msg |
|---|---|---|---:|---:|---:|
| Deliver | None | Null | 1.225 us | 198 B | 0.0035 |
| Deliver | None | Kafka | 2.721 us | 1,643 B | 0.0030 |
| Deliver | None | Database | 184.885 us | 9,125 B | 0.99 |
| Deliver | PerKey | Null | 1.221 us | 245 B | 0.0009 |
| Deliver | PerKey | Kafka | 66.466 us | 1,603 B | 0.066 |
| Deliver | PerKey | Database | 184.103 us | 9,189 B | 1.02 |

#### HeadersParser (`HeadersParserBenchmarks`)

Per parse.

| Method | Case | Mean | Allocated |
|---|---|---:|---:|
| Parse | One | 61.25 ns | 152 B |
| Parse | Three | 134.71 ns | 400 B |
| Parse | Escaped | 74.25 ns | 152 B |

### GC-delta

Each cell: the `reported` round (round 3 of 3, after warm-up) of runs 1-3, median, spread = max − min. Gen0/1/2 per 100k units and rate are medians of the same rounds. Defaults: all three modes for every verb.

#### Kafka-only (`kafka`)

| Cell (B/msg) | run 1 | run 2 | run 3 | median | spread | Gen0/1/2 per 100k (median) | rate/s (median) |
|---|---:|---:|---:|---:|---:|---|---:|
| kafka-only None w500 h0 | 800.0 | 800.0 | 800.0 | **800.0** | 0.0 | 9.5 / 0 / 0 | 74,571.0 |
| kafka-only None w500 h1 | 1,104.0 | 1,104.0 | 1,104.0 | **1,104.0** | 0.0 | 13 / 0 / 0 | 71,582.0 |
| kafka-only None w500 h4 | 1,696.0 | 1,696.0 | 1,696.1 | **1,696.0** | 0.1 | 20 / 0 / 0 | 67,815.0 |
| kafka-only PerKey w500 h0 | 800.0 | 800.0 | 800.0 | **800.0** | 0.0 | 9.5 / 0 / 0 | 73,589.0 |
| kafka-only PerKey w500 h1 | 1,104.0 | 1,104.0 | 1,104.0 | **1,104.0** | 0.0 | 13.5 / 0 / 0 | 72,607.0 |
| kafka-only PerKey w500 h4 | 1,696.0 | 1,696.0 | 1,696.0 | **1,696.0** | 0.0 | 20.5 / 0 / 0 | 69,068.0 |
| kafka-only Strict w1 h0 | 800.0 | 800.0 | 800.0 | **800.0** | 0.0 | 10 / 0 / 0 | 22,988.0 |
| kafka-only Strict w1 h1 | 1,104.0 | 1,104.0 | 1,104.0 | **1,104.0** | 0.0 | 15 / 0 / 0 | 22,495.0 |
| kafka-only Strict w1 h4 | 1,696.0 | 1,696.0 | 1,696.0 | **1,696.0** | 0.0 | 20 / 0 / 0 | 19,547.0 |

#### Steady term, null / Kafka / dead-letter (`term`)

| Cell (B/msg) | run 1 | run 2 | run 3 | median | spread | Gen0/1/2 per 100k (median) | rate/s (median) |
|---|---:|---:|---:|---:|---:|---|---:|
| term None null b500 h1 | 192.8 | 192.8 | 192.8 | **192.8** | 0.0 | 0 / 0 / 0 | 1,962,762 |
| term None kafka b500 h1 | 1,544.6 | 1,538.9 | 1,548.7 | **1,544.6** | 9.8 | 5 / 0 / 0 | 569,662 |
| term None dlq b500 h1 | 3,683.8 | 3,598.8 | 3,695.1 | **3,683.8** | 96.3 | 20 / 0 / 0 | 354,386 |
| term PerKey null b500 h1 | 244.1 | 244.1 | 244.1 | **244.1** | 0.0 | 0 / 0 / 0 | 3,807,212 |
| term PerKey kafka b500 h1 | 1,561.6 | 1,566.4 | 1,745.5 | **1,566.4** | 183.9 | 19 / 0 / 0 | 15,181.0 |
| term PerKey dlq b500 h1 | 3,849.4 | 3,841.0 | 4,015.7 | **3,849.4** | 174.7 | 44 / 0 / 0 | 14,326.0 |
| term Strict null b500 h1 | 192.9 | 192.9 | 192.9 | **192.9** | 0.0 | 0 / 0 / 0 | 3,903,741 |
| term Strict kafka b500 h1 | 2,505.2 | 2,490.6 | 2,803.0 | **2,505.2** | 312.4 | 5 / 0 / 0 | 22,345.0 |
| term Strict dlq b500 h1 | 4,852.0 | 4,819.0 | 4,989.2 | **4,852.0** | 170.2 | 60 / 0 / 0 | 18,862.0 |

#### Idle (`idle`)

| Cell (B/s) | run 1 | run 2 | run 3 | median | spread | Gen0/1/2 per 100k (median) | rate/s (median) |
|---|---:|---:|---:|---:|---:|---|---:|
| idle None null interval 1s | 3,475.2 | 5,739.2 | 6,294.1 | **5,739.2** | 2,818.9 | 0 / 0 / 0 | 1.0 |
| idle None kafka interval 1s | 3,924.0 | 5,831.7 | 3,839.2 | **3,924.0** | 1,992.5 | 0 / 0 / 0 | 1.0 |
| idle PerKey null interval 1s | 2,531.5 | 5,630.1 | 2,493.1 | **2,531.5** | 3,137.0 | 0 / 0 / 0 | 1.0 |
| idle PerKey kafka interval 1s | 2,573.6 | 3,461.9 | 10,997.3 | **3,461.9** | 8,423.7 | 0 / 0 / 0 | 1.0 |
| idle Strict null interval 1s | 3,050.4 | 7,723.7 | 8,365.6 | **7,723.7** | 5,315.2 | 0 / 0 / 0 | 1.0 |
| idle Strict kafka interval 1s | 2,498.7 | 2,759.5 | 7,076.0 | **2,759.5** | 4,577.3 | 0 / 0 / 0 | 1.0 |

#### Steady term, Database destination (`term`)

| Cell (B/msg) | run 1 | run 2 | run 3 | median | spread | Gen0/1/2 per 100k (median) | rate/s (median) |
|---|---:|---:|---:|---:|---:|---|---:|
| term None database b500 h1 | 8,930.8 | 9,011.8 | 9,253.5 | **9,011.8** | 322.7 | 110 / 5 / 5 | 4,794.0 |
| term PerKey database b500 h1 | 8,916.8 | 9,095.9 | 9,199.9 | **9,095.9** | 283.1 | 110 / 30 / 0 | 4,368.0 |
| term Strict database b500 h1 | 8,918.9 | 9,018.5 | 9,262.0 | **9,018.5** | 343.1 | 110 / 0 / 0 | 5,409.0 |

#### Steady session (`session`)

| Cell (B/row, read + commit) | run 1 | run 2 | run 3 | median | spread | read median | commit median | Gen0/1/2 per 100k (median) | rate/s (median) |
|---|---:|---:|---:|---:|---:|---:|---:|---|---:|
| session None batch 500 h1 | 597.7 | 598.0 | 597.7 | **597.7** | 0.3 | 568.0 | 29.0 | 2 / 0 / 0 | 133,156 |
| session None batch 500 h3 | 958.2 | 958.8 | 958.0 | **958.2** | 0.8 | 929.0 | 29.0 | 7 / 1 / 0 | 122,974 |
| session None batch 100 h1 | 631.2 | 631.2 | 631.3 | **631.2** | 0.1 | 602.0 | 29.0 | 3 / 1 / 0 | 49,616.0 |
| session None batch 100 h3 | 991.4 | 991.6 | 991.4 | **991.4** | 0.2 | 962.0 | 29.0 | 8 / 1 / 0 | 49,455.0 |
| session None batch 10 h1 | 1,154.1 | 1,154.0 | 1,153.7 | **1,154.0** | 0.4 | 934.0 | 220.0 | 10 / 0 / 0 | 12,538.0 |
| session None batch 10 h3 | 1,514.1 | 1,514.1 | 1,514.0 | **1,514.1** | 0.1 | 1,294.0 | 220.0 | 20 / 0 / 0 | 14,639.0 |
| session None batch 1 h1 | 6,329.0 | 6,329.0 | 6,325.3 | **6,329.0** | 3.7 | 4,201.0 | 2,128.0 | 80 / 0 / 0 | 1,614.0 |
| session None batch 1 h3 | 6,689.0 | 6,687.9 | 6,689.0 | **6,689.0** | 1.1 | 4,561.0 | 2,128.0 | 80 / 0 / 0 | 1,512.0 |
| session PerKey batch 500 h1 | 601.3 | 601.3 | 601.7 | **601.3** | 0.4 | 572.0 | 29.0 | 3 / 1 / 0 | 116,249 |
| session PerKey batch 500 h3 | 961.4 | 961.3 | 961.4 | **961.4** | 0.1 | 932.0 | 29.0 | 6 / 1 / 0 | 134,376 |
| session PerKey batch 100 h1 | 651.3 | 651.4 | 651.4 | **651.4** | 0.1 | 622.0 | 29.0 | 3 / 1 / 0 | 113,562 |
| session PerKey batch 100 h3 | 1,011.4 | 1,011.4 | 1,011.3 | **1,011.4** | 0.1 | 982.0 | 29.0 | 8 / 1 / 0 | 77,212.0 |
| session PerKey batch 10 h1 | 1,358.3 | 1,358.3 | 1,358.2 | **1,358.3** | 0.1 | 1,138.0 | 220.0 | 20 / 0 / 0 | 13,145.0 |
| session PerKey batch 10 h3 | 1,718.3 | 1,718.3 | 1,718.3 | **1,718.3** | 0.0 | 1,498.0 | 220.0 | 20 / 0 / 0 | 15,031.0 |
| session PerKey batch 1 h1 | 8,369.2 | 8,369.4 | 8,367.1 | **8,369.2** | 2.3 | 6,241.0 | 2,128.0 | 100 / 0 / 0 | 1,482.0 |
| session PerKey batch 1 h3 | 8,729.3 | 8,729.5 | 8,729.2 | **8,729.3** | 0.3 | 6,601.0 | 2,128.0 | 100 / 0 / 0 | 1,553.0 |
| session Strict batch 500 h1 | 599.8 | 600.0 | 600.7 | **600.0** | 0.9 | 571.0 | 29.0 | 3 / 1 / 0 | 220,005 |
| session Strict batch 500 h3 | 959.9 | 959.9 | 961.6 | **959.9** | 1.7 | 931.0 | 29.0 | 6 / 1 / 0 | 207,073 |
| session Strict batch 100 h1 | 638.6 | 638.5 | 638.6 | **638.6** | 0.1 | 609.0 | 29.0 | 3 / 1 / 0 | 118,916 |
| session Strict batch 100 h3 | 1,000.0 | 1,000.0 | 999.4 | **1,000.0** | 0.6 | 971.0 | 29.0 | 7 / 1 / 0 | 116,100 |
| session Strict batch 10 h1 | 1,227.7 | 1,227.7 | 1,227.5 | **1,227.7** | 0.2 | 1,008.0 | 220.0 | 10 / 0 / 0 | 13,851.0 |
| session Strict batch 10 h3 | 1,586.0 | 1,587.7 | 1,587.7 | **1,587.7** | 1.7 | 1,368.0 | 220.0 | 20 / 0 / 0 | 14,154.0 |
| session Strict batch 1 h1 | 7,065.1 | 7,065.1 | 7,065.1 | **7,065.1** | 0.0 | 4,937.0 | 2,128.0 | 80 / 0 / 0 | 1,677.0 |
| session Strict batch 1 h3 | 7,425.2 | 7,425.2 | 7,422.9 | **7,425.2** | 2.3 | 5,297.0 | 2,128.0 | 100 / 0 / 0 | 1,476.0 |

#### Database send (`dest`)

| Cell (B/send) | run 1 | run 2 | run 3 | median | spread | Gen0/1/2 per 100k (median) | rate/s (median) |
|---|---:|---:|---:|---:|---:|---|---:|
| dest max-in-flight 1 h1 | 6,444.9 | 6,445.5 | 6,440.9 | **6,444.9** | 4.6 | 60 / 5 / 5 | 6,055.0 |
| dest max-in-flight 1 h3 | 6,661.9 | 6,661.8 | 6,660.0 | **6,661.8** | 1.9 | 60 / 5 / 0 | 5,440.0 |
| dest max-in-flight 8 h1 | 6,811.3 | 6,812.1 | 6,811.6 | **6,811.6** | 0.8 | 60 / 5 / 0 | 15,140.0 |
| dest max-in-flight 8 h3 | 7,027.4 | 7,027.6 | 7,027.9 | **7,027.6** | 0.5 | 65 / 5 / 5 | 16,028.0 |

#### Fixed cost (`fixed`)

| Cell (B/call) | run 1 | run 2 | run 3 | median | spread | B/row median | Gen0/1/2 per 100k (median) | rate/s (median) |
|---|---:|---:|---:|---:|---:|---:|---|---:|
| None read 1 | 4,193.5 | 4,193.5 | 4,193.5 | **4,193.5** | 0.0 | 4,194.0 | 0 / 0 / 0 | 5,110.0 |
| None read 10 | 9,265.5 | 9,265.5 | 9,266.6 | **9,265.5** | 1.1 | 927.0 | 0 / 0 / 0 | 5,178.0 |
| None read 100 | 60,090.6 | 60,094.5 | 60,092.7 | **60,092.7** | 3.9 | 601.0 | 500 / 0 / 0 | 4,167.0 |
| None read 500 | 284,285 | 284,229 | 284,211 | **284,229** | 74.8 | 568.0 | 3500 / 0 / 0 | 2,277.0 |
| None read 1000 | 564,670 | 564,669 | 564,729 | **564,670** | 59.7 | 565.0 | 6500 / 0 / 0 | 1,545.0 |
| None commit-remove 1 | 2,129.1 | 2,128.6 | 2,128.6 | **2,128.6** | 0.5 | – | 0 / 0 / 0 | 7,260.0 |
| None commit-remove 10 | 2,200.6 | 2,200.6 | 2,201.1 | **2,200.6** | 0.5 | – | 0 / 0 / 0 | 7,541.0 |
| None commit-remove 100 | 2,921.1 | 2,921.1 | 2,920.6 | **2,921.1** | 0.5 | – | 200 / 0 / 0 | 7,073.0 |
| None commit-fail (tx) 10 | 5,208.7 | 5,217.9 | 5,211.5 | **5,211.5** | 9.2 | – | 0 / 0 / 0 | 3,804.0 |
| None read 500 exclude 0 | 284,294 | 284,251 | 284,191 | **284,251** | 103.3 | 569.0 | 3000 / 0 / 0 | 2,202.0 |
| None read 500 exclude 500 | 284,328 | 284,182 | 284,353 | **284,328** | 170.9 | 569.0 | 3000 / 0 / 0 | 1,625.0 |
| None read 500 exclude 5000 | 284,307 | 284,358 | 284,364 | **284,358** | 56.6 | 569.0 | 4000 / 0 / 0 | 1,046.0 |
| None read-empty | 3,600.3 | 3,600.3 | 3,600.9 | **3,600.3** | 0.6 | – | 0 / 0 / 0 | 5,392.0 |
| None check | 2,832.3 | 2,832.3 | 2,832.6 | **2,832.3** | 0.3 | – | 0 / 0 / 0 | 6,980.0 |
| None summary | 1,752.3 | 1,752.3 | 1,752.3 | **1,752.3** | 0.0 | – | 0 / 0 / 0 | 5,806.0 |
| None stats | 5,430.6 | 5,426.0 | 5,428.3 | **5,428.3** | 4.6 | – | 0 / 0 / 0 | 2,524.0 |
| PerKey read 1 | 5,595.2 | 5,593.5 | 5,593.5 | **5,593.5** | 1.7 | 5,594.0 | 0 / 0 / 0 | 3,918.0 |
| PerKey read 10 | 10,665.5 | 10,667.2 | 10,665.5 | **10,665.5** | 1.7 | 1,067.0 | 0 / 0 / 0 | 3,190.0 |
| PerKey read 100 | 61,493.8 | 61,495.8 | 61,492.2 | **61,493.8** | 3.6 | 615.0 | 1000 / 0 / 0 | 2,395.0 |
| PerKey read 500 | 285,494 | 285,293 | 285,354 | **285,354** | 200.9 | 571.0 | 3500 / 0 / 0 | 1,070.0 |
| PerKey read 1000 | 565,500 | 565,670 | 565,605 | **565,605** | 169.9 | 566.0 | 6500 / 0 / 0 | 712.0 |
| PerKey commit-remove 1 | 2,129.1 | 2,128.6 | 2,129.1 | **2,129.1** | 0.5 | – | 200 / 0 / 0 | 7,496.0 |
| PerKey commit-remove 10 | 2,200.6 | 2,200.6 | 2,200.6 | **2,200.6** | 0.0 | – | 0 / 0 / 0 | 5,892.0 |
| PerKey commit-remove 100 | 2,920.6 | 2,920.6 | 2,918.4 | **2,920.6** | 2.2 | – | 0 / 0 / 0 | 7,065.0 |
| PerKey commit-fail (tx) 10 | 5,209.6 | 5,218.8 | 5,218.8 | **5,218.8** | 9.2 | – | 0 / 0 / 0 | 3,684.0 |
| PerKey read-empty | 5,000.3 | 5,000.3 | 5,000.3 | **5,000.3** | 0.0 | – | 100 / 0 / 0 | 4,238.0 |
| PerKey check | 2,832.3 | 2,832.3 | 2,832.3 | **2,832.3** | 0.0 | – | 0 / 0 / 0 | 6,850.0 |
| PerKey summary | 1,752.3 | 1,752.3 | 1,752.3 | **1,752.3** | 0.0 | – | 0 / 0 / 0 | 6,805.0 |
| PerKey stats | 5,925.3 | 5,916.1 | 5,934.5 | **5,925.3** | 18.4 | – | 0 / 0 / 0 | 2,189.0 |
| Strict read 1 | 4,289.5 | 4,289.5 | 4,289.5 | **4,289.5** | 0.0 | 4,290.0 | 0 / 0 / 0 | 4,926.0 |
| Strict read 10 | 9,361.5 | 9,361.5 | 9,361.5 | **9,361.5** | 0.0 | 936.0 | 0 / 0 / 0 | 4,702.0 |
| Strict read 100 | 60,186.5 | 60,186.5 | 60,188.4 | **60,186.5** | 1.9 | 602.0 | 1000 / 0 / 0 | 4,141.0 |
| Strict read 500 | 284,757 | 284,329 | 284,338 | **284,338** | 427.7 | 569.0 | 3500 / 0 / 0 | 2,250.0 |
| Strict read 1000 | 565,924 | 564,815 | 564,729 | **564,815** | 1,195.6 | 565.0 | 7000 / 0 / 0 | 1,548.0 |
| Strict commit-remove 1 | 2,128.6 | 2,128.6 | 2,128.6 | **2,128.6** | 0.0 | – | 0 / 0 / 0 | 7,722.0 |
| Strict commit-remove 10 | 2,200.6 | 2,200.6 | 2,201.1 | **2,200.6** | 0.5 | – | 0 / 0 / 0 | 7,647.0 |
| Strict commit-remove 100 | 2,920.6 | 2,920.6 | 2,920.6 | **2,920.6** | 0.0 | – | 0 / 0 / 0 | 7,425.0 |
| Strict commit-fail (tx) 10 | 5,220.7 | 5,210.6 | 5,216.3 | **5,216.3** | 10.1 | – | 0 / 0 / 0 | 2,930.0 |
| Strict read-empty | 3,696.3 | 3,696.3 | 3,696.3 | **3,696.3** | 0.0 | – | 0 / 0 / 0 | 5,009.0 |
| Strict check | 2,832.3 | 2,832.3 | 2,831.0 | **2,832.3** | 1.3 | – | 0 / 0 / 0 | 6,865.0 |
| Strict summary | 1,752.3 | 1,752.3 | 1,752.3 | **1,752.3** | 0.0 | – | 0 / 0 / 0 | 6,011.0 |
| Strict stats | 5,599.0 | 5,622.8 | 5,601.6 | **5,601.6** | 23.8 | – | 0 / 0 / 0 | 2,312.0 |

### Heap over time and churn

Heap after full GC, fresh row objects. Absolute heap depends on what earlier scenarios of the same `all` process left behind (run 2 sat higher throughout), so trend within a run is the figure, not the level.

| Scenario | Run 1 first / last / max (KB) | Run 2 first / last / max (KB) | Run 3 first / last / max (KB) | Trend |
|---|---|---|---|---|
| flat term PerKey kafka b500 h1, 1M msgs, every 100k | 2,524 / 777 / 2,567 | 3,010 / 1,133 / 3,010 | 2,512 / 807 / 2,615 | flat (checkpoints 1-9 within ±60 KB, the 1M checkpoint drops) |
| flat session None batch 500, 1M rows, every 100k | 5,029 / 5,013 / 5,037 | 9,647 / 9,507 / 9,647 | 5,039 / 5,019 / 5,041 | flat |
| flat dest MaxInFlight 8, 200k sends, every 20k | 7,584 / 7,593 / 7,593 | 14,410 / 14,329 / 14,412 | 7,585 / 7,551 / 7,589 | flat (+9 KB in run 1, falling in runs 2-3) |
| churn PerKey, 4,000 sessions, heap at 1k / 2k / 3k / 4k | 5,068 / 5,075 / 5,074 / 5,081 | 9,613 / 9,581 / 9,581 / 9,482 | 5,019 / 5,076 / 5,038 / 5,057 | flat, relay backends left 0 at every quarter in every run |

No rising trend.

### Notes

- **Spike cross-check** (earlier Kafka and PostgreSQL allocation spikes). Within ±15%: kafka-only (800 / 1,104 / 1,696 vs 800 / 1,124 / 1,580, h4 +7%), term null (+8 B, inside the core-routing +16 B/msg), term Kafka (None +5.5%, PerKey 0%, Strict −5.5%), dead-letter None (3,684 vs ~3,790), session steady every listed cell (+8 to +168 B/row, ≤ 2%; rows are route NULL so the +56 B/row route read is not paid), fixed cost (read 1 +2-3%, commit, fail, check and summary within 15 B, stats within 3%, read-empty vs the spike +1-3%), Database send (within 3 B/send), BDN session, Kafka send (h4 +9%), DeliveryTerm None (+3-6%), Database send Send1 (+14%). Term Database has no spike cell (new). Flat and churn absolute heaps differ from the spike (different process contents, spike session/churn under Server GC). Both are flat, as in the spike.
- **Idle B/s is a high-noise cell.** Spread across runs is 2.0-8.4 KB/s. Single rounds range 1.7-11.4 KB/s. `--profile` (diagnosis only, not in the tables) attributes it to the `DeliveryTerm.WaitAsync` CTS + timer + `OperationCanceledException` path (`src/Pernix.OutboxRelay.Core/Delivery/DeliveryTerm.cs:751-763`), ~2 waits/s at 1 s polling: None = `TimerQueueTimer`, `OperationCanceledException`, channel `QueueUserWorkItemCallback` in equal shares, Strict = `AsyncStateMachineBox<WaitAsync>`. The spike's 18-23 KB/s is not reproduced on `2671eb5` with this harness (medians 2.5-7.7 KB/s). A change claiming an idle reduction should use a longer window (`--seconds 120`) and at least 5 interleaved runs per side.
- **BDN DeliveryTerm PerKey Kafka.** 1,603 B agrees with GC-delta 1,566 B (and spike GC-delta 1,568 B). The spike BDN 2,267 B included the idle wait between gated releases under its in-process toolchain.
- **BDN vs GC-delta, shared cells.** Kafka send = kafka-only (800 / 1,105 / 1,696 vs 800 / 1,104 / 1,696). Term: None null +2.7%, PerKey null +0.4%, PerKey Kafka +2.3%, Database +1.0-1.3%, None Kafka +6.4%. Database send Send8 +1.2-1.3%, an excess of ~627 B (h1) and ~739 B (h3) per invocation of 8 sends. Database send Send1 is +1,291 B (h1) and +1,183 B (h3) over GC-delta `dest max-in-flight 1`, and at MaxInFlight 1 an invocation is one send. The async state-machine box of the start-then-await wrapper explains only part of the Send1 gap. The rest, ~650 B at h1 and ~450 B at h3, is not attributed. Session per-call methods (Read1, ReadEmpty, Commit1, Check PerKey, Summary) sit a constant ~375 B above the `fixed` per-call figures (Check None +272 B), the BDN per-invocation cost, as in the spike (Read1 4,519 vs 4,112). Per-row methods match (Read500 569 vs 568, Commit100 33 vs 29 + per-call share). Three gaps are outside the design's ±5% BDN vs GC-delta target: Database send Send1 (+20% h1, +18% h3), the session per-call methods (~+375 B, ~+9% on Read1) and term None Kafka (+6.4%). They are measured figures, and their cause is not attributed beyond what this note shows.
- **Excluded-id reads (`fixed`) run for None only.** `PostgreSqlOutboxSession.cs:137` rejects a non-empty exclude list for PerKey and Strict.
- **Loop wait throws, product finding.** `DeliveryTerm.cs:759` `WaitToReadAsync(wake.Token)` throws `OperationCanceledException` on each wait timeout, caught at `:761`. BDN Exceptions/msg above: Database 0.99-1.02, PerKey Kafka 0.066, null ≤ 0.0035.
- **Harness reworked after measurement** (`d53bf6d` and this commit): term cell order, churn backend delta column, migration loader, `--headers` validation; no baseline figure affected.
- **`--job dry`** adds a second job beside `BenchConfig`, so each dry case runs twice. Not used for these figures.
- **Gating.** Later changes gate on interleaved base vs branch runs on one machine ([`docs/development.md#measuring-allocations`](../../docs/development.md#measuring-allocations): median per cell, regression and improvement rules against base spread), not on these absolute numbers, which are machine dependent. This initial baseline section stays unchanged. Each later allocation change appends a dated section `## <date> — <change>` on merge with its commit, machine and the cells it changed, before and after.

## 2026-11-09 — Kafka routes

| Item | Value |
|---|---|
| Base commit | `d3b9b11` |
| Branch commit | `bc5f1c8` |
| Date | 2026-11-09, runs 10:40:51-11:00:57 UTC |
| Machine | Apple M5, 10 cores, 24 GB RAM, macOS 27.2 (26B123), arm64, .NET 10.0.2 / SDK 10.0.102, Release, header `gcServer=False latency=Batch` |
| Harness | Each side its own: base = `d3b9b11` benchmarks, branch = branch benchmarks (`Infrastructure/KafkaMock.cs` constructor change only) |
| Runs | 3 interleaved per side, one process each, `reported` round, median |

Changed cells, before = base median, after = branch median (B/msg). Every cell is a measured reduction.

| Cell (B/msg) | before | after | Δ |
|---|---:|---:|---:|
| kafka-only None w500 h0 | 800.0 | 656.0 | -144.0 |
| kafka-only None w500 h1 | 1,104.0 | 840.0 | -264.0 |
| kafka-only None w500 h4 | 1,696.0 | 1,048.0 | -648.0 |
| kafka-only PerKey w500 h0 | 800.0 | 656.0 | -144.0 |
| kafka-only PerKey w500 h1 | 1,104.0 | 840.0 | -264.0 |
| kafka-only PerKey w500 h4 | 1,696.0 | 1,048.0 | -648.0 |
| kafka-only Strict w1 h0 | 800.0 | 656.0 | -144.0 |
| kafka-only Strict w1 h1 | 1,104.0 | 840.0 | -264.0 |
| kafka-only Strict w1 h4 | 1,696.0 | 1,048.0 | -648.0 |
| term None kafka b500 h1 | 1,488.8 | 1,254.8 | -234.0 |
| term None dlq b500 h1 | 3,674.7 | 2,594.7 | -1,080.0 |
| term PerKey kafka b500 h1 | 1,629.8 | 1,377.1 | -252.7 |
| term PerKey dlq b500 h1 | 3,921.2 | 2,847.5 | -1,073.7 |
| term Strict kafka b500 h1 | 2,590.9 | 2,366.9 | -224.0 |
| term Strict dlq b500 h1 | 4,936.1 | 3,821.1 | -1,115.0 |

## 2026-11-09 — Database routes

| Item | Value |
|---|---|
| Base commit | `f19bd4e` (dead-letter base: `f19bd4e` + harness commit `19f820e` cherry-picked) |
| Branch commit | `7419679` |
| Date | 2026-11-09, runs 14:41:46-14:59:47 UTC |
| Machine | Apple M5, 10 cores, 24 GB RAM, macOS 27.2 (26B123), arm64, .NET 10.0.2 / SDK 10.0.102, Release, header `gcServer=False latency=Batch` |
| Harness | Each side its own: base = `f19bd4e` benchmarks, dead-letter base = `f19bd4e` + `19f820e` (`dest-dlq`), branch = branch benchmarks (`Infrastructure/PgEnvironment.cs` constructor change, `dest-dlq` cell, `--prepared` probe off by default) |
| Runs | 3 interleaved per side, one process each, `reported` round, median |

Changed cells, before = base median, after = branch median. Every cell is a measured reduction.

| Cell | before | after | Δ |
|---|---:|---:|---:|
| dest max-in-flight 1 h1 (B/send) | 6,444.7 | 1,841.7 | -4,603.0 |
| dest max-in-flight 1 h3 (B/send) | 6,659.8 | 2,057.2 | -4,602.6 |
| dest max-in-flight 8 h1 (B/send) | 6,811.8 | 2,219.8 | -4,592.0 |
| dest max-in-flight 8 h3 (B/send) | 7,027.9 | 2,436.0 | -4,591.9 |
| term None database b500 h1 (B/msg) | 9,150.5 | 4,470.8 | -4,679.7 |
| term PerKey database b500 h1 (B/msg) | 9,135.4 | 4,411.9 | -4,723.5 |
| term Strict database b500 h1 (B/msg) | 9,114.8 | 4,429.7 | -4,685.1 |
| dest-dlq max-in-flight 1 h1 (B/send) | 6,502.1 | 1,897.3 | -4,604.8 |
| dest-dlq max-in-flight 1 h3 (B/send) | 6,717.9 | 2,115.6 | -4,602.3 |
| dest-dlq max-in-flight 8 h1 (B/send) | 6,867.7 | 2,276.0 | -4,591.7 |
| dest-dlq max-in-flight 8 h3 (B/send) | 7,083.8 | 2,491.9 | -4,591.9 |
| BDN Send1 h1 (B/op) | 7,736 | 3,047 | -4,689 |
| BDN Send1 h3 (B/op) | 7,951 | 3,263 | -4,688 |
| BDN Send8 h1 (B/op) | 6,886 | 2,335 | -4,551 |
| BDN Send8 h3 (B/op) | 7,098 | 2,601 | -4,497 |

Heap over time, branch, `flat --target dest` (max in flight 8, 200k sends, heap after full GC every 20k), first / last / max (KB): run 1 12,145 / 12,154 / 12,154, run 2 11,743 / 11,751 / 11,751, run 3 12,088 / 12,092 / 12,093. Flat.

Retained slot state: slots per table ≤ `MaxInFlight`. Server prepared statements: 1 per connector, at most `MaxInFlight` connectors per instance, stable from warm-up to the last round and from the first to the last flat checkpoint (`--prepared` probe, 3 runs).

Max-in-flight 8 gate: GC-delta `dest` tripped for h1 and h3. The max-in-flight 8 minus max-in-flight 1 difference is 11.0 B (h1) and 10.7 B (h3) per send larger on the branch than on base, 8.2 B over the limit. BDN `Send8` vs `Send1` not tripped. Cause (profiled, diagnosis only): Npgsql async read completion (`NpgsqlConnector.ReadMessageLong`, `NpgsqlReadBuffer.EnsureLong` state machine boxes inside `ExecuteNonQueryAsync`), more frequent at max in flight 8 because the branch sends faster. The prepare path shows 0 sampled bytes. Decision: no source change, recorded as a per-send cost of about 11 B/send at max in flight > 1. The −4.6 KB/send reduction stands.

## 2026-11-09 — Core loop

| Item | Value |
|---|---|
| Base commit | `2e8f4c0` |
| Branch commit | `b3f4fc8` |
| Date | runs 2026-11-09 20:23:39 UTC to 2026-11-10 04:20:07 UTC |
| Machine | Apple M5, 10 cores, 24 GB RAM, macOS 27.2 (26B123), arm64, .NET 10.0.2 / SDK 10.0.102, Release, header `gcServer=False latency=Batch` |
| Harness | Each side its own: base = `2e8f4c0` benchmarks (`Task<DeliveryResult>`), branch = branch benchmarks (`ValueTask<DeliveryResult>` type adaptation only) |
| Runs | 3 interleaved per side (idle 5, BDN `DeliveryTerm` 4), one process each, `reported` round or BDN `Allocated`, median |

Changed cells, before = base median, after = branch median. Every cell listed is a measured reduction, except the 2 marked rows.

| Cell | before | after | Δ |
|---|---:|---:|---:|
| term None kafka b500 h1 (B/msg) | 1,264.0 | 864.5 | -399.5 |
| term PerKey kafka b500 h1 (B/msg) | 1,294.0 | 823.3 | -470.7 |
| term Strict kafka b500 h1 (B/msg) | 2,227.3 | 880.4 | -1,346.9 |
| term PerKey null b500 h1 (B/msg) | 244.1 | 192.9 | -51.2 |
| term None dlq b500 h1 (B/msg) | 2,583.6 | 2,361.4 | -222.2 |
| term PerKey dlq b500 h1 (B/msg) | 2,765.1 | 2,469.9 | -295.2 |
| term Strict dlq b500 h1 (B/msg) | 3,688.5 | 2,528.4 | -1,160.1 |
| term None database b500 h1 (B/msg) | 4,307.0 | 1,985.2 | -2,321.8 |
| term PerKey database b500 h1 (B/msg) | 4,315.2 | 1,981.8 | -2,333.4 |
| term Strict database b500 h1 (B/msg) | 4,293.2 | 1,992.3 | -2,300.9 |
| kafka-only, all 9 cells h0 / h1 / h4 (B/msg) | 656.0 / 840.0 / 1,048.0 | 448.0 / 632.0 / 840.0 | -208.0 |
| dest max-in-flight 1 h1 (B/send) | 1,841.3 | 1,682.4 | -158.9 |
| dest max-in-flight 1 h3 (B/send) | 2,058.7 | 1,898.9 | -159.8 |
| dest max-in-flight 8 h1 (B/send) | 2,219.8 | 2,192.3 | -27.5 |
| dest max-in-flight 8 h3 (B/send) | 2,435.8 | 2,408.3 | -27.5 |
| dest-dlq max-in-flight 1 h1 (B/send) | 1,899.7 | 1,737.2 | -162.5 |
| dest-dlq max-in-flight 1 h3 (B/send) | 2,115.3 | 1,955.3 | -160.0 |
| dest-dlq max-in-flight 8 h1 (B/send) | 2,275.7 | 2,248.7 | -27.0 |
| dest-dlq max-in-flight 8 h3 (B/send) | 2,492.0 | 2,464.8 | -27.2 |
| idle None null interval 1s (B/s) | 3,155.0 | 96.8 | -3,058.2 |
| idle None kafka interval 1s (B/s) | 3,002.1 | 107.6 | -2,894.5 |
| idle PerKey null interval 1s (B/s) | 4,832.3 | 108.6 | -4,723.7 |
| idle PerKey kafka interval 1s (B/s) | 4,921.5 | 108.0 | -4,813.5 |
| idle Strict null interval 1s (B/s), not a measured reduction, see note | 4,347.9 | 94.1 | -4,253.8 |
| idle Strict kafka interval 1s (B/s) | 3,793.3 | 80.7 | -3,712.6 |
| BDN DeliveryTerm None Kafka (B/msg) | 1,315 | 857 | -458 |
| BDN DeliveryTerm None Database (B/msg) | 4,476 | 1,985 | -2,492 |
| BDN DeliveryTerm PerKey Null (B/msg) | 244 | 193 | -51 |
| BDN DeliveryTerm PerKey Kafka (B/msg) | 1,338 | 826 | -512 |
| BDN DeliveryTerm PerKey Database (B/msg) | 4,471 | 1,982 | -2,489 |
| BDN KafkaSendBenchmarks Send h0 / h1 / h4 (B/msg) | 656 / 840 / 1,048 | 448 / 632 / 840 | -208 |
| BDN DatabaseSendBenchmarks Send1 MaxInFlight 1 h1 (B/send), regressed, accepted, see note | 3,047 | 3,079 | +32 |

No measured change: term None null and Strict null (192.8 / 192.9 B/msg, expected reduction corrected: the null destination completes synchronously on both sides, so there is no async box to remove). Also BDN DeliveryTerm None Null (193 / 193), and BDN DatabaseSendBenchmarks Send1 h3 (3,263 / 3,295, base spread 131), Send8 h1 (2,341 / 2,348) and Send8 h3 (2,629 / 2,623).

Exceptions/msg, BDN `DeliveryTermBenchmarks` (`// Exceptions:` line, already per message), base median → branch median: None Null 0.0002 → 0, None Kafka 0.0035 → 0, None Database 0.9529 → 0, PerKey Null 0.0002 → 0, PerKey Kafka 0.0657 → 0, PerKey Database 0.9350 → 0.

Heap over time, after full GC, first / last / max (KB). Every run is flat:

- `flat --target term` (PerKey kafka b500 h1, 1M msgs, every 100k), branch: run 1 2,863 / 1,232 / 2,863, run 2 2,837 / 1,234 / 2,863, run 3 2,841 / 1,237 / 2,868. Base: run 1 2,896 / 1,048 / 2,910, run 2 2,867 / 1,046 / 2,902, run 3 2,872 / 1,035 / 2,899. The 1M checkpoint drops, as in the initial baseline.
- `flat --target dest` (max in flight 8, 200k sends, every 20k), branch: run 1 11,763 / 11,772 / 11,772, run 2 11,935 / 11,946 / 11,947, run 3 11,942 / 11,943 / 11,952. Base: run 1 11,797 / 11,800 / 11,800, run 2 12,022 / 12,031 / 12,031, run 3 11,909 / 11,913 / 11,922.

BDN Send1 MaxInFlight 1 h1 +32 B (EventPipe `GCAllocationTick` trace, diagnosis only). Product +16 B: the pooled `SendCoreAsync` `StateMachineBox` (176 B) misses its cache on the single BDN thread and replaces base's 160 B Task box. Harness +16 B: the benchmark's own `SendAsync` box grows from 112 to 128 B through the wider `ValueTaskAwaiter`. Accepted: the two-part cause is confirmed and adds up to the measured +32. Product: +16 B box only on a single-thread pool miss, replacing base's Task box rather than adding to it. Harness: +16 B from the `ValueTask` awaiter width, not in the service. Service-shaped cells are what count: dest mif1 -159, term database -2.3 KB/msg.

idle Strict null: no measured change by rule (base spread 5,332 B/s). Every branch run is below every base run (max 136 vs min 2,105), and the profile shows the `WaitAsync` timer path gone.

Harness note: `--profile` samples 0 KB on this machine (the `AllocProfile` listener receives no `GCAllocationTick`). This defect is left for a later change. Use an EventPipe trace until it is fixed.

## 2026-11-10 — PostgreSQL session

| Item | Value |
|---|---|
| Base commit | `c49b74e` |
| Branch commit | `90da6de` (final head, re-measure). `term`, `dest`, `dest-dlq` and `flat --target term` figures are from `9e90230` |
| Date | re-measure 2026-11-10 12:27:18 UTC to 13:16:01 UTC. `term`, `dest`, `dest-dlq`, `flat --target term`: 09:30:29 UTC to 10:37:19 UTC |
| Machine | Apple M5, 10 cores, 24 GB RAM, macOS 27.2 (26B123), arm64, .NET 10.0.2 / SDK 10.0.102, Release, header `gcServer=False latency=Batch` |
| Harness | Same both sides: branch `benchmarks/` rsynced into the base worktree. `churn` forced-GC runs and `fixed` runs 4-5 use harness `fe5f338` (forced full GC before churn checkpoints). All other runs use the harness at `90da6de`, identical to base |
| Runs | 3 interleaved per side (`fixed` 5, `churn` 3 + 6 forced-GC), one process each, `reported` round, BDN `Allocated` or `DatabaseAllocationTests` table, median |

Re-measured at final head `90da6de`. Every cell that runs the session or the parser was re-run, with the same verdicts as at `9e90230` except `fixed` None stats and `churn`, below. `term`, `dest`, `dest-dlq` and `flat --target term` keep their `9e90230` figures, because their code paths are unchanged.

Changed cells, before = base median, after = branch median. Every cell listed is a measured reduction.

| Cell | before | after | Δ |
|---|---:|---:|---:|
| BDN SessionBenchmarks Read500 None (B/op) | 569 | 299 | -270 |
| BDN SessionBenchmarks Read1 None (B/op) | 4,568 | 1,758 | -2,810 |
| BDN SessionBenchmarks ReadEmpty None (B/op) | 3,976 | 1,435 | -2,541 |
| BDN SessionBenchmarks Commit100 None (B/op) | 33 | 25 | -8 |
| BDN SessionBenchmarks Commit1 None (B/op) | 2,502 | 1,668 | -834 |
| BDN SessionBenchmarks Check None (B/op) | 3,208 | 1,703 | -1,505 |
| BDN SessionBenchmarks Summary None (B/op) | 2,128 | 1,590 | -538 |
| BDN SessionBenchmarks Read500 PerKey (B/op) | 571 | 299 | -272 |
| BDN SessionBenchmarks Read1 PerKey (B/op) | 5,968 | 1,760 | -4,208 |
| BDN SessionBenchmarks ReadEmpty PerKey (B/op) | 5,376 | 1,436 | -3,940 |
| BDN SessionBenchmarks Commit100 PerKey (B/op) | 33 | 25 | -8 |
| BDN SessionBenchmarks Commit1 PerKey (B/op) | 2,502 | 1,669 | -833 |
| BDN SessionBenchmarks Check PerKey (B/op) | 3,208 | 1,702 | -1,506 |
| BDN SessionBenchmarks Summary PerKey (B/op) | 2,128 | 1,590 | -538 |
| BDN HeadersParserBenchmarks Parse One (B/op) | 152 | 96 | -56 |
| BDN HeadersParserBenchmarks Parse Three (B/op) | 400 | 248 | -152 |
| session None batch 500 h1 (B/row) | 598.1 | 320.2 | -277.9 |
| session None batch 500 h3 (B/row) | 958.8 | 472.2 | -486.6 |
| session None batch 100 h1 (B/row) | 631.1 | 328.2 | -302.9 |
| session None batch 100 h3 (B/row) | 991.9 | 480.9 | -511.0 |
| session None batch 10 h1 (B/row) | 1,154.1 | 541.8 | -612.3 |
| session None batch 10 h3 (B/row) | 1,514.1 | 693.7 | -820.4 |
| session None batch 1 h1 (B/row) | 6,327.3 | 2,679.8 | -3,647.5 |
| session None batch 1 h3 (B/row) | 6,688.5 | 2,832.3 | -3,856.2 |
| session PerKey batch 500 h1 (B/row) | 601.7 | 319.3 | -282.4 |
| session PerKey batch 500 h3 (B/row) | 961.6 | 471.4 | -490.2 |
| session PerKey batch 100 h1 (B/row) | 651.2 | 328.2 | -323.0 |
| session PerKey batch 100 h3 (B/row) | 1,011.3 | 480.4 | -530.9 |
| session PerKey batch 10 h1 (B/row) | 1,357.5 | 545.0 | -812.5 |
| session PerKey batch 10 h3 (B/row) | 1,717.5 | 697.0 | -1,020.5 |
| session PerKey batch 1 h1 (B/row) | 8,361.4 | 2,712.1 | -5,649.3 |
| session PerKey batch 1 h3 (B/row) | 8,729.4 | 2,872.3 | -5,857.1 |
| session Strict batch 500 h1 (B/row) | 600.4 | 320.1 | -280.3 |
| session Strict batch 500 h3 (B/row) | 959.7 | 472.2 | -487.5 |
| session Strict batch 100 h1 (B/row) | 638.4 | 328.7 | -309.7 |
| session Strict batch 100 h3 (B/row) | 999.7 | 481.2 | -518.5 |
| session Strict batch 10 h1 (B/row) | 1,227.7 | 545.8 | -681.9 |
| session Strict batch 10 h3 (B/row) | 1,587.7 | 697.8 | -889.9 |
| session Strict batch 1 h1 (B/row) | 7,065.0 | 2,718.5 | -4,346.5 |
| session Strict batch 1 h3 (B/row) | 7,425.1 | 2,869.0 | -4,556.1 |
| fixed None read 1 (B/call) | 4,193.5 | 1,385.5 | -2,808.0 |
| fixed None read 10 (B/call) | 9,265.5 | 4,049.5 | -5,216.0 |
| fixed None read 100 (B/call) | 60,144.6 | 30,744.9 | -29,399.7 |
| fixed None read 500 (B/call) | 284,593.2 | 149,775.0 | -134,818.2 |
| fixed None read 1000 (B/call) | 564,724.4 | 297,828.6 | -266,895.8 |
| fixed None commit-remove 1 (B/call) | 2,128.6 | 1,296.6 | -832.0 |
| fixed None commit-remove 10 (B/call) | 2,201.1 | 1,368.6 | -832.5 |
| fixed None commit-remove 100 (B/call) | 2,920.6 | 2,088.6 | -832.0 |
| fixed None commit-fail 10 (B/call) | 5,216.3 | 2,581.3 | -2,635.0 |
| fixed None read 500 exclude 0 (B/call) | 284,240.4 | 149,754.7 | -134,485.7 |
| fixed None read 500 exclude 500 (B/call) | 284,352.9 | 149,627.0 | -134,725.9 |
| fixed None read 500 exclude 5000 (B/call) | 284,387.9 | 149,637.4 | -134,750.5 |
| fixed None read-empty (B/call) | 3,600.9 | 1,064.3 | -2,536.6 |
| fixed None check (B/call) | 2,832.3 | 1,328.3 | -1,504.0 |
| fixed None summary (B/call) | 1,752.6 | 1,216.3 | -536.3 |
| fixed PerKey read 1 (B/call) | 5,593.5 | 1,385.5 | -4,208.0 |
| fixed PerKey read 10 (B/call) | 10,665.5 | 4,049.5 | -6,616.0 |
| fixed PerKey read 100 (B/call) | 61,492.2 | 30,690.4 | -30,801.8 |
| fixed PerKey read 500 (B/call) | 285,315.8 | 149,179.6 | -136,136.2 |
| fixed PerKey read 1000 (B/call) | 565,505.1 | 297,140.2 | -268,364.9 |
| fixed PerKey commit-remove 1 (B/call) | 2,128.6 | 1,296.6 | -832.0 |
| fixed PerKey commit-remove 10 (B/call) | 2,200.6 | 1,368.6 | -832.0 |
| fixed PerKey commit-remove 100 (B/call) | 2,920.6 | 2,088.6 | -832.0 |
| fixed PerKey commit-fail 10 (B/call) | 5,213.3 | 2,576.7 | -2,636.6 |
| fixed PerKey read-empty (B/call) | 5,000.3 | 1,064.3 | -3,936.0 |
| fixed PerKey check (B/call) | 2,832.3 | 1,328.3 | -1,504.0 |
| fixed PerKey summary (B/call) | 1,752.3 | 1,216.3 | -536.0 |
| fixed Strict read 1 (B/call) | 4,289.5 | 1,385.5 | -2,904.0 |
| fixed Strict read 10 (B/call) | 9,361.5 | 4,049.5 | -5,312.0 |
| fixed Strict read 100 (B/call) | 60,188.4 | 30,694.1 | -29,494.3 |
| fixed Strict read 500 (B/call) | 284,345.0 | 149,483.1 | -134,861.9 |
| fixed Strict read 1000 (B/call) | 564,769.0 | 297,814.9 | -266,954.1 |
| fixed Strict commit-remove 1 (B/call) | 2,128.6 | 1,296.6 | -832.0 |
| fixed Strict commit-remove 10 (B/call) | 2,200.6 | 1,368.6 | -832.0 |
| fixed Strict commit-remove 100 (B/call) | 2,920.6 | 2,088.6 | -832.0 |
| fixed Strict commit-fail 10 (B/call) | 5,217.2 | 2,582.2 | -2,635.0 |
| fixed Strict read-empty (B/call) | 3,696.3 | 1,064.3 | -2,632.0 |
| fixed Strict check (B/call) | 2,832.3 | 1,328.3 | -1,504.0 |
| fixed Strict summary (B/call) | 1,752.3 | 1,216.3 | -536.0 |
| dest-dlq max-in-flight 1 h1 (B/send) | 1,739.8 | 1,681.8 | -58.0 |
| dest-dlq max-in-flight 1 h3 (B/send) | 1,953.7 | 1,899.4 | -54.3 |
| dest-dlq max-in-flight 8 h1 (B/send) | 2,248.7 | 2,192.5 | -56.2 |
| dest-dlq max-in-flight 8 h3 (B/send) | 2,464.3 | 2,408.2 | -56.1 |
| DatabaseAllocationTests None Database (B/msg) | 5,756 | 4,478 | -1,278 |
| DatabaseAllocationTests None Kafka (B/msg) | 1,533 | 1,240 | -293 |
| DatabaseAllocationTests PerKey Database (B/msg) | 5,531 | 4,390 | -1,141 |
| DatabaseAllocationTests PerKey Kafka (B/msg) | 1,510 | 1,227 | -283 |
| DatabaseAllocationTests Strict Database (B/msg) | 5,451 | 4,310 | -1,141 |
| DatabaseAllocationTests Strict Kafka (B/msg) | 3,968 | 2,825 | -1,143 |

No measured change: BDN `HeadersParserBenchmarks` Escaped (152 / 152), `fixed` stats PerKey and Strict, every `term` cell and every `dest` cell (guards: `InMemorySession` or main sends). `fixed` None stats is regressed by rule over 5 runs per side, +8.1 B/call (5,430.7 → 5,438.8, base spread 2.4). Every branch run (5,436.8-5,439.2) is above every base run (5,429.4-5,431.8). 3 EventPipe trace pairs ran, and none reproduced the +8.1 B/call gap. In the `--rounds 20` pair, no per-call allocation has a frame in a changed file, and the one type above sampling noise is boxed `ConnectionState` from `Npgsql.NpgsqlConnection.set_FullState` on the unchanged stats path. The 8.1 B are not identified. Accepted: the stats file is unchanged and runs once per stats interval.

Heap over time, after full GC, first / last / max (KB). Every `flat` run is flat:

- `flat --target session` (None, batch 500, 1M rows, every 100k), branch: run 1 7,990 / 8,046 / 8,046, run 2 8,074 / 8,117 / 8,118, run 3 8,067 / 8,119 / 8,119. Base: run 1 7,977 / 8,040 / 8,040, run 2 7,996 / 8,049 / 8,049, run 3 8,114 / 8,160 / 8,160.
- `flat --target term` (PerKey kafka b500 h1, 1M msgs, every 100k), branch: run 1 2,856 / 1,226 / 2,891, run 2 2,859 / 1,233 / 2,869, run 3 2,858 / 1,235 / 2,911. Base: run 1 2,854 / 1,233 / 2,887, run 2 2,852 / 1,233 / 2,864, run 3 3,247 / 1,236 / 3,247. The 1M checkpoint drops, as in the initial baseline.
- `churn` (PerKey sessions, 4,000 sessions, heap at 1,000 / 2,000 / 3,000 / 4,000), harness `90da6de` (no forced GC), branch: run 1 7,887 / 7,887 / 7,887 / 7,907, run 2 7,959 / 7,958 / 8,139 / 8,413, run 3 8,007 / 8,007 / 8,007 / 8,311. Base: run 1 8,276 / 8,274 / 8,275 / 8,276, run 2 7,987 / 8,005 / 8,024 / 8,026, run 3 8,064 / 8,103 / 8,103 / 8,106.
- `churn`, harness `fe5f338` (forced full GC before each checkpoint), branch: run 1 7,976 / 7,978 / 7,979 / 7,977, run 2 8,054 / 8,054 / 8,228 / 8,119, run 3 8,970 / 8,154 / 8,154 / 8,153, run 4 8,041 / 8,056 / 8,055 / 8,077, run 5 8,024 / 8,026 / 8,025 / 8,024, run 6 8,062 / 8,077 / 8,077 / 8,295. Base: run 1 8,143 / 8,141 / 8,142 / 8,142, run 2 8,171 / 8,213 / 8,213 / 8,235, run 3 8,265 / 8,265 / 8,286 / 8,307, run 4 8,156 / 8,156 / 8,189 / 8,212, run 5 8,261 / 8,253 / 8,287 / 8,287, run 6 8,257 / 8,257 / 8,256 / 8,316.
- `churn --sessions 16000`, branch, harness `fe5f338`, heap at 4,000 / 8,000 / 12,000 / 16,000: 8,892 / 7,421 / 7,389 / 7,294.
- 0 relay backends at every quarter of every `churn` run, both sides. Branch runs 2 and 3 at `90da6de` (+455, +304 KB) and branch runs 2 and 6 at `fe5f338` (+174, +218 KB) cross the `GetTotalMemory` rule. Base runs do not. 17 gcdumps over 5 branch runs (b1 2, b2 3, c1-c3 4 each) show live bytes of 4.4-4.9 MB (bytes/1000, against `GetTotalMemory` 7,692-8,701 KB, bytes/1024). Second → last dump live bytes fall in every run with both: b2 −469 KB, c1 −498 KB, c2 −503 KB, c3 −327 KB (bytes/1000). In run b2 the 6 largest growers total +3,064 B: thread-pool state (`SharedArrayPoolThreadLocalArray[]` +1,368 B +3, `ThreadPoolWorkQueueThreadLocals` +72 B +1, `Thread` +72 B +1), `Object[]` +704 B +13, and 2 unexplained Npgsql +1 instances (`ColumnInfo[]` +664 B, `StateMachineBox<IBackendMessage, NpgsqlConnector+<ReadMessageLong>>` +184 B). No session, `OutboxRowReader`, `HeadersParser` or `PostgreSqlInsertSlot` instance is retained, and 1 Npgsql `PreparedStatementManager` is present, constant. Per-type diffs were read for b2, c1, c2 and c3. From the second dump on, no type grows in every run, and c1-c3 have no growing type from the second-to-last to the last dump. Sessions are unpooled, so prepared statements end with each connector and backend. A caught jump shows `GetTotalMemory` −770 KB against live −37,548 B, so the crossings are GC heap space, not live objects. Accepted as no retention.

## 2026-11-10 — Allocation guard

| Item | Value |
|---|---|
| Base commit | `2bb3ac1` |
| Branch commit | Verdict: set 1 on `c589b3f`, set 2 (dlq 10M and default `flat --target term`) on `418cdde`. Ceiling runs on `85e6bf6`. Later commits up to the final head change only docs and `tests/Pernix.OutboxRelay.Unit.Tests/Allocation/AllocationCeilings.cs` |
| Date | 2026-11-10. Verdict 17:07:52 UTC to 17:58 UTC. Ceiling runs 18:06:36 UTC to 19:56:53 UTC |
| Machine | Apple M5, 10 cores, 24 GB RAM, macOS 27.2 (26B123), arm64, .NET 10.0.2 / SDK 10.0.102, Release, header `gcServer=False latency=Batch runtimeEvents=off profile=False` |
| Harness | Same both sides: branch `benchmarks/` rsynced into the base worktree before set 1 and again after `418cdde` (forced full GC before flat term checkpoints) |
| Runs | Verdict: 3 interleaved per side, one process each, `reported` round, median. Ceilings: 5 guard runs plus 1 passing run |

Changed cells, before = base median, after = branch median. Every cell listed is a measured reduction (dead-letter header merge, `src/Pernix.OutboxRelay.Destination.Kafka/KafkaDeadLetterHeaders.cs`).

| Cell | before | after | Δ |
|---|---:|---:|---:|
| term None dlq b500 h1 (B/dlq-msg) | 2,366.1 | 2,024.1 | -342.0 |
| term PerKey dlq b500 h1 (B/dlq-msg) | 2,469.4 | 2,103.9 | -365.5 |
| term Strict dlq b500 h1 (B/dlq-msg) | 2,528.4 | 2,168.4 | -360.0 |
| kafka-dlq None w500 h0 (B/dlq-msg) | 1,732.9 | 1,408.0 | -324.9 |
| kafka-dlq None w500 h1 (B/dlq-msg) | 1,836.9 | 1,496.0 | -340.9 |
| kafka-dlq None w500 h4 (B/dlq-msg) | 2,292.9 | 1,904.1 | -388.8 |
| kafka-dlq PerKey w500 h0 (B/dlq-msg) | 1,732.9 | 1,408.0 | -324.9 |
| kafka-dlq PerKey w500 h1 (B/dlq-msg) | 1,836.9 | 1,496.0 | -340.9 |
| kafka-dlq PerKey w500 h4 (B/dlq-msg) | 2,292.9 | 1,904.1 | -388.8 |
| kafka-dlq Strict w1 h0 (B/dlq-msg) | 1,720.1 | 1,408.0 | -312.1 |
| kafka-dlq Strict w1 h1 (B/dlq-msg) | 1,824.1 | 1,496.0 | -328.1 |
| kafka-dlq Strict w1 h4 (B/dlq-msg) | 2,280.1 | 1,904.1 | -376.0 |

No measured change: the 9 other `term` cells (null, kafka, database) and all 9 `kafka` cells. `kafka-dlq` is a new verb (`KafkaOnlyScenario`, every send a dead letter).

Heap over time, after full GC, first / last / max (KB):

- `flat --target term` (PerKey kafka b500 h1, 1M msgs, every 100k), harness `418cdde`, branch: run 1 2,855 / 1,239 / 2,865, run 2 2,837 / 1,236 / 2,868, run 3 2,850 / 1,236 / 2,897. Base: run 1 2,840 / 1,231 / 2,893, run 2 2,857 / 1,245 / 2,887, run 3 2,858 / 1,237 / 2,866. Flat, 0 of 8 over margin in every run.
- `flat --target term --modes None --dests dlq --n 10000000` (10M msgs, every 100k), harness `418cdde`, branch: run 1 5,353 / 4,819 / 9,923, run 2 5,417 / 4,703 / 9,336, run 3 5,511 / 4,678 / 9,406. Base: run 1 6,355 / 4,707 / 8,938, run 2 5,449 / 4,684 / 10,101, run 3 5,255 / 4,877 / 10,211. Both sides cross the absolute rule with the same non-monotone shape. Relative rule (max after second − second): base median 3,782 KB, spread 1,531 KB, limit 5,313 KB, branch median 3,582 KB, pass. gcdumps: the typical → highest difference is in-flight dead-letter sends, bounded by the term's held-row capacity.

Harness: the `--profile` 0 KB defect found in the core loop section is fixed. The listener did receive the events, but the profile window (`RoundRunner` `Start()`/`Stop()`) missed the reported round. Samples are now attributed by event time to the reported round's measured window.

Allocation guard (`AllocationGuardTests`, explicit, not CI): 27 GC-delta ceilings plus `FlatTerm`, `FlatSession` and `Churn`, measured on this machine at `85e6bf6` over 5 runs, limit = median + margin. Ceilings: `tests/Pernix.OutboxRelay.Unit.Tests/Allocation/AllocationCeilings.cs`.

Post-rework guard run: harness `e57a866` adds shape prints. Later commits change only docs. The full guard ran on `e57a866`, 2026-11-10 21:22:24 UTC to 21:39:12 UTC, 16 min 48 s wall time: 7/7 facts passed, 27/27 cells pass. The ceilings are unchanged and hold on the shipped harness.

The shape prints (`SessionScenario.FlatAsync`, `FixedCostScenario.GroupAsync`, `TermScenario.IdleCellAsync`, `ChurnScenario`) run before any warm-up, checkpoint or measured window, so they are outside every measured window.

## 2026-11-11 — Database destination removed

| Item | Value |
|---|---|
| Measured commit | `135c542` (branch head). Later commits change only this file |
| Date | 2026-11-11, 15:53:44 UTC to 16:09:53 UTC, 16 min 2 s test duration |
| Machine | Apple M5, 10 cores, 24 GB RAM, macOS 27.2, arm64, .NET 10.0.14, header `gcServer=False latency=Batch runtimeEvents=off profile=False`, `pg=testcontainers postgres:17.11` |
| Runs | 1 explicit guard run (`AllocationGuardTests`, `--explicit only`), ceilings unchanged from `85e6bf6` except the 3 retired rows |

Retired, the cells no longer exist: `term * database b500 h1` (3 guard ceilings and the `--dests database` term run), `dest`, `dest-dlq`, `flat --target dest` and BDN `DatabaseSendBenchmarks`. `DatabaseAllocationTests` was deleted with the integration suite. No retired figure above this section is edited.

Guard re-run, kept cells, figure / ceiling / ceiling-run median. 7/7 facts passed, 24/24 cells pass.

| Cell | figure | ceiling | median |
|---|---:|---:|---:|
| term None null b500 h1 (B/msg) | 192.8 | 212.1 | 192.8 |
| term None kafka b500 h1 (B/msg) | 896.6 | 1,038.4 | 844.9 |
| term None dlq b500 h1 (B/dlq-msg) | 2,151.9 | 2,343.7 | 2,130.6 |
| term PerKey null b500 h1 (B/msg) | 192.9 | 212.2 | 192.9 |
| term PerKey kafka b500 h1 (B/msg) | 823.3 | 905.6 | 823.3 |
| term PerKey dlq b500 h1 (B/dlq-msg) | 2,103.1 | 2,311.9 | 2,101.7 |
| term Strict null b500 h1 (B/msg) | 192.9 | 212.2 | 192.9 |
| term Strict kafka b500 h1 (B/msg) | 880.4 | 968.4 | 880.4 |
| term Strict dlq b500 h1 (B/dlq-msg) | 2,168.4 | 2,385.2 | 2,168.4 |
| session None batch 500 h1 (B/row) | 319.9 | 352.3 | 320.3 |
| session PerKey batch 500 h1 (B/row) | 319.4 | 351.2 | 319.3 |
| session Strict batch 500 h1 (B/row) | 320.2 | 351.9 | 319.9 |
| None read-empty (B/call) | 1,064.3 | 1,170.7 | 1,064.3 |
| None check (B/call) | 1,328.3 | 1,461.1 | 1,328.3 |
| PerKey read-empty (B/call) | 1,064.3 | 1,170.7 | 1,064.3 |
| PerKey check (B/call) | 1,328.3 | 1,461.1 | 1,328.3 |
| Strict read-empty (B/call) | 1,064.3 | 1,170.7 | 1,064.3 |
| Strict check (B/call) | 1,328.3 | 1,461.1 | 1,328.3 |
| idle None null interval 1s (B/s) | 152.7 | 335.2 | 135.2 |
| idle None kafka interval 1s (B/s) | 74.3 | 292.0 | 92.0 |
| idle PerKey null interval 1s (B/s) | 93.9 | 296.9 | 96.9 |
| idle PerKey kafka interval 1s (B/s) | 85.8 | 295.7 | 95.7 |
| idle Strict null interval 1s (B/s) | 75.3 | 284.5 | 84.5 |
| idle Strict kafka interval 1s (B/s) | 56.0 | 295.0 | 95.0 |

Largest headroom loss: `term None kafka b500 h1`, +51.7 B/msg over the median, inside its 193.5 margin (3 × ceiling-run spread 64.5). Next is `term None dlq b500 h1`, +21.3 B/dlq-msg inside its 213.1 margin. Every other non-idle kept cell is within 1.4 B of its median. Idle cells are within idle noise. Kafka declared `MaxInFlight => null` before the removal, so the cap never bound on the Kafka path, and the Kafka hot path is unchanged.

Heap over time, after full GC, first / last / max (KB):

- `flat --target term` (PerKey kafka b500 h1, 1M msgs, every 100k): 2,830 / 1,236 / 2,869. Flat, the 1M checkpoint drops, as before.
- `flat --target session` (None, batch 500, 1M rows, every 100k): 8,131 / 8,145 / 8,145. Flat.
- `churn` (PerKey sessions, 4,000 sessions, heap at 1,000 / 2,000 / 3,000 / 4,000): 8,086 / 8,087 / 8,086 / 8,085, 0 relay backends at every quarter.

## 2026-11-14 — Raw Kafka produce

| Item | Value |
|---|---|
| Base commit | `e545671` |
| Branch commit | Verdict runs on `210edc4` (merged head). Ceiling runs on `835b04a`. Commits between them change only docs |
| Package | `Nanov.Kafka` `[2.16.0.3]` (main and dead-letter sends through `RawProduce`). Stock `Confluent.Kafka` stays for admin, over one bundled librdkafka 2.16.0 |
| Date | Verdict: base 1 2026-11-13 21:07:18-21:27:54 UTC, branch 1 to branch 3 2026-11-13 22:13:06-23:55:35 UTC. Ceiling runs 2026-11-14 00:01:12-01:21:12 UTC, passing run 2026-11-14 01:22:13-01:38:14 UTC |
| Machine | Apple M5, 10 cores, 24 GB RAM, macOS 27.2 (26B123), arm64, .NET 10.0.2 / SDK 10.0.102, Release, header `gcServer=False latency=Batch pg=testcontainers postgres:17.11 otel=False profile=False runtimeEvents=off` |
| Harness | Same both sides: `git diff e545671 210edc4 -- benchmarks tests/Pernix.OutboxRelay.Unit.Tests/Allocation` is empty, so no rsync |
| Runs | Verdict: 3 interleaved per side, one process each, `reported` round, median. Ceilings: 5 guard runs plus 1 passing run |

Changed cells, before = base median, after = branch median. Every cell listed is a measured reduction (`Nanov.Kafka` raw produce, inline headers, pooled send slots).

| Cell | before | after | Δ |
|---|---:|---:|---:|
| kafka-only None w500 h0 (B/msg) | 448.0 | 0.0 | -448.0 |
| kafka-only None w500 h1 (B/msg) | 632.0 | 0.0 | -632.0 |
| kafka-only None w500 h4 (B/msg) | 840.0 | 0.0 | -840.0 |
| kafka-only PerKey w500 h0 (B/msg) | 448.0 | 0.0 | -448.0 |
| kafka-only PerKey w500 h1 (B/msg) | 632.0 | 0.0 | -632.0 |
| kafka-only PerKey w500 h4 (B/msg) | 840.0 | 0.0 | -840.0 |
| kafka-only Strict w1 h0 (B/msg) | 448.0 | 0.0 | -448.0 |
| kafka-only Strict w1 h1 (B/msg) | 632.0 | 0.0 | -632.0 |
| kafka-only Strict w1 h4 (B/msg) | 840.0 | 0.0 | -840.0 |
| kafka-dlq None w500 h0 (B/dlq-msg) | 1,408.0 | 304.0 | -1,104.0 |
| kafka-dlq None w500 h1 (B/dlq-msg) | 1,496.0 | 320.0 | -1,176.0 |
| kafka-dlq None w500 h4 (B/dlq-msg) | 1,904.1 | 368.0 | -1,536.1 |
| kafka-dlq PerKey w500 h0 (B/dlq-msg) | 1,408.0 | 304.0 | -1,104.0 |
| kafka-dlq PerKey w500 h1 (B/dlq-msg) | 1,496.0 | 320.0 | -1,176.0 |
| kafka-dlq PerKey w500 h4 (B/dlq-msg) | 1,904.1 | 368.0 | -1,536.1 |
| kafka-dlq Strict w1 h0 (B/dlq-msg) | 1,408.0 | 304.0 | -1,104.0 |
| kafka-dlq Strict w1 h1 (B/dlq-msg) | 1,496.0 | 320.0 | -1,176.0 |
| kafka-dlq Strict w1 h4 (B/dlq-msg) | 1,904.1 | 368.0 | -1,536.1 |
| term None kafka b500 h1 (B/msg) | 886.8 | 213.1 | -673.7 |
| term None dlq b500 h1 (B/dlq-msg) | 2,040.7 | 911.4 | -1,129.3 |
| term PerKey kafka b500 h1 (B/msg) | 823.3 | 191.5 | -631.8 |
| term PerKey dlq b500 h1 (B/dlq-msg) | 2,102.7 | 929.3 | -1,173.4 |
| term Strict kafka b500 h1 (B/msg) | 880.3 | 248.3 | -632.0 |
| term Strict dlq b500 h1 (B/dlq-msg) | 2,168.4 | 992.3 | -1,176.1 |
| guard term None kafka b500 h1 (B/msg) | 886.9 | 210.6 | -676.3 |
| guard term None dlq b500 h1 (B/dlq-msg) | 2,149.4 | 928.3 | -1,221.1 |
| guard term PerKey kafka b500 h1 (B/msg) | 823.3 | 191.5 | -631.8 |
| guard term PerKey dlq b500 h1 (B/dlq-msg) | 2,100.7 | 929.5 | -1,171.2 |
| guard term Strict kafka b500 h1 (B/msg) | 880.4 | 248.4 | -632.0 |
| guard term Strict dlq b500 h1 (B/dlq-msg) | 2,168.4 | 992.4 | -1,176.0 |

No measured change: `term` null per mode, guard `term` null, guard `session`, `fixed` and `idle`. Guard `session None batch 500 h1` (−0.3 B/row) and guard `idle Strict kafka interval 1s` (−51.6 B/s) are reductions by rule, not claimed (no reduction expected). `term` database cells no longer exist (`## 2026-11-11 — Database destination removed`).

Heap over time, after full GC, first / last / max (KB):

- `flat --target term` (PerKey kafka b500 h1, 1M msgs, every 100k), branch: run 1 2,782 / 1,185 / 2,813, run 2 2,801 / 1,183 / 2,805, run 3 2,791 / 1,191 / 2,812. Base: run 1 2,869 / 1,224 / 2,869, run 2 2,836 / 1,225 / 2,881, run 3 2,852 / 1,233 / 2,866. Flat in every run, and the 1M checkpoint drops, as before. Memory parity: branch max median 2,812 KB ≤ base max median + base spread 2,884 KB.
- Guard `FlatTerm`, branch: run 1 2,801 / 1,195 / 2,819, run 2 2,785 / 1,184 / 2,810, run 3 2,811 / 1,183 / 2,814. Base: run 1 2,861 / 1,229 / 2,869, run 2 2,896 / 1,229 / 2,896, run 3 2,850 / 1,229 / 2,881. Flat.
- Guard `FlatSession`, branch: run 1 7,898 / 7,914 / 7,914, run 2 7,970 / 8,022 / 8,022, run 3 8,168 / 8,186 / 8,186. Base: run 1 8,009 / 8,062 / 8,063, run 2 8,026 / 8,037 / 8,037, run 3 8,129 / 8,173 / 8,173. Flat, no change.
- Guard `Churn`: 0 relay backends at every quarter of every run, both sides.

Allocation guard: the 6 guard `term` kafka and dlq ceilings re-set on `835b04a` over 5 runs, limit = median + margin: None kafka 1,038.4 → 260.9 B/msg, None dlq 2,343.7 → 1,010.7 B/dlq-msg, PerKey kafka 905.6 → 210.8 B/msg, PerKey dlq 2,311.9 → 1,022.8 B/dlq-msg, Strict kafka 968.4 → 273.2 B/msg, Strict dlq 2,385.2 → 1,091.6 B/dlq-msg. The other 18 ceilings are unchanged from `85e6bf6`. Ceilings: `tests/Pernix.OutboxRelay.Unit.Tests/Allocation/AllocationCeilings.cs`.

## 2026-11-24 — Re-measure on main

Full harness run on `main` after the embeddable relay, the provider package split and the CI/CD change (#2, #3, #4). No product change is measured here: this section records where `main` stands and compares it with the initial baseline.

### Method

| Item | Value |
|---|---|
| Product commit | `ecc4861` (`main`) |
| Harness commit | `ecc4861` |
| Date | 2026-11-24, runs 19:30-20:31 UTC |
| Machine | Apple M6, 12 cores (12 logical / 12 physical), 32 GB RAM, macOS 27.2.1 (26B131), arm64. Not the machine of the sections above (Apple M5, 10 cores, 24 GB) |
| .NET | runtime 10.0.15 (Microsoft.NETCore.App 10.0.15), SDK 10.0.404, Release build |
| GC mode | workstation, non-concurrent (header: `gcServer=False latency=Batch`; BDN job `Concurrent=False Server=False`) |
| PostgreSQL | Testcontainers `postgres:17.11` (header `pg=testcontainers postgres:17.11`), Docker 29.9.3 |
| Kafka | `Nanov.Kafka` `[2.16.0.3]` for sends, Confluent.Kafka 2.16.0 / librdkafka 2.16.0, librdkafka mock cluster |
| BenchmarkDotNet | 0.15.8, `[MemoryDiagnoser]`, default out-of-process toolchain |
| Other | `otel=False profile=False runtimeEvents=off` on every reported run. **The machine was not idle.** Other workloads ran on it during the runs: 1-minute load average 9.4 at the start of run 1, 5.9 at the start of run 2, 90.5 at the start of run 3, 10.0 at the start of BDN and 75.6 at its end. See the first note |

Commands (repo root, run in this order, one process each):

```sh
dotnet build -c Release benchmarks/Pernix.OutboxRelay.Benchmarks
dotnet run -c Release --no-build --project benchmarks/Pernix.OutboxRelay.Benchmarks -- all   # run 1
dotnet run -c Release --no-build --project benchmarks/Pernix.OutboxRelay.Benchmarks -- all   # run 2
dotnet run -c Release --no-build --project benchmarks/Pernix.OutboxRelay.Benchmarks -- all   # run 3
dotnet run -c Release --no-build --project benchmarks/Pernix.OutboxRelay.Benchmarks -- bdn --filter '*'
```

`all` runs: 16 min 14 s, 18 min 11 s, 16 min 43 s. `bdn --filter '*'`: 24 cases, 9 min 41 s. Every process exited 0.

### BDN

`Allocated` per operation as BDN reports it. Mean is context only, and in this run it is distorted by the load on the machine (for example `Read1` None: mean 1,302 us, median 274 us).

#### Session (`SessionBenchmarks`)

`Read500` and `Commit100` per row, other methods per call.

| Method | Mode | Mean | Allocated |
|---|---|---:|---:|
| Read500 | None | 6.770 us | 299 B |
| Read1 | None | 1,302.106 us | 1,607 B |
| ReadEmpty | None | 233.449 us | 1,421 B |
| Commit100 | None | 3.890 us | 24 B |
| Commit1 | None | 137.524 us | 1,670 B |
| Check | None | 150.454 us | 1,702 B |
| Summary | None | 168.839 us | 1,588 B |
| Read500 | PerKey | 1.174 us | 299 B |
| Read1 | PerKey | 175.036 us | 1,760 B |
| ReadEmpty | PerKey | 108.732 us | 1,436 B |
| Commit100 | PerKey | 2.325 us | 25 B |
| Commit1 | PerKey | 141.432 us | 1,671 B |
| Check | PerKey | 122.513 us | 1,705 B |
| Summary | PerKey | 152.268 us | 1,592 B |

#### Kafka send (`KafkaSendBenchmarks`)

Per message, 500 sends then 500 awaits per invocation. BDN prints `-` for these rows, shown here as 0 B.

| Method | Headers | Mean | Allocated |
|---|---|---:|---:|
| Send | 0 | 12.81 us | 0 B |
| Send | 1 | 17.29 us | 0 B |
| Send | 4 | 14.89 us | 0 B |

#### DeliveryTerm (`DeliveryTermBenchmarks`)

Per delivered message.

| Method | Mode | Destination | Mean | Allocated |
|---|---|---|---:|---:|
| Deliver | None | Null | 1.226 us | 193 B |
| Deliver | None | Kafka | 3.716 us | 220 B |
| Deliver | PerKey | Null | 1.232 us | 193 B |
| Deliver | PerKey | Kafka | 64.157 us | 194 B |

#### HeadersParser (`HeadersParserBenchmarks`)

Per parse.

| Method | Case | Mean | Allocated |
|---|---|---:|---:|
| Parse | One | 59.29 ns | 96 B |
| Parse | Three | 258.69 ns | 248 B |
| Parse | Escaped | 103.52 ns | 152 B |

### GC-delta

Each cell: the `reported` round (round 3 of 3, after warm-up) of runs 1-3, median, spread = max − min. Gen0/1/2 per 100k units and rate are medians of the same rounds. Rates are context only and were measured under load.

#### Kafka-only (`kafka`, B/msg)

| Cell | run 1 | run 2 | run 3 | median | spread | Gen0/1/2 per 100k (median) | rate/s (median) |
|---|---:|---:|---:|---:|---:|---|---:|
| kafka-only None w500 h0 | 0.0 | 0.0 | 0.0 | **0.0** | 0.0 | 0 / 0 / 0 | 84,510 |
| kafka-only None w500 h1 | 0.0 | 0.0 | 0.0 | **0.0** | 0.0 | 0 / 0 / 0 | 83,424 |
| kafka-only None w500 h4 | 0.0 | 0.0 | 0.0 | **0.0** | 0.0 | 0 / 0 / 0 | 84,126 |
| kafka-only PerKey w500 h0 | 0.0 | 0.0 | 0.0 | **0.0** | 0.0 | 0 / 0 / 0 | 82,418 |
| kafka-only PerKey w500 h1 | 0.0 | 0.0 | 0.0 | **0.0** | 0.0 | 0 / 0 / 0 | 81,446 |
| kafka-only PerKey w500 h4 | 0.0 | 0.0 | 0.0 | **0.0** | 0.0 | 0 / 0 / 0 | 82,040 |
| kafka-only Strict w1 h0 | 0.0 | 0.0 | 0.0 | **0.0** | 0.0 | 0 / 0 / 0 | 31,216 |
| kafka-only Strict w1 h1 | 0.0 | 0.0 | 0.0 | **0.0** | 0.0 | 0 / 0 / 0 | 29,775 |
| kafka-only Strict w1 h4 | 0.0 | 0.0 | 0.0 | **0.0** | 0.0 | 0 / 0 / 0 | 24,510 |

#### Kafka dead-letter only (`kafka-dlq`, B/dlq-msg)

| Cell | run 1 | run 2 | run 3 | median | spread | Gen0/1/2 per 100k (median) | rate/s (median) |
|---|---:|---:|---:|---:|---:|---|---:|
| kafka-dlq None w500 h0 | 304.0 | 304.0 | 304.0 | **304.0** | 0.0 | 3 / 0 / 0 | 82,884 |
| kafka-dlq None w500 h1 | 320.0 | 320.0 | 320.0 | **320.0** | 0.0 | 3.5 / 0 / 0 | 82,134 |
| kafka-dlq None w500 h4 | 368.0 | 368.0 | 368.0 | **368.0** | 0.0 | 3.5 / 0 / 0 | 81,467 |
| kafka-dlq PerKey w500 h0 | 304.0 | 304.0 | 304.0 | **304.0** | 0.0 | 2.5 / 0 / 0 | 84,032 |
| kafka-dlq PerKey w500 h1 | 320.0 | 320.0 | 320.0 | **320.0** | 0.0 | 3 / 0 / 0 | 76,435 |
| kafka-dlq PerKey w500 h4 | 368.0 | 368.0 | 368.0 | **368.0** | 0.0 | 3.5 / 0 / 0 | 76,361 |
| kafka-dlq Strict w1 h0 | 304.0 | 304.0 | 304.0 | **304.0** | 0.0 | 5 / 0 / 0 | 25,936 |
| kafka-dlq Strict w1 h1 | 320.0 | 320.0 | 320.0 | **320.0** | 0.0 | 5 / 0 / 0 | 31,820 |
| kafka-dlq Strict w1 h4 | 368.0 | 368.0 | 368.0 | **368.0** | 0.0 | 0 / 0 / 0 | 30,358 |

#### Steady term, null / Kafka / dead-letter (`term`)

| Cell | run 1 | run 2 | run 3 | median | spread | Gen0/1/2 per 100k (median) | rate/s (median) |
|---|---:|---:|---:|---:|---:|---|---:|
| term None null b500 h1 (B/msg) | 192.8 | 192.8 | 192.8 | **192.8** | 0.0 | 0 / 0 / 0 | 2,393,968 |
| term None kafka b500 h1 (B/msg) | 201.6 | 207.6 | 200.4 | **201.6** | 7.2 | 0.5 / 0.5 / 0.5 | 644,474 |
| term None dlq b500 h1 (B/dlq-msg) | 895.4 | 895.5 | 884.8 | **895.4** | 10.7 | 2 / 0 / 0 | 511,855 |
| term PerKey null b500 h1 (B/msg) | 192.9 | 192.9 | 192.9 | **192.9** | 0.0 | 0 / 0 / 0 | 3,716,983 |
| term PerKey kafka b500 h1 (B/msg) | 191.6 | 191.6 | 191.6 | **191.6** | 0.0 | 2 / 0 / 0 | 16,834 |
| term PerKey dlq b500 h1 (B/dlq-msg) | 928.7 | 930.3 | 930.4 | **930.3** | 1.7 | 4 / 0 / 0 | 16,604 |
| term Strict null b500 h1 (B/msg) | 192.9 | 192.9 | 192.9 | **192.9** | 0.0 | 0 / 0 / 0 | 6,281,160 |
| term Strict kafka b500 h1 (B/msg) | 248.2 | 248.4 | 248.3 | **248.3** | 0.2 | 5 / 0 / 0 | 18,219 |
| term Strict dlq b500 h1 (B/dlq-msg) | 992.2 | 992.0 | 992.4 | **992.2** | 0.4 | 5 / 5 / 5 | 19,741 |

#### Idle (`idle`, B/s)

| Cell | run 1 | run 2 | run 3 | median | spread | Gen0/1/2 per 100k (median) | rate/s (median) |
|---|---:|---:|---:|---:|---:|---|---:|
| idle None null interval 1s | 93.3 | 56.0 | 57.1 | **57.1** | 37.3 | 0 / 0 / 0 | 1 |
| idle None kafka interval 1s | 85.3 | 95.2 | 130.9 | **95.2** | 45.6 | 0 / 0 / 0 | 1 |
| idle PerKey null interval 1s | 86.9 | 56.0 | 132.0 | **86.9** | 76.0 | 0 / 0 / 0 | 1 |
| idle PerKey kafka interval 1s | 177.1 | 97.1 | 132.0 | **132.0** | 80.0 | 0 / 0 / 0 | 1 |
| idle Strict null interval 1s | 96.5 | 183.2 | 56.0 | **96.5** | 127.2 | 0 / 0 / 0 | 1 |
| idle Strict kafka interval 1s | 140.5 | 99.2 | 96.3 | **99.2** | 44.2 | 0 / 0 / 0 | 1 |

#### Steady session (`session`, B/row, read + commit)

| Cell | run 1 | run 2 | run 3 | median | spread | read median | commit median | Gen0/1/2 per 100k (median) | rate/s (median) |
|---|---:|---:|---:|---:|---:|---:|---:|---|---:|
| session None batch 500 h1 | 319.4 | 320.1 | 319.5 | **319.5** | 0.7 | 299 | 21 | 1 / 0 / 0 | 172,666 |
| session None batch 500 h3 | 471.6 | 471.5 | 471.7 | **471.6** | 0.2 | 451 | 21 | 1 / 1 / 1 | 160,747 |
| session None batch 100 h1 | 327.9 | 327.8 | 328.0 | **327.9** | 0.2 | 307 | 21 | 1 / 1 / 0 | 80,382 |
| session None batch 100 h3 | 480.0 | 480.0 | 480.0 | **480.0** | 0.0 | 459 | 21 | 1 / 0 / 0 | 96,956 |
| session None batch 10 h1 | 541.3 | 541.3 | 542.2 | **541.3** | 0.9 | 405 | 136 | 10 / 10 / 0 | 15,013 |
| session None batch 10 h3 | 692.8 | 692.7 | 693.3 | **692.8** | 0.6 | 557 | 136 | 10 / 0 / 0 | 16,237 |
| session None batch 1 h1 | 2,678.0 | 2,678.9 | 2,678.7 | **2,678.7** | 0.9 | 1,384 | 1,294 | 20 / 0 / 0 | 1,668 |
| session None batch 1 h3 | 2,831.4 | 2,829.7 | 2,830.9 | **2,830.9** | 1.7 | 1,536 | 1,295 | 20 / 0 / 0 | 1,515 |
| session PerKey batch 500 h1 | 319.3 | 319.3 | 319.2 | **319.3** | 0.1 | 298 | 21 | 0 / 0 / 0 | 213,834 |
| session PerKey batch 500 h3 | 471.2 | 471.2 | 471.2 | **471.2** | 0.0 | 450 | 21 | 1 / 0 / 0 | 178,276 |
| session PerKey batch 100 h1 | 328.1 | 328.2 | 328.2 | **328.2** | 0.1 | 307 | 21 | 1 / 0 / 0 | 148,209 |
| session PerKey batch 100 h3 | 480.1 | 480.1 | 480.2 | **480.1** | 0.1 | 459 | 21 | 2 / 0 / 0 | 132,603 |
| session PerKey batch 10 h1 | 543.4 | 544.3 | 544.9 | **544.3** | 1.5 | 408 | 136 | 0 / 0 / 0 | 15,687 |
| session PerKey batch 10 h3 | 696.4 | 697.0 | 695.7 | **696.4** | 1.3 | 560 | 136 | 10 / 0 / 0 | 17,869 |
| session PerKey batch 1 h1 | 2,712.3 | 2,710.3 | 2,704.3 | **2,710.3** | 8.0 | 1,416 | 1,294 | 20 / 0 / 0 | 1,719 |
| session PerKey batch 1 h3 | 2,871.9 | 2,872.3 | 2,872.1 | **2,872.1** | 0.4 | 1,576 | 1,296 | 20 / 0 / 0 | 1,638 |
| session Strict batch 500 h1 | 319.8 | 319.7 | 319.8 | **319.8** | 0.1 | 299 | 21 | 0 / 0 / 0 | 179,987 |
| session Strict batch 500 h3 | 471.8 | 471.7 | 472.5 | **471.8** | 0.8 | 451 | 21 | 1 / 0 / 0 | 200,013 |
| session Strict batch 100 h1 | 328.4 | 328.4 | 328.5 | **328.4** | 0.1 | 308 | 21 | 1 / 0 / 0 | 156,180 |
| session Strict batch 100 h3 | 480.7 | 480.7 | 480.8 | **480.7** | 0.1 | 460 | 21 | 2 / 0 / 0 | 148,619 |
| session Strict batch 10 h1 | 545.7 | 546.0 | 545.6 | **545.7** | 0.4 | 409 | 137 | 10 / 0 / 0 | 19,628 |
| session Strict batch 10 h3 | 695.9 | 697.2 | 697.6 | **697.2** | 1.7 | 561 | 136 | 0 / 0 / 0 | 18,275 |
| session Strict batch 1 h1 | 2,715.2 | 2,720.1 | 2,719.6 | **2,719.6** | 4.9 | 1,424 | 1,295 | 20 / 0 / 0 | 1,697 |
| session Strict batch 1 h3 | 2,872.1 | 2,829.1 | 2,871.7 | **2,871.7** | 43.0 | 1,576 | 1,296 | 20 / 0 / 0 | 1,725 |

#### Fixed cost (`fixed`, B/call)

| Cell | run 1 | run 2 | run 3 | median | spread | B/row median | Gen0/1/2 per 100k (median) | rate/s (median) |
|---|---:|---:|---:|---:|---:|---:|---|---:|
| None read 1 | 1,385.6 | 1,385.6 | 1,385.6 | **1,385.6** | 0.0 | 1,386 | 0 / 0 / 0 | 6,511 |
| None read 10 | 4,049.6 | 4,049.6 | 4,049.6 | **4,049.6** | 0.0 | 405 | 0 / 0 / 0 | 5,064 |
| None read 100 | 30,692.3 | 30,696.9 | 30,695.1 | **30,695.1** | 4.6 | 307 | 500 / 0 / 0 | 4,523 |
| None read 500 | 149,283 | 149,186 | 149,220 | **149,220** | 96.9 | 298 | 1500 / 0 / 0 | 2,673 |
| None read 1000 | 297,354 | 297,442 | 297,436 | **297,436** | 88.8 | 297 | 3000 / 0 / 0 | 1,698 |
| None commit-remove 1 | 1,296.6 | 1,296.6 | 1,294.3 | **1,296.6** | 2.3 | – | 0 / 0 / 0 | 11,018 |
| None commit-remove 10 | 1,368.6 | 1,368.6 | 1,363.9 | **1,368.6** | 4.7 | – | 0 / 0 / 0 | 10,467 |
| None commit-remove 100 | 2,088.6 | 2,086.3 | 2,088.6 | **2,088.6** | 2.3 | – | 200 / 0 / 0 | 9,641 |
| None commit-fail (tx) 10 | 2,580.4 | 2,576.9 | 2,582.3 | **2,580.4** | 5.4 | – | 0 / 0 / 0 | 4,901 |
| None read 500 exclude 0 | 149,236 | 149,257 | 149,252 | **149,252** | 20.7 | 299 | 1000 / 0 / 0 | 2,573 |
| None read 500 exclude 500 | 149,345 | 149,387 | 149,273 | **149,345** | 113.7 | 299 | 2000 / 0 / 0 | 2,261 |
| None read 500 exclude 5000 | 149,505 | 149,438 | 149,541 | **149,505** | 103.1 | 299 | 1000 / 0 / 0 | 1,024 |
| None read-empty | 1,064.3 | 1,064.3 | 1,063.5 | **1,064.3** | 0.8 | – | 0 / 0 / 0 | 4,222 |
| None check | 1,328.3 | 1,327.1 | 1,328.3 | **1,328.3** | 1.2 | – | 0 / 0 / 0 | 7,618 |
| None summary | 1,216.3 | 1,216.3 | 1,216.3 | **1,216.3** | 0.0 | – | 0 / 0 / 0 | 10,319 |
| None stats | 5,435.2 | 5,432.4 | 5,428.6 | **5,432.4** | 6.6 | – | 100 / 0 / 0 | 2,553 |
| PerKey read 1 | 1,385.6 | 1,385.6 | 1,385.6 | **1,385.6** | 0.0 | 1,386 | 0 / 0 / 0 | 4,189 |
| PerKey read 10 | 4,049.6 | 4,032.6 | 4,049.6 | **4,049.6** | 17.0 | 405 | 0 / 0 / 0 | 5,068 |
| PerKey read 100 | 30,689.6 | 30,691.4 | 30,689.6 | **30,689.6** | 1.8 | 307 | 500 / 0 / 0 | 3,676 |
| PerKey read 500 | 149,123 | 149,121 | 149,142 | **149,123** | 20.7 | 298 | 1500 / 0 / 0 | 1,737 |
| PerKey read 1000 | 297,203 | 297,143 | 297,174 | **297,174** | 60.0 | 297 | 3000 / 0 / 0 | 861 |
| PerKey commit-remove 1 | 1,296.6 | 1,296.6 | 1,296.6 | **1,296.6** | 0.0 | – | 0 / 0 / 0 | 6,531 |
| PerKey commit-remove 10 | 1,368.6 | 1,368.6 | 1,368.6 | **1,368.6** | 0.0 | – | 0 / 0 / 0 | 9,297 |
| PerKey commit-remove 100 | 2,088.6 | 2,081.5 | 2,088.6 | **2,088.6** | 7.1 | – | 0 / 0 / 0 | 8,023 |
| PerKey commit-fail (tx) 10 | 2,584.1 | 2,579.5 | 2,584.1 | **2,584.1** | 4.6 | – | 0 / 0 / 0 | 4,633 |
| PerKey read-empty | 1,064.3 | 1,058.4 | 1,064.3 | **1,064.3** | 5.9 | – | 0 / 0 / 0 | 5,382 |
| PerKey check | 1,328.3 | 1,302.4 | 1,328.3 | **1,328.3** | 25.9 | – | 0 / 0 / 0 | 8,685 |
| PerKey summary | 1,216.3 | 1,209.0 | 1,216.3 | **1,216.3** | 7.3 | – | 0 / 0 / 0 | 8,977 |
| PerKey stats | 5,930.8 | 5,857.8 | 5,930.8 | **5,930.8** | 73.0 | – | 100 / 0 / 0 | 2,196 |
| Strict read 1 | 1,385.6 | 1,377.1 | 1,385.6 | **1,385.6** | 8.5 | 1,386 | 0 / 0 / 0 | 6,043 |
| Strict read 10 | 4,049.6 | 4,019.9 | 4,049.6 | **4,049.6** | 29.7 | 405 | 0 / 0 / 0 | 4,719 |
| Strict read 100 | 30,694.2 | 30,695.8 | 30,694.2 | **30,694.2** | 1.6 | 307 | 500 / 0 / 0 | 3,906 |
| Strict read 500 | 149,239 | 149,259 | 149,217 | **149,239** | 42.6 | 298 | 1500 / 0 / 0 | 1,640 |
| Strict read 1000 | 297,339 | 297,620 | 297,459 | **297,459** | 281.2 | 297 | 2500 / 0 / 0 | 1,499 |
| Strict commit-remove 1 | 1,296.6 | 1,296.6 | 1,296.6 | **1,296.6** | 0.0 | – | 0 / 0 / 0 | 8,927 |
| Strict commit-remove 10 | 1,368.6 | 1,368.6 | 1,368.6 | **1,368.6** | 0.0 | – | 0 / 0 / 0 | 8,657 |
| Strict commit-remove 100 | 2,088.6 | 2,088.6 | 2,088.6 | **2,088.6** | 0.0 | – | 0 / 0 / 0 | 8,583 |
| Strict commit-fail (tx) 10 | 2,586.0 | 2,586.9 | 2,588.7 | **2,586.9** | 2.7 | – | 0 / 0 / 0 | 4,167 |
| Strict read-empty | 1,064.3 | 1,064.3 | 1,064.3 | **1,064.3** | 0.0 | – | 100 / 0 / 0 | 6,361 |
| Strict check | 1,328.3 | 1,328.3 | 1,328.3 | **1,328.3** | 0.0 | – | 0 / 0 / 0 | 7,941 |
| Strict summary | 1,216.3 | 1,216.3 | 1,216.3 | **1,216.3** | 0.0 | – | 0 / 0 / 0 | 8,837 |
| Strict stats | 5,628.1 | 5,631.4 | 5,625.9 | **5,628.1** | 5.5 | – | 0 / 0 / 0 | 1,976 |

### Heap over time and churn

Heap after full GC, first / last / max (KB).

| Scenario | Run 1 | Run 2 | Run 3 | Trend |
|---|---|---|---|---|
| flat term PerKey kafka b500 h1, 1M msgs, every 100k | 2,446 / 829 / 2,446 | 2,424 / 834 / 2,424 | 2,420 / 827 / 2,420 | flat, the 1M checkpoint drops, as before |
| flat session None batch 500, 1M rows, every 100k | 4,588 / 4,573 / 4,588 | 4,484 / 4,470 / 4,484 | 4,495 / 4,471 / 4,495 | flat |

`churn` (PerKey, 4,000 sessions, heap at 1,000 / 2,000 / 3,000 / 4,000, KB): run 1 4,561 / 4,563 / 4,737 / 4,563, run 2 4,497 / 4,839 / 4,940 / 5,060, run 3 4,477 / 4,480 / 4,480 / 4,458. 0 relay backends at every quarter of every run. Run 2 rises by 563 KB. `churn` heap is printed, not asserted, and earlier sections found such crossings to be GC heap space, not live objects (`## 2026-11-10 — PostgreSQL session`). No gcdump was taken in this run.

### Delta vs the 2026-11-09 initial baseline

before = initial baseline median (Apple M5, product commit `2671eb5`), after = this run's median. The two sides ran on different machines, runtimes (10.0.2 vs 10.0.15) and dates, not interleaved, so this table is a record and not a gate verdict. The changes come from the sections between the two: Kafka routes, core loop, PostgreSQL session, allocation guard (dead-letter header merge) and raw Kafka produce.

| Cell | before | after | Δ |
|---|---:|---:|---:|
| kafka-only None w500 h0 (B/msg) | 800.0 | 0.0 | -800.0 |
| kafka-only None w500 h1 (B/msg) | 1,104.0 | 0.0 | -1,104.0 |
| kafka-only None w500 h4 (B/msg) | 1,696.0 | 0.0 | -1,696.0 |
| kafka-only PerKey w500 h0 (B/msg) | 800.0 | 0.0 | -800.0 |
| kafka-only PerKey w500 h1 (B/msg) | 1,104.0 | 0.0 | -1,104.0 |
| kafka-only PerKey w500 h4 (B/msg) | 1,696.0 | 0.0 | -1,696.0 |
| kafka-only Strict w1 h0 (B/msg) | 800.0 | 0.0 | -800.0 |
| kafka-only Strict w1 h1 (B/msg) | 1,104.0 | 0.0 | -1,104.0 |
| kafka-only Strict w1 h4 (B/msg) | 1,696.0 | 0.0 | -1,696.0 |
| term None null b500 h1 (B/msg) | 192.8 | 192.8 | 0.0 |
| term None kafka b500 h1 (B/msg) | 1,544.6 | 201.6 | -1,343.0 |
| term None dlq b500 h1 (B/dlq-msg) | 3,683.8 | 895.4 | -2,788.4 |
| term PerKey null b500 h1 (B/msg) | 244.1 | 192.9 | -51.2 |
| term PerKey kafka b500 h1 (B/msg) | 1,566.4 | 191.6 | -1,374.8 |
| term PerKey dlq b500 h1 (B/dlq-msg) | 3,849.4 | 930.3 | -2,919.1 |
| term Strict null b500 h1 (B/msg) | 192.9 | 192.9 | 0.0 |
| term Strict kafka b500 h1 (B/msg) | 2,505.2 | 248.3 | -2,256.9 |
| term Strict dlq b500 h1 (B/dlq-msg) | 4,852.0 | 992.2 | -3,859.8 |
| idle None null interval 1s (B/s) | 5,739.2 | 57.1 | -5,682.1 |
| idle None kafka interval 1s (B/s) | 3,924.0 | 95.2 | -3,828.8 |
| idle PerKey null interval 1s (B/s) | 2,531.5 | 86.9 | -2,444.6 |
| idle PerKey kafka interval 1s (B/s) | 3,461.9 | 132.0 | -3,329.9 |
| idle Strict null interval 1s (B/s) | 7,723.7 | 96.5 | -7,627.2 |
| idle Strict kafka interval 1s (B/s) | 2,759.5 | 99.2 | -2,660.3 |
| session None batch 500 h1 (B/row) | 597.7 | 319.5 | -278.2 |
| session None batch 500 h3 (B/row) | 958.2 | 471.6 | -486.6 |
| session None batch 100 h1 (B/row) | 631.2 | 327.9 | -303.3 |
| session None batch 100 h3 (B/row) | 991.4 | 480.0 | -511.4 |
| session None batch 10 h1 (B/row) | 1,154.0 | 541.3 | -612.7 |
| session None batch 10 h3 (B/row) | 1,514.1 | 692.8 | -821.3 |
| session None batch 1 h1 (B/row) | 6,329.0 | 2,678.7 | -3,650.3 |
| session None batch 1 h3 (B/row) | 6,689.0 | 2,830.9 | -3,858.1 |
| session PerKey batch 500 h1 (B/row) | 601.3 | 319.3 | -282.0 |
| session PerKey batch 500 h3 (B/row) | 961.4 | 471.2 | -490.2 |
| session PerKey batch 100 h1 (B/row) | 651.4 | 328.2 | -323.2 |
| session PerKey batch 100 h3 (B/row) | 1,011.4 | 480.1 | -531.3 |
| session PerKey batch 10 h1 (B/row) | 1,358.3 | 544.3 | -814.0 |
| session PerKey batch 10 h3 (B/row) | 1,718.3 | 696.4 | -1,021.9 |
| session PerKey batch 1 h1 (B/row) | 8,369.2 | 2,710.3 | -5,658.9 |
| session PerKey batch 1 h3 (B/row) | 8,729.3 | 2,872.1 | -5,857.2 |
| session Strict batch 500 h1 (B/row) | 600.0 | 319.8 | -280.2 |
| session Strict batch 500 h3 (B/row) | 959.9 | 471.8 | -488.1 |
| session Strict batch 100 h1 (B/row) | 638.6 | 328.4 | -310.2 |
| session Strict batch 100 h3 (B/row) | 1,000.0 | 480.7 | -519.3 |
| session Strict batch 10 h1 (B/row) | 1,227.7 | 545.7 | -682.0 |
| session Strict batch 10 h3 (B/row) | 1,587.7 | 697.2 | -890.5 |
| session Strict batch 1 h1 (B/row) | 7,065.1 | 2,719.6 | -4,345.5 |
| session Strict batch 1 h3 (B/row) | 7,425.2 | 2,871.7 | -4,553.5 |
| None read 1 (B/call) | 4,193.5 | 1,385.6 | -2,807.9 |
| None read 10 (B/call) | 9,265.5 | 4,049.6 | -5,215.9 |
| None read 100 (B/call) | 60,092.7 | 30,695.1 | -29,397.6 |
| None read 500 (B/call) | 284,229 | 149,220 | -135,009 |
| None read 1000 (B/call) | 564,670 | 297,436 | -267,234 |
| None commit-remove 1 (B/call) | 2,128.6 | 1,296.6 | -832.0 |
| None commit-remove 10 (B/call) | 2,200.6 | 1,368.6 | -832.0 |
| None commit-remove 100 (B/call) | 2,921.1 | 2,088.6 | -832.5 |
| None commit-fail (tx) 10 (B/call) | 5,211.5 | 2,580.4 | -2,631.1 |
| None read 500 exclude 0 (B/call) | 284,251 | 149,252 | -134,999 |
| None read 500 exclude 500 (B/call) | 284,328 | 149,345 | -134,983 |
| None read 500 exclude 5000 (B/call) | 284,358 | 149,505 | -134,853 |
| None read-empty (B/call) | 3,600.3 | 1,064.3 | -2,536.0 |
| None check (B/call) | 2,832.3 | 1,328.3 | -1,504.0 |
| None summary (B/call) | 1,752.3 | 1,216.3 | -536.0 |
| None stats (B/call) | 5,428.3 | 5,432.4 | +4.1 |
| PerKey read 1 (B/call) | 5,593.5 | 1,385.6 | -4,207.9 |
| PerKey read 10 (B/call) | 10,665.5 | 4,049.6 | -6,615.9 |
| PerKey read 100 (B/call) | 61,493.8 | 30,689.6 | -30,804.2 |
| PerKey read 500 (B/call) | 285,354 | 149,123 | -136,231 |
| PerKey read 1000 (B/call) | 565,605 | 297,174 | -268,431 |
| PerKey commit-remove 1 (B/call) | 2,129.1 | 1,296.6 | -832.5 |
| PerKey commit-remove 10 (B/call) | 2,200.6 | 1,368.6 | -832.0 |
| PerKey commit-remove 100 (B/call) | 2,920.6 | 2,088.6 | -832.0 |
| PerKey commit-fail (tx) 10 (B/call) | 5,218.8 | 2,584.1 | -2,634.7 |
| PerKey read-empty (B/call) | 5,000.3 | 1,064.3 | -3,936.0 |
| PerKey check (B/call) | 2,832.3 | 1,328.3 | -1,504.0 |
| PerKey summary (B/call) | 1,752.3 | 1,216.3 | -536.0 |
| PerKey stats (B/call) | 5,925.3 | 5,930.8 | +5.5 |
| Strict read 1 (B/call) | 4,289.5 | 1,385.6 | -2,903.9 |
| Strict read 10 (B/call) | 9,361.5 | 4,049.6 | -5,311.9 |
| Strict read 100 (B/call) | 60,186.5 | 30,694.2 | -29,492.3 |
| Strict read 500 (B/call) | 284,338 | 149,239 | -135,099 |
| Strict read 1000 (B/call) | 564,815 | 297,459 | -267,356 |
| Strict commit-remove 1 (B/call) | 2,128.6 | 1,296.6 | -832.0 |
| Strict commit-remove 10 (B/call) | 2,200.6 | 1,368.6 | -832.0 |
| Strict commit-remove 100 (B/call) | 2,920.6 | 2,088.6 | -832.0 |
| Strict commit-fail (tx) 10 (B/call) | 5,216.3 | 2,586.9 | -2,629.4 |
| Strict read-empty (B/call) | 3,696.3 | 1,064.3 | -2,632.0 |
| Strict check (B/call) | 2,832.3 | 1,328.3 | -1,504.0 |
| Strict summary (B/call) | 1,752.3 | 1,216.3 | -536.0 |
| Strict stats (B/call) | 5,601.6 | 5,628.1 | +26.5 |
| BDN SessionBenchmarks Read500 None (B/op) | 569 | 299 | -270 |
| BDN SessionBenchmarks Read1 None (B/op) | 4,568 | 1,607 | -2,961 |
| BDN SessionBenchmarks ReadEmpty None (B/op) | 3,976 | 1,421 | -2,555 |
| BDN SessionBenchmarks Commit100 None (B/op) | 33 | 24 | -9 |
| BDN SessionBenchmarks Commit1 None (B/op) | 2,502 | 1,670 | -832 |
| BDN SessionBenchmarks Check None (B/op) | 3,104 | 1,702 | -1,402 |
| BDN SessionBenchmarks Summary None (B/op) | 2,129 | 1,588 | -541 |
| BDN SessionBenchmarks Read500 PerKey (B/op) | 571 | 299 | -272 |
| BDN SessionBenchmarks Read1 PerKey (B/op) | 5,968 | 1,760 | -4,208 |
| BDN SessionBenchmarks ReadEmpty PerKey (B/op) | 5,376 | 1,436 | -3,940 |
| BDN SessionBenchmarks Commit100 PerKey (B/op) | 33 | 25 | -8 |
| BDN SessionBenchmarks Commit1 PerKey (B/op) | 2,502 | 1,671 | -831 |
| BDN SessionBenchmarks Check PerKey (B/op) | 3,208 | 1,705 | -1,503 |
| BDN SessionBenchmarks Summary PerKey (B/op) | 2,127 | 1,592 | -535 |
| BDN KafkaSendBenchmarks Send h0 (B/op) | 800 | 0 | -800 |
| BDN KafkaSendBenchmarks Send h1 (B/op) | 1,105 | 0 | -1,105 |
| BDN KafkaSendBenchmarks Send h4 (B/op) | 1,696 | 0 | -1,696 |
| BDN DeliveryTerm Deliver None Null (B/op) | 198 | 193 | -5 |
| BDN DeliveryTerm Deliver None Kafka (B/op) | 1,643 | 220 | -1,423 |
| BDN DeliveryTerm Deliver PerKey Null (B/op) | 245 | 193 | -52 |
| BDN DeliveryTerm Deliver PerKey Kafka (B/op) | 1,603 | 194 | -1,409 |
| BDN HeadersParserBenchmarks Parse One (B/op) | 152 | 96 | -56 |
| BDN HeadersParserBenchmarks Parse Three (B/op) | 400 | 248 | -152 |
| BDN HeadersParserBenchmarks Parse Escaped (B/op) | 152 | 152 | 0 |

Not in this table: `kafka-dlq` (verb added on 2026-11-10, after the initial baseline) and the retired Database destination cells (`term * database`, `dest`, BDN `DatabaseSendBenchmarks`, `flat --target dest`, removed on 2026-11-11).

### Notes

- **Load.** The method calls for an idle machine and this run did not have one. The allocation figures hold anyway. Spread across the three runs is 0.0-10.7 B for every `kafka`, `kafka-dlq` and `term` cell, at most 43.0 B/row for `session` and at most 281.2 B/call (0.1% of the cell) for `fixed`. Rates, BDN Mean and Gen counts are the affected columns. Do not compare them with other sections.
- **Against the latest sections.** No `kafka`, `kafka-dlq` or `term` cell moved by more than 16.0 B since `## 2026-11-14 — Raw Kafka produce`: kafka-only 0.0 B/msg, kafka-dlq 304.0 / 320.0 / 368.0 B/dlq-msg, `term` kafka 201.6 / 191.6 / 248.3 B/msg (213.1 / 191.5 / 248.3 there), `term` dlq 895.4 / 930.3 / 992.2 B/dlq-msg (911.4 / 929.3 / 992.3 there). Those figures come from another machine, so the two differences on the None cells are not attributed. #2, #3 and #4 are not allocation changes and appended no section.
- **Idle** stays a high-noise cell: medians 57.1-132.0 B/s, spread 37.3-127.2 B/s.
- **Gating** is unchanged: interleaved base vs branch runs on one machine ([`docs/development.md#measuring-allocations`](../../docs/development.md#measuring-allocations)).
