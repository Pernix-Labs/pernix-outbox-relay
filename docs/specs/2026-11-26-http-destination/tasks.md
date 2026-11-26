---
status: approved
createdAt: 2026-11-26T20:07:56Z
updatedAt: 2026-11-26T20:10:21Z
approvedAt: 2026-11-26T20:10:21Z
completedAt: null
---

# Tasks: HTTP destination

**Spec ID:** 2026-11-26-http-destination
**Requirements:** [requirements.md](./requirements.md)
**Design:** [design.md](./design.md)

<!--
DAG grammar — parsed by scripts/dag/*.sh in the orca-spec-workflow skill. One task:

  - [ ] <coord> <title>          coord = 1, 2.1, 4.1.2 … unique, dotted numeric
                                 (indented here so the parser never reads this example as a task)
  - Deps: 1, 2.1                 optional; coords this task waits on; absent = root
  - Files: src/a.ts, src/b.ts    declared write-set; tasks sharing a file MUST be dep-ordered
  - Agent: any                   optional, default any = claude:opus:medium; any | claude[:model[:effort]] | codex[:model[:effort]] | cursor[:model] | opencode
                                 opencode uses its configured model; per-task OpenCode models are not supported by worker-start.
                                 Valid combos: scripts/dag/agent-matrix.sh list
  - Verify: standard             optional; standard | none | <custom instruction line>. default standard
  - Isolation: worktree          optional; task runs in fresh child worktree, merged back on done
  - <body: purpose, leverage, acceptance criteria — self-contained; worker sees ONLY this + docs refs>

Checkboxes = plan notation, never ticked. Doc FROZEN once approved; runtime state lives in
orca (task-list) and docs/specs/<id>/journal.md.
-->

Common rules for every task:
- Package dir `src/Pernix.OutboxRelay.Destination.Http/` (below: `Http/`). Unit tests in `tests/Pernix.OutboxRelay.Unit.Tests/Http/`, namespace `Pernix.OutboxRelay.Unit.Tests.Http`. Integration tests in `tests/Pernix.OutboxRelay.Integration.Tests/Http/`.
- Names, keys and defaults: design.md "Configuration surface (final names)" and "Data Models". Working names in requirements.md are superseded by the final names.
- Send-path code (render, forward, gate, send, classify) follows ADR 0005 (`docs/adr/0005-allocation-conscious-hot-path.md`) and design.md "R3: allocation per request". Startup code may allocate freely.
- No secret, URL path/query, header value, body or response body in any log, fault text or validation message (Req14).
- Unit test command: `dotnet test --project tests/Pernix.OutboxRelay.Unit.Tests --filter-namespace "Pernix.OutboxRelay.Unit.Tests.Http"`.
- Commit only your own task's files: stage explicit paths and `git commit -- <paths>`.

## Measurement spike

- [ ] 1 AllocProfile spike: Http framework floor on an in-memory loopback
  - Files: benchmarks/Pernix.OutboxRelay.Benchmarks/Infrastructure/HttpLoopback.cs, benchmarks/Pernix.OutboxRelay.Benchmarks/Scenarios/HttpFloorScenario.cs, benchmarks/Pernix.OutboxRelay.Benchmarks/Program.cs, benchmarks/Pernix.OutboxRelay.Benchmarks/Infrastructure/HarnessOptions.cs, benchmarks/Pernix.OutboxRelay.Benchmarks/BASELINE.md
  - Verify: `dotnet run -c Release --project benchmarks/Pernix.OutboxRelay.Benchmarks -- loopback` reports ≤ 8 B/req, and `-- http-floor --profile` prints the per-type table; BASELINE.md has `## Http destination` with the measured table
  - Purpose: replace the design's R3 estimates with measured bytes per object before any Http code or ceiling exists. Design refs: "R3: allocation per request" (Round 0 table, Round 1 C3/C6/C7, Budget (Round 1), Measurement plan and cells).
  - `HttpLoopback`: `SocketsHttpHandler.ConnectCallback` returns an in-memory duplex `Stream`. It consumes each request (headers to `\r\n\r\n`, then `Content-Length` bytes) and writes the C6 reference response: `HTTP/1.1 200 OK` with `Date`, `Content-Type: application/json`, `Content-Length: 100`, `Server`, `X-Request-Id` (unique, written from a preallocated counter buffer), `Cache-Control: no-store`, plus a 100 B JSON body. A `--response 204` variant sends `204` with 2 headers. The fake is allocation-free after warm-up (preallocated buffers) and counts connects.
  - Target `loopback`: the fake alone. Fails the harness if > 8 B/req.
  - Target `http-floor` (modes None/PerKey/Strict, headers h0/h1/h4, window 500 like `KafkaOnlyScenario`): a minimal send path written in the scenario, no package needed. One `SocketsHttpHandler` with design O8 settings, `HttpMessageInvoker`, static shared `Uri`, `new HttpRequestMessage`, `new ReadOnlyMemoryContent(payload)`, headers via `TryAddWithoutValidation`, `Content-Type` as a cached string, a pooled deadline CTS (`CancelAfter` + `TryReset`), `[AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]`, response disposed unread.
  - Also measure: templated URL (`/orders/{id}`, render to `char[]`, one string, one `Uri`) with and without `UriCreationOptions.DangerousDisablePathAndQueryCanonicalization`. Assert connect count after warm-up = peak concurrency (the connection is kept when the body is unread, C6).
  - Write in BASELINE.md `## Http destination`: machine, commit, the `--profile` per-type table mapped to design objects O1-O15 (bytes/msg each), the totals per cell, the C3 flag result, and the connect-count result. No `AllocationCeilings` change in this task.

## Core

- [ ] 2 Core `DeadLetterHeaderNames`, Kafka uses it, wire output unchanged
  - Files: src/Pernix.OutboxRelay.Core/Abstractions/DeadLetterHeaderNames.cs, src/Pernix.OutboxRelay.Destination.Kafka/KafkaDeadLetterHeaders.cs
  - Verify: `dotnet build OutboxRelay.slnx` and `dotnet test --project tests/Pernix.OutboxRelay.Unit.Tests` green, Kafka wire and header parity tests unchanged
  - Purpose: decision Q3. Header names live in Core so transports cannot drift. Merge logic stays per transport. Design ref: "Core: `DeadLetterHeaderNames`".
  - Public static class `DeadLetterHeaderNames` in `Pernix.OutboxRelay.Core.Abstractions` with constants `Reason` (`x-dead-letter-reason`), `RetryAttempts` (`x-retry-attempts`), `OriginalDestination` (`x-original-destination`), `OriginalOutbox` (`x-original-outbox`), `OriginalId` (`x-original-id`), `FailedAt` (`x-failed-at`), `OriginalRoute` (`x-original-route`), and `IReadOnlySet<string> All` (OrdinalIgnoreCase).
  - `KafkaDeadLetterHeaders` takes its names and `Names` set from Core. Its `Merge`, `NameBytes` and formats are unchanged. No other Core change (requirements NG14).

## Package

- [ ] 3 Package scaffold: project, options, credentials section, registration, log
  - Files: src/Pernix.OutboxRelay.Destination.Http/Pernix.OutboxRelay.Destination.Http.csproj, src/Pernix.OutboxRelay.Destination.Http/AssemblyInfo.cs, src/Pernix.OutboxRelay.Destination.Http/Options/HttpDestinationOptions.cs, src/Pernix.OutboxRelay.Destination.Http/Options/HttpRouteOptions.cs, src/Pernix.OutboxRelay.Destination.Http/Options/ForwardHeadersOptions.cs, src/Pernix.OutboxRelay.Destination.Http/Options/StatusCodesOptions.cs, src/Pernix.OutboxRelay.Destination.Http/Options/AuthenticationOptions.cs, src/Pernix.OutboxRelay.Destination.Http/Options/SigningOptions.cs, src/Pernix.OutboxRelay.Destination.Http/Options/TlsOptions.cs, src/Pernix.OutboxRelay.Destination.Http/Options/HealthProbeOptions.cs, src/Pernix.OutboxRelay.Destination.Http/HttpCredential.cs, src/Pernix.OutboxRelay.Destination.Http/HttpCredentialSection.cs, src/Pernix.OutboxRelay.Destination.Http/HttpDestinationFactory.cs, src/Pernix.OutboxRelay.Destination.Http/HttpDestinationServiceCollectionExtensions.cs, src/Pernix.OutboxRelay.Destination.Http/HttpLog.cs, OutboxRelay.slnx, tests/Pernix.OutboxRelay.Unit.Tests/Pernix.OutboxRelay.Unit.Tests.csproj, tests/Pernix.OutboxRelay.Integration.Tests/Pernix.OutboxRelay.Integration.Tests.csproj, tests/Pernix.OutboxRelay.Unit.Tests/Http/HttpRegistrationTests.cs, tests/Pernix.OutboxRelay.Unit.Tests/Http/HttpCredentialSectionTests.cs
  - Verify: `dotnet build OutboxRelay.slnx` and the unit test command green
  - Purpose: Req1. Package skeleton others build on. Design refs: "Configuration surface (final names)", "Data Models", "`HttpDestinationServiceCollectionExtensions`", "`HttpCredentialSection`", "`HttpLog`".
  - csproj: assembly name, root namespace and `PackageId` `Pernix.OutboxRelay.Destination.Http`, net10.0, imports `src/RelayPackage.props` like `Destination.Kafka`, references Core only, no third-party package. `InternalsVisibleTo` for the unit tests, integration tests and `Pernix.OutboxRelay.Benchmarks`.
  - Options classes per design Data Models, final names, including `HttpVersion`.
  - `HttpCredentialSection : IRelaySectionValidator` binds `OutboxRelay:HttpCredentials:<name>` per element in its constructor (pattern `KafkaClusterSection`), with fields `Username`, `Password`, `Token`, `ClientId`, `ClientSecret`, `ApiKey`, `SigningSecret`, `CertificatePassword`. Unknown keys fail with section-relative paths and never values.
  - `HttpDestinationFactory`: `Transport => "Http"`. `Validate` checks unknown sibling keys and binds `Http` with `RelayOptionsBinder.BindSection<HttpDestinationOptions>`, full rules come in task 11. `Create` throws `NotImplementedException` until task 12.
  - `AddHttpDestination()` on `IServiceCollection` and `IHostApplicationBuilder`, repeat-safe (`TryAdd*`), pattern `KafkaDestinationServiceCollectionExtensions`.
  - `HttpLog` events 300 `HttpClientCreated`, 301 `HttpTokenRefreshFailed`, 302 `HttpProbeFailed`, with fields per design.
  - Add the project to `OutboxRelay.slnx` and to both test csproj. The integration csproj adds `<FrameworkReference Include="Microsoft.AspNetCore.App" />` if absent (Kestrel for later tasks).
  - Tests: repeat registration registers no duplicates; credentials binding and unknown keys; an unregistered Http transport fails with the existing message (Req1 AC6); `HttpCredentials` is ignored when the package is not registered (Req1 AC7).

- [ ] 4 `ValueTemplate`: compile, fold, render, percent-encoding, dot-segment rule
  - Deps: 3
  - Files: src/Pernix.OutboxRelay.Destination.Http/ValueTemplate.cs, src/Pernix.OutboxRelay.Destination.Http/RenderContext.cs, tests/Pernix.OutboxRelay.Unit.Tests/Http/ValueTemplateTests.cs
  - Verify: unit test command green
  - Purpose: Req4, Req5 AC5-AC6, NG1/NG5. Design refs: "`ValueTemplate`", "Per-row render faults (Req4)", R3 Round 1 C2/C3.
  - Placeholders exactly: `{outbox}`, `{destination}`, `{id}`, `{route}`, `{key}`, `{key:hex}`, `{key:base64url}`, `{header:<name>}`. Compile into a segment array. `{outbox}`, `{destination}` and, on a `Routes` entry, `{route}` fold into literals, encoded once. A template with no per-row segment is constant (`Constant` string built once).
  - Targets: `Url`, with RFC 3986 percent-encoding of everything outside unreserved and uppercase `%XX`, and `HeaderValue`, with no URL escaping and CR/LF/NUL giving a fault.
  - Render: measure, then one `string.Create`. A URL renders into a caller-provided `char[]`, then one string. Scratch via `stackalloc` ≤ 256 B, else `ArrayPool`.
  - Faults, all `MessageFault(Permanent: true)` with fixed text naming the placeholder, never the value:
    - `{key}` with a null key or invalid UTF-8.
    - `{key:hex}` or `{key:base64url}` with a null key.
    - `{header:x}` absent from the row.
    - **Dot-segment rule (K3):** a per-row value rendering to exactly `.` or `..` as a whole URL path segment.
  - Acceptance checks:
    - One test per placeholder.
    - Encoding table.
    - Folding: a URL with only `{outbox}`, `{destination}` or `{route}` is constant.
    - CR/LF/NUL.
    - **K3:** `.` and `..` as a whole segment fault, while `a.b`, `...` and `.` inside a query value do not.
    - A rendered constant allocates 0 B (`GC.GetAllocatedBytesForCurrentThread` delta).

- [ ] 5 Header forwarding, header plan, per-slot string caches
  - Deps: 3
  - Files: src/Pernix.OutboxRelay.Destination.Http/HeaderForwarding.cs, src/Pernix.OutboxRelay.Destination.Http/HeaderValueCache.cs, src/Pernix.OutboxRelay.Destination.Http/PrefixedNameCache.cs, tests/Pernix.OutboxRelay.Unit.Tests/Http/HeaderForwardingTests.cs
  - Verify: unit test command green
  - Purpose: Req5, Req6 AC2. Design refs: "`HeaderPlan` / `HeaderForwarding`", R3 O4/O5, Round 1 C4.
  - `ForwardHeaders.Mode` `All` (default) / `None` / `Listed` + `Names`, `Exclude`, `NamePrefix`. Reserved names are never forwarded: `Connection`, `Keep-Alive`, `Proxy-Connection`, `Transfer-Encoding`, `TE`, `Trailer`, `Upgrade`, `Host`, `Content-Length`, `Expect`, `Authorization`, `Proxy-Authorization`, `Cookie`. A row `content-type` is moved to the content headers. Names set by `Headers` or relay-owned headers are skipped (later source wins).
  - Per row: RFC 9110 token check on the name (`SearchValues<char>`), CR/LF/NUL check on value bytes, case-duplicate check. Each is a permanent `MessageFault` naming the header, never the value.
  - `HeaderValueCache`, per slot, 16 names: hit on reference-equal `byte[]`, else length check then `SequenceEqual`, miss decodes and overwrites. `PrefixedNameCache`, 32 entries, overflow concats. `Listed` precomputes prefixed names at compile.
  - Adds to `HttpRequestHeaders` only via `TryAddWithoutValidation`.
  - Acceptance checks: precedence, each reserved name, each mode, `Exclude`, `NamePrefix`, faults. Cache hits allocate 0 B and a forwarded name is the same string reference.

- [ ] 6 `HttpSendGate`, `SendContext`, `HttpSendSlot` (K1, K2)
  - Deps: 3
  - Files: src/Pernix.OutboxRelay.Destination.Http/HttpSendGate.cs, src/Pernix.OutboxRelay.Destination.Http/SendContext.cs, src/Pernix.OutboxRelay.Destination.Http/HttpSendSlot.cs, tests/Pernix.OutboxRelay.Unit.Tests/Http/HttpSendGateTests.cs
  - Verify: unit test command green, run 20× (`for i in $(seq 20); do <unit test command> || exit 1; done`) for the race tests
  - Purpose: Req8 AC4/AC6/AC7. Zero-allocation slot gate under contention. Design refs: "`HttpSendSlot` / `HttpSendGate`", R3 Round 1 "`HttpSendGate` (replaces O10)", "Round 1 close". Implement exactly as specified there, including D1 (generation-stamped `WaiterEntry`), D2 (registrations disposed before `TryReset`) and D4 (registrations disposed outside the gate lock, no `Cancel`/completion under the lock, `RunContinuationsAsynchronously = true`).
  - Exactly `MaxRequestsInFlight` slots, an `int[]` free-index stack, a `WaiterEntry[]` FIFO ring and one `System.Threading.Lock`. No `Concurrent*` collections. Return hands the slot to the oldest live waiter.
  - `SendContext` pool: grows to peak pending sends, never trimmed, freed at dispose. The deadline CTS covers the slot wait and the request.
  - Acceptance checks (K1, K2), each a unit test:
    1. Timeout while waiting yields a timeout outcome and no slot is consumed.
    2. Term cancel while waiting.
    3. Cancel-vs-hand-off race (forced interleaving): exactly one wins, no leak.
    4. A cancelled waiter's context is re-rented and re-enqueued before hand-off passes its tombstone: each grant goes to exactly one live waiter, slot invariant holds.
    5. A stale `WaitLink` callback cannot fire after context return (reused CTS).
    6. Cancel fires concurrently with context return: no deadlock, completes within a bounded wait of 5 s.
    7. FIFO order.
    8. After N mixed outcomes (success, fault, timeout, cancel, simulated mapper throw): `free + held == MaxRequestsInFlight`, and contexts rented == returned.
    9. A contended wait allocates 0 B after warm-up.

- [ ] 7 Authentication: static schemes and `OAuth2TokenCache`
  - Deps: 9
  - Files: src/Pernix.OutboxRelay.Destination.Http/AuthPlan.cs, src/Pernix.OutboxRelay.Destination.Http/OAuth2TokenCache.cs, tests/Pernix.OutboxRelay.Unit.Tests/Http/AuthPlanTests.cs, tests/Pernix.OutboxRelay.Unit.Tests/Http/OAuth2TokenCacheTests.cs
  - Verify: unit test command green
  - Purpose: Req9. Design refs: "`AuthPlan` / `OAuth2TokenCache`", "Configuration surface" (credential fields per use).
  - `Basic`, `Bearer` and `ApiKey` (`HeaderName`, `ValuePrefix`) header pairs are built once from `HttpCredential`.
  - `OAuth2ClientCredentials`:
    - Request: `POST TokenUrl` form `grant_type=client_credentials` plus `scope` and `audience`, with the client credentials sent per `ClientCredentialsIn` (`Header` = Basic, or `Body`).
    - Response: parse `access_token` and `expires_in` with `Utf8JsonReader`.
    - Caching: cached until `expires_in` − 60 s, refreshed single-flight, `Bearer <token>` built once per fetch. `Invalidate(headerValue)` on `401` clears only the same token.
    - Handler: owns a `SocketsHttpHandler` from `HttpHandlerFactory` (task 9) with HTTP 1.1.
    - Failure: `DestinationFault` text plus a `HttpTokenRefreshFailed` log naming the destination and token host only.
  - Acceptance checks: header construction per scheme, expiry, N concurrent callers → 1 fetch, `401` invalidation without a stampede, failure → fault, and the cached fast path allocates 0 B. Use a stub `HttpMessageHandler`.

- [ ] 8 Signing: `StandardWebhooks`, `HmacSha256`
  - Deps: 3
  - Files: src/Pernix.OutboxRelay.Destination.Http/RequestSigner.cs, tests/Pernix.OutboxRelay.Unit.Tests/Http/RequestSignerTests.cs
  - Verify: unit test command green
  - Purpose: Req10. Design refs: "`RequestSigner`", R3 O13.
  - Uses a per-slot `IncrementalHash` (HMAC-SHA256), with the hash written into `stackalloc` and the input appended piecewise.
  - `StandardWebhooks` headers:
    - `webhook-id`: rendered `Signing.WebhookId`, default `{outbox}-{id}`.
    - `webhook-timestamp`: Unix seconds, cached per slot per second.
    - `webhook-signature`: `v1,` + base64 HMAC over `id.timestamp.body`, with the `whsec_`-decoded secret.
  - `HmacSha256`: `HeaderName` = `ValuePrefix` + hex or base64 of HMAC(body).
  - Signs the final content. When a mapper replaced the content, it is read via `ReadAsByteArrayAsync`.
  - Acceptance checks: published Standard Webhooks test vectors, `HmacSha256` against `HMACSHA256.HashData`, and one string per send otherwise 0 B after warm-up.

- [ ] 9 `HttpHandlerFactory`: handler settings, TLS, `HttpVersion`
  - Deps: 3
  - Files: src/Pernix.OutboxRelay.Destination.Http/HttpHandlerFactory.cs, src/Pernix.OutboxRelay.Destination.Http/HandlerSettings.cs, tests/Pernix.OutboxRelay.Unit.Tests/Http/HttpHandlerFactoryTests.cs
  - Verify: unit test command green
  - Purpose: Req8 AC5/AC8, Req11, design-gate `HttpVersion` amendment. Design refs: "`HttpHandlerFactory`", R3 O8 (every property), Round 1 C8 (amended).
  - Settings:
    - `AllowAutoRedirect = false`, `UseCookies = false`, `AutomaticDecompression = None`, proxy from env.
    - `ActivityHeadersPropagator = null`, `PreAuthenticate = false`, `Credentials = null`.
    - `MaxConnectionsPerServer = MaxRequestsInFlight`, `PooledConnectionLifetime` 5 min, `PooledConnectionIdleTimeout` 90 s, `ConnectTimeout`.
    - `MaxResponseDrainSize` 64 KB, `ResponseDrainTimeout` 2 s, `EnableMultipleHttp2Connections = true`.
  - Version map: `"1.1"` → `Version11` + `RequestVersionOrLower`. `"2"` and `"2-cleartext"` → `Version20` + `RequestVersionExact`.
  - TLS: `CaCertificatePath` PEM bundle via a custom-root chain policy, client certificate from PEM cert+key or from PFX + `CertificatePassword`, loaded once. No setting disables validation (Q5).
  - Acceptance checks: each setting asserted on the built handler, the version map, CA trust accepts a cert from the private CA and rejects others, and a client cert is presented. Use generated test certs.

- [ ] 10 `HttpDeadLetterHeaders`
  - Deps: 2, 3
  - Files: src/Pernix.OutboxRelay.Destination.Http/HttpDeadLetterHeaders.cs, tests/Pernix.OutboxRelay.Unit.Tests/Http/HttpDeadLetterHeadersTests.cs
  - Verify: unit test command green
  - Purpose: Req12 AC2/AC4. Design refs: "`HttpDeadLetterHeaders`", R3 O15.
  - Adds the dead-letter headers as strings directly onto the request headers, with names from Core `DeadLetterHeaderNames`, replacing producer headers of those names. Formats are equal to `KafkaDeadLetterHeaders.Merge`, including failed-at `yyyy-MM-dd'T'HH:mm:ss.fff'Z'`.
  - Caches: configured outbox and destination name strings (bounded), and attempts strings 0-100.
  - Acceptance checks: same values as Kafka `Merge` for the same message, a spoofed producer `x-original-id` is replaced, `x-original-route` only when a route exists, and ≤ 2 strings per dlq send after warm-up.

- [ ] 11 Plan compiler, endpoint map, status table, factory `Validate`
  - Deps: 4, 5, 7, 8, 9
  - Files: src/Pernix.OutboxRelay.Destination.Http/HttpPlanCompiler.cs, src/Pernix.OutboxRelay.Destination.Http/HttpDestinationPlan.cs, src/Pernix.OutboxRelay.Destination.Http/HttpEndpointMap.cs, src/Pernix.OutboxRelay.Destination.Http/HttpEndpoint.cs, src/Pernix.OutboxRelay.Destination.Http/StatusTable.cs, src/Pernix.OutboxRelay.Destination.Http/HttpDestinationFactory.cs, tests/Pernix.OutboxRelay.Unit.Tests/Http/HttpConfigValidationTests.cs, tests/Pernix.OutboxRelay.Unit.Tests/Http/StatusTableTests.cs, tests/Pernix.OutboxRelay.Unit.Tests/Http/HttpEndpointMapTests.cs
  - Verify: unit test command green
  - Purpose: Req2, Req3, Req7 AC1-AC5/AC9, Req13 AC1. Every configuration-stage rule in one pass. Design refs: "`HttpPlanCompiler`", "`HttpEndpointMap` / `HttpEndpoint`", "`StatusTable`", "Data Models" (config-stage rules list, incl. `HttpVersion`), "Configuration surface" (`HttpVersion` table and the h2c decision).
  - `Validate` compiles and caches the plan by destination name (pattern `KafkaDestinationFactory`, OrdinalIgnoreCase). All failures are reported together, with paths `OutboxRelay.Destinations[<name>].Http.<key>` and no values.
  - `StatusCodes` classes `Delivered` / `Rejected` / `Retried` / `Paused`:
    - Defaults: `2xx` / `400, 413, 414, 415, 422` / empty / everything else, incl. `500`.
    - A listed class replaces its default set (Q7). Entries are `NNN`, `Nxx` or `NNN-NNN`, within 100-599, and no code may be in two classes.
    - Fault `DeliveryResult` is cached per endpoint × code, with text `HTTP 503` or `route 'x': HTTP 503`.
  - Endpoint map: `Url` xor `Routes`, `Resolve` allocates 0 B (pattern `KafkaAddressMap`), and a static URL gets one shared `Uri`.
  - URL rules: absolute `http`/`https`, literal scheme/host/port, no userinfo or fragment, placeholders only in path/query, literal segments canonical, `{route}` only in `Routes` entries.
  - `HttpVersion` rules: `"2"` requires every URL to be `https`, and `"2-cleartext"` requires every URL to be `http`.
  - Header names are tokens, not reserved, not `Authorization`. `Method` is POST/PUT/PATCH. Timeout and in-flight ranges as specified.
  - Credentials must exist with the fields their use needs. TLS files must exist and parse. Hook names must be registered (lookup seam, filled by task 13).
  - Acceptance checks: one test per rule and per `HttpVersion` value/scheme combination, and failure texts carry no values.

- [ ] 12 `HttpDestination`: send path, exception mapping, `ValidateAsync`, `Create`
  - Deps: 6, 10, 11
  - Files: src/Pernix.OutboxRelay.Destination.Http/HttpDestination.cs, src/Pernix.OutboxRelay.Destination.Http/HttpDestinationFactory.cs, tests/Pernix.OutboxRelay.Unit.Tests/Http/HttpDestinationSendTests.cs, tests/Pernix.OutboxRelay.Unit.Tests/Http/HttpDestinationValidateTests.cs
  - Verify: unit test command green
  - Purpose: Req6, Req7 AC6-AC8, Req8, Req12 AC1/AC3, Req13 AC2-AC5, Req15 AC3. Design refs: "`HttpDestination : IDestination`", "Send flow" (sequence and the exception mapping table incl. `ObjectDisposedException`), "Architecture" rules, R3 O1-O15 (adopted options only).
  - `SendBound = SendTimeout`, and `Routes` comes from the plan.
  - `SendAsync`, `[AsyncMethodBuilder(typeof(PoolingAsyncValueTaskMethodBuilder<>))]`:
    1. Resolve the endpoint, then acquire a gate context and slot (task 6).
    2. Build the request: version from the plan, `ReadOnlyMemoryContent(payload)`, content type precedence (row `content-type` > `ContentType` > `application/octet-stream`), forwarded headers, `Headers`, auth, dead-letter headers.
    3. Sign, then send via `HttpMessageInvoker` with the deadline token.
    4. Classify via the status table and dispose the response unread.
    5. Return the context and slot in `finally`.
  - Never throws for delivery failures. Map exceptions exactly per the design table.
  - One handler per instance, disposed with it, logging `HttpClientCreated`.
  - `ValidateAsync`: fetch an OAuth2 token if configured, then run `HealthProbe` if configured (`Method` default `HEAD`, `Expected` default `2xx`). It never sends a request to a main `Url`. Failures are `DestinationValidationException` naming the destination and host. `Create` builds instances, and a probe instance (`IsProbe`) only validates.
  - Acceptance checks, via a stub `HttpMessageHandler` seam:
    - Every row of the exception table, including shutdown rethrow and `ObjectDisposedException`.
    - Timeout → `DestinationFault`.
    - `Retry-After` ignored.
    - `3xx` not followed.
    - Content-type precedence.
    - `Content-Length` = payload length.
    - Dead-letter send to the single `Url`.
    - Probe and token behavior, and no request to a main `Url` in `ValidateAsync`.
    - `traceparent` forwarded unchanged.
    - Fault texts without URL, key or values.

- [ ] 13 Hooks: `IHttpRequestMapper`, `IHttpResponseClassifier`
  - Deps: 12
  - Files: src/Pernix.OutboxRelay.Destination.Http/IHttpRequestMapper.cs, src/Pernix.OutboxRelay.Destination.Http/IHttpResponseClassifier.cs, src/Pernix.OutboxRelay.Destination.Http/HttpDestinationBuilder.cs, src/Pernix.OutboxRelay.Destination.Http/HttpDestinationServiceCollectionExtensions.cs, src/Pernix.OutboxRelay.Destination.Http/HttpDestination.cs, tests/Pernix.OutboxRelay.Unit.Tests/Http/HttpHooksTests.cs
  - Verify: unit test command green
  - Purpose: Req16, Q10. Design refs: "Hooks (Req16, Q10)", "`HttpDestinationServiceCollectionExtensions`".
  - Signatures exactly as in the design.
  - Registration: `AddHttpDestination(o => o.AddRequestMapper<T>("name").AddResponseClassifier<T>("name"))` registers keyed singletons. The same name registered with a different type throws.
  - Mapper: called after the config build and before auth and signing. A non-null result returns without sending.
  - Classifier: replaces the status table and gets a ≤ 4 KB body prefix, from `ArrayPool`, read only when a classifier is configured.
  - A hook throwing → `DestinationFault("<ExceptionType> in request mapper '<name>'")`. An unregistered name fails config (fills the task 11 seam).
  - Acceptance checks: short-circuit, a mapper body is signed, the classifier is used, throw → fault, the gate context is returned on a throw (K2), and an unregistered name fails.

- [ ] 14 Image registration and packaging
  - Deps: 13
  - Files: src/Pernix.OutboxRelay.Host/Program.cs, src/Pernix.OutboxRelay.Host/Pernix.OutboxRelay.Host.csproj, Dockerfile, tests/Pernix.OutboxRelay.Unit.Tests/PackageGraphTests.cs
  - Verify: `dotnet test --project tests/Pernix.OutboxRelay.Unit.Tests --filter-class "Pernix.OutboxRelay.Unit.Tests.PackageGraphTests"` green, `dotnet pack` lists the new package, `docker build .` succeeds
  - Purpose: Req1 AC4/AC5, G7, Req16 AC5.
  - `Program.cs`: `builder.AddOutboxRelay().AddPostgreSqlOutbox().AddKafkaDestination().AddHttpDestination();`. No hooks are registered.
  - Host csproj references the package. The `Dockerfile` copies the new csproj for restore, as it does for `Destination.Kafka`.
  - `PackageGraphTests`: `Pernix.OutboxRelay.Destination.Http` depends on Core only (no Hosting, no `Destination.Kafka`, no third-party package), with the lockstep version.

- [ ] 15 Integration tests against Kestrel
  - Deps: 13
  - Files: tests/Pernix.OutboxRelay.Integration.Tests/Http/HttpTestEndpoint.cs, tests/Pernix.OutboxRelay.Integration.Tests/Http/HttpDeliveryTests.cs, tests/Pernix.OutboxRelay.Integration.Tests/Http/HttpDeadLetterTests.cs, tests/Pernix.OutboxRelay.Integration.Tests/Http/HttpTlsAndVersionTests.cs, tests/Pernix.OutboxRelay.Integration.Tests/Http/HttpEmbeddedTests.cs, tests/Pernix.OutboxRelay.Integration.Tests/LogSecrecyTests.cs
  - Verify: `dotnet test --project tests/Pernix.OutboxRelay.Integration.Tests --filter-namespace "Pernix.OutboxRelay.Integration.Tests.Http"` and `--filter-class "Pernix.OutboxRelay.Integration.Tests.LogSecrecyTests"` green (Docker required for PostgreSQL/Kafka)
  - Purpose: Req19 AC2, Req14 AC3, Req17 AC1. Design ref: "Integration Testing".
  - Uses an in-process Kestrel endpoint (`127.0.0.1:0`) recording requests, with the existing PostgreSQL fixtures.
  - Delivery tests:
    - End to end in each ordering mode: `Url`, and `Routes` with held routes staying held. Body bytes and headers are exact.
    - A minimal CloudEvents binary config gives conformant `ce-*` headers.
  - Dead-letter tests: Http → Http, Http → Kafka, Kafka → Http.
  - Fault tests: `503` pause then resume, timeout pause then resume, `422` dead-letter.
  - Connection reuse: connections = peak concurrency with the C6-style response body unread.
  - TLS: private CA plus client certificate (generated certs).
  - HTTP versions:
    - `"2"` over TLS with ALPN h2 succeeds over HTTP/2.
    - `"2-cleartext"` against an h2c endpoint succeeds.
    - `"2"` against an HTTP/1.1-only TLS endpoint gives `DestinationFault` with no downgrade.
  - Embedded: `AddOutboxRelay().AddPostgreSqlOutbox().AddHttpDestination()` delivers.
  - `LogSecrecyTests`: an Http config with credentials and templated URLs leaks none of them.

## Measurement and guard

- [ ] 16 Harness Http cells (HTTP/1.1)
  - Deps: 1, 13
  - Files: benchmarks/Pernix.OutboxRelay.Benchmarks/Pernix.OutboxRelay.Benchmarks.csproj, benchmarks/Pernix.OutboxRelay.Benchmarks/Infrastructure/HttpLoopback.cs, benchmarks/Pernix.OutboxRelay.Benchmarks/Infrastructure/HttpMock.cs, benchmarks/Pernix.OutboxRelay.Benchmarks/Infrastructure/HttpStub.cs, benchmarks/Pernix.OutboxRelay.Benchmarks/Infrastructure/TermHost.cs, benchmarks/Pernix.OutboxRelay.Benchmarks/Infrastructure/HarnessOptions.cs, benchmarks/Pernix.OutboxRelay.Benchmarks/Scenarios/HttpOnlyScenario.cs, benchmarks/Pernix.OutboxRelay.Benchmarks/Scenarios/TermScenario.cs, benchmarks/Pernix.OutboxRelay.Benchmarks/Program.cs, benchmarks/Pernix.OutboxRelay.Benchmarks/BASELINE.md
  - Verify: each new target and dest runs: `dotnet run -c Release --project benchmarks/Pernix.OutboxRelay.Benchmarks -- http`, `-- http-dlq`, `-- http-stub`, `-- term --modes None --dests http,http-tpl,http-sat,http-dlq`, `-- idle --modes None --dests http --seconds 30`
  - Purpose: Req18 AC2/AC5. Real package on the loopback. Design ref: "Measurement plan and cells" (cell table).
  - The benchmarks csproj references the package. `HttpMock` builds an `HttpDestination` through the internal factory path, with the `HttpLoopback` (task 1) handler injected via `ConnectCallback`.
  - `HttpStub` is an `HttpMessageHandler` returning a preallocated response, isolating relay-owned bytes.
  - Targets: `http` and `http-dlq` (modes, headers h0/h1/h4, `--response 204` option) and `http-stub`.
  - Term dests:
    - `http`: static URL.
    - `http-tpl`: `/orders/{id}?key={key}`.
    - `http-sat`: `MaxRequestsInFlight` 8.
    - `http-dlq`: `PermanentFaultDestination` main with an Http dead letter. `TermHost.Create` takes a dead-letter factory instead of hard-wiring Kafka, and Kafka dlq behavior is unchanged.
  - Idle dest: `http`.
  - Real-socket evidence run: once, against a child-process loopback socket server. Result recorded in BASELINE.md `## Http destination`.
  - BASELINE.md `## Http destination`: per-cell results, and relay-owned bytes (from `http-stub` + `--profile` relay types) against the design targets: ≤ 64 B/msg static and saturated, ≤ 64 B + URL string templated, ≤ 192 B/dlq-msg. Any miss is listed with its cause. No `AllocationCeilings` change.

- [ ] 17 h2 loopback and `http2` cell
  - Deps: 16
  - Files: benchmarks/Pernix.OutboxRelay.Benchmarks/Infrastructure/HttpLoopback.cs, benchmarks/Pernix.OutboxRelay.Benchmarks/Infrastructure/Http2LoopbackFramer.cs, benchmarks/Pernix.OutboxRelay.Benchmarks/Infrastructure/HarnessOptions.cs, benchmarks/Pernix.OutboxRelay.Benchmarks/Scenarios/TermScenario.cs, benchmarks/Pernix.OutboxRelay.Benchmarks/BASELINE.md
  - Verify: `dotnet run -c Release --project benchmarks/Pernix.OutboxRelay.Benchmarks -- loopback --http2` ≤ 8 B/req and `-- term --modes None --dests http2` runs. Or, on fallback, BASELINE.md records the reason
  - Purpose: design-gate `HttpVersion` amendment. Measure the h2 floor. Design ref: "Measurement plan and cells" (h2 loopback bullet, h2 targets).
  - Prior-knowledge h2c framer in the fake stream:
    - Connection: read the 24 B preface and 9 B frame headers, send SETTINGS + ACK, ack the client's SETTINGS, send connection and stream `WINDOW_UPDATE` after consumed DATA, ignore PRIORITY/PING/WINDOW_UPDATE.
    - Per stream: one preformatted HEADERS frame (HPACK: `0x88` `:status 200` + C6 headers as literals without indexing, `X-Request-Id` overwritten in place) plus a DATA frame with `END_STREAM`.
    - Fixed buffers only, no HPACK encoder.
  - Term dest `http2` = `HttpVersion` `"2-cleartext"`.
  - Fallback allowed per design: if the fake allocates (self-cell > 8 B/req) or needs more than a minimal framer (~300 lines), keep the `http2` cell harness-only, or drop it, and record the reason in BASELINE.md `## Http destination`. Task 18 then skips the `http2` guard row.
  - Record the h2 results in BASELINE.md against the relay-owned target ≤ 64 B/msg, with the framework floor recorded.

- [ ] 18 Allocation guard: Http ceilings from measurement
  - Deps: 14, 15, 17
  - Files: tests/Pernix.OutboxRelay.Unit.Tests/Allocation/AllocationCeilings.cs, tests/Pernix.OutboxRelay.Unit.Tests/Allocation/AllocationGuardTests.cs, BENCHMARKS.md, benchmarks/Pernix.OutboxRelay.Benchmarks/BASELINE.md
  - Verify: `dotnet test --project tests/Pernix.OutboxRelay.Unit.Tests --filter-class "Pernix.OutboxRelay.Unit.Tests.Allocation.AllocationGuardTests" --explicit only --output Detailed` green
  - Purpose: Req18 AC3-AC5, G6. Design ref: "Measurement plan and cells" (guard list). Ceilings come only from measurement on the implementation commit, never from design estimates.
  - Guard cells (9, or 8 if task 17 took the fallback):
    - `http-stub None w500 h1`.
    - `term None|PerKey|Strict http b500 h1`.
    - `term None http-tpl b500 h1`.
    - `term None http-sat b500 h1`.
    - `term None http-dlq b500 h1`.
    - `term None http2 b500 h1`.
    - `idle None http interval 1s`.
  - `AllocationGuardTests`: Term/1 and Term/3 runs gain `http`. A new `Http/1` run covers `None` `http-tpl,http-sat,http-dlq,http2` and `http-stub`, and Idle gains `None` `http`.
  - Ceiling rows use the existing rule, from 5 guard runs on the implementation commit: Limit = median + margin (`TenPercentReason`, `ThreeSpread` or `IdleNoise`), with `Exceeds`/`Within` text. Update `MeasuredOn`.
  - The existing 24 ceilings are unchanged. Re-run them, and stop and report if any existing cell regresses.
  - Record the guard runs in BASELINE.md and the Http summary in `BENCHMARKS.md`.

## Documentation

- [ ] 19 Docs and ADR
  - Deps: 14, 15
  - Files: docs/configuration.md, docs/embedding.md, README.md, docs/product.md, docs/adr/0015-http-destination.md, docs/adr/README.md
  - Verify: every `Http` key, default and `HttpCredentials` field in design "Configuration surface" appears in `docs/configuration.md`, and the links resolve
  - Purpose: Req20, Req17 AC2. Closeout (not this task) updates `docs/architecture/`.
  - `docs/configuration.md`:
    - Every `Http` key with its default, final names.
    - `HttpVersion` values and the h2c rule.
    - The `HttpCredentials` section with its env form `OutboxRelay__HttpCredentials__<name>__<field>`.
    - The three config shapes: minimal, routed+OAuth2+CloudEvents binary, fully custom.
    - Behavior notes:
      - `Retry-After` is ignored and pause backoff applies.
      - `500` pauses by default, and a poison row stalls the outbox: alert on `outbox.destination_faults` and move `500` to `Retried` per contract.
      - One dead host pauses all routes.
      - Ordering across timeouts on `PerKey`/`Strict`.
      - OTel HttpClient instrumentation records `url.full`.
      - Third-party forwarding guidance (`ForwardHeaders` `Listed` or `Exclude`).
      - The CloudEvents binary recipe.
      - Adding the `System.Net.Http` meter.
  - `docs/embedding.md`: `AddHttpDestination` and the hooks.
  - `README.md` and `docs/product.md` key features name the Http destination.
  - ADR 0015, from `docs/adr/_template.md`, records:
    - No templating engine.
    - Raw body, no envelopes.
    - Status-class model with defaults.
    - `HttpMessageInvoker` + pooled gate (R3 outcome).
    - The `HttpVersion` setting.
    - Dead-letter names in Core.
  - Index the ADR in `docs/adr/README.md`.
