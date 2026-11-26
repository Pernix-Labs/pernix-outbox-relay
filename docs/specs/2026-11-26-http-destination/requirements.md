---
status: approved
createdAt: 2026-11-26T19:43:00Z
updatedAt: 2026-11-26T19:47:38Z
approvedAt: 2026-11-26T19:47:38Z
---

# Requirements: HTTP destination

**Spec ID:** 2026-11-26-http-destination
**Author:** Architect (Orca task task_83f1c3508f12)

## Introduction

Base: `main` `a2345be` (v1.0.0). New destination package `Pernix.OutboxRelay.Destination.Http`, `"Transport": "Http"`. Delivers outbox rows to HTTP/API endpoints, one request per row. Body = raw payload bytes. Flexibility comes from config (URL/header placeholders, status-code classes, closed auth and signing sets), not a templating engine.

Human rulings (2026-11-26, verbatim):
- "Name: Pernix.OutboxRelay.Destination.Http"
- "Templating engine is out."
- "Challenge R3 during the design. Have spec C back and forth with architect to find pooled object strategies and minimize allocations under load."
- Earlier: "It needs to be flexible enough to support a wide range of custom endpoint contracts. In other words, I prefer not making it opinionated if possible."
- Request body = the raw payload bytes as is. No envelopes.

Current state, relevant facts:
- `OutgoingMessage` (`src/Pernix.OutboxRelay.Core/Abstractions/DestinationRecords.cs`) is all a destination sees: `Payload` (`byte[]`), `Key` (`byte[]?`, not guaranteed UTF-8), `Headers` (`MessageHeaders`, names ordinal-unique, values UTF-8 bytes, immutable and shared), `Route` (`string?`, matches `^[a-z0-9][a-z0-9._-]{0,62}$`, URL-safe as is), `Source.Outbox`, `Source.Destination`, `Source.Id`, `DeadLetter` (`Reason`, `Attempts`, `FailedAt`, dead-letter sends only).
- `{outbox}` + `{id}` is unique per row and stable across retries, re-reads after lock loss and dead-letter resends. Not stable if an operator re-inserts a row (correct).
- No attempt count on main sends: `OutboxMessage.RetryCount` exists but `DeliveryTerm` does not copy it into `OutgoingMessage` (F1). No `tx_id`, position or `created_at` (F2).
- `DeliveryResult` is closed: `Delivered`, `MessageFault(Error, Permanent)`, `DestinationFault(Error)`. `MessageFault(Permanent: false)` spends one retry with backoff. Kafka never returns it. HTTP is its first user.
- `IDestination.SendBound` must be > 0 or null (`RelayStartupValidator`).
- `DestinationFault` pauses every route of the outbox (ADR 0009 R7). Core holds rows whose route is not in the destination's `Routes` (ADR 0009). Core refuses a dead-letter destination that declares `Routes`.
- ADR 0004: a send the endpoint did not confirm is a `DestinationFault`. Delivery stays at least once.
- Dead-letter header names live in `KafkaDeadLetterHeaders` (internal, `src/Pernix.OutboxRelay.Destination.Kafka/KafkaDeadLetterHeaders.cs`): `x-dead-letter-reason`, `x-retry-attempts`, `x-original-destination`, `x-original-outbox`, `x-original-id`, `x-failed-at`, `x-original-route`.
- Kafka binds its own `KafkaClusters` section through `IRelaySectionValidator`, and destination options through `RelayOptionsBinder.BindSection<T>` (unknown keys refused).
- Kafka log events use ids 200+ (`KafkaLog`).
- Image composes `builder.AddOutboxRelay().AddPostgreSqlOutbox().AddKafkaDestination()` (`src/Pernix.OutboxRelay.Host/Program.cs`).
- ADR 0005: hot path allocation-conscious. Measured `term` cells today: None kafka 213.1, PerKey kafka 191.5, Strict kafka 248.3 B/msg. Allocation guard: `tests/Pernix.OutboxRelay.Unit.Tests/Allocation/` (`AllocationGuardTests`, `AllocationCeilings`). Harness: `benchmarks/Pernix.OutboxRelay.Benchmarks` (targets `term`, `kafka`, `kafka-dlq`, ...).

## Alignment with Product Vision

`docs/product.md` "Configured only": a domain points the relay at an HTTP endpoint with config, no relay code. Key feature 2 "transports added as modules": Http is the second destination module, beside Kafka. "At least once, never lost": anything unclassified pauses, so a misclassification never loses a row. Embedders reference the package only if they use it.

## Goals

- **G1.** Package `Pernix.OutboxRelay.Destination.Http` registers transport `Http` with one call chained from `AddOutboxRelay()`. Image registers it too.
- **G2.** Minimal config `{ "Transport": "Http", "Http": { "Url": "..." } }` delivers each row as `POST` with body = raw payload bytes.
- **G3.** Config expresses a wide range of endpoint contracts without a templating engine: per-route endpoints, method, URL and header placeholders, header forwarding, status-code classes, auth, signing, TLS.
- **G4.** Every HTTP outcome maps to a `DeliveryResult`. `SendAsync` never throws for delivery failures. Unclassified outcomes are `DestinationFault`.
- **G5.** No secret, URL path/query, header value, body or response body appears in logs, fault text or validation messages.
- **G6.** Http cells exist in the benchmark harness and the allocation guard, with ceilings set from measurement. Design challenges R3 and records each pooling choice against ADR 0005.
- **G7.** Tests, docs and packaging updated. `dotnet pack` yields the new package beside the existing four.

## Non-goals

- **NG1.** Templating engine (Scriban, Liquid, Handlebars) over payload or metadata. Human ruling.
- **NG2.** Built-in body envelopes (JSON metadata envelope, CloudEvents structured mode). Human ruling: body = raw payload bytes. CloudEvents binary mode is covered by header forwarding (Req5, Req17).
- **NG3.** Batching (internal coalescing or a Core `SendBatchAsync`). Framing and partial-failure responses are contract-specific. Revisit when a target needs it.
- **NG4.** `GET` and `DELETE` methods, and a "no body" switch. Not portable with a body. Defer until asked.
- **NG5.** Expressions, conditionals or defaults in placeholders, and a raw (unescaped) placeholder. Fixed set only (Req4).
- **NG6.** Query-string API keys. They leak into access logs.
- **NG7.** Explicit `Proxy` URL setting. Standard proxy env vars only.
- **NG8.** Following redirects.
- **NG9.** `DelegatingHandler` by name (E3). On demand only.
- **NG10.** Per-route pause in Core (R4). Separate Core change.
- **NG11.** Monotonic sequence for ordering-sensitive receivers (F2, R5).
- **NG12.** Own request-duration histogram. .NET's `System.Net.Http` meter covers it (Req15).
- **NG13.** `IHttpClientFactory`. Its default logging handler logs the full request URI.
- **NG14.** Changes to Kafka destination behavior, delivery, ordering, leadership, health, metric or trace semantics in Core. Only Core change: dead-letter header name constants move to Core (Req12). No `OutgoingMessage.Attempt`, no `DestinationFault.RetryAfter` in v1.

## Requirements

### Requirement 1: Package and registration

**User Story:** As .NET service owner, I want one package and one registration call for HTTP delivery, so that my host pulls only what it uses.

#### Acceptance Criteria

1. WHEN repo builds THEN project `src/Pernix.OutboxRelay.Destination.Http` SHALL exist with assembly name, root namespace and `PackageId` `Pernix.OutboxRelay.Destination.Http`, net10.0, lockstep version (`src/RelayPackage.props`).
2. WHEN host calls the Http registration (name per design, chained from `AddOutboxRelay()` on `IServiceCollection` and `IHostApplicationBuilder`) THEN package SHALL register an `IDestinationFactory` with `Transport` `Http` and the `HttpCredentials` section validator (Req9).
3. IF registration called more than once THEN it SHALL register no duplicates and startup SHALL NOT fail because of it (same rule as Kafka).
4. WHEN `.nuspec` inspected THEN it SHALL depend on `Pernix.OutboxRelay.Core` only among relay packages, SHALL NOT depend on Hosting or `Destination.Kafka`, and SHALL add no third-party dependency beyond what design justifies.
5. WHEN image composes relay THEN `Program.cs` SHALL register the Http destination after `AddKafkaDestination()`, and `Pernix.OutboxRelay.Host.csproj` SHALL reference it.
6. IF config names `"Transport": "Http"` and package not registered THEN startup SHALL fail with the existing unregistered-transport message.
7. IF `OutboxRelay:HttpCredentials` present and package not registered THEN relay SHALL ignore the section.

### Requirement 2: Endpoint per destination and per route

**User Story:** As producing team, I want one URL or a URL per route, so that one destination can address several endpoints of one API.

#### Acceptance Criteria

1. WHEN `Http` block configured THEN it SHALL declare exactly one of `Url` or `Routes`.
2. WHEN `Routes` configured THEN each `Routes.<route>` entry SHALL be an object with required `Url` and optional `Method`, `Headers` and `ContentType` overrides. Shared keys apply to every route. Route `Headers` SHALL merge by name over shared `Headers`.
3. WHEN destination declares `Routes` THEN destination SHALL expose the fixed key set as its routes, so Core holds rows with other routes (ADR 0009).
4. WHEN destination declares `Url` THEN destination routes SHALL be null.
5. IF destination is named in `DeadLetter.Destination` THEN it SHALL use `Url` (existing Core rule).
6. WHEN `Routes` configured THEN it SHALL be an object keyed by route. Route sugar (route list with a shared `{route}` `Url` template) SHALL NOT exist in v1.

### Requirement 3: Method

**User Story:** As producing team, I want to choose the HTTP method, so that create, replace and patch contracts all work.

#### Acceptance Criteria

1. WHEN `Method` unset THEN destination SHALL send `POST`.
2. WHEN `Method` set THEN it SHALL be one of `POST`, `PUT`, `PATCH` (case per design), at destination or route level.
3. IF `Method` has any other value THEN configuration stage SHALL fail naming the path and value.

### Requirement 4: URL placeholders

**User Story:** As producing team, I want row values in the URL path and query, so that I address resource-style endpoints without code.

#### Acceptance Criteria

1. WHEN destination starts THEN each URL SHALL be compiled once into literal and placeholder segments. Sends SHALL NOT parse the URL text.
2. WHEN URL rendered THEN supported placeholders SHALL be exactly: `{outbox}`, `{destination}` (configured names, percent-encoded once at startup), `{id}` (row id digits), `{route}` (row route, URL-safe by route rule), `{key}` (key as UTF-8, RFC 3986 percent-encoding of everything outside unreserved), `{key:hex}`, `{key:base64url}` (key bytes encoded), `{header:<name>}` (row header value, percent-encoded).
3. WHEN URL configured THEN scheme, host and port SHALL be literal and SHALL be an absolute `http` or `https` URI. Placeholders SHALL be allowed only in path and query.
4. IF a URL has an unknown placeholder, a placeholder outside path/query, or `{route}` on a single-`Url` destination THEN configuration stage SHALL fail naming the path.
5. IF `{key}` used AND key is null or invalid UTF-8 THEN send SHALL return `MessageFault(Permanent: true)`.
6. IF `{key:hex}` or `{key:base64url}` used AND key is null THEN send SHALL return `MessageFault(Permanent: true)`.
7. IF `{header:<name>}` used AND row has no such header THEN send SHALL return `MessageFault(Permanent: true)`.
8. WHEN placeholder rendered into URL THEN encoding SHALL always apply. No unescaped variant SHALL exist.

### Requirement 5: Request headers

**User Story:** As producing team, I want row headers forwarded and static or templated headers added, so that idempotency, tracing and CloudEvents binary contracts work from config.

#### Acceptance Criteria

1. WHEN request built THEN headers SHALL come from three sources in order, last wins on equal name (case-insensitive): forwarded row headers, then `Headers` map, then relay-owned headers (auth, signing, `Content-Type`, `Content-Length`, dead-letter metadata).
2. WHEN `ForwardHeaders` unset THEN destination SHALL forward all row headers (`All`), minus AC3 and AC7 exclusions. `ForwardHeaders` SHALL accept `All`, `None`, or a list of names.
3. WHEN `ExcludeHeaders` set THEN listed row headers SHALL NOT be forwarded.
4. WHEN `ForwardPrefix` set THEN it SHALL be prepended to each forwarded row header name.
5. WHEN `Headers` value has placeholders THEN it SHALL use the Req4 set without URL escaping, compiled once at startup. Header values with no placeholder SHALL be built once at startup.
6. IF a rendered header value contains CR, LF or NUL THEN send SHALL return `MessageFault(Permanent: true)`.
7. WHEN row headers forwarded THEN `Connection`, `Keep-Alive`, `Proxy-Connection`, `Transfer-Encoding`, `TE`, `Trailer`, `Upgrade`, `Host`, `Content-Length`, `Expect`, `Authorization`, `Proxy-Authorization`, `Cookie` SHALL NOT be forwarded.
8. IF `Headers` names a hop-by-hop/framing header from AC7, or `Authorization` THEN configuration stage SHALL fail naming the path (use `Auth`).
9. IF a forwarded row header name is not an RFC 9110 token, OR its value has CR, LF or NUL THEN send SHALL return `MessageFault(Permanent: true)`. Destination SHALL NOT drop it silently.
10. IF two forwarded row headers differ only by case THEN send SHALL return `MessageFault(Permanent: true)`.

### Requirement 6: Body and content type

**User Story:** As producing team, I want my payload bytes sent as is, so that the endpoint contract is whatever I write.

#### Acceptance Criteria

1. WHEN request built THEN body SHALL be `OutgoingMessage.Payload` bytes unchanged, with no copy of the payload (no envelope, no re-encoding).
2. WHEN a forwarded row header `content-type` exists THEN it SHALL be the request `Content-Type`.
3. WHEN no forwarded `content-type` AND `ContentType` set (destination or route) THEN that value SHALL be used.
4. WHEN neither THEN `Content-Type` SHALL be `application/octet-stream`.
5. WHEN `Content-Length` sent THEN it SHALL equal payload length (no chunked transfer for a known-length body).

### Requirement 7: Response classification

**User Story:** As producing team, I want to map status codes to outcomes, so that each endpoint's contract decides delivered, rejected, retried or paused.

#### Acceptance Criteria

1. WHEN `Response` configured THEN it SHALL map codes to classes `Delivered`, `Reject`, `Retry`, `Pause`. Entries SHALL accept a code (`"204"`), a class (`"2xx"`) or a range (`"500-504"`).
2. WHEN status in `Delivered` THEN send SHALL return `Delivered`. `Reject` → `MessageFault(Permanent: true)`. `Retry` → `MessageFault(Permanent: false)`. `Pause` → `DestinationFault`.
3. WHEN `Response` unset THEN defaults SHALL be `Delivered` = `2xx`, `Reject` = `400`, `413`, `414`, `415`, `422`, `Retry` = empty, everything else `Pause`, including `500`. Config MAY move `500` to `Retry` through `Response`.
4. WHEN a class is listed THEN its list SHALL replace that class's default set (what you see is the set). Unlisted codes SHALL go to `Pause`.
5. IF a code is outside 100-599 or appears in more than one class THEN configuration stage SHALL fail naming the path.
6. WHEN response is `3xx` THEN destination SHALL NOT follow it (`AllowAutoRedirect = false`) and SHALL classify it by the table.
7. WHEN response has a body THEN destination SHALL drain it up to a small cap (design) for connection reuse, and SHALL NOT parse, log or store it.
8. WHEN `Retry-After` present THEN destination SHALL ignore it in v1. The outbox pause backoff applies (`Retry.InitialDelay`, doubling). Design SHALL keep the result shape open for an additive `DestinationFault.RetryAfter` later.
9. WHEN fault text built THEN it SHALL be `HTTP <code>`, prefixed `route '<route>': ` on a `Routes` destination, and SHALL NOT contain URL, key, header values or response text.

### Requirement 8: Transport failures, timeouts and concurrency

**User Story:** As operator, I want network failures and timeouts to pause, not lose or dead-letter rows, so that an outage costs no retries.

#### Acceptance Criteria

1. WHEN connect, DNS, TLS failure, connection reset, own timeout or token-fetch failure occurs THEN send SHALL return `DestinationFault` with the exception type and socket error (for example `HttpRequestException: ConnectionRefused`), never the URL.
2. WHEN any delivery failure occurs THEN `SendAsync` SHALL NOT throw. Only caller cancellation (shutdown) MAY surface as cancellation.
3. WHEN `Timeout` unset THEN it SHALL be `00:00:30`. It SHALL be > 0 and SHALL become `SendBound`.
4. WHEN send runs THEN `Timeout` SHALL cover the whole `SendAsync`: in-flight slot wait, token fetch, request, response headers.
5. WHEN `ConnectTimeout` unset THEN it SHALL be `00:00:10`. IF it exceeds `Timeout` THEN configuration stage SHALL fail.
6. WHEN `MaxInFlight` set THEN it SHALL be ≥ 1 and bound requests in flight per destination instance. Default SHALL be `64`.
7. WHEN all slots busy THEN a send SHALL wait for a slot within its `Timeout`, and on expiry SHALL return `DestinationFault`.
8. WHEN destination instance disposed THEN its handler and connections SHALL be disposed (one handler per destination instance, like the Kafka producer).

### Requirement 9: Authentication and credentials

**User Story:** As operator, I want auth modes with secrets in a named section, so that credentials survive destination reordering and never appear in logs.

#### Acceptance Criteria

1. WHEN `Auth.Mode` set THEN it SHALL be one of `None` (default), `Basic`, `Bearer`, `ApiKey`, `OAuth2ClientCredentials`.
2. WHEN `Basic` or `Bearer` THEN the `Authorization` value SHALL be built once at startup from `Auth.Credential`.
3. WHEN `ApiKey` THEN destination SHALL send `Auth.Header` with optional `Auth.Prefix` plus key, built once at startup.
4. WHEN `OAuth2ClientCredentials` THEN destination SHALL fetch a token from `TokenUrl` with optional `Scope`, `Audience`, `ClientAuth` (`Basic` or `Body`), cache it until `expires_in` minus 60 s, refresh it single-flight, and share it across all instances of the destination.
5. IF endpoint returns `401` under `OAuth2ClientCredentials` THEN cached token SHALL be invalidated and send SHALL return `DestinationFault`.
6. IF token fetch fails THEN send SHALL return `DestinationFault`.
7. WHEN package registered THEN it SHALL bind `OutboxRelay:HttpCredentials:<name>` with fields `Username`, `Password`, `Token`, `ClientId`, `ClientSecret`, `Key`, via an `IRelaySectionValidator`, strict binding (unknown keys refused).
8. IF `Auth.Credential` names a missing credential, OR the credential lacks a field the mode needs THEN configuration stage SHALL fail naming the path, never the value.
9. WHEN `Tls.ClientCertificate` set THEN mTLS SHALL combine with any `Auth.Mode`.

### Requirement 10: Signing

**User Story:** As producing team, I want built-in body signing, so that receivers verify Standard Webhooks or HMAC signatures without relay code.

#### Acceptance Criteria

1. WHEN `Signing.Scheme` is `StandardWebhooks` THEN destination SHALL send `webhook-id` (default `{outbox}-{id}`, configurable), `webhook-timestamp` (Unix seconds) and `webhook-signature` = `v1,` + base64 HMAC-SHA256 over `id.timestamp.body` with the `whsec_`-decoded secret.
2. WHEN `Signing.Scheme` is `HmacSha256` THEN destination SHALL send `Signing.Header` = optional `Prefix` + HMAC-SHA256 of body in `Encoding` `hex` or `base64`.
3. WHEN signing configured THEN secret SHALL come from `HttpCredentials.<name>.Key`.
4. WHEN signing and a request mapper (Req16) both configured THEN signing SHALL run after the mapper and cover the final bytes.
5. WHEN a signature computed THEN result SHALL verify with a reference implementation (Svix library or published test vectors for `StandardWebhooks`).

### Requirement 11: TLS and proxy

**User Story:** As operator, I want private CAs and client certificates, so that internal endpoints work without disabling validation.

#### Acceptance Criteria

1. WHEN `Tls.CaCertificate` set THEN PEM bundle SHALL be added to server certificate validation.
2. WHEN `Tls.ClientCertificate` set THEN destination SHALL present it: PEM cert and key paths, or PFX path plus credential.
3. IF a certificate file is missing or does not parse THEN configuration stage SHALL fail naming the path.
4. WHEN TLS configured THEN no setting SHALL disable server certificate validation. Private CAs use `Tls.CaCertificate`.
5. WHEN `HTTPS_PROXY`, `HTTP_PROXY`, `NO_PROXY` set THEN destination SHALL honour them.

### Requirement 12: Dead letter

**User Story:** As operator, I want HTTP and Kafka to dead-letter into each other with one header set, so that one runbook covers both.

#### Acceptance Criteria

1. WHEN Http main destination dead-letters to a Kafka destination THEN delivery SHALL work with no new code.
2. WHEN Http destination receives a send with `DeadLetter` set THEN it SHALL send to its single `Url` with the same body and add `x-dead-letter-reason`, `x-retry-attempts`, `x-original-destination`, `x-original-outbox`, `x-original-id`, `x-failed-at`, `x-original-route`, replacing producer headers with those names, values formatted as `KafkaDeadLetterHeaders.Merge` formats them.
3. WHEN dead-letter send classified THEN Req7 table SHALL apply unchanged.
4. WHEN dead-letter headers built by any transport THEN header names SHALL come from constants in Core, shared by Kafka and Http. Merge logic SHALL stay per transport. `KafkaDeadLetterHeaders` SHALL use the Core names, with Kafka dead-letter output unchanged.

### Requirement 13: Startup validation

**User Story:** As operator, I want bad config and bad credentials caught at startup, so that I fix them before rows stall.

#### Acceptance Criteria

1. WHEN configuration stage runs (`Validate`, no I/O) THEN it SHALL check every rule marked "configuration stage" in Req2-Req11, plus unknown keys (`RelayOptionsBinder.BindSection<T>`), and report all failures with the other config failures.
2. WHEN destination stage runs (`ValidateAsync`) THEN destination SHALL NOT send a request to any main `Url`.
3. WHEN `OAuth2ClientCredentials` configured THEN destination stage SHALL fetch one token, and fail on error.
4. WHEN optional `Probe` block set (`Url`, `Method` default `HEAD`, expected status class) THEN destination stage SHALL send it and fail on an unexpected class.
5. WHEN a validation message built THEN it SHALL name the destination and host only.

### Requirement 14: Secrecy

**User Story:** As security owner, I want no secret or row data in logs or faults, so that the relay cannot leak them.

#### Acceptance Criteria

1. WHEN destination logs, builds fault text or a validation message THEN it SHALL NOT include URL path or query, header values, body, response body, or credentials.
2. WHEN request sent THEN no handler SHALL log the request URI.
3. WHEN `LogSecrecyTests` run THEN they SHALL cover Http and stay green.

### Requirement 15: Observability

**User Story:** As operator, I want Http events and standard HTTP metrics, so that I see token and probe failures and request latency.

#### Acceptance Criteria

1. WHEN package logs THEN it SHALL use event ids 300-309: `HttpTokenRefreshFailed`, `HttpProbeFailed`, `HttpClientCreated`. Fields: destination, route, host, method, status code, exception type.
2. WHEN host adds the `System.Net.Http` meter THEN `http.client.request.duration` SHALL be recorded. Destination SHALL NOT add its own histogram.
3. WHEN row has `traceparent` THEN the forwarded value SHALL reach the endpoint unchanged (handler `ActivityHeadersPropagator = null`).
4. WHEN delivery outcomes recorded THEN Core metrics and events 110-125 SHALL cover them unchanged.

### Requirement 16: Extension hooks for embedded hosts

**User Story:** As .NET service owner, I want code hooks for contracts config cannot express, so that I am not blocked by the config surface.

Both hooks in v1, host code only, registered by name. Image registers none. A mapper may replace the body: the raw-body ruling means no built-in envelopes or templating in relay config.

#### Acceptance Criteria

1. WHEN host registers `IHttpRequestMapper` by name AND config names it in `RequestMapper` THEN destination SHALL call `DeliveryResult? Map(OutgoingMessage message, HttpRequestMessage request)` after building the request from config and before auth and signing. Null = send. Non-null = return it without sending.
2. WHEN host registers `IHttpResponseClassifier` by name AND config names it in `ResponseClassifier` THEN destination SHALL call `DeliveryResult Classify(HttpResponseMessage response, ReadOnlySpan<byte> bodyPrefix)` instead of the Req7 table.
3. IF config names a mapper or classifier not registered THEN startup SHALL fail like an unregistered transport.
4. IF a hook throws THEN send SHALL return `DestinationFault` with the exception type (design confirms).
5. WHEN image runs THEN it SHALL register no hook.

### Requirement 17: CloudEvents binary recipe

**User Story:** As producing team, I want CloudEvents binary mode from config, so that I need no special mode.

#### Acceptance Criteria

1. WHEN producer writes `ce-id`, `ce-type`, `ce-source`, `ce-specversion`, `content-type` row headers AND minimal config used THEN request SHALL be a conformant CloudEvents HTTP binary-mode request.
2. WHEN docs written THEN they SHALL include this recipe, with constant attributes (`"ce-source": "/orders"`, `"ce-specversion": "1.0"`) and `"ce-id": "{outbox}-{id}"` through `Headers`.

### Requirement 18: Allocation and benchmarks

**User Story:** As maintainer, I want Http send cost measured and guarded, so that ADR 0005 holds for the new transport.

#### Acceptance Criteria

1. WHEN design written THEN it SHALL challenge R3 (proposal estimate 1-2 KB/msg vs Kafka 191.5-248.3 B/msg, unmeasured) and record, per option, the measured or reasoned cost and the ADR 0005 justification: `HttpRequestMessage`/content/header reuse, pooled `CancellationTokenSource` (`TryReset`), precompiled URL/header segments, cached static header strings, header-string caching for forwarded values, `SocketsHttpHandler` configuration, `IncrementalHash` reuse, `ReadOnlyMemoryContent` over payload. Design SHALL show the pooling iteration with the coordinator and a per-request allocation budget per object.
2. WHEN harness runs THEN it SHALL have an `http` target (and `http-dlq`) against an in-process loopback endpoint, and `term` SHALL accept `http` dest, in every ordering mode.
3. WHEN allocation guard runs THEN `AllocationCeilings` SHALL include Http `term` cells (None, PerKey, Strict, main and dlq), ceilings set from measurement on the implementation, not from estimates.
4. WHEN Http cells added THEN existing ceilings SHALL NOT move and no existing cell SHALL regress.
5. WHEN measured THEN results SHALL be recorded in `BENCHMARKS.md` and `benchmarks/Pernix.OutboxRelay.Benchmarks/BASELINE.md`, with the machine and commit.

### Requirement 19: Tests

**User Story:** As maintainer, I want unit and integration coverage of the contract, so that classification and secrecy cannot regress.

#### Acceptance Criteria

1. WHEN unit suite runs THEN it SHALL cover config binding and every configuration-stage failure, placeholder rendering and encoding, header precedence and refusals, content type precedence, classification defaults and overrides, signing vectors, auth header construction, OAuth2 caching and single-flight refresh, dead-letter headers, fault-text secrecy.
2. WHEN integration suite runs THEN it SHALL deliver rows end to end from PostgreSQL to a test HTTP endpoint in each ordering mode, including `Routes`, dead letter to Http and to Kafka, pause on `5xx`/timeout and resume.
3. WHEN `PackageGraphTests` run THEN they SHALL include the Http package and stay green.
4. WHEN existing suites run THEN they SHALL stay green.

### Requirement 20: Documentation

**User Story:** As operator or service owner, I want docs for the Http destination, so that I configure it without reading code.

#### Acceptance Criteria

1. WHEN change lands THEN `docs/configuration.md` SHALL document every `Http` key, default and the `HttpCredentials` section with env form (`OutboxRelay__HttpCredentials__<name>__<field>`).
2. WHEN change lands THEN docs SHALL state: `Retry-After` is ignored and pause backoff applies, `500` pauses by default and a poison message returning `500` stalls the outbox (alert on `outbox.destination_faults`, move `500` to `Retry` per contract) (R6), one dead host pauses all routes (R4), order across timeouts on `PerKey`/`Strict` (R5), OTel HttpClient instrumentation records `url.full` (R2), third-party guidance: `All` forwarding is the default, so set `ForwardHeaders` to a list or use `ExcludeHeaders`.
3. WHEN change lands THEN `docs/embedding.md`, README, `docs/architecture/` and `docs/product.md` key features SHALL name the Http destination. An ADR SHALL record the settled alternatives (no templating engine, raw body, classification model).

## Non-Functional Requirements

### Code Architecture and Modularity
- Package depends on Core only. Only Core change: dead-letter header name constants (Req12 AC4). No other Core change.
- Precompiled URL/header plans built at startup, immutable, shared across sends.

### Performance
- Send path follows ADR 0005: no per-send URL parsing, no LINQ, no payload copy, static strings built once.
- Ceilings from measurement (Req18).
- `MaxInFlight` bounds connections per destination instance.

### Security
- SSRF guard: literal scheme/host/port, placeholders only in path/query, mandatory encoding, no redirects (R1). A mapper (Req16) can still change the URL: host-owned code.
- No producer-chosen credentials (`Authorization`, `Proxy-Authorization`, `Cookie` dropped).
- Secrecy per Req14.

### Reliability
- At least once: unconfirmed send = `DestinationFault` (ADR 0004). Unclassified outcome = `DestinationFault`.
- Idempotency via `"Idempotency-Key": "{outbox}-{id}"` header, no dedicated setting.

### Usability
- Minimal config is one key (`Url`). Every other key has a default.
- Config keys and response class names SHALL read unambiguously. Design picks final names (for example `Rejected`/`Retryable`/`Paused` over `Reject`/`Retry`/`Pause`, one consistent tense). Names in this doc are working names.

## Risks

- **R1. SSRF through templated URLs.** Mitigated per Security NFR. Residual: mapper code.
- **R2. Secret and data leakage in logs.** Templated paths/queries can carry keys and header values. Mitigated per Req14. Residual: a host enabling OTel HttpClient instrumentation records `url.full` in spans. Documented.
- **R3. Allocations per request.** `HttpClient` allocates per request (`HttpRequestMessage`, header collections, `HttpResponseMessage`, header strings). Header values must become strings, so each forwarded header costs a UTF-8 decode per send, which ADR 0005's byte headers avoided for Kafka. Estimate 1-2 KB/msg vs Kafka 191.5-248.3 B/msg, not measured. Design challenges it (Req18).
- **R4. One dead route host pauses every route** (ADR 0009 R7). More likely with HTTP. Mitigation: one destination per host. Documented.
- **R5. Order on `PerKey` and `Strict` across timeouts.** A timed-out request may be processed after its resend: receiver sees A, B, late A. Kafka avoids this with unbounded sends. HTTP cannot. Mitigation: idempotency key. Strict-order receivers need a sequence the relay cannot provide (F2).
- **R6. Poison message returning `500`** stalls the outbox under a `Pause` default. See Q1.

## Decisions

Human delegation (2026-11-26, verbatim): "I think you can answer most yourself. Keep it flexible, unopinionated, high-performance/low-alloc focused, declarative config api, clear descriptive naming. Open a thread if something genuinly needs my product judgement."

1. [x] **Q1. Default class of `500`.** Decided (foreman, 2026-11-26, human-delegated): `Pause` by default. `500` movable to `Retry` through `Response` config. Docs cover the poison-message stall and the `outbox.destination_faults` alert.
2. [x] **Q2. Core changes in scope.** Decided (foreman, 2026-11-26, human-delegated): neither `OutgoingMessage.Attempt` nor `DestinationFault.RetryAfter` in v1. Docs state `Retry-After` is ignored and pause backoff applies. Core shape stays open for an additive `RetryAfter` later.
3. [x] **Q3. Dead-letter header constants.** Decided (foreman, 2026-11-26, human-delegated): move the dead-letter header names to Core. Merge logic stays per transport.
4. [x] **Q4. Default `ContentType`.** Decided (foreman, 2026-11-26, human-delegated): `application/octet-stream`. Row `content-type` header wins, then `ContentType` config.
5. [x] **Q5. Allow disabling TLS validation.** Decided (foreman, 2026-11-26, human-delegated): no.
6. [x] **Q6. Route sugar.** Decided (foreman, 2026-11-26, human-delegated): not in v1.
7. [x] **Q7. `Response` class lists replace or extend defaults.** Decided (foreman, 2026-11-26, human-delegated): replace per class. Unlisted codes go to `Pause`.
8. [x] **Q8. Default `ForwardHeaders`.** Decided (foreman, 2026-11-26, human-delegated): `All`, with `ExcludeHeaders` or a name list, plus third-party guidance in docs.
9. [x] **Q9. `MaxInFlight` default.** Decided (foreman, 2026-11-26, human-delegated): `64`.
10. [x] **Q10. Extension hooks E1/E2 in v1.** Decided (foreman, 2026-11-26, human-delegated): yes, both hooks in v1, host code only, registered by name, image registers none. A mapper replacing the body does not conflict: the ruling means no built-in envelopes or templating in relay config.
