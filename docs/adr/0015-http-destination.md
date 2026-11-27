# 0015. Add an Http destination with a raw body, a status-class contract and an owned handler

**Status:** Accepted
**Date:** 2026-11-27
**Deciders:** Apostolos Stephanou
**Related specs:** [2026-11-26-http-destination](../specs/2026-11-26-http-destination/)

## Context

Kafka was the only transport. Some consumers expose only an HTTP endpoint (webhooks, ledger services, partner APIs), so each team would put a forwarder between a topic and the endpoint. The relay already has the transport seams (`IDestinationFactory`, `IDestination`, `IRelaySectionValidator`) and the per-transport package layout of [ADR 0012](./0012-lean-hosting-and-role-named-provider-packages.md).

Constraints:

- Product principle "configured only": a domain onboards with configuration and DDL, no relay code.
- Human ruling: the body is the raw payload bytes. No envelopes.
- [ADR 0005](./0005-allocation-conscious-hot-path.md): the send path avoids avoidable allocations and measures where a gain is unclear. The proposal estimated 1-2 KB/msg for HTTP against 191.5-248.3 B/msg for the Kafka delivery loop, unmeasured (risk R3).
- [ADR 0004](./0004-unconfirmed-send-is-a-destination-fault.md) and [ADR 0009](./0009-explicit-per-row-routing.md) stay as they are: an unconfirmed send is a destination fault, and each row reaches only an address configured for its route.
- Core changes stay minimal (NG14).

## Decision

Ship `Pernix.OutboxRelay.Destination.Http` (transport `Http`, `AddHttpDestination`), which sends one request per row with the payload as body, classifies the status code through a configured class table, and sends through its own `SocketsHttpHandler` behind an `HttpMessageInvoker`.

- **D1. No templating engine.** URLs, header values and `Signing.WebhookId` take a fixed placeholder set: `{outbox}`, `{destination}`, `{route}`, `{id}`, `{key}`, `{key:hex}`, `{key:base64url}`, `{header:<name>}`. No expressions, conditionals, defaults or unescaped variant. Every URL value is percent-encoded. Scheme, host and port are literal, and placeholders are allowed only in path and query (SSRF guard). Templates compile once at startup into literal and placeholder segments. A template with no per-row placeholder is a constant built once.
- **D2. Raw body, no envelopes.** The request body is `OutgoingMessage.Payload`, not copied or re-encoded. No JSON metadata envelope and no CloudEvents structured mode. CloudEvents binary mode is header forwarding plus `Headers` for constant attributes. Contracts that configuration cannot express use two host-code hooks registered by name, `IHttpRequestMapper` and `IHttpResponseClassifier`. The image registers none.
- **D3. Status-class model with defaults.** `StatusCodes` maps codes to four classes named for what happens to the row: `Delivered`, `Rejected` (permanent message fault), `Retried` (transient message fault), `Paused` (destination fault). Defaults: `Delivered` = `2xx`, `Rejected` = `400`, `413`, `414`, `415`, `422`, `Retried` empty, every other code `Paused`, `500` included. A listed class replaces its default set. Redirects are not followed and `Retry-After` is ignored: the outbox pause backoff applies. Transport failures and `SendTimeout` are destination faults.
- **D4. `HttpMessageInvoker` + pooled gate (R3 outcome).** No `HttpClient` and no `IHttpClientFactory`. Each `HttpDestination` owns one `SocketsHttpHandler` (no proxy unless `HTTPS_PROXY`, `HTTP_PROXY` or `ALL_PROXY` is set, no redirects, no cookies, no decompression, no trace propagator) and one `HttpMessageInvoker`, disposed with the leadership term. An `HttpSendGate` bounds requests in flight to `MaxRequestsInFlight` slots. A slot holds the reusable per-request state (HMAC instance, header-string caches, URL buffer). A pooled `SendContext` per pending send owns the deadline `CancellationTokenSource` (reset with `TryReset`) and the waiter. Static URLs share one `Uri`, constant headers share one string, status results are cached per endpoint and code. `SendAsync` uses `PoolingAsyncValueTaskMethodBuilder`. `HttpRequestMessage` is built per send, not reused: reuse is undocumented and a mapper or the handler may keep a reference.
- **D5. The `HttpVersion` setting.** Per destination, `"1.1"` (default), `"2"` (ALPN over TLS, `RequestVersionExact`, no downgrade) or `"2-cleartext"` (prior-knowledge h2c). `"2"` requires `https` and `"2-cleartext"` requires `http` on every URL of the destination, so h2c is available for sidecars and meshes but a scheme typo never becomes cleartext HTTP/2. The token endpoint always uses HTTP/1.1.
- **D6. Dead-letter names in Core.** The dead-letter header names move to a public `DeadLetterHeaderNames` in `Pernix.OutboxRelay.Core.Abstractions`, shared by Kafka and Http. Merge logic stays per transport. Kafka dead-letter output is unchanged. An `Http` destination with a single `Url` can be a dead-letter destination, and Http and Kafka dead-letter into each other with one header set.
- **D7. Core change scope.** Core changes are D6 and `RelayOptionsBinder` array-index support (foreman ruling 2026-11-26): a numeric index is accepted only for `T[]`, `List<T>` and `IReadOnlyList<T>` with scalar `T` (`string`, `int`, `TimeSpan`, enum). An index on a non-collection is still an unknown setting. Element values are validated by type and never echoed. No `OutgoingMessage.Attempt` and no `DestinationFault.RetryAfter` in v1. The result shape stays open for an additive `RetryAfter`.
- **D8. Known v1 allocation gap.** On a real socket with many sends in flight, each suspended `HttpDestination.SendAsync` allocates its state machine box: ~350 B/msg relay-owned at window 500 on `None` over HTTP/1.1, ~430 B/msg on HTTP/2. `Strict` (window 1) allocates 0 B relay-owned. .NET 10's `PoolingAsyncValueTaskMethodBuilder` has a fixed small cache (one box per thread, one per core) and no size setting, and `SendAsync` is already one state machine. The gap ships recorded, not guarded away. Measurements: [`BENCHMARKS.md`, "Http destination"](../../BENCHMARKS.md#http-destination).

## Alternatives considered

- **A templating engine (Scriban, Handlebars, expressions in placeholders)** — turns configuration into code, widens the SSRF and injection surface, and allocates per render. Rejected with NG1 and NG5. Contracts beyond the placeholder set go through the request mapper.
- **Built-in envelopes (JSON metadata envelope, CloudEvents structured mode)** — rejected by the raw-body ruling. Each envelope is one more contract to version. Binary mode and the request mapper cover the cases.
- **`HttpClient` from `IHttpClientFactory`** — adds a linked `CancellationTokenSource`, a state machine and handler-pipeline layers per request (about 300-450 B estimated) and logging handlers that record the request URI. The relay needs none of `HttpClient`'s timeout or buffering features and owns handler lifetime per term already.
- **Reuse `HttpRequestMessage` per slot** — about 80 B saved, but resend is undocumented, `HttpResponseMessage.RequestMessage` aliases it and a mapper may keep it. Rejected under ADR 0005: correctness over a micro-gain.
- **`500` retried by default** — a transient server error would spend retries and dead-letter healthy rows during an outage. Chosen default pauses, which stalls the outbox on a poison row answered `500`. The docs cover the `outbox.destination_faults` alert and moving `500` to `Retried` per endpoint contract.
- **Honour `Retry-After`** — needs `DestinationFault.RetryAfter` in Core, outside v1 scope.
- **h2c through `"2"` on an `http` URL** — a scheme typo would silently send cleartext HTTP/2. Rejected for the explicit `"2-cleartext"` value.
- **Allow disabling TLS validation** — rejected. Private CAs use `Tls.CaCertificatePath`.
- **Custom pooling for the `SendAsync` box** (foreman option O3) — rejected for v1 under ADR 0005 clarity. The runtime knob `DOTNET_SYSTEM_THREADING_POOLINGASYNCVALUETASKSCACHESIZE` was measured and is a no-op in .NET 10.
- **Dead-letter header names kept per transport** — two copies of one operator contract would drift. Moving the names, not the merge, keeps Core free of transport wire formats.

## Consequences

- Domains with an HTTP endpoint onboard with configuration only. `HttpCredentials` holds secrets apart from `Destinations[]`, bound strictly and never echoed.
- A destination fault on any route pauses the whole outbox, so one dead host pauses every route of a `Routes` destination. One destination per host avoids it. Per-route pause is a separate Core change (NG10).
- On `PerKey` and `Strict`, a timed-out request can land after its resend. Receivers deduplicate on an idempotency key. The relay provides no sequence number (NG11).
- A host that enables OpenTelemetry HttpClient tracing records `url.full`, which can carry row data from templated URLs. The relay itself never logs URL path, query, header values, bodies or credentials.
- Request duration comes from the `System.Net.Http` meter. The relay adds no histogram of its own.
- The image depends on one more package. The Http package depends on Core only and has no native assets.
- Http delivery costs more than Kafka per message: 1,504.8 B/msg on the HTTP/1.1 delivery loop, mostly the handler stack and response. Allocation ceilings for the Http cells are set from measurement. D8 is a known gap to revisit if the runtime adds a cache setting.
