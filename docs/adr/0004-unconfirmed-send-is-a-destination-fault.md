# 0004. Treat an unconfirmed send as a destination fault

**Status:** Accepted
**Date:** 2026-11-03
**Deciders:** Apostolos Stephanou

## Context

With `Ordering.Mode` `None` a message fault spends one of the row's retries and, once retries run out, dead-letters or drops the row. A destination fault spends nothing: the row stays untouched and the outbox pauses with backoff. Every send result has to be one or the other.

Kafka can end a send without saying whether the record landed. `Local_MsgTimedOut` means the producer gave up after `message.timeout.ms` (30 s), and a delivery report whose status is not `Persisted` means the broker did not confirm the write. Neither says anything about the message itself, and during a broker or partition outage every send in flight ends this way. Spec `kafka-none` classified `Local_MsgTimedOut` as a transient message fault. The question came up again as `kafka-none` carry-forward R4 and as `failure-handling` Q1.

## Decision

A send the broker did not confirm is a `DestinationFault`: it spends no retry, and the row is sent again after the pause.

- `KafkaErrorClassifier` maps `Local_MsgTimedOut` to `DestinationFault`, like every other code that is not one of the four permanent message fault codes.
- `KafkaDestination.SendAsync` maps a delivery report with a status other than `Persisted` to `DestinationFault("<Status>: not confirmed")`.
- A timed-out send that landed is sent again, so it reaches the topic twice. Duplicates are accepted under the at-least-once contract (design page Appendix D, K11). Consumers deduplicate.

## Alternatives considered

- **`DestinationFault` only while the producer reports all brokers down, else a transient `MessageFault`** (`failure-handling` Q1 O2). The result depends on when the producer's broker state is sampled relative to the timeout, so the same outage can spend retries or not.
- **Keep a transient `MessageFault`, and reclassify in Core when every send in flight times out** (`failure-handling` Q1 O3). A heuristic that still spends retries on partial outages, so healthy rows on an unavailable partition can be dead-lettered or dropped.

## Consequences

- A broker or partition outage never spends retries, so it cannot dead-letter or drop healthy rows.
- Kafka has no transient message fault. `Retry.MaxRetryAttempts` is spent only by future transports that report one, and Kafka permanent faults skip retries anyway.
- A single partition outage pauses the whole outbox, including rows for healthy partitions.
- A send that timed out but landed is duplicated. The relay never counts it as lost.
- A future transport must map "not confirmed" results to `DestinationFault` too, or the rule stops holding for it.
