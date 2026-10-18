# ADR 0007: Delivery semantics and the in-memory reference broker

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0005, ADR-0008 |

## Context

Recorded retrospectively for the first public release. Subscribers must know
what happens when a handler fails. The ports model this with
`DeliverySemantics` (`AtMostOnce`, `AtLeastOnce`) on the `Subscription` and
explicit `Ack`/`Nack` on `Delivery`. The `memory` package provides
`Broker[T]`, the only transport in this module, used by tests, by dependent
libraries and for process-local event fan-out.

## Decision drivers

- D1 Semantics must be explicit per subscription, not implied by the
  transport.
- D2 The reference broker must be deterministic enough to test against.
- D3 No persistence or exactly-once claims (NG1, NG2 in PRD-0001).

## Considered options

1. **Two explicit modes with settlement-driven redelivery (chosen)**
   - Pro: covers the common cases; "not acknowledged" has one meaning
     (handler error, `Nack` or no settlement all redeliver).
   - Con: no dead-letter handling or retry limit; a poison message under
     `AtLeastOnce` is retried until the subscription ends.
2. **Handler return value as the only signal**
   - Pro: simpler handler code.
   - Con: cannot express "acknowledge, but report an error", and adapters
     with explicit acknowledgement map less directly.
3. **Configurable retry policy (backoff, limits) in the core**
   - Pro: production-ready behaviour.
   - Con: a policy surface every adapter must honour; belongs in adapters or
     higher-level libraries.

## Decision

We will expose `AtMostOnce` and `AtLeastOnce` on `Subscription`. The memory
broker:

- invokes `AtMostOnce` handlers once per enqueued envelope and ignores the
  result;
- redelivers `AtLeastOnce` envelopes after a fixed `retryDelay` (the
  `NewBroker` argument) with an incremented attempt number until an attempt
  is acknowledged, the subscription context ends or the broker closes;
- identifies consumers by subject and consumer name, rejecting duplicates
  with `ErrConsumerExists`;
- hands each envelope to a subject's consumers in consumer-name order;
- does not persist envelopes; an envelope published while a subject has no
  consumer is dropped.

## Consequences

### Positive
- Deterministic ordering and fixed delays make the broker easy to test
  against, including with `testing/synctest`.
- Dependent libraries can run their own tests on the memory broker.

### Negative
- Ordered, blocking hand-off means a consumer with a full buffer delays the
  consumers after it.
- Redelivery is unbounded.

### Follow-ups
- None.

## Links

- [PRD-0001](../prd/0001-messaging.md), FR-MSG-11 to FR-MSG-19
