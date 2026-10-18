# ADR 0005: Concurrency and cancellation model

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0007 |

## Context

Recorded retrospectively for the first public release. Messaging code runs
across goroutines and must shut down cleanly. The code passes a
`context.Context` as the first argument of every port method,
`Subscribe` runs for the lifetime of a subscription instead of returning a
handle, buffers are bounded, and settlement uses an atomic state per
delivery attempt.

## Decision drivers

- D1 Every blocking call must be cancellable.
- D2 Memory must stay bounded under slow consumers.
- D3 Shutdown must not leak goroutines or leave callers blocked.
- D4 Settlement mistakes (double ack) must be detectable.

## Considered options

1. **Blocking `Subscribe` bound to its context (chosen)**
   - Pro: lifetime is visible in the caller's code; cancellation is the only
     unsubscribe mechanism; errors are returned where the subscription runs.
   - Con: callers start a goroutine per subscription and cannot observe the
     moment of registration.
2. **`Subscribe` returning a subscription handle with `Unsubscribe`**
   - Pro: registration is complete when the call returns.
   - Con: two lifecycles (context and handle) to reconcile; terminal errors
     need a separate channel.
3. **Unbounded queues**
   - Pro: `Publish` never blocks.
   - Con: memory grows without limit under a slow consumer.

## Decision

We will:

- take `ctx context.Context` first in every operation that can block and
  return the context's error when it ends;
- make `Subscribe` block after registration until its context ends, the
  transport closes (`ErrClosed`) or a terminal error occurs;
- bound each memory-broker consumer's buffer by `Subscription.MaxInFlight`,
  so `Publish` applies back-pressure and blocks until space, consumer
  departure or context end;
- bound fan-out parallelism by `maxParallel`;
- make `Close` idempotent and wake every blocked subscriber and publisher;
- give each delivery attempt a one-shot atomic settlement state
  (`ErrDeliverySettled` on a second `Ack`/`Nack`);
- process a consumer's deliveries sequentially;
- document concurrency safety on every exported type: the broker and the
  decorators are safe for concurrent use when their dependencies are.

## Consequences

### Positive
- No goroutine outlives its subscription context or the broker.
- A slow consumer slows its publishers instead of exhausting memory.

### Negative
- A consumer with a full buffer blocks the publisher and so delays the
  consumers of the same subject that come after it (consumers are served in
  name order, see ADR-0007).
- Registration is not observable; publishes racing with a new subscription
  may find no consumer (PRD-0001 Q1).

### Follow-ups
- PRD-0001 Q1.

## Links

- [PRD-0001](../prd/0001-messaging.md), FR-MSG-15 to FR-MSG-18, NFR-MSG-01,
  NFR-MSG-02
