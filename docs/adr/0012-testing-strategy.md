# ADR 0012: Testing strategy

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0008, ADR-0011 |

## Context

Recorded retrospectively for the first public release. Most behaviour of
this module is about timing and goroutines: redelivery delays, blocking
publishes, cancellation, close. The earlier tests waited on real time
(`time.Sleep`, one-second `time.After` timeouts, `runtime.Gosched` polling
loops), which made them slow on failure and dependent on scheduler timing.

## Decision drivers

- D1 Tests must be deterministic and pass with `-race -count=3`.
- D2 Time-dependent assertions (for example "no retry before the delay")
  must be exact rather than "probably".
- D3 Every functional requirement must be traceable to a test.

## Considered options

1. **`testing/synctest` bubbles for time and concurrency (chosen)**
   - Pro: virtual time makes delays exact and free; `synctest.Wait` replaces
     polling; deadlocks fail the test immediately.
   - Con: code inside a bubble must not block on real I/O; requires Go 1.25.
2. **Injectable clock interface**
   - Pro: works on older Go.
   - Con: production API surface added for tests; goroutine coordination
     still needs polling.
3. **Real time with generous timeouts**
   - Pro: no tooling.
   - Con: slow, flaky under load, cannot prove absence of an event.

## Decision

We will:

- write unit tests inside each package, using unexported state only where
  no public observation exists (the broker's consumer registry);
- run every test involving time, goroutines or blocking inside
  `synctest.Test`, using `synctest.Wait` instead of polling and `time.Sleep`
  only to advance virtual time;
- keep the public conformance suite on real time for networked adapters and
  run it in a bubble for the memory broker (ADR-0008);
- keep runnable examples (`Example…`) for the main entry points and use them
  as the README's code;
- run all tests with the race detector in CI;
- cite requirement IDs (`// FR-MSG-NN`) on the tests listed in PRD-0001's
  acceptance table.

## Consequences

### Positive
- Retry-delay and timeout assertions are exact and cost no wall-clock
  time.

### Negative
- Examples cannot use synctest; the broker example republishes until the
  subscriber is registered.

### Follow-ups
- None.

## Links

- [testing/synctest](https://pkg.go.dev/testing/synctest)
- [PRD-0001](../prd/0001-messaging.md), NFR-MSG-01, NFR-MSG-03
