# ADR 0008: Public adapter conformance suite

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0002, ADR-0007 |

## Context

Recorded retrospectively for the first public release. Interfaces alone
cannot express behavioural contracts such as "`Subscribe` keeps running
after registration" or "headers survive the round trip". The `conformance`
package exports `Factory` and `Run(t, factory)`, and this module runs it
against the memory broker and against an adapter that requires a deadline.

## Decision drivers

- D1 Third-party adapters must be able to check themselves against the port
  contracts.
- D2 The suite must not hang on a broken adapter.
- D3 The suite must work for networked adapters with asynchronous
  registration and must not require test-only hooks.

## Considered options

1. **Public `conformance` package called from the adapter's tests (chosen)**
   - Pro: adapters import one function; the contract evolves with the
     library.
   - Con: the suite is part of the public API, so tightening it can break
     adapters' tests.
2. **Documented contract only**
   - Pro: no API surface.
   - Con: each adapter reinvents the tests, inconsistently.
3. **Suite under `internal/`**
   - Pro: free to change.
   - Con: unusable by the adapters it exists for.

## Decision

We will keep `conformance` public. `Run` uses real time (a one-second
deadline shared by `Publish` and `Subscribe`, republishing every 10 ms until
delivery) so networked adapters work; purely in-process adapters may run it
inside a `testing/synctest` bubble, as this module does for the memory
broker. Any terminal result from `Subscribe` after cancellation is accepted;
returning before cancellation fails the suite.

## Consequences

### Positive
- Adapters get a shared definition of "works".

### Negative
- The suite currently covers the at-least-once round trip only; at-most-once,
  settlement and close behaviour are verified for the memory broker but not
  required of adapters.
- Changes to `Run` need the same care as other public API changes.

### Follow-ups
- Extend the suite (settlement, at-most-once, streams) through new
  requirements and ADRs.

## Links

- [PRD-0001](../prd/0001-go-messaging.md), FR-MSG-20
