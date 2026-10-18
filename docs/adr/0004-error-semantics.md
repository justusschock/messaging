# ADR 0004: Error semantics

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0005 |

## Context

Recorded retrospectively for the first public release. Callers need to
distinguish invalid input, authorization denials, closed transports and
settlement mistakes, while keeping the underlying cause (an authorizer's
reason, a target's failure) for logs. The code declares sentinel errors in
`errors.go` (`ErrInvalidEnvelope`, `ErrInvalidSubscription`,
`ErrUnauthorized`, `ErrDeliverySettled`, `ErrConsumerExists`, `ErrClosed`,
`ErrNilDependency`) and wraps them with `fmt.Errorf("%w: …")`.

## Decision drivers

- D1 Errors must be matchable with `errors.Is` across decorator layers.
- D2 Root causes must not be lost when a decorator adds meaning.
- D3 Fan-out must report every failing target, not only the first.

## Considered options

1. **Sentinel errors wrapped with context (chosen)**
   - Pro: stable, cheap to match; messages carry detail such as which field
     was invalid.
   - Con: no structured fields; callers parse nothing but must not rely on
     message text.
2. **Typed error structs**
   - Pro: structured data (field name, target index).
   - Con: more API surface; `errors.As` boilerplate for callers who only need
     a category.
3. **Error codes**
   - Pro: serializable.
   - Con: un-idiomatic in Go; loses wrapping.

## Decision

We will report error categories as exported sentinel values and wrap them
with context using `%w`:

- Validation errors wrap `ErrInvalidEnvelope` or `ErrInvalidSubscription`
  with the failing rule.
- An authorization denial wraps both `ErrUnauthorized` and the authorizer's
  error (`%w: %w`), so either can be matched.
- `FanoutPublisher` wraps each target error as `publisher N: …` in input
  order and joins them with `errors.Join`; a failing target does not cancel
  the others.
- Operations that wait return the context's error (`context.Canceled`,
  `context.DeadlineExceeded`, or the cancellation cause) unwrapped.
- Handler errors from `HandlerRequester` are returned unchanged.
- The library never logs.

## Consequences

### Positive
- `errors.Is(err, messaging.ErrUnauthorized)` and
  `errors.Is(err, policyErr)` both hold for one denial.
- Fan-out callers see every failure.

### Negative
- Message text is not part of the contract, but the fan-out format is
  asserted by tests and should be treated as stable.
- In the memory broker, handler errors under `AtMostOnce` are discarded and,
  under `AtLeastOnce`, only trigger redelivery; they are not surfaced to the
  `Subscribe` caller.

### Follow-ups
- None.

## Links

- [PRD-0001](../prd/0001-go-messaging.md), FR-MSG-03, FR-MSG-05 to FR-MSG-09
