# ADR 0010: Public API boundary

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0002, ADR-0008 |

## Context

Recorded retrospectively for the first public release. Before the release
every exported identifier was reviewed against two questions: does a user or
an adapter author need it, and does any known consumer use it? Known
consumers include libraries that accept `Publisher`/`Subscriber` and
construct `memory.NewBroker`, and applications that implement
`Publisher[T]` and build `Envelope[T]` values directly.

## Decision drivers

- D1 The public surface is exactly what users and adapter authors need.
- D2 No consumer breaks.
- D3 Implementation details can change without a breaking release.

## Considered options

1. **Keep the current exported surface; keep implementation details
   unexported (chosen)**
   - Pro: the review found no exported implementation detail; no consumer
     breaks.
   - Con: none identified.
2. **Move `memory` under `internal/`**
   - Pro: frees the broker's behaviour from compatibility promises.
   - Con: dependent libraries and applications construct it; it is the
     reference implementation users test against.
3. **Hide `Stream[T]` until it has an implementation**
   - Pro: smaller surface.
   - Con: adapters lose the shared contract; removing an exported type is
     itself a breaking change.

## Decision

The public API consists of:

- package `messaging` (module root): `Envelope`, `NewEnvelope`, `MessageID`,
  `NewMessageID`, `Subscription`, `DeliverySemantics`, `AtMostOnce`,
  `AtLeastOnce`, the ports `Publisher`, `Subscriber`, `Delivery`, `Handler`,
  `Requester`, `RequestHandler`, `Stream`, the authorization types
  `Authorizer`, `AuthorizationRequest`, `AuthorizedPublisher`,
  `AuthorizedSubscriber` and their constructors, `FanoutPublisher`,
  `NewFanoutPublisher`, `HandlerRequester`, `NewHandlerRequester`, and the
  sentinel errors;
- package `memory`: `Broker`, `NewBroker`;
- package `conformance`: `Factory`, `Run` (for adapter implementers,
  ADR-0008).

Implementation details are unexported and stay so: the broker's consumer
registry, the `delivery` type behind `Delivery`, its settlement states, and
the header-copy helper. Test doubles (`publisherFunc`, `subscriberFunc`,
`authorizerFunc`) live in `_test.go` files. No package was moved to
`internal/` and no identifier was unexported for this release; there is no
`internal/` directory today, and new implementation-only packages go there.
The module has no `Clock` abstraction: tests control time with
`testing/synctest` (ADR-0012).

## Consequences

### Positive
- Consumers compile unchanged.
- The broker's internals can change freely.

### Negative
- Everything listed above is covered by semantic versioning; while the
  module is `v0`, breaking changes are allowed in minor releases but must be
  listed in `CHANGELOG.md` and backed by an ADR.

### Follow-ups
- None.

## Links

- [PRD-0001](../prd/0001-messaging.md)
