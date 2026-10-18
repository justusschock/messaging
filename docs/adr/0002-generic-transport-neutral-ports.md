# ADR 0002: Generic, transport-neutral ports and decorators

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0006, ADR-0008 |

## Context

Recorded retrospectively for the first public release. The library's purpose
is to let application and library code send and receive messages without
depending on a broker client. The code defines a value type `Envelope[T]`,
small generic interfaces (`Publisher[T]`, `Subscriber[T]`, `Delivery[T]`,
`Requester[Req, Resp]`, `Stream[T]`), function types (`Handler[T]`,
`RequestHandler[Req, Resp]`) and concrete decorators
(`AuthorizedPublisher`, `AuthorizedSubscriber`, `FanoutPublisher`,
`HandlerRequester`) built with `New…` constructors that return an error.

## Decision drivers

- D1 Callers should handle typed payloads without casts or serialization in
  their business logic.
- D2 Transports should be swappable, including an in-memory one for tests.
- D3 Cross-cutting concerns should compose without changing transports.
- D4 Interfaces should be small enough for adapters to implement fully.

## Considered options

1. **Generic interfaces over a generic envelope (chosen)**
   - Pro: compile-time payload types; one adapter type per payload type;
     decorators are ordinary implementations of the same interface.
   - Con: an adapter carrying several payload types needs one instance per
     type (or a type such as `[]byte` and its own codec); generic types
     cannot have generic methods.
2. **Non-generic interfaces with `any` or `[]byte` payloads**
   - Pro: one adapter instance serves every message type.
   - Con: every handler casts or decodes; payload type errors surface at run
     time.
3. **Concrete broker type with pluggable drivers**
   - Pro: one place for shared behaviour.
   - Con: couples callers to the library's struct and lifecycle; decorators
     become driver hooks rather than composable values.

For construction, a `New…(dependencies…) (*T, error)` constructor was
preferred over functional options: every decorator has only required
dependencies, and returning an error lets nil dependencies be rejected
up front (`ErrNilDependency`) instead of failing on first use.

## Decision

We will expose messaging as small generic interfaces over `Envelope[T]`,
implement helpers as decorators that satisfy the same interfaces, and build
them with constructors that validate their required dependencies and return
an error. `Stream[T]` is defined as a port for adapters even though this
module ships no implementation.

## Consequences

### Positive
- Decorators stack in any order (`AuthorizedPublisher` around a
  `FanoutPublisher` around adapters).
- Libraries such as caches and job queues accept a `Publisher[T]` and a
  `Subscriber[T]` and stay transport-agnostic.

### Negative
- Adding a method to a port is a breaking change for every adapter, so the
  interfaces must stay minimal.
- `Stream[T]` has no reference implementation or conformance test yet.

### Follow-ups
- PRD-0001 Q2 tracks a possible stream conformance suite.

## Links

- [PRD-0001](../prd/0001-go-messaging.md), FR-MSG-01
