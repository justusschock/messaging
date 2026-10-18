# ADR 0003: Dependency policy

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0006 |

## Context

Recorded retrospectively for the first public release. A messaging
abstraction sits low in an application's dependency graph; every dependency
it pulls in reaches every user. The module depends on the standard library
and on one module, `github.com/justusschock/go-concurrency`, whose
`ParallelMap` bounds the parallel calls in `FanoutPublisher` and whose
`ErrInvalidLimit` the constructor wraps. Message IDs are UUIDv7 strings
generated with `crypto/rand` and `time` rather than a UUID library.

## Decision drivers

- D1 Minimal transitive dependencies for users.
- D2 Reuse of tested concurrency primitives instead of re-implementing them.
- D3 No dependency on any broker client in the core packages.

## Considered options

1. **Standard library plus `go-concurrency` (chosen)**
   - Pro: tiny module graph; the bounded-parallelism logic (admission,
     cancellation, waiting) is shared with sibling libraries and tested
     there.
   - Con: `FanoutPublisher`'s error for an invalid limit is another
     module's sentinel (`concurrency.ErrInvalidLimit`), so that module's
     API is partly exposed.
2. **Standard library only**
   - Pro: zero dependencies.
   - Con: duplicates a bounded worker group with its cancellation edge cases.
3. **Third-party UUID and errgroup-style modules**
   - Pro: well-known implementations.
   - Con: more modules for roughly twenty lines of code (UUIDv7) and a
     primitive already available from `go-concurrency`.

## Decision

We will depend only on the standard library and `go-concurrency`. Any new
dependency, including another `justusschock` library, needs an ADR. Broker
clients and protocol libraries belong in adapter modules, not in this one.

## Consequences

### Positive
- Importing the module adds one small module to a build.

### Negative
- Changes to `go-concurrency`'s error values or `ParallelMap` semantics
  affect `FanoutPublisher`.

### Follow-ups
- None.

## Links

- [PRD-0001](../prd/0001-go-messaging.md), NFR-MSG-04
