# ADR 0006: Envelope ownership and UUIDv7 message IDs

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0002, ADR-0003 |

## Context

Recorded retrospectively for the first public release. The library defines no
wire or persistence format: envelopes are in-memory Go values and adapters
choose their own encoding. Two properties of the envelope are nevertheless
observable to adapters and users. First, `Envelope[T]` is a struct value
whose `Headers` field is a map, so naive copies alias the map across
goroutines and components. Second, `MessageID` is a string whose format users
may store, log or sort. Version v0.1.0 generated 32 random hex digits;
v0.2.0 switched to canonical UUIDv7 strings.

## Decision drivers

- D1 A component must not be able to change headers another component holds.
- D2 Message IDs should be unique, sortable by creation time and readable by
  common tooling.
- D3 No serialization assumptions in the core (NG5 in PRD-0001).

## Considered options

For headers:

1. **Copy the header map at every boundary (chosen)**
   - Pro: no aliasing between caller, decorators, transport and handlers; no
     locks needed.
   - Con: one map allocation per hop.
2. **Immutable header type with accessor methods**
   - Pro: aliasing impossible by construction.
   - Con: heavier API; harder for adapters to map to protocol headers.
3. **Document "do not mutate" only**
   - Pro: zero cost.
   - Con: data races become user bugs that are hard to find.

For IDs:

1. **UUIDv7 string generated in-module (chosen)**
   - Pro: time-ordered, 74 random bits, recognised by databases and log
     tools; no dependency (ADR-0003).
   - Con: reveals the creation millisecond; no monotonic counter within one
     millisecond, so IDs from the same millisecond are unordered.
2. **Random 128-bit hex (v0.1.0)**
   - Pro: simplest.
   - Con: not ordered, not a recognised format.
3. **UUID library**
   - Pro: full RFC 9562 support.
   - Con: a dependency for one function.

## Decision

We will treat envelopes as values whose header map is owned by exactly one
holder: `WithHeader` and `Clone` return copies, and every decorator, the
requester, fan-out and the memory broker pass copies across their
boundaries. Payloads are copied with Go value semantics only; deep-copying
them is the caller's responsibility. `NewMessageID` produces lowercase
canonical UUIDv7 strings from `time.Now` and `crypto/rand`. `MessageID`
remains a `string` type so adapters can carry any ID their protocol uses.

## Consequences

### Positive
- Header races between components are impossible as long as callers use the
  helpers.
- IDs sort by creation time across milliseconds.

### Negative
- A per-hop map copy.
- Payload aliasing (pointers, maps, slices) is still possible and documented
  as such.

### Follow-ups
- None.

## Links

- [PRD-0001](../prd/0001-messaging.md), FR-MSG-02 to FR-MSG-04
- RFC 9562, section 5.7 (UUID version 7)
