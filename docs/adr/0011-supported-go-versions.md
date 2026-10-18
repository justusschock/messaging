# ADR 0011: Supported Go versions

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0012 |

## Context

Recorded retrospectively for the first public release. The module declared
`go 1.22` and CI tested 1.22 through 1.26. The tests now use
`testing/synctest`, whose stable API (`synctest.Test`) exists from Go 1.25.
Go 1.23 also changed timer semantics: a stopped timer no longer delivers a
stale value, so the pattern "drain the channel when `Stop` returns false"
can block forever. The memory broker used that pattern and was fixed when
the floor was raised.

## Decision drivers

- D1 Tests must use `testing/synctest` (ADR-0012).
- D2 Users on the Go releases supported upstream must be able to use the
  module.
- D3 CI must test exactly what is claimed.

## Considered options

1. **Floor 1.25; test 1.25, oldstable and stable (chosen)**
   - Pro: synctest available everywhere; the two upstream-supported releases
     plus the floor are tested.
   - Con: users on Go 1.22 to 1.24 must upgrade.
2. **Keep floor 1.22 and guard synctest tests with build tags**
   - Pro: wider compatibility.
   - Con: two test variants; the older releases are unsupported upstream.
3. **Track only the two upstream-supported releases**
   - Pro: smallest matrix.
   - Con: the floor would move every six months without a code reason.

## Decision

We will declare `go 1.25` in `go.mod` and test Go `1.25.x`, `oldstable` and
`stable` in CI. The floor is raised only for a concrete reason (a needed
language or library feature) and with a new ADR; it never exceeds the
oldest upstream-supported release without one.

## Consequences

### Positive
- Timer behaviour is the post-1.23 semantics everywhere.

### Negative
- Go 1.22 to 1.24 users stay on v0.2.0.

### Follow-ups
- None.

## Links

- [Go release policy](https://go.dev/doc/devel/release#policy)
- [PRD-0001](../prd/0001-go-messaging.md), NFR-MSG-06
