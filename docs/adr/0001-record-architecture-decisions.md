# ADR 0001: Record architecture decisions

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 |

## Context

Recorded retrospectively for the first public release. `go-messaging` was
built before its decisions were written down. Public contributors need to
know what the library promises, why it is shaped the way it is, and how to
propose changes without re-arguing settled questions.

## Decision drivers

- D1 Decisions must be discoverable next to the code they govern.
- D2 The reasoning behind a decision must survive later changes to it.
- D3 Requirements and decisions must be reviewable in pull requests like code.

## Considered options

1. **Markdown PRDs and MADR-style ADRs in `docs/`**
   - Pro: versioned with the code, reviewed in pull requests, readable on
     GitHub without tooling.
   - Con: needs discipline to keep the index and statuses current.
2. **Wiki or issue tracker**
   - Pro: easy to edit.
   - Con: not versioned with the code; edits bypass review; history of a
     decision is hard to follow.
3. **No written record (code and commit messages only)**
   - Pro: no overhead.
   - Con: the "why" and the rejected options are lost; contributors cannot
     tell contracts from accidents.

## Decision

We will record requirements as PRDs (`docs/prd/NNNN-title.md`) and
decisions as ADRs (`docs/adr/NNNN-title.md`) in MADR style, using the
templates in those directories.

- **Docs first.** Behaviour is specified in a PRD and the design in an ADR
  before or together with the code that implements it, in the same pull
  request.
- **Lifecycle.** New documents start as `Proposed` and become `Accepted`
  when merged.
- **Immutability.** An accepted ADR is not edited except for its status line.
  A changed decision gets a new ADR with `Supersedes: ADR-NNNN`, and the old
  one becomes `Superseded by ADR-MMMM`.
- **PRDs stay current.** Requirement IDs (`FR-MSG-NN`, `NFR-MSG-NN`) are never
  reused or renumbered; tests cite the IDs they verify.
- **Index.** [docs/README.md](../README.md) lists every PRD and ADR with its
  status and is updated whenever a document is added or changes status.

## Consequences

### Positive
- Reviewers see requirement and design changes next to the code change.
- Superseded decisions remain readable with their reasoning.

### Negative
- Every behaviour change carries documentation work.

### Follow-ups
- None.

## Links

- [MADR](https://adr.github.io/madr/)
- [AGENTS.md](../../AGENTS.md)
