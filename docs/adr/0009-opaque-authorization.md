# ADR 0009: Opaque authorization decorators

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0002, ADR-0004 |

## Context

Recorded retrospectively for the first public release. Publishing and
subscribing often need access control, but identity models and policy
languages differ between applications. The code defines an `Authorizer`
interface taking an `AuthorizationRequest{Action, Subject, Attributes}` and
two decorators, `AuthorizedPublisher` and `AuthorizedSubscriber`.

## Decision drivers

- D1 Access control must be addable without changing transports.
- D2 The library must not take a position on identities, tokens or policies
  (NG4 in PRD-0001).
- D3 A denied operation must never reach the transport.

## Considered options

1. **Opaque `Authorizer` port consulted by decorators (chosen)**
   - Pro: any policy engine fits; identities travel in the context or
     headers as the application chooses.
   - Con: the request carries only action, subject and string attributes;
     richer inputs must come from the context.
2. **Built-in role or ACL model**
   - Pro: works out of the box.
   - Con: wrong for most users; pulls identity concerns into the library.
3. **Authorization inside each transport**
   - Pro: can use protocol-level credentials.
   - Con: duplicated per adapter; untestable without the transport.

## Decision

We will provide authorization as decorators over the ports. The publisher
decorator validates the envelope and authorizes action `publish` with a copy
of the headers as attributes; the subscriber decorator validates the
subscription and handler and authorizes action `subscribe` with attribute
`consumer`. Denials wrap `ErrUnauthorized` and the authorizer's error, and
the wrapped port is not called. Validation runs before authorization, so the
authorizer only sees well-formed requests.

## Consequences

### Positive
- Policy is a single interface for applications to implement.

### Negative
- Per-message authorization on the receive side is not provided; the
  subscriber decorator authorizes the subscription once.

### Follow-ups
- None.

## Links

- [PRD-0001](../prd/0001-go-messaging.md), FR-MSG-07, FR-MSG-08
