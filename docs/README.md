# go-messaging documentation

Requirements (PRDs) and architecture decisions (ADRs) for `go-messaging`.
The process is described in [ADR-0001](adr/0001-record-architecture-decisions.md)
and [AGENTS.md](../AGENTS.md). New documents start from the
[PRD template](prd/TEMPLATE.md) or the [ADR template](adr/TEMPLATE.md).

## Product requirements

| PRD | Title | Status | Implementation |
|---|---|---|---|
| [0001](prd/0001-go-messaging.md) | Typed, transport-neutral messaging | Accepted | Complete |

## Architecture decisions

| ADR | Title | Status |
|---|---|---|
| [0001](adr/0001-record-architecture-decisions.md) | Record architecture decisions | Accepted |
| [0002](adr/0002-generic-transport-neutral-ports.md) | Generic, transport-neutral ports and decorators | Accepted |
| [0003](adr/0003-dependency-policy.md) | Dependency policy | Accepted |
| [0004](adr/0004-error-semantics.md) | Error semantics | Accepted |
| [0005](adr/0005-concurrency-and-cancellation.md) | Concurrency and cancellation model | Accepted |
| [0006](adr/0006-envelope-ownership-and-message-ids.md) | Envelope ownership and UUIDv7 message IDs | Accepted |
| [0007](adr/0007-delivery-semantics-and-memory-broker.md) | Delivery semantics and the in-memory reference broker | Accepted |
| [0008](adr/0008-adapter-conformance-suite.md) | Public adapter conformance suite | Accepted |
| [0009](adr/0009-opaque-authorization.md) | Opaque authorization decorators | Accepted |
| [0010](adr/0010-public-api-boundary.md) | Public API boundary | Accepted |
| [0011](adr/0011-supported-go-versions.md) | Supported Go versions | Accepted |
| [0012](adr/0012-testing-strategy.md) | Testing strategy | Accepted |
