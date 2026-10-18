# Changelog

All notable changes to this project are documented in this file. The format
is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the
project follows [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the major version is 0, minor releases may contain breaking changes;
each one is listed here.

## [Unreleased]

Preparation for the first public release.

### Changed

- **Breaking:** the module now requires Go 1.25 (was 1.22), because the tests
  use `testing/synctest`. CI tests Go 1.25, oldstable and stable
  ([ADR-0011](docs/adr/0011-supported-go-versions.md)).
- The public API is unchanged: the review recorded in
  [ADR-0010](docs/adr/0010-public-api-boundary.md) found no exported
  implementation details, so no package moved to `internal/` and no
  identifier was unexported.
- The conformance suite explicitly accepts any `Subscribe` result after
  cancellation (behaviour unchanged).

### Fixed

- `memory.Broker` no longer drains a retry timer's channel after `Stop`.
  With Go 1.23+ timer semantics, which apply whenever the main module
  declares `go 1.23` or later, the drain could block an at-least-once
  subscriber forever if its context ended or the broker closed just as a
  redelivery timer fired.

### Added

- Package and identifier documentation for every exported API.
- `memory.ExampleBroker`, used as the README quick start.
- Tests run on `testing/synctest` virtual time; new tests cover consumer-name
  ordering, redelivery cancellation, `Close` releasing blocked publishers and
  fan-out admission after cancellation. Tests cite requirement IDs.
- Requirements ([PRD-0001](docs/prd/0001-messaging.md)) and retrospective
  architecture decisions in [docs/](docs/README.md).
- `NOTICE`, `AGENTS.md`, a golangci-lint configuration, Dependabot, and a CI
  workflow with SHA-pinned actions and a lint job.

## [0.2.0] - 2026-07-17

### Changed

- `NewMessageID` generates canonical lowercase UUIDv7 strings instead of 32
  random hex digits.
- The module supports Go 1.22 and newer and depends on `concurrency`
  v0.2.0.

## [0.1.0] - 2026-07-17

### Added

- Typed envelopes and transport-neutral pub/sub, request/reply and stream
  ports.
- Authorization decorators, in-process request/reply and bounded fan-out.
- Bounded in-memory broker with at-most-once and at-least-once delivery.
- Pub/sub adapter conformance suite.

[Unreleased]: https://github.com/justusschock/messaging/compare/v0.2.0...HEAD
[0.2.0]: https://github.com/justusschock/messaging/compare/v0.1.0...v0.2.0
[0.1.0]: https://github.com/justusschock/messaging/releases/tag/v0.1.0
