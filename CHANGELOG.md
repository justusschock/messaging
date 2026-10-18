# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the major version is `0`, a minor release may break the API.

## [Unreleased]

## [0.1.0] - 2026-10-18

First public release.

### Added

- `Envelope[T]` (`NewEnvelope`) with validated metadata, UUIDv7 message IDs
  from `NewMessageID`, and headers copied at every boundary.
- Transport-neutral generic ports: `Publisher[T]`, `Subscriber[T]`,
  `Delivery[T]`, `Requester[Req, Resp]` and `Stream[T]`.
- Explicit `AtMostOnce` and `AtLeastOnce` delivery semantics with one-shot
  `Ack` and `Nack`.
- Authorization decorators `AuthorizedPublisher` and `AuthorizedSubscriber`
  that consult an opaque `Authorizer`.
- `FanoutPublisher`: bounded parallel fan-out that joins target errors in
  input order.
- `HandlerRequester`: in-process request/reply over a `RequestHandler`.
- Package `memory`: `memory.Broker`, a bounded in-process broker with
  back-pressure and at-least-once redelivery.
- Package `conformance`: `Run`, a test suite for transport adapters.
- Sentinel errors for `errors.Is`, such as `ErrInvalidEnvelope`,
  `ErrUnauthorized`, `ErrDeliverySettled` and `ErrClosed`.
- Requirements and architecture decision records under `docs/`.

Requires Go 1.25 or newer.

[Unreleased]: https://github.com/justusschock/messaging/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/justusschock/messaging/releases/tag/v0.1.0
