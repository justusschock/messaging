// Package messaging defines typed, transport-neutral messaging contracts and
// small composition helpers.
//
// The core types are [Envelope], an immutable-by-convention message with
// metadata and a typed payload, and the generic ports [Publisher],
// [Subscriber], [Delivery], [Requester] and [Stream]. Transports implement
// the ports; application code depends only on them.
//
// The package also provides decorators that compose with any transport:
// [AuthorizedPublisher] and [AuthorizedSubscriber] consult an opaque
// [Authorizer], [FanoutPublisher] publishes to several targets with bounded
// parallelism, and [HandlerRequester] adapts an in-process
// [RequestHandler] to [Requester].
//
// Every helper copies envelope headers at its boundaries, so callers and
// transports never share a header map. Payloads are copied with normal Go
// value semantics; pointers, maps and slices inside a payload remain shared.
//
// Errors are reported through sentinel values such as [ErrInvalidEnvelope]
// that support [errors.Is]. Blocking operations take a [context.Context] and
// return its error when it ends.
//
// The memory sub-package provides a bounded in-process broker; the
// conformance sub-package provides a test suite for transport adapters.
package messaging
