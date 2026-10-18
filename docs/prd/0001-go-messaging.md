# PRD 0001: Typed, transport-neutral messaging

| Field | Value |
|---|---|
| Status | Accepted |
| Implementation | Complete |
| Owners | Justus Perillieux |
| Related ADRs | ADR-0002, ADR-0003, ADR-0004, ADR-0005, ADR-0006, ADR-0007, ADR-0008, ADR-0009, ADR-0010, ADR-0011, ADR-0012 |
| Last updated | 2026-10-18 |

Recorded retrospectively for the first public release; describes the library
as implemented.

## 1. Problem and context

Go services that exchange events or requests tend to couple their business
logic to one broker client: its message type, its acknowledgement API, its
subscription model. Swapping the transport, testing without it, or adding a
cross-cutting concern such as authorization then means touching every call
site.

`go-messaging` gives such code a small set of typed contracts to depend on
instead: an envelope with metadata and a generic payload, and ports for
publish/subscribe, request/reply and streams. Around those contracts it
provides composable decorators (authorization, bounded fan-out, in-process
request/reply), a bounded in-memory broker for tests and process-local
messaging, and a conformance suite so third-party transport adapters can prove
they honour the contracts.

## 2. Users and use cases

- As an application developer, I want to publish and consume typed messages
  through interfaces so that business logic does not depend on a broker
  client.
- As an application developer, I want an in-process broker with explicit
  at-most-once and at-least-once semantics so that I can test message
  handlers and wire local components without infrastructure.
- As a library author (for example a cache or job queue), I want to accept
  any `Publisher`/`Subscriber` so that my users choose the transport.
- As a transport adapter author, I want a conformance suite so that I can
  show my adapter satisfies the port contracts.
- As a platform developer, I want to enforce authorization and fan out to
  several targets with bounded parallelism without changing the transport.

## 3. Goals and non-goals

### Goals

- G1 Generic, transport-neutral ports for pub/sub, request/reply and streams.
- G2 Envelopes with validated metadata, sortable unique IDs and copy-safe
  headers.
- G3 Explicit delivery semantics with one-shot settlement.
- G4 Composable decorators: authorization, bounded fan-out, in-process
  request/reply.
- G5 A bounded in-memory reference broker with back-pressure.
- G6 A public conformance suite for adapters.

### Non-goals

- NG1 Durable or networked brokers: they belong in adapters built on the ports.
- NG2 Transactions or exactly-once delivery claims.
- NG3 Schema registries, subject wildcards, routing languages and protocol
  adapters.
- NG4 Credential parsing, authentication, identity storage or policy
  evaluation: the authorization decorator stays opaque.
- NG5 Payload deep-copying or serialization: payloads follow Go value
  semantics.

## 4. Functional requirements

| ID | Requirement | Priority |
|---|---|---|
| FR-MSG-01 | The root package defines generic ports `Publisher[T]`, `Subscriber[T]` (with `Handler[T]` and `Delivery[T]`), `Requester[Req, Resp]` (with `RequestHandler[Req, Resp]`) and `Stream[T]`; the decorators and the memory broker implement the ports they wrap. | MUST |
| FR-MSG-02 | `NewMessageID` returns a canonical lowercase UUIDv7 string whose first 48 bits are the Unix millisecond time and whose remaining bits are cryptographically random; IDs from a later millisecond sort after earlier ones. | MUST |
| FR-MSG-03 | `NewEnvelope` sets a new ID, the current UTC time and an empty header map and validates the result. `Envelope.Validate` rejects an empty ID, an empty subject, a subject with whitespace, a zero `SentAt` and empty header keys with `ErrInvalidEnvelope`; `WithHeader` rejects an empty key. Every helper validates envelopes it receives before acting on them. | MUST |
| FR-MSG-04 | Header maps are never shared across a boundary: `WithHeader` and `Clone` copy them, and every decorator, the requester, fan-out and the broker pass copies to the next component and to handlers. | MUST |
| FR-MSG-05 | `Subscription.Validate` requires a non-empty subject without whitespace, a non-empty consumer, `AtMostOnce` or `AtLeastOnce` semantics and a positive `MaxInFlight`; failures wrap `ErrInvalidSubscription`. Subscribers validate before registering. | MUST |
| FR-MSG-06 | Constructors and operations reject nil collaborators (publisher, subscriber, authorizer, handler, request handler, fan-out targets) with an error wrapping `ErrNilDependency`. | MUST |
| FR-MSG-07 | `AuthorizedPublisher` validates the envelope, then calls the `Authorizer` with action `publish`, the subject and a copy of the headers as attributes. A denial returns an error matching both `ErrUnauthorized` and the authorizer's error, and the wrapped publisher is not called. | MUST |
| FR-MSG-08 | `AuthorizedSubscriber` validates the subscription and handler, then calls the `Authorizer` with action `subscribe`, the subject and the attribute `consumer`. A denial returns an error matching both `ErrUnauthorized` and the authorizer's error, and the wrapped subscriber is not called. | MUST |
| FR-MSG-09 | `FanoutPublisher` publishes a separate clone to every target with at most `maxParallel` concurrent calls; a failing target does not stop others; target errors are wrapped as `publisher N: …` in input order and joined. A non-positive limit wraps `concurrency.ErrInvalidLimit`. A cancelled context stops admission of further targets and is returned. | MUST |
| FR-MSG-10 | `HandlerRequester` validates the request, passes a clone to the handler, returns handler errors unchanged, validates the response and returns a clone of it. | MUST |
| FR-MSG-11 | The memory broker identifies a consumer by subject and consumer name; a second active subscription with the same pair fails with `ErrConsumerExists`. | MUST |
| FR-MSG-12 | Under `AtLeastOnce`, the memory broker redelivers an envelope after its configured retry delay until an attempt is acknowledged; a handler error, a `Nack` and a missing settlement all lead to redelivery. | MUST |
| FR-MSG-13 | Each delivery attempt carries a 1-based attempt number that increases by one per redelivery. | MUST |
| FR-MSG-14 | Under `AtMostOnce`, the memory broker invokes the handler once per enqueued envelope and never redelivers. | MUST |
| FR-MSG-15 | Each delivery attempt can be settled once; a further `Ack` or `Nack` returns `ErrDeliverySettled`. | MUST |
| FR-MSG-16 | `Subscribe` blocks after registration and returns only when its context ends (with the context error), the broker closes (`ErrClosed`) or a terminal error occurs. Cancellation also ends a pending redelivery wait. Unsubscribing releases publishers blocked on that consumer. | MUST |
| FR-MSG-17 | Each consumer has a buffer of `MaxInFlight` envelopes. When it is full, `Publish` blocks until space frees up, the consumer leaves or the publishing context ends, in which case it returns the context error. | MUST |
| FR-MSG-18 | `Broker.Close` is idempotent, ends active subscriptions with `ErrClosed`, releases blocked publishers, and makes later `Publish` and `Subscribe` calls return `ErrClosed`. | MUST |
| FR-MSG-19 | The memory broker hands an envelope to the consumers of its subject in consumer-name order. | SHOULD |
| FR-MSG-20 | `conformance.Run` verifies an at-least-once round trip through an adapter (ID, payload and headers preserved; acknowledgement succeeds), gives `Publish` and `Subscribe` a shared context with a one-second deadline, tolerates asynchronous registration by republishing, and requires `Subscribe` to keep running until the context ends. | MUST |

## 5. Non-functional requirements

| ID | Requirement | Measure |
|---|---|---|
| NFR-MSG-01 | Exported types state their concurrency safety; the broker and the decorators are safe for concurrent use. | `go test -race -count=3 ./...` passes. |
| NFR-MSG-02 | Memory use is bounded by configuration: per-consumer buffers by `MaxInFlight`, fan-out parallelism by `maxParallel`. | FR-MSG-09 and FR-MSG-17 tests. |
| NFR-MSG-03 | Tests are deterministic and do not wait on wall-clock time; time and concurrency are tested with `testing/synctest`. | No `time.Sleep`/wall-clock polling outside synctest bubbles. |
| NFR-MSG-04 | Dependencies are limited to the standard library and `github.com/justusschock/go-concurrency` (ADR-0003). | `go.mod` require block. |
| NFR-MSG-05 | The library never logs; it reports through return values. Errors are sentinel values matched with `errors.Is`. | Code review; error tests. |
| NFR-MSG-06 | The module supports Go 1.25 and newer (ADR-0011). | CI matrix `1.25.x`, `oldstable`, `stable`. |

## 6. API sketch

```go
package messaging

type Envelope[T any] struct {
	ID      MessageID
	Subject string
	SentAt  time.Time
	Headers map[string]string
	Payload T
}
func NewEnvelope[T any](subject string, payload T) (Envelope[T], error)
func (Envelope[T]) WithHeader(key, value string) (Envelope[T], error)
func (Envelope[T]) Clone() Envelope[T]
func (Envelope[T]) Validate() error

type Publisher[T any] interface{ Publish(context.Context, Envelope[T]) error }
type Subscriber[T any] interface {
	Subscribe(context.Context, Subscription, Handler[T]) error
}
type Delivery[T any] interface {
	Envelope() Envelope[T]
	Attempt() uint
	Ack(context.Context) error
	Nack(context.Context, error) error
}

func NewAuthorizedPublisher[T any](Publisher[T], Authorizer) (*AuthorizedPublisher[T], error)
func NewFanoutPublisher[T any]([]Publisher[T], int) (*FanoutPublisher[T], error)
func NewHandlerRequester[Req, Resp any](RequestHandler[Req, Resp]) (*HandlerRequester[Req, Resp], error)

package memory
func NewBroker[T any](retryDelay time.Duration) *Broker[T]

package conformance
type Factory func(*testing.T) (messaging.Publisher[string], messaging.Subscriber[string], func())
func Run(*testing.T, Factory)
```

## 7. Acceptance criteria

| Criterion | Requirement(s) | Test(s) |
|---|---|---|
| Helpers satisfy the ports; an adapter passes the suite | FR-MSG-01 | `TestDecoratorsImplementPorts`, `TestMemoryBroker` |
| Message IDs are UUIDv7 with the current millisecond and sort by time | FR-MSG-02 | `TestNewMessageIDIsUUIDv7String` |
| Envelope construction and validation | FR-MSG-03 | `TestNewEnvelopeInitializesValidMetadata`, `TestEnvelopeRejectsInvalidMetadata`, `TestEnvelopeWithHeaderRejectsEmptyKey`, `TestEnvelopeCopiesHeadersAndValidates`, `TestBrokerValidatesInputs` |
| Headers are copied at every boundary | FR-MSG-04 | `TestEnvelopeCopiesHeadersAndValidates`, `TestAuthorizedPublisherCopiesBoundaryHeaders`, `TestFanoutClonesEnvelopePerPublisher`, `TestHandlerRequesterCopiesTransportBoundaries`, `TestBrokerCopiesEnvelopeAtDeliveryBoundaries` |
| Subscription validation | FR-MSG-05 | `TestSubscriptionValidation`, `TestSubscriptionRejectsInvalidValues`, `TestBrokerValidatesInputs` |
| Nil collaborators are rejected | FR-MSG-06 | `TestAuthorizedConstructorsRejectNilDependencies`, `TestAuthorizedSubscriberRejectsNilHandler`, `TestHandlerRequesterRejectsNilHandler`, `TestFanoutRejectsInvalidConfiguration`, `TestBrokerValidatesInputs` |
| Publish authorization | FR-MSG-07 | `TestAuthorizedPublisherDeniesBeforePublish`, `TestAuthorizedPublisherValidatesBeforeAuthorization`, `TestAuthorizedPublisherCopiesBoundaryHeaders` |
| Subscribe authorization | FR-MSG-08 | `TestAuthorizedSubscriberDeniesBeforeSubscribe`, `TestAuthorizedSubscriberValidatesBeforeAuthorization` |
| Bounded fan-out with joined errors | FR-MSG-09 | `TestFanoutCallsAllAndJoinsIndexedErrors`, `TestFanoutLimitsParallelPublishers`, `TestFanoutValidatesBeforePublishing`, `TestFanoutStopsAdmittingAfterCancel`, `TestFanoutRejectsInvalidConfiguration`, `TestFanoutClonesEnvelopePerPublisher`, `ExampleFanoutPublisher` |
| In-process request/reply | FR-MSG-10 | `TestHandlerRequesterValidatesResponse`, `TestHandlerRequesterValidatesBeforeCallingHandler`, `TestHandlerRequesterRejectsInvalidResponse`, `TestHandlerRequesterReturnsHandlerError`, `TestHandlerRequesterCopiesTransportBoundaries` |
| Duplicate consumers are rejected | FR-MSG-11 | `TestBrokerRejectsDuplicateConsumer` |
| At-least-once redelivery after the retry delay | FR-MSG-12 | `TestBrokerAtLeastOnceRetriesUntilAck`, `TestBrokerAtLeastOnceRetriesAfterNack`, `ExampleBroker` |
| Attempt numbers increase | FR-MSG-13 | `TestBrokerAtLeastOnceRetriesUntilAck` |
| At-most-once never redelivers | FR-MSG-14 | `TestBrokerAtMostOnceDoesNotRetryHandlerError` |
| One-shot settlement | FR-MSG-15 | `TestBrokerDeliverySettlementIsOneShot` |
| Subscriber lifetime and cancellation | FR-MSG-16 | `TestBrokerAtLeastOnceRetryStopsOnCancel`, `TestBrokerUnsubscribeUnblocksPublisher`, `ExampleBroker` |
| Bounded buffers and back-pressure | FR-MSG-17 | `TestBrokerFullConsumerBufferHonorsPublishContext`, `TestBrokerUnsubscribeUnblocksPublisher` |
| Close semantics | FR-MSG-18 | `TestBrokerCloseIsIdempotentAndUnblocksSubscribers`, `TestBrokerCloseUnblocksPublisher` |
| Consumer-name order | FR-MSG-19 | `TestBrokerDeliversToConsumersInNameOrder` |
| Conformance suite | FR-MSG-20 | `TestMemoryBroker`, `TestRunProvidesBoundedOperationContext` |

## 8. Source references

None. The UUIDv7 layout follows RFC 9562, section 5.7.

## 9. Open questions

- Q1 `Subscribe` registers the consumer asynchronously from the caller's point
  of view, so an envelope published before registration has no consumer. The
  conformance suite and the broker example work around this by republishing.
  A registration signal (for example a callback or a separate `Register`
  step) would remove the workaround; it would be a new API and needs an ADR.
- Q2 `Stream[T]` has no implementation in this module. It is kept as a port
  for adapters; whether a conformance suite for streams is needed is open.
