# messaging

`messaging` provides typed, transport-neutral messaging contracts for Go:
a generic envelope, small publish/subscribe, request/reply and stream
interfaces, and composable helpers around them. It is for application and
library authors who want message-driven code that does not depend on one
broker client, and for transport authors who want a shared contract and a
conformance suite to test against.

## Features

- `Envelope[T]` with validated metadata, UUIDv7 message IDs and headers
  copied at every boundary.
- Generic ports: `Publisher[T]`, `Subscriber[T]`, `Delivery[T]`,
  `Requester[Req, Resp]` and `Stream[T]`.
- Explicit `AtMostOnce` and `AtLeastOnce` delivery with one-shot `Ack`/`Nack`.
- Decorators: opaque authorization (`AuthorizedPublisher`,
  `AuthorizedSubscriber`), bounded parallel fan-out (`FanoutPublisher`) and
  in-process request/reply (`HandlerRequester`).
- `memory.Broker`: a bounded in-process broker with back-pressure, for tests
  and process-local messaging.
- `conformance.Run`: a test suite for third-party transport adapters.

## Installation

```sh
go get github.com/justusschock/messaging@latest
```

Requires Go 1.25 or newer. The only dependency outside the standard library
is [`github.com/justusschock/concurrency`](https://github.com/justusschock/concurrency),
used to bound fan-out parallelism.

## Quick start

Publish and consume with at-least-once delivery on the in-memory broker
(from [`memory/example_test.go`](memory/example_test.go)):

```go
package main

import (
	"context"
	"fmt"
	"time"

	"github.com/justusschock/messaging"
	"github.com/justusschock/messaging/memory"
)

func main() {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	broker := memory.NewBroker[string](10 * time.Millisecond)
	defer func() { _ = broker.Close() }()

	subscription := messaging.Subscription{
		Subject:     "orders.created",
		Consumer:    "audit",
		Semantics:   messaging.AtLeastOnce,
		MaxInFlight: 8,
	}
	received := make(chan string, 1)
	stopped := make(chan error, 1)
	go func() {
		stopped <- broker.Subscribe(ctx, subscription, func(ctx context.Context, delivery messaging.Delivery[string]) error {
			select {
			case received <- delivery.Envelope().Payload:
			default: // a duplicate from the publish loop below
			}
			return delivery.Ack(ctx)
		})
	}()

	envelope, err := messaging.NewEnvelope("orders.created", "order-42")
	if err != nil {
		fmt.Println(err)
		return
	}
	// Subscribe registers the consumer asynchronously, and an envelope
	// published before registration has no consumer. Publish until the
	// subscriber has received one.
	for delivered := false; !delivered; {
		if err := broker.Publish(ctx, envelope); err != nil {
			fmt.Println(err)
			return
		}
		select {
		case payload := <-received:
			fmt.Println("received", payload)
			delivered = true
		case <-time.After(time.Millisecond):
		}
	}

	cancel()
	fmt.Println("subscriber stopped:", <-stopped)
	// Prints:
	// received order-42
	// subscriber stopped: context canceled
}
```

More examples: [`ExampleFanoutPublisher`](example_test.go).

### Envelopes

An envelope owns its header map. `NewEnvelope`, `WithHeader` and `Clone`
return copies, and every helper copies headers at its boundaries. Payloads
are copied with normal Go value semantics; pointers, maps and slices inside a
payload stay shared.

```go
envelope, err := messaging.NewEnvelope("orders.created", Order{ID: "order-42"})
if err != nil {
	return err
}
envelope, err = envelope.WithHeader("trace", traceID)
if err != nil {
	return err
}
return publisher.Publish(ctx, envelope)
```

`NewMessageID` returns canonical lowercase UUIDv7 strings. Subjects must be
non-empty and contain no whitespace; envelopes also need a non-empty ID, a
non-zero `SentAt` and non-empty header keys.

### Delivery semantics

- `AtMostOnce`: the handler runs once per envelope. Handler errors and
  missing settlement do not cause a redelivery.
- `AtLeastOnce`: an attempt that is not acknowledged (handler error, `Nack`,
  or no settlement) is redelivered after the broker's retry delay with the
  next attempt number, until it is acknowledged, the subscription context
  ends or the broker closes.

Each attempt can be settled once; a second `Ack` or `Nack` returns
`ErrDeliverySettled`.

### Composition

```go
fanout, err := messaging.NewFanoutPublisher(
	[]messaging.Publisher[Event]{auditPublisher, eventPublisher, metricsPublisher},
	2, // at most two targets in parallel
)
if err != nil {
	return err
}
secured, err := messaging.NewAuthorizedPublisher[Event](fanout, authorizer)
if err != nil {
	return err
}
return secured.Publish(ctx, envelope)
```

`FanoutPublisher` runs every target, wraps failures as `publisher N: …` in
input order and joins them with `errors.Join`. The authorization decorators
call your `Authorizer` with action `publish` (headers as attributes) or
`subscribe` (attribute `consumer`); a denial matches both `ErrUnauthorized`
and the authorizer's error, and the wrapped port is not called.
`HandlerRequester` adapts a `RequestHandler` to `Requester`, validating and
copying both the request and the response.

### Adapter conformance

```go
func TestAdapter(t *testing.T) {
	conformance.Run(t, func(t *testing.T) (messaging.Publisher[string], messaging.Subscriber[string], func()) {
		adapter := newAdapter(t)
		return adapter, adapter, func() { _ = adapter.Close() }
	})
}
```

The suite checks an at-least-once round trip (ID, payload and headers
preserved), gives `Publish` and `Subscribe` a shared one-second deadline, and
requires `Subscribe` to keep running until its context ends. Purely
in-process adapters can run it inside a `testing/synctest` bubble.

## Packages

| Package | Import path | Purpose |
|---|---|---|
| `messaging` | `github.com/justusschock/messaging` | Envelope, ports, errors and decorators. |
| `memory` | `github.com/justusschock/messaging/memory` | Bounded in-process broker implementing `Publisher` and `Subscriber`. |
| `conformance` | `github.com/justusschock/messaging/conformance` | Test suite for transport adapters. |

Packages under `internal/`, if any, are not part of the API.

## Design and guarantees

- **Concurrency.** `memory.Broker` is safe for concurrent use. The decorators
  are safe for concurrent use when the components they wrap are. A
  consumer's deliveries are processed sequentially.
- **Cancellation.** Every blocking operation takes a `context.Context` and
  returns its error when it ends. `Subscribe` blocks until its context ends
  or the transport closes; cancelling the context is how you unsubscribe.
- **Bounded resources.** Each broker consumer buffers at most `MaxInFlight`
  envelopes; when the buffer is full, `Publish` blocks (back-pressure).
  Fan-out runs at most `maxParallel` targets at once.
- **Errors.** Sentinel errors (`ErrInvalidEnvelope`, `ErrInvalidSubscription`,
  `ErrUnauthorized`, `ErrDeliverySettled`, `ErrConsumerExists`, `ErrClosed`,
  `ErrNilDependency`) are matched with `errors.Is`. `NewFanoutPublisher`
  wraps `concurrency.ErrInvalidLimit` for a non-positive limit. The library
  never logs.
- **Not provided.** Durable or networked brokers, exactly-once delivery,
  subject wildcards or routing, credential handling or policy evaluation, and
  payload serialization. These belong in adapters or higher-level packages.
- **Stability.** The module is `v0.x`: minor releases may break the API.
  Every break is listed in the [changelog](CHANGELOG.md).

## Documentation

- API reference: [pkg.go.dev/github.com/justusschock/messaging](https://pkg.go.dev/github.com/justusschock/messaging)
- Requirements and design decisions: [docs/](docs/README.md)

## Contributing

Contributions go through pull requests; nothing is pushed to `main`
directly. Read [AGENTS.md](AGENTS.md) first: it describes the docs-first
process (PRDs and ADRs), the public API rules, and the checks to run before
opening a pull request.

## License

Apache License 2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
