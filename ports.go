package messaging

import (
	"context"
	"fmt"
	"strings"
	"unicode"
)

// DeliverySemantics selects how a subscriber treats unacknowledged deliveries.
type DeliverySemantics uint8

const (
	// AtMostOnce invokes the handler once per message; handler errors and
	// missing settlement do not cause a redelivery.
	AtMostOnce DeliverySemantics = iota + 1
	// AtLeastOnce redelivers a message until the handler acknowledges it.
	AtLeastOnce
)

// Subscription identifies a consumer of a subject and configures delivery.
//
// Subject and Consumer together identify the consumer; a transport may reject
// a second active subscription with the same pair ([ErrConsumerExists]).
// MaxInFlight bounds how many messages the transport buffers for the consumer.
type Subscription struct {
	Subject     string
	Consumer    string
	Semantics   DeliverySemantics
	MaxInFlight int
}

// Validate reports whether the subscription is well-formed. Subject must be
// non-empty without whitespace, Consumer non-empty, Semantics one of
// [AtMostOnce] or [AtLeastOnce], and MaxInFlight positive. Failures wrap
// [ErrInvalidSubscription].
func (subscription Subscription) Validate() error {
	switch {
	case subscription.Subject == "":
		return fmt.Errorf("%w: empty subject", ErrInvalidSubscription)
	case strings.IndexFunc(subscription.Subject, unicode.IsSpace) >= 0:
		return fmt.Errorf("%w: subject contains whitespace", ErrInvalidSubscription)
	case subscription.Consumer == "":
		return fmt.Errorf("%w: empty consumer", ErrInvalidSubscription)
	case subscription.Semantics != AtMostOnce && subscription.Semantics != AtLeastOnce:
		return fmt.Errorf("%w: unsupported delivery semantics", ErrInvalidSubscription)
	case subscription.MaxInFlight <= 0:
		return fmt.Errorf("%w: max in flight must be positive", ErrInvalidSubscription)
	default:
		return nil
	}
}

// Publisher publishes envelopes to a transport.
//
// Publish returns after the transport has accepted the envelope, or with the
// context's error if ctx ends first. Implementations must not retain or
// mutate the caller's header map.
type Publisher[T any] interface {
	Publish(context.Context, Envelope[T]) error
}

// Delivery is one delivery attempt of an envelope to a handler.
//
// Envelope returns a copy of the delivered envelope and Attempt the 1-based
// attempt number. Ack and Nack settle the attempt; each attempt can be
// settled once, and a second settlement returns [ErrDeliverySettled].
type Delivery[T any] interface {
	Envelope() Envelope[T]
	Attempt() uint
	Ack(context.Context) error
	Nack(context.Context, error) error
}

// Handler processes one delivery. Under [AtLeastOnce] semantics a delivery
// that the handler does not acknowledge is redelivered.
type Handler[T any] func(context.Context, Delivery[T]) error

// Subscriber runs a handler for a subscription.
//
// Subscribe validates the subscription, registers the consumer and then
// blocks, invoking handler for each delivery, until ctx ends, the transport
// closes, or a terminal error occurs. Successful registration alone does not
// make it return.
type Subscriber[T any] interface {
	Subscribe(context.Context, Subscription, Handler[T]) error
}

// Requester sends a request envelope and waits for the response envelope.
type Requester[Req, Resp any] interface {
	Request(context.Context, Envelope[Req]) (Envelope[Resp], error)
}

// RequestHandler answers a request envelope with a response envelope.
type RequestHandler[Req, Resp any] func(context.Context, Envelope[Req]) (Envelope[Resp], error)

// Stream is a bidirectional sequence of envelopes. Send and Receive block
// until they complete or ctx ends; Close releases the stream.
//
// The package defines the port only; implementations live in transports.
type Stream[T any] interface {
	Send(context.Context, Envelope[T]) error
	Receive(context.Context) (Envelope[T], error)
	Close() error
}
