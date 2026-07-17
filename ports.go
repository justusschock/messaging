package messaging

import (
	"context"
	"fmt"
	"strings"
	"unicode"
)

type DeliverySemantics uint8

const (
	AtMostOnce DeliverySemantics = iota + 1
	AtLeastOnce
)

type Subscription struct {
	Subject     string
	Consumer    string
	Semantics   DeliverySemantics
	MaxInFlight int
}

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

type Publisher[T any] interface {
	Publish(context.Context, Envelope[T]) error
}

type Delivery[T any] interface {
	Envelope() Envelope[T]
	Attempt() uint
	Ack(context.Context) error
	Nack(context.Context, error) error
}

type Handler[T any] func(context.Context, Delivery[T]) error

type Subscriber[T any] interface {
	Subscribe(context.Context, Subscription, Handler[T]) error
}

type Requester[Req, Resp any] interface {
	Request(context.Context, Envelope[Req]) (Envelope[Resp], error)
}

type RequestHandler[Req, Resp any] func(context.Context, Envelope[Req]) (Envelope[Resp], error)

type Stream[T any] interface {
	Send(context.Context, Envelope[T]) error
	Receive(context.Context) (Envelope[T], error)
	Close() error
}
