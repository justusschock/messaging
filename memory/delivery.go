package memory

import (
	"context"
	"sync/atomic"

	"github.com/justusschock/go-messaging"
)

const (
	unsettled uint32 = iota
	acknowledged
	negativelyAcknowledged
)

type delivery[T any] struct {
	envelope messaging.Envelope[T]
	attempt  uint
	state    atomic.Uint32
}

func (delivery *delivery[T]) Envelope() messaging.Envelope[T] {
	return delivery.envelope.Clone()
}

func (delivery *delivery[T]) Attempt() uint {
	return delivery.attempt
}

func (delivery *delivery[T]) Ack(context.Context) error {
	if !delivery.state.CompareAndSwap(unsettled, acknowledged) {
		return messaging.ErrDeliverySettled
	}
	return nil
}

func (delivery *delivery[T]) Nack(context.Context, error) error {
	if !delivery.state.CompareAndSwap(unsettled, negativelyAcknowledged) {
		return messaging.ErrDeliverySettled
	}
	return nil
}
