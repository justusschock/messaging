package memory

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/justusschock/go-messaging"
)

type consumer[T any] struct {
	subscription messaging.Subscription
	messages     chan messaging.Envelope[T]
	done         chan struct{}
}

type Broker[T any] struct {
	mu         sync.RWMutex
	consumers  map[string]map[string]*consumer[T]
	retryDelay time.Duration
	closed     bool
}

func NewBroker[T any](retryDelay time.Duration) *Broker[T] {
	return &Broker[T]{
		consumers:  make(map[string]map[string]*consumer[T]),
		retryDelay: retryDelay,
	}
}

func (broker *Broker[T]) Publish(ctx context.Context, envelope messaging.Envelope[T]) error {
	if err := envelope.Validate(); err != nil {
		return err
	}

	broker.mu.RLock()
	if broker.closed {
		broker.mu.RUnlock()
		return messaging.ErrClosed
	}
	byName := broker.consumers[envelope.Subject]
	names := make([]string, 0, len(byName))
	for name := range byName {
		names = append(names, name)
	}
	sort.Strings(names)
	consumers := make([]*consumer[T], 0, len(names))
	for _, name := range names {
		consumers = append(consumers, byName[name])
	}
	broker.mu.RUnlock()

	for _, consumer := range consumers {
		select {
		case consumer.messages <- envelope.Clone():
		case <-consumer.done:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func (broker *Broker[T]) Subscribe(ctx context.Context, subscription messaging.Subscription, handler messaging.Handler[T]) error {
	if err := subscription.Validate(); err != nil {
		return err
	}
	if handler == nil {
		return fmt.Errorf("%w: handler", messaging.ErrNilDependency)
	}

	registered, err := broker.register(subscription)
	if err != nil {
		return err
	}
	defer broker.unregister(registered)

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-registered.done:
			return messaging.ErrClosed
		case envelope := <-registered.messages:
			switch subscription.Semantics {
			case messaging.AtMostOnce:
				_ = handler(ctx, &delivery[T]{envelope: envelope.Clone(), attempt: 1})
			case messaging.AtLeastOnce:
				if err := broker.deliverUntilAck(ctx, registered, envelope, handler); err != nil {
					return err
				}
			}
		}
	}
}

func (broker *Broker[T]) Close() error {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	if broker.closed {
		return nil
	}
	broker.closed = true
	for _, byName := range broker.consumers {
		for _, consumer := range byName {
			close(consumer.done)
		}
	}
	broker.consumers = make(map[string]map[string]*consumer[T])
	return nil
}

func (broker *Broker[T]) register(subscription messaging.Subscription) (*consumer[T], error) {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	if broker.closed {
		return nil, messaging.ErrClosed
	}
	byName := broker.consumers[subscription.Subject]
	if byName == nil {
		byName = make(map[string]*consumer[T])
		broker.consumers[subscription.Subject] = byName
	}
	if _, exists := byName[subscription.Consumer]; exists {
		return nil, fmt.Errorf("%w: %s/%s", messaging.ErrConsumerExists, subscription.Subject, subscription.Consumer)
	}
	registered := &consumer[T]{
		subscription: subscription,
		messages:     make(chan messaging.Envelope[T], subscription.MaxInFlight),
		done:         make(chan struct{}),
	}
	byName[subscription.Consumer] = registered
	return registered, nil
}

func (broker *Broker[T]) unregister(registered *consumer[T]) {
	broker.mu.Lock()
	defer broker.mu.Unlock()
	byName := broker.consumers[registered.subscription.Subject]
	if byName[registered.subscription.Consumer] != registered {
		return
	}
	delete(byName, registered.subscription.Consumer)
	close(registered.done)
	if len(byName) == 0 {
		delete(broker.consumers, registered.subscription.Subject)
	}
}

func (broker *Broker[T]) deliverUntilAck(ctx context.Context, registered *consumer[T], envelope messaging.Envelope[T], handler messaging.Handler[T]) error {
	for attempt := uint(1); ; attempt++ {
		delivery := &delivery[T]{envelope: envelope.Clone(), attempt: attempt}
		_ = handler(ctx, delivery)
		if delivery.state.Load() == acknowledged {
			return nil
		}

		timer := time.NewTimer(broker.retryDelay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				<-timer.C
			}
			return ctx.Err()
		case <-registered.done:
			if !timer.Stop() {
				<-timer.C
			}
			return messaging.ErrClosed
		case <-timer.C:
		}
	}
}
