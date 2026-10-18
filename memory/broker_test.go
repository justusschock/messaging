package memory

import (
	"context"
	"errors"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/justusschock/go-messaging"
)

// retryDelay is the broker redelivery delay used by the tests. Inside a
// synctest bubble it costs no wall-clock time.
const retryDelay = time.Second

// FR-MSG-12, FR-MSG-13
func TestBrokerAtLeastOnceRetriesUntilAck(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		broker := newBroker(t)
		ctx, cancel := context.WithCancel(t.Context())
		var mu sync.Mutex
		var attempts []uint
		observed := func() []uint {
			mu.Lock()
			defer mu.Unlock()
			return slices.Clone(attempts)
		}
		subscribed := subscribe(broker, ctx, messaging.Subscription{
			Subject: "events", Consumer: "retry", Semantics: messaging.AtLeastOnce, MaxInFlight: 1,
		}, func(ctx context.Context, delivery messaging.Delivery[int]) error {
			mu.Lock()
			attempts = append(attempts, delivery.Attempt())
			count := len(attempts)
			mu.Unlock()
			switch count {
			case 1:
				return errors.New("handler failed")
			case 2:
				return nil // returns without settling
			default:
				return delivery.Ack(ctx)
			}
		})
		requireRegistered(t, broker, "events", "retry")

		publish(t, broker, t.Context(), "events")
		synctest.Wait()
		for _, step := range []struct {
			advance time.Duration
			want    []uint
		}{
			{advance: retryDelay / 2, want: []uint{1}},
			{advance: retryDelay, want: []uint{1, 2}},
			{advance: retryDelay, want: []uint{1, 2, 3}},
			{advance: 10 * retryDelay, want: []uint{1, 2, 3}},
		} {
			advance(step.advance)
			if got := observed(); !slices.Equal(got, step.want) {
				t.Fatalf("attempts=%v, want %v", got, step.want)
			}
		}
		cancel()
		if err := <-subscribed; !errors.Is(err, context.Canceled) {
			t.Fatalf("Subscribe error=%v", err)
		}
	})
}

// FR-MSG-12
func TestBrokerAtLeastOnceRetriesAfterNack(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		broker := newBroker(t)
		ctx, cancel := context.WithCancel(t.Context())
		var calls atomic.Int32
		subscribed := subscribe(broker, ctx, messaging.Subscription{
			Subject: "events", Consumer: "nack", Semantics: messaging.AtLeastOnce, MaxInFlight: 1,
		}, func(ctx context.Context, delivery messaging.Delivery[int]) error {
			if calls.Add(1) == 1 {
				return delivery.Nack(ctx, errors.New("retry"))
			}
			return delivery.Ack(ctx)
		})
		requireRegistered(t, broker, "events", "nack")

		publish(t, broker, t.Context(), "events")
		advance(retryDelay / 2)
		if calls.Load() != 1 {
			t.Fatalf("calls before retry delay=%d, want 1", calls.Load())
		}
		advance(retryDelay)
		if calls.Load() != 2 {
			t.Fatalf("calls after retry delay=%d, want 2", calls.Load())
		}
		advance(10 * retryDelay)
		if calls.Load() != 2 {
			t.Fatalf("calls after ack=%d, want 2", calls.Load())
		}
		cancel()
		if err := <-subscribed; !errors.Is(err, context.Canceled) {
			t.Fatalf("Subscribe error=%v", err)
		}
	})
}

// FR-MSG-14
func TestBrokerAtMostOnceDoesNotRetryHandlerError(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		broker := newBroker(t)
		ctx, cancel := context.WithCancel(t.Context())
		var calls atomic.Int32
		subscribed := subscribe(broker, ctx, messaging.Subscription{
			Subject: "events", Consumer: "once", Semantics: messaging.AtMostOnce, MaxInFlight: 1,
		}, func(context.Context, messaging.Delivery[int]) error {
			calls.Add(1)
			return errors.New("handler failed")
		})
		requireRegistered(t, broker, "events", "once")

		publish(t, broker, t.Context(), "events")
		advance(10 * retryDelay)
		if calls.Load() != 1 {
			t.Fatalf("calls=%d, want 1", calls.Load())
		}
		cancel()
		if err := <-subscribed; !errors.Is(err, context.Canceled) {
			t.Fatalf("Subscribe error=%v", err)
		}
	})
}

// FR-MSG-16
func TestBrokerAtLeastOnceRetryStopsOnCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		broker := newBroker(t)
		ctx, cancel := context.WithCancel(t.Context())
		var calls atomic.Int32
		subscribed := subscribe(broker, ctx, messaging.Subscription{
			Subject: "events", Consumer: "cancel", Semantics: messaging.AtLeastOnce, MaxInFlight: 1,
		}, func(context.Context, messaging.Delivery[int]) error {
			calls.Add(1)
			return nil
		})
		requireRegistered(t, broker, "events", "cancel")

		publish(t, broker, t.Context(), "events")
		synctest.Wait()
		cancel() // while waiting for the retry delay
		if err := <-subscribed; !errors.Is(err, context.Canceled) {
			t.Fatalf("Subscribe error=%v", err)
		}
		if calls.Load() != 1 {
			t.Fatalf("calls=%d, want 1", calls.Load())
		}
	})
}

// FR-MSG-11
func TestBrokerRejectsDuplicateConsumer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		broker := newBroker(t)
		ctx, cancel := context.WithCancel(t.Context())
		subscription := messaging.Subscription{Subject: "events", Consumer: "worker", Semantics: messaging.AtMostOnce, MaxInFlight: 1}
		subscribed := subscribe(broker, ctx, subscription, func(context.Context, messaging.Delivery[int]) error { return nil })
		requireRegistered(t, broker, "events", "worker")

		err := broker.Subscribe(t.Context(), subscription, func(context.Context, messaging.Delivery[int]) error { return nil })
		if !errors.Is(err, messaging.ErrConsumerExists) {
			t.Fatalf("Subscribe error=%v", err)
		}
		cancel()
		if err := <-subscribed; !errors.Is(err, context.Canceled) {
			t.Fatalf("Subscribe error=%v", err)
		}
	})
}

// FR-MSG-17
func TestBrokerFullConsumerBufferHonorsPublishContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		broker := newBroker(t)
		ctx, cancel := context.WithCancel(t.Context())
		release := make(chan struct{})
		var calls atomic.Int32
		subscribed := subscribe(broker, ctx, messaging.Subscription{
			Subject: "events", Consumer: "blocked", Semantics: messaging.AtMostOnce, MaxInFlight: 1,
		}, func(context.Context, messaging.Delivery[int]) error {
			calls.Add(1)
			<-release
			return nil
		})
		requireRegistered(t, broker, "events", "blocked")

		publish(t, broker, t.Context(), "events") // taken by the handler
		synctest.Wait()
		if calls.Load() != 1 {
			t.Fatalf("handler calls=%d, want 1", calls.Load())
		}
		publish(t, broker, t.Context(), "events") // fills the buffer
		publishCtx, publishCancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
		defer publishCancel()
		start := time.Now()
		envelope := newEnvelope(t, "events")
		if err := broker.Publish(publishCtx, envelope); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Publish error=%v", err)
		}
		if elapsed := time.Since(start); elapsed != 20*time.Millisecond {
			t.Fatalf("Publish blocked for %v, want 20ms", elapsed)
		}

		close(release)
		synctest.Wait()
		if calls.Load() != 2 {
			t.Fatalf("handler calls=%d, want 2", calls.Load())
		}
		cancel()
		if err := <-subscribed; !errors.Is(err, context.Canceled) {
			t.Fatalf("Subscribe error=%v", err)
		}
	})
}

// FR-MSG-15
func TestBrokerDeliverySettlementIsOneShot(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		broker := newBroker(t)
		ctx, cancel := context.WithCancel(t.Context())
		results := make(chan [3]error, 1)
		subscribed := subscribe(broker, ctx, messaging.Subscription{
			Subject: "events", Consumer: "settle", Semantics: messaging.AtMostOnce, MaxInFlight: 1,
		}, func(ctx context.Context, delivery messaging.Delivery[int]) error {
			results <- [3]error{
				delivery.Ack(ctx),
				delivery.Ack(ctx),
				delivery.Nack(ctx, errors.New("late nack")),
			}
			return nil
		})
		requireRegistered(t, broker, "events", "settle")

		publish(t, broker, t.Context(), "events")
		settled := <-results
		if settled[0] != nil || !errors.Is(settled[1], messaging.ErrDeliverySettled) || !errors.Is(settled[2], messaging.ErrDeliverySettled) {
			t.Fatalf("settlement errors=%v", settled)
		}
		cancel()
		if err := <-subscribed; !errors.Is(err, context.Canceled) {
			t.Fatalf("Subscribe error=%v", err)
		}
	})
}

// FR-MSG-16, FR-MSG-17
func TestBrokerUnsubscribeUnblocksPublisher(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		broker := newBroker(t)
		ctx, cancel := context.WithCancel(t.Context())
		subscribed := subscribe(broker, ctx, messaging.Subscription{
			Subject: "events", Consumer: "blocked", Semantics: messaging.AtMostOnce, MaxInFlight: 1,
		}, func(ctx context.Context, _ messaging.Delivery[int]) error {
			<-ctx.Done()
			return ctx.Err()
		})
		requireRegistered(t, broker, "events", "blocked")
		publish(t, broker, t.Context(), "events") // taken by the handler
		synctest.Wait()
		publish(t, broker, t.Context(), "events") // fills the buffer

		published := make(chan error, 1)
		envelope := newEnvelope(t, "events")
		go func() { published <- broker.Publish(t.Context(), envelope) }()
		synctest.Wait()
		select {
		case err := <-published:
			t.Fatalf("Publish returned before unsubscribe: %v", err)
		default:
		}

		cancel()
		if err := <-published; err != nil {
			t.Fatalf("Publish error=%v", err)
		}
		if err := <-subscribed; !errors.Is(err, context.Canceled) {
			t.Fatalf("Subscribe error=%v", err)
		}
	})
}

// FR-MSG-18
func TestBrokerCloseIsIdempotentAndUnblocksSubscribers(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		broker := NewBroker[int](retryDelay)
		subscription := messaging.Subscription{Subject: "events", Consumer: "worker", Semantics: messaging.AtMostOnce, MaxInFlight: 1}
		subscribed := subscribe(broker, t.Context(), subscription, func(context.Context, messaging.Delivery[int]) error { return nil })
		requireRegistered(t, broker, "events", "worker")

		if err := broker.Close(); err != nil {
			t.Fatal(err)
		}
		if err := broker.Close(); err != nil {
			t.Fatal(err)
		}
		if err := <-subscribed; !errors.Is(err, messaging.ErrClosed) {
			t.Fatalf("Subscribe error=%v", err)
		}
		if err := broker.Publish(t.Context(), newEnvelope(t, "events")); !errors.Is(err, messaging.ErrClosed) {
			t.Fatalf("Publish error=%v", err)
		}
		if err := broker.Subscribe(t.Context(), subscription, func(context.Context, messaging.Delivery[int]) error { return nil }); !errors.Is(err, messaging.ErrClosed) {
			t.Fatalf("Subscribe after close error=%v", err)
		}
	})
}

// FR-MSG-18
func TestBrokerCloseUnblocksPublisher(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		broker := NewBroker[int](retryDelay)
		release := make(chan struct{})
		subscribed := subscribe(broker, t.Context(), messaging.Subscription{
			Subject: "events", Consumer: "blocked", Semantics: messaging.AtMostOnce, MaxInFlight: 1,
		}, func(context.Context, messaging.Delivery[int]) error {
			<-release
			return nil
		})
		requireRegistered(t, broker, "events", "blocked")
		publish(t, broker, t.Context(), "events") // taken by the handler
		synctest.Wait()
		publish(t, broker, t.Context(), "events") // fills the buffer

		published := make(chan error, 1)
		envelope := newEnvelope(t, "events")
		go func() { published <- broker.Publish(t.Context(), envelope) }()
		synctest.Wait()

		if err := broker.Close(); err != nil {
			t.Fatal(err)
		}
		if err := <-published; err != nil {
			t.Fatalf("Publish error=%v", err)
		}
		close(release)
		if err := <-subscribed; !errors.Is(err, messaging.ErrClosed) {
			t.Fatalf("Subscribe error=%v", err)
		}
	})
}

// FR-MSG-19
func TestBrokerDeliversToConsumersInNameOrder(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		broker := newBroker(t)
		ctx, cancel := context.WithCancel(t.Context())
		names := []string{"charlie", "alpha", "bravo"}
		release := make(map[string]chan struct{}, len(names))
		var results []<-chan error
		for _, name := range names {
			release[name] = make(chan struct{})
			results = append(results, subscribe(broker, ctx, messaging.Subscription{
				Subject: "events", Consumer: name, Semantics: messaging.AtMostOnce, MaxInFlight: 1,
			}, func(ctx context.Context, _ messaging.Delivery[int]) error {
				select {
				case <-release[name]:
				case <-ctx.Done():
				}
				return nil
			}))
			requireRegistered(t, broker, "events", name)
		}
		buffered := func(name string) int {
			broker.mu.RLock()
			defer broker.mu.RUnlock()
			return len(broker.consumers["events"][name].messages)
		}

		publish(t, broker, t.Context(), "events") // every handler holds one
		synctest.Wait()
		publish(t, broker, t.Context(), "events") // every buffer is full
		published := make(chan error, 1)
		envelope := newEnvelope(t, "events")
		go func() { published <- broker.Publish(t.Context(), envelope) }()

		// Freeing charlie first must not let the publisher skip ahead: it
		// waits for alpha, then bravo, and only then serves charlie.
		for _, step := range []struct {
			free string
			want map[string]int
		}{
			{free: "charlie", want: map[string]int{"alpha": 1, "bravo": 1, "charlie": 0}},
			{free: "alpha", want: map[string]int{"alpha": 1, "bravo": 1, "charlie": 0}},
			{free: "bravo", want: map[string]int{"alpha": 1, "bravo": 1, "charlie": 1}},
		} {
			synctest.Wait()
			select {
			case err := <-published:
				t.Fatalf("Publish returned before %s was freed: %v", step.free, err)
			default:
			}
			release[step.free] <- struct{}{}
			synctest.Wait()
			for name, want := range step.want {
				if got := buffered(name); got != want {
					t.Fatalf("after freeing %s: %s buffered %d, want %d", step.free, name, got, want)
				}
			}
		}
		if err := <-published; err != nil {
			t.Fatalf("Publish error=%v", err)
		}
		cancel()
		for _, result := range results {
			if err := <-result; !errors.Is(err, context.Canceled) {
				t.Fatalf("Subscribe error=%v", err)
			}
		}
	})
}

// FR-MSG-04
func TestBrokerCopiesEnvelopeAtDeliveryBoundaries(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		broker := newBroker(t)
		ctx, cancel := context.WithCancel(t.Context())
		var aliased, delivered atomic.Bool
		subscribed := subscribe(broker, ctx, messaging.Subscription{
			Subject: "events", Consumer: "copy", Semantics: messaging.AtMostOnce, MaxInFlight: 1,
		}, func(_ context.Context, delivery messaging.Delivery[int]) error {
			first := delivery.Envelope()
			first.Headers["trace"] = "handler"
			aliased.Store(delivery.Envelope().Headers["trace"] != "original")
			delivered.Store(true)
			return nil
		})
		requireRegistered(t, broker, "events", "copy")
		envelope, err := newEnvelope(t, "events").WithHeader("trace", "original")
		if err != nil {
			t.Fatal(err)
		}

		if err := broker.Publish(t.Context(), envelope); err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if !delivered.Load() || aliased.Load() {
			t.Fatalf("delivered=%v aliased=%v", delivered.Load(), aliased.Load())
		}
		if envelope.Headers["trace"] != "original" {
			t.Fatalf("publisher envelope headers=%v", envelope.Headers)
		}
		cancel()
		if err := <-subscribed; !errors.Is(err, context.Canceled) {
			t.Fatalf("Subscribe error=%v", err)
		}
	})
}

// FR-MSG-03, FR-MSG-05, FR-MSG-06
func TestBrokerValidatesInputs(t *testing.T) {
	broker := newBroker(t)

	if err := broker.Publish(t.Context(), messaging.Envelope[int]{}); !errors.Is(err, messaging.ErrInvalidEnvelope) {
		t.Fatalf("Publish error=%v", err)
	}
	if err := broker.Subscribe(t.Context(), messaging.Subscription{}, func(context.Context, messaging.Delivery[int]) error { return nil }); !errors.Is(err, messaging.ErrInvalidSubscription) {
		t.Fatalf("Subscribe error=%v", err)
	}
	valid := messaging.Subscription{Subject: "events", Consumer: "worker", Semantics: messaging.AtMostOnce, MaxInFlight: 1}
	if err := broker.Subscribe(t.Context(), valid, nil); !errors.Is(err, messaging.ErrNilDependency) {
		t.Fatalf("nil handler error=%v", err)
	}
}

// newBroker returns a broker that is closed when the test ends.
func newBroker(t *testing.T) *Broker[int] {
	t.Helper()
	broker := NewBroker[int](retryDelay)
	t.Cleanup(func() {
		if err := broker.Close(); err != nil {
			t.Errorf("Close: %v", err)
		}
	})
	return broker
}

func newEnvelope(t *testing.T, subject string) messaging.Envelope[int] {
	t.Helper()
	envelope, err := messaging.NewEnvelope(subject, 1)
	if err != nil {
		t.Fatal(err)
	}
	return envelope
}

func publish(t *testing.T, broker *Broker[int], ctx context.Context, subject string) {
	t.Helper()
	if err := broker.Publish(ctx, newEnvelope(t, subject)); err != nil {
		t.Fatalf("Publish: %v", err)
	}
}

// advance moves the bubble's fake clock forward by d and waits until every
// other goroutine in the bubble is blocked again.
func advance(d time.Duration) {
	time.Sleep(d)
	synctest.Wait()
}

// subscribe runs Subscribe in a goroutine and returns its result channel.
func subscribe[T any](broker *Broker[T], ctx context.Context, subscription messaging.Subscription, handler messaging.Handler[T]) <-chan error {
	result := make(chan error, 1)
	go func() { result <- broker.Subscribe(ctx, subscription, handler) }()
	return result
}

// requireRegistered waits until every goroutine in the bubble is blocked and
// then checks that the consumer is registered.
func requireRegistered[T any](t *testing.T, broker *Broker[T], subject, name string) {
	t.Helper()
	synctest.Wait()
	broker.mu.RLock()
	consumer := broker.consumers[subject][name]
	broker.mu.RUnlock()
	if consumer == nil {
		t.Fatalf("consumer %q not registered", name)
	}
}
