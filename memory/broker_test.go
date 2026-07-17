package memory

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/justusschock/go-messaging"
)

func TestBrokerAtLeastOnceRetriesUntilAck(t *testing.T) {
	broker := NewBroker[int](time.Millisecond)
	defer broker.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	attempts := make(chan uint, 2)
	var calls atomic.Int32
	subscribed := subscribe(t, broker, ctx, messaging.Subscription{
		Subject: "events", Consumer: "retry", Semantics: messaging.AtLeastOnce, MaxInFlight: 1,
	}, func(ctx context.Context, delivery messaging.Delivery[int]) error {
		attempts <- delivery.Attempt()
		if calls.Add(1) == 2 {
			if err := delivery.Ack(ctx); err != nil {
				t.Error(err)
			}
			close(done)
		}
		return nil
	})
	waitRegistered(t, broker, "events", "retry")
	envelope, err := messaging.NewEnvelope("events", 1)
	if err != nil {
		t.Fatal(err)
	}

	if err := broker.Publish(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("retry timed out")
	}
	if calls.Load() != 2 {
		t.Fatalf("calls=%d", calls.Load())
	}
	if first, second := <-attempts, <-attempts; first != 1 || second != 2 {
		t.Fatalf("attempts=%d,%d", first, second)
	}
	cancel()
	if err := <-subscribed; !errors.Is(err, context.Canceled) {
		t.Fatalf("Subscribe error=%v", err)
	}
}

func TestBrokerAtLeastOnceRetriesAfterNack(t *testing.T) {
	broker := NewBroker[int](time.Millisecond)
	defer broker.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	var calls atomic.Int32
	subscribed := subscribe(t, broker, ctx, messaging.Subscription{
		Subject: "events", Consumer: "nack", Semantics: messaging.AtLeastOnce, MaxInFlight: 1,
	}, func(ctx context.Context, delivery messaging.Delivery[int]) error {
		switch calls.Add(1) {
		case 1:
			return delivery.Nack(ctx, errors.New("retry"))
		case 2:
			if err := delivery.Ack(ctx); err != nil {
				return err
			}
			close(done)
		}
		return nil
	})
	waitRegistered(t, broker, "events", "nack")
	envelope, err := messaging.NewEnvelope("events", 1)
	if err != nil {
		t.Fatal(err)
	}

	if err := broker.Publish(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("nack retry timed out")
	}
	if calls.Load() != 2 {
		t.Fatalf("calls=%d", calls.Load())
	}
	cancel()
	<-subscribed
}

func TestBrokerAtMostOnceDoesNotRetryHandlerError(t *testing.T) {
	broker := NewBroker[int](time.Millisecond)
	defer broker.Close()
	ctx, cancel := context.WithCancel(context.Background())
	called := make(chan struct{})
	var once sync.Once
	var calls atomic.Int32
	subscribed := subscribe(t, broker, ctx, messaging.Subscription{
		Subject: "events", Consumer: "once", Semantics: messaging.AtMostOnce, MaxInFlight: 1,
	}, func(context.Context, messaging.Delivery[int]) error {
		calls.Add(1)
		once.Do(func() { close(called) })
		return errors.New("handler failed")
	})
	waitRegistered(t, broker, "events", "once")
	envelope, err := messaging.NewEnvelope("events", 1)
	if err != nil {
		t.Fatal(err)
	}

	if err := broker.Publish(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}
	select {
	case <-called:
	case <-time.After(time.Second):
		t.Fatal("handler timed out")
	}
	time.Sleep(10 * time.Millisecond)
	if calls.Load() != 1 {
		t.Fatalf("calls=%d", calls.Load())
	}
	cancel()
	<-subscribed
}

func TestBrokerRejectsDuplicateConsumer(t *testing.T) {
	broker := NewBroker[int](time.Millisecond)
	defer broker.Close()
	ctx, cancel := context.WithCancel(context.Background())
	subscription := messaging.Subscription{Subject: "events", Consumer: "worker", Semantics: messaging.AtMostOnce, MaxInFlight: 1}
	subscribed := subscribe(t, broker, ctx, subscription, func(context.Context, messaging.Delivery[int]) error { return nil })
	waitRegistered(t, broker, "events", "worker")

	err := broker.Subscribe(context.Background(), subscription, func(context.Context, messaging.Delivery[int]) error { return nil })
	if !errors.Is(err, messaging.ErrConsumerExists) {
		t.Fatalf("Subscribe error=%v", err)
	}
	cancel()
	<-subscribed
}

func TestBrokerFullConsumerBufferHonorsPublishContext(t *testing.T) {
	broker := NewBroker[int](time.Millisecond)
	defer broker.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	handlerStarted := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })
	subscribed := subscribe(t, broker, ctx, messaging.Subscription{
		Subject: "events", Consumer: "blocked", Semantics: messaging.AtMostOnce, MaxInFlight: 1,
	}, func(context.Context, messaging.Delivery[int]) error {
		select {
		case <-handlerStarted:
		default:
			close(handlerStarted)
		}
		<-release
		return nil
	})
	waitRegistered(t, broker, "events", "blocked")
	envelope, err := messaging.NewEnvelope("events", 1)
	if err != nil {
		t.Fatal(err)
	}

	if err := broker.Publish(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}
	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	if err := broker.Publish(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}
	publishCtx, publishCancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer publishCancel()
	if err := broker.Publish(publishCtx, envelope); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("Publish error=%v", err)
	}

	releaseOnce.Do(func() { close(release) })
	cancel()
	<-subscribed
}

func TestBrokerDeliverySettlementIsOneShot(t *testing.T) {
	broker := NewBroker[int](time.Millisecond)
	defer broker.Close()
	ctx, cancel := context.WithCancel(context.Background())
	results := make(chan [3]error, 1)
	subscribed := subscribe(t, broker, ctx, messaging.Subscription{
		Subject: "events", Consumer: "settle", Semantics: messaging.AtMostOnce, MaxInFlight: 1,
	}, func(ctx context.Context, delivery messaging.Delivery[int]) error {
		results <- [3]error{
			delivery.Ack(ctx),
			delivery.Ack(ctx),
			delivery.Nack(ctx, errors.New("late nack")),
		}
		return nil
	})
	waitRegistered(t, broker, "events", "settle")
	envelope, err := messaging.NewEnvelope("events", 1)
	if err != nil {
		t.Fatal(err)
	}

	if err := broker.Publish(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}
	select {
	case settled := <-results:
		if settled[0] != nil || !errors.Is(settled[1], messaging.ErrDeliverySettled) || !errors.Is(settled[2], messaging.ErrDeliverySettled) {
			t.Fatalf("settlement errors=%v", settled)
		}
	case <-time.After(time.Second):
		t.Fatal("settlement timed out")
	}
	cancel()
	<-subscribed
}

func TestBrokerUnsubscribeUnblocksPublisher(t *testing.T) {
	broker := NewBroker[int](time.Millisecond)
	defer broker.Close()
	ctx, cancel := context.WithCancel(context.Background())
	handlerStarted := make(chan struct{})
	subscribed := subscribe(t, broker, ctx, messaging.Subscription{
		Subject: "events", Consumer: "blocked", Semantics: messaging.AtMostOnce, MaxInFlight: 1,
	}, func(ctx context.Context, _ messaging.Delivery[int]) error {
		select {
		case <-handlerStarted:
		default:
			close(handlerStarted)
		}
		<-ctx.Done()
		return ctx.Err()
	})
	waitRegistered(t, broker, "events", "blocked")
	envelope, err := messaging.NewEnvelope("events", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := broker.Publish(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}
	select {
	case <-handlerStarted:
	case <-time.After(time.Second):
		t.Fatal("handler did not start")
	}
	if err := broker.Publish(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}
	published := make(chan error, 1)
	go func() { published <- broker.Publish(context.Background(), envelope) }()
	select {
	case err := <-published:
		t.Fatalf("Publish returned before unsubscribe: %v", err)
	case <-time.After(10 * time.Millisecond):
	}

	cancel()
	select {
	case err := <-published:
		if err != nil {
			t.Fatalf("Publish error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("publisher remained blocked")
	}
	if err := <-subscribed; !errors.Is(err, context.Canceled) {
		t.Fatalf("Subscribe error=%v", err)
	}
}

func TestBrokerCloseIsIdempotentAndUnblocksSubscribers(t *testing.T) {
	broker := NewBroker[int](time.Millisecond)
	subscription := messaging.Subscription{Subject: "events", Consumer: "worker", Semantics: messaging.AtMostOnce, MaxInFlight: 1}
	subscribed := subscribe(t, broker, context.Background(), subscription, func(context.Context, messaging.Delivery[int]) error { return nil })
	waitRegistered(t, broker, "events", "worker")

	if err := broker.Close(); err != nil {
		t.Fatal(err)
	}
	if err := broker.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-subscribed:
		if !errors.Is(err, messaging.ErrClosed) {
			t.Fatalf("Subscribe error=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("subscriber remained blocked")
	}
	envelope, err := messaging.NewEnvelope("events", 1)
	if err != nil {
		t.Fatal(err)
	}
	if err := broker.Publish(context.Background(), envelope); !errors.Is(err, messaging.ErrClosed) {
		t.Fatalf("Publish error=%v", err)
	}
	if err := broker.Subscribe(context.Background(), subscription, func(context.Context, messaging.Delivery[int]) error { return nil }); !errors.Is(err, messaging.ErrClosed) {
		t.Fatalf("Subscribe after close error=%v", err)
	}
}

func TestBrokerCopiesEnvelopeAtDeliveryBoundaries(t *testing.T) {
	broker := NewBroker[int](time.Millisecond)
	defer broker.Close()
	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	subscribed := subscribe(t, broker, ctx, messaging.Subscription{
		Subject: "events", Consumer: "copy", Semantics: messaging.AtMostOnce, MaxInFlight: 1,
	}, func(_ context.Context, delivery messaging.Delivery[int]) error {
		first := delivery.Envelope()
		first.Headers["trace"] = "handler"
		if delivery.Envelope().Headers["trace"] != "original" {
			result <- errors.New("delivery envelope headers aliased")
			return nil
		}
		result <- nil
		return nil
	})
	waitRegistered(t, broker, "events", "copy")
	envelope, err := messaging.NewEnvelope("events", 1)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err = envelope.WithHeader("trace", "original")
	if err != nil {
		t.Fatal(err)
	}

	if err := broker.Publish(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("delivery timed out")
	}
	if envelope.Headers["trace"] != "original" {
		t.Fatalf("publisher envelope headers=%v", envelope.Headers)
	}
	cancel()
	<-subscribed
}

func TestBrokerValidatesInputs(t *testing.T) {
	broker := NewBroker[int](time.Millisecond)
	defer broker.Close()

	if err := broker.Publish(context.Background(), messaging.Envelope[int]{}); !errors.Is(err, messaging.ErrInvalidEnvelope) {
		t.Fatalf("Publish error=%v", err)
	}
	if err := broker.Subscribe(context.Background(), messaging.Subscription{}, func(context.Context, messaging.Delivery[int]) error { return nil }); !errors.Is(err, messaging.ErrInvalidSubscription) {
		t.Fatalf("Subscribe error=%v", err)
	}
	valid := messaging.Subscription{Subject: "events", Consumer: "worker", Semantics: messaging.AtMostOnce, MaxInFlight: 1}
	if err := broker.Subscribe(context.Background(), valid, nil); !errors.Is(err, messaging.ErrNilDependency) {
		t.Fatalf("nil handler error=%v", err)
	}
}

func subscribe[T any](t *testing.T, broker *Broker[T], ctx context.Context, subscription messaging.Subscription, handler messaging.Handler[T]) <-chan error {
	t.Helper()
	result := make(chan error, 1)
	go func() { result <- broker.Subscribe(ctx, subscription, handler) }()
	return result
}

func waitRegistered[T any](t *testing.T, broker *Broker[T], subject, name string) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		broker.mu.RLock()
		consumer := broker.consumers[subject][name]
		broker.mu.RUnlock()
		if consumer != nil {
			return
		}
		runtime.Gosched()
	}
	t.Fatalf("consumer %q not registered", name)
}
