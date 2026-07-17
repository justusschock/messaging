package messaging

import (
	"context"
	"errors"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/justusschock/go-concurrency"
)

func TestFanoutCallsAllAndJoinsIndexedErrors(t *testing.T) {
	first, second := errors.New("first"), errors.New("second")
	var calls atomic.Int32
	fanout, err := NewFanoutPublisher([]Publisher[int]{
		publisherFunc[int](func(context.Context, Envelope[int]) error {
			calls.Add(1)
			return first
		}),
		publisherFunc[int](func(context.Context, Envelope[int]) error {
			calls.Add(1)
			return second
		}),
	}, 2)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := NewEnvelope("events", 1)
	if err != nil {
		t.Fatal(err)
	}

	err = fanout.Publish(context.Background(), envelope)
	if calls.Load() != 2 || !errors.Is(err, first) || !errors.Is(err, second) || err.Error() != "publisher 0: first\npublisher 1: second" {
		t.Fatalf("calls=%d error=%v", calls.Load(), err)
	}
}

func TestFanoutClonesEnvelopePerPublisher(t *testing.T) {
	publishers := make([]Publisher[int], 2)
	for index := range publishers {
		index := index
		publishers[index] = publisherFunc[int](func(_ context.Context, envelope Envelope[int]) error {
			if envelope.Headers["trace"] != "original" {
				return errors.New("publisher received aliased headers")
			}
			envelope.Headers["publisher"] = string(rune('0' + index))
			return nil
		})
	}
	fanout, err := NewFanoutPublisher(publishers, 2)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := NewEnvelope("events", 1)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err = envelope.WithHeader("trace", "original")
	if err != nil {
		t.Fatal(err)
	}

	if err := fanout.Publish(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}
	if _, exists := envelope.Headers["publisher"]; exists {
		t.Fatalf("caller headers=%v", envelope.Headers)
	}
}

func TestFanoutLimitsParallelPublishers(t *testing.T) {
	var current atomic.Int32
	var maximum atomic.Int32
	var calls atomic.Int32
	release := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(release) }) })

	publishers := make([]Publisher[int], 6)
	for index := range publishers {
		publishers[index] = publisherFunc[int](func(context.Context, Envelope[int]) error {
			active := current.Add(1)
			for {
				observed := maximum.Load()
				if active <= observed || maximum.CompareAndSwap(observed, active) {
					break
				}
			}
			calls.Add(1)
			<-release
			current.Add(-1)
			return nil
		})
	}
	fanout, err := NewFanoutPublisher(publishers, 2)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := NewEnvelope("events", 1)
	if err != nil {
		t.Fatal(err)
	}
	result := make(chan error, 1)
	go func() { result <- fanout.Publish(context.Background(), envelope) }()

	deadline := time.Now().Add(time.Second)
	for calls.Load() < 2 && time.Now().Before(deadline) {
		runtime.Gosched()
	}
	if calls.Load() != 2 {
		t.Fatalf("calls before release=%d", calls.Load())
	}
	if maximum.Load() > 2 {
		t.Fatalf("max parallel=%d", maximum.Load())
	}
	releaseOnce.Do(func() { close(release) })
	if err := <-result; err != nil {
		t.Fatal(err)
	}
	if calls.Load() != int32(len(publishers)) || maximum.Load() > 2 {
		t.Fatalf("calls=%d max parallel=%d", calls.Load(), maximum.Load())
	}
}

func TestFanoutRejectsInvalidConfiguration(t *testing.T) {
	publisher := publisherFunc[int](func(context.Context, Envelope[int]) error { return nil })

	if _, err := NewFanoutPublisher[int](nil, 1); !errors.Is(err, ErrNilDependency) {
		t.Fatalf("empty publishers error=%v", err)
	}
	if _, err := NewFanoutPublisher([]Publisher[int]{publisher}, 0); !errors.Is(err, concurrency.ErrInvalidLimit) {
		t.Fatalf("limit error=%v", err)
	}
	if _, err := NewFanoutPublisher([]Publisher[int]{nil}, 1); !errors.Is(err, ErrNilDependency) {
		t.Fatalf("nil publisher error=%v", err)
	}
}

func TestFanoutValidatesBeforePublishing(t *testing.T) {
	called := false
	fanout, err := NewFanoutPublisher([]Publisher[int]{publisherFunc[int](func(context.Context, Envelope[int]) error {
		called = true
		return nil
	})}, 1)
	if err != nil {
		t.Fatal(err)
	}

	err = fanout.Publish(context.Background(), Envelope[int]{})
	if !errors.Is(err, ErrInvalidEnvelope) || called {
		t.Fatalf("Publish error=%v called=%v", err, called)
	}
}
