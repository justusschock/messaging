package messaging

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"testing/synctest"

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
	synctest.Test(t, func(t *testing.T) {
		var current, maximum, calls atomic.Int32
		release := make(chan struct{})
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
		go func() { result <- fanout.Publish(t.Context(), envelope) }()

		synctest.Wait()
		if calls.Load() != 2 || maximum.Load() != 2 {
			t.Fatalf("before release: calls=%d max parallel=%d, want 2 and 2", calls.Load(), maximum.Load())
		}
		close(release)
		if err := <-result; err != nil {
			t.Fatal(err)
		}
		if calls.Load() != int32(len(publishers)) || maximum.Load() != 2 {
			t.Fatalf("calls=%d max parallel=%d", calls.Load(), maximum.Load())
		}
	})
}

func TestFanoutStopsAdmittingAfterCancel(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		release := make(chan struct{})
		publishers := make([]Publisher[int], 4)
		for index := range publishers {
			publishers[index] = publisherFunc[int](func(context.Context, Envelope[int]) error {
				calls.Add(1)
				<-release
				return nil
			})
		}
		fanout, err := NewFanoutPublisher(publishers, 1)
		if err != nil {
			t.Fatal(err)
		}
		envelope, err := NewEnvelope("events", 1)
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(t.Context())
		result := make(chan error, 1)
		go func() { result <- fanout.Publish(ctx, envelope) }()

		synctest.Wait() // the first target runs, the second waits for admission
		cancel()
		synctest.Wait()
		close(release)
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("Publish error=%v", err)
		}
		if calls.Load() != 1 {
			t.Fatalf("calls=%d, want 1", calls.Load())
		}
	})
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
