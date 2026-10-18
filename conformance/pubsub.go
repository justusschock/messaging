package conformance

import (
	"context"
	"testing"
	"time"

	"github.com/justusschock/messaging"
)

// Factory creates the publisher and subscriber under test, typically the same
// adapter value, and an optional cleanup function that [Run] calls when it
// finishes.
type Factory func(t *testing.T) (messaging.Publisher[string], messaging.Subscriber[string], func())

// Run verifies an at-least-once publish/subscribe round trip: the subscriber
// receives and acknowledges the message with its ID, payload and headers
// preserved, and Subscribe keeps running until its context ends.
//
// Publish and Subscribe share one context with a one-second deadline, so a
// context-aware adapter fails the test instead of hanging it. Run publishes
// the same envelope repeatedly until it is delivered, so adapters that
// register subscriptions asynchronously need no test-only synchronization.
func Run(t *testing.T, factory Factory) {
	t.Helper()
	publisher, subscriber, cleanup := factory(t)
	if publisher == nil || subscriber == nil {
		t.Fatal("conformance factory returned a nil messaging port")
	}
	if cleanup != nil {
		defer cleanup()
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	type result struct {
		envelope messaging.Envelope[string]
		err      error
	}
	received := make(chan result, 1)
	subscription := messaging.Subscription{
		Subject: "conformance.events", Consumer: "conformance", Semantics: messaging.AtLeastOnce, MaxInFlight: 1,
	}
	subscribed := make(chan error, 1)
	go func() {
		subscribed <- subscriber.Subscribe(ctx, subscription, func(ctx context.Context, delivery messaging.Delivery[string]) error {
			envelope := delivery.Envelope()
			if err := delivery.Ack(ctx); err != nil {
				select {
				case received <- result{err: err}:
				default:
				}
				return err
			}
			select {
			case received <- result{envelope: envelope}:
			default:
			}
			return nil
		})
	}()

	envelope, err := messaging.NewEnvelope("conformance.events", "payload")
	if err != nil {
		t.Fatal(err)
	}
	envelope, err = envelope.WithHeader("trace", "preserved")
	if err != nil {
		t.Fatal(err)
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if err := publisher.Publish(ctx, envelope); err != nil {
			t.Fatalf("Publish: %v", err)
		}
		select {
		case delivered := <-received:
			if delivered.err != nil {
				t.Fatalf("Ack: %v", delivered.err)
			}
			if delivered.envelope.ID != envelope.ID {
				t.Fatalf("ID=%q, want %q", delivered.envelope.ID, envelope.ID)
			}
			if delivered.envelope.Payload != envelope.Payload {
				t.Fatalf("Payload=%q, want %q", delivered.envelope.Payload, envelope.Payload)
			}
			if delivered.envelope.Headers["trace"] != "preserved" {
				t.Fatalf("Headers=%v", delivered.envelope.Headers)
			}
			cancel()
			select {
			case err := <-subscribed:
				// Only termination is required; the returned error after
				// cancellation is adapter-defined.
				_ = err
			case <-time.After(time.Second):
				t.Fatal("subscriber did not stop during cleanup")
			}
			return
		case err := <-subscribed:
			t.Fatalf("Subscribe returned before delivery: %v", err)
		case <-ticker.C:
		case <-ctx.Done():
			t.Fatalf("delivery timed out: %v", context.Cause(ctx))
		}
	}
}
