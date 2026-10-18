package memory_test

import (
	"context"
	"fmt"
	"time"

	"github.com/justusschock/go-messaging"
	"github.com/justusschock/go-messaging/memory"
)

// FR-MSG-12, FR-MSG-16
func ExampleBroker() {
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
	// Output:
	// received order-42
	// subscriber stopped: context canceled
}
