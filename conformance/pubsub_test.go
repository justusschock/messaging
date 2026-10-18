package conformance_test

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/justusschock/go-messaging"
	"github.com/justusschock/go-messaging/conformance"
	"github.com/justusschock/go-messaging/memory"
)

// The in-memory broker performs no I/O, so the suite runs inside a synctest
// bubble and its real-time deadlines and polling cost no wall-clock time.

// FR-MSG-01, FR-MSG-20
func TestMemoryBroker(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		conformance.Run(t, func(t *testing.T) (messaging.Publisher[string], messaging.Subscriber[string], func()) {
			t.Helper()
			broker := memory.NewBroker[string](time.Millisecond)
			return broker, broker, func() {
				if err := broker.Close(); err != nil {
					t.Errorf("Close: %v", err)
				}
			}
		})
	})
}

// FR-MSG-20
func TestRunProvidesBoundedOperationContext(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		broker := memory.NewBroker[string](time.Millisecond)
		adapter := &deadlineAdapter{broker: broker}
		conformance.Run(t, func(*testing.T) (messaging.Publisher[string], messaging.Subscriber[string], func()) {
			return adapter, adapter, func() {
				if err := broker.Close(); err != nil {
					t.Errorf("Close: %v", err)
				}
			}
		})
	})
}

var errMissingDeadline = errors.New("operation context has no deadline")

type deadlineAdapter struct {
	broker *memory.Broker[string]
}

func (adapter *deadlineAdapter) Publish(ctx context.Context, envelope messaging.Envelope[string]) error {
	if _, ok := ctx.Deadline(); !ok {
		return errMissingDeadline
	}
	timer := time.NewTimer(time.Millisecond)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return adapter.broker.Publish(ctx, envelope)
	}
}

func (adapter *deadlineAdapter) Subscribe(ctx context.Context, subscription messaging.Subscription, handler messaging.Handler[string]) error {
	if _, ok := ctx.Deadline(); !ok {
		return errMissingDeadline
	}
	return adapter.broker.Subscribe(ctx, subscription, handler)
}
