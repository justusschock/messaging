package conformance_test

import (
	"testing"
	"time"

	"github.com/justusschock/go-messaging"
	"github.com/justusschock/go-messaging/conformance"
	"github.com/justusschock/go-messaging/memory"
)

func TestMemoryBroker(t *testing.T) {
	conformance.Run(t, func(t *testing.T) (messaging.Publisher[string], messaging.Subscriber[string], func()) {
		t.Helper()
		broker := memory.NewBroker[string](time.Millisecond)
		return broker, broker, func() {
			if err := broker.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		}
	})
}
