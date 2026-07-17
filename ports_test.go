package messaging

import (
	"errors"
	"testing"
)

func TestSubscriptionValidation(t *testing.T) {
	valid := Subscription{Subject: "events", Consumer: "worker", Semantics: AtLeastOnce, MaxInFlight: 4}
	if err := valid.Validate(); err != nil {
		t.Fatal(err)
	}

	valid.MaxInFlight = 0
	if err := valid.Validate(); !errors.Is(err, ErrInvalidSubscription) {
		t.Fatalf("Validate=%v", err)
	}
}

func TestSubscriptionRejectsInvalidValues(t *testing.T) {
	valid := Subscription{Subject: "events", Consumer: "worker", Semantics: AtMostOnce, MaxInFlight: 1}
	tests := map[string]func(Subscription) Subscription{
		"empty subject": func(subscription Subscription) Subscription {
			subscription.Subject = ""
			return subscription
		},
		"subject whitespace": func(subscription Subscription) Subscription {
			subscription.Subject = "event stream"
			return subscription
		},
		"empty consumer": func(subscription Subscription) Subscription {
			subscription.Consumer = ""
			return subscription
		},
		"unsupported semantics": func(subscription Subscription) Subscription {
			subscription.Semantics = 0
			return subscription
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			if err := mutate(valid).Validate(); !errors.Is(err, ErrInvalidSubscription) {
				t.Fatalf("Validate=%v", err)
			}
		})
	}
}
