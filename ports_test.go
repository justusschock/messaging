package messaging

import (
	"context"
	"errors"
	"testing"
)

// FR-MSG-05
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

// FR-MSG-05
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

// Compile-time checks that the helpers implement the ports they decorate.
var (
	_ Publisher[int]       = (*AuthorizedPublisher[int])(nil)
	_ Subscriber[int]      = (*AuthorizedSubscriber[int])(nil)
	_ Publisher[int]       = (*FanoutPublisher[int])(nil)
	_ Requester[int, bool] = (*HandlerRequester[int, bool])(nil)
)

// FR-MSG-01
func TestDecoratorsImplementPorts(t *testing.T) {
	var requester Requester[int, string]
	requester, err := NewHandlerRequester(func(_ context.Context, request Envelope[int]) (Envelope[string], error) {
		return NewEnvelope(request.Subject+".reply", "ok")
	})
	if err != nil {
		t.Fatal(err)
	}
	request, err := NewEnvelope("ports", 1)
	if err != nil {
		t.Fatal(err)
	}
	if response, err := requester.Request(t.Context(), request); err != nil || response.Payload != "ok" {
		t.Fatalf("Request=(%+v,%v)", response, err)
	}
}
