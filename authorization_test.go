package messaging

import (
	"context"
	"errors"
	"testing"
)

// FR-MSG-07
func TestAuthorizedPublisherDeniesBeforePublish(t *testing.T) {
	denied := errors.New("denied")
	called := false
	publisher, err := NewAuthorizedPublisher[int](
		publisherFunc[int](func(context.Context, Envelope[int]) error {
			called = true
			return nil
		}),
		authorizerFunc(func(_ context.Context, request AuthorizationRequest) error {
			if request.Action != "publish" || request.Subject != "events" {
				t.Fatalf("request=%+v", request)
			}
			return denied
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := NewEnvelope("events", 1)
	if err != nil {
		t.Fatal(err)
	}

	err = publisher.Publish(context.Background(), envelope)
	if !errors.Is(err, ErrUnauthorized) || !errors.Is(err, denied) || called {
		t.Fatalf("Publish error=%v called=%v", err, called)
	}
}

// FR-MSG-04, FR-MSG-07
func TestAuthorizedPublisherCopiesBoundaryHeaders(t *testing.T) {
	publisher, err := NewAuthorizedPublisher[int](
		publisherFunc[int](func(_ context.Context, envelope Envelope[int]) error {
			envelope.Headers["trace"] = "transport"
			return nil
		}),
		authorizerFunc(func(_ context.Context, request AuthorizationRequest) error {
			if request.Attributes["trace"] != "original" {
				t.Fatalf("Attributes=%v", request.Attributes)
			}
			request.Attributes["trace"] = "authorizer"
			return nil
		}),
	)
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

	if err := publisher.Publish(context.Background(), envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Headers["trace"] != "original" {
		t.Fatalf("caller headers=%v", envelope.Headers)
	}
}

// FR-MSG-07
func TestAuthorizedPublisherValidatesBeforeAuthorization(t *testing.T) {
	called := false
	publisher, err := NewAuthorizedPublisher[int](
		publisherFunc[int](func(context.Context, Envelope[int]) error { return nil }),
		authorizerFunc(func(context.Context, AuthorizationRequest) error {
			called = true
			return nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	err = publisher.Publish(context.Background(), Envelope[int]{})
	if !errors.Is(err, ErrInvalidEnvelope) || called {
		t.Fatalf("Publish error=%v authorization called=%v", err, called)
	}
}

// FR-MSG-08
func TestAuthorizedSubscriberDeniesBeforeSubscribe(t *testing.T) {
	denied := errors.New("denied")
	called := false
	subscriber, err := NewAuthorizedSubscriber[int](
		subscriberFunc[int](func(context.Context, Subscription, Handler[int]) error {
			called = true
			return nil
		}),
		authorizerFunc(func(_ context.Context, request AuthorizationRequest) error {
			if request.Action != "subscribe" || request.Subject != "events" {
				t.Fatalf("request=%+v", request)
			}
			if request.Attributes["consumer"] != "worker" {
				t.Fatalf("Attributes=%v", request.Attributes)
			}
			return denied
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	subscription := Subscription{Subject: "events", Consumer: "worker", Semantics: AtMostOnce, MaxInFlight: 1}

	err = subscriber.Subscribe(context.Background(), subscription, func(context.Context, Delivery[int]) error { return nil })
	if !errors.Is(err, ErrUnauthorized) || !errors.Is(err, denied) || called {
		t.Fatalf("Subscribe error=%v called=%v", err, called)
	}
}

// FR-MSG-08
func TestAuthorizedSubscriberValidatesBeforeAuthorization(t *testing.T) {
	called := false
	subscriber, err := NewAuthorizedSubscriber[int](
		subscriberFunc[int](func(context.Context, Subscription, Handler[int]) error { return nil }),
		authorizerFunc(func(context.Context, AuthorizationRequest) error {
			called = true
			return nil
		}),
	)
	if err != nil {
		t.Fatal(err)
	}

	err = subscriber.Subscribe(context.Background(), Subscription{}, func(context.Context, Delivery[int]) error { return nil })
	if !errors.Is(err, ErrInvalidSubscription) || called {
		t.Fatalf("Subscribe error=%v authorization called=%v", err, called)
	}
}

// FR-MSG-06
func TestAuthorizedSubscriberRejectsNilHandler(t *testing.T) {
	subscriber, err := NewAuthorizedSubscriber[int](
		subscriberFunc[int](func(context.Context, Subscription, Handler[int]) error { return nil }),
		authorizerFunc(func(context.Context, AuthorizationRequest) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	subscription := Subscription{Subject: "events", Consumer: "worker", Semantics: AtMostOnce, MaxInFlight: 1}

	if err := subscriber.Subscribe(context.Background(), subscription, nil); !errors.Is(err, ErrNilDependency) {
		t.Fatalf("Subscribe error=%v", err)
	}
}

// FR-MSG-06
func TestAuthorizedConstructorsRejectNilDependencies(t *testing.T) {
	authorizer := authorizerFunc(func(context.Context, AuthorizationRequest) error { return nil })
	publisher := publisherFunc[int](func(context.Context, Envelope[int]) error { return nil })
	subscriber := subscriberFunc[int](func(context.Context, Subscription, Handler[int]) error { return nil })

	if _, err := NewAuthorizedPublisher[int](nil, authorizer); !errors.Is(err, ErrNilDependency) {
		t.Fatalf("publisher error=%v", err)
	}
	if _, err := NewAuthorizedPublisher[int](publisher, nil); !errors.Is(err, ErrNilDependency) {
		t.Fatalf("publisher authorizer error=%v", err)
	}
	if _, err := NewAuthorizedSubscriber[int](nil, authorizer); !errors.Is(err, ErrNilDependency) {
		t.Fatalf("subscriber error=%v", err)
	}
	if _, err := NewAuthorizedSubscriber[int](subscriber, nil); !errors.Is(err, ErrNilDependency) {
		t.Fatalf("subscriber authorizer error=%v", err)
	}
}
