package messaging

import (
	"context"
	"errors"
	"testing"
)

func TestHandlerRequesterValidatesResponse(t *testing.T) {
	requester, err := NewHandlerRequester(func(_ context.Context, request Envelope[int]) (Envelope[int], error) {
		return NewEnvelope(request.Subject+".reply", request.Payload*2)
	})
	if err != nil {
		t.Fatal(err)
	}
	request, err := NewEnvelope("double", 4)
	if err != nil {
		t.Fatal(err)
	}

	response, err := requester.Request(context.Background(), request)
	if err != nil || response.Payload != 8 {
		t.Fatalf("Request=(%+v,%v)", response, err)
	}
}

func TestHandlerRequesterValidatesBeforeCallingHandler(t *testing.T) {
	called := false
	requester, err := NewHandlerRequester(func(context.Context, Envelope[int]) (Envelope[int], error) {
		called = true
		return Envelope[int]{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = requester.Request(context.Background(), Envelope[int]{})
	if !errors.Is(err, ErrInvalidEnvelope) || called {
		t.Fatalf("Request error=%v called=%v", err, called)
	}
}

func TestHandlerRequesterRejectsInvalidResponse(t *testing.T) {
	requester, err := NewHandlerRequester(func(context.Context, Envelope[int]) (Envelope[int], error) {
		return Envelope[int]{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	request, err := NewEnvelope("double", 4)
	if err != nil {
		t.Fatal(err)
	}

	_, err = requester.Request(context.Background(), request)
	if !errors.Is(err, ErrInvalidEnvelope) {
		t.Fatalf("Request error=%v", err)
	}
}

func TestHandlerRequesterCopiesTransportBoundaries(t *testing.T) {
	var sourceResponse Envelope[int]
	requester, err := NewHandlerRequester(func(_ context.Context, request Envelope[int]) (Envelope[int], error) {
		request.Headers["trace"] = "handler"
		response, err := NewEnvelope("double.reply", request.Payload*2)
		if err != nil {
			return Envelope[int]{}, err
		}
		response, err = response.WithHeader("source", "original")
		sourceResponse = response
		return response, err
	})
	if err != nil {
		t.Fatal(err)
	}
	request, err := NewEnvelope("double", 4)
	if err != nil {
		t.Fatal(err)
	}
	request, err = request.WithHeader("trace", "original")
	if err != nil {
		t.Fatal(err)
	}

	response, err := requester.Request(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	sourceResponse.Headers["source"] = "changed"
	if request.Headers["trace"] != "original" {
		t.Fatalf("request headers=%v", request.Headers)
	}
	if response.Headers["source"] != "original" {
		t.Fatalf("response headers=%v", response.Headers)
	}
}

func TestHandlerRequesterReturnsHandlerError(t *testing.T) {
	want := errors.New("handler failed")
	requester, err := NewHandlerRequester(func(context.Context, Envelope[int]) (Envelope[int], error) {
		return Envelope[int]{}, want
	})
	if err != nil {
		t.Fatal(err)
	}
	request, err := NewEnvelope("double", 4)
	if err != nil {
		t.Fatal(err)
	}

	_, err = requester.Request(context.Background(), request)
	if !errors.Is(err, want) {
		t.Fatalf("Request error=%v", err)
	}
}

func TestHandlerRequesterRejectsNilHandler(t *testing.T) {
	if _, err := NewHandlerRequester[int, int](nil); !errors.Is(err, ErrNilDependency) {
		t.Fatalf("constructor error=%v", err)
	}
}
