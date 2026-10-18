package messaging

import (
	"context"
	"fmt"
)

// AuthorizationRequest describes an operation for an [Authorizer]. Action is
// "publish" or "subscribe". Attributes is a copy owned by the authorizer.
type AuthorizationRequest struct {
	Action     string
	Subject    string
	Attributes map[string]string
}

// Authorizer decides whether an operation is allowed. It returns nil to allow
// the operation and any error to deny it. The package does not interpret
// identities, credentials or policies; those belong to the implementation.
type Authorizer interface {
	Authorize(context.Context, AuthorizationRequest) error
}

// AuthorizedPublisher is a [Publisher] decorator that authorizes every publish
// before forwarding it. It is safe for concurrent use if the wrapped publisher
// and authorizer are.
type AuthorizedPublisher[T any] struct {
	next       Publisher[T]
	authorizer Authorizer
}

// NewAuthorizedPublisher wraps next with authorizer. It returns
// [ErrNilDependency] if either is nil.
func NewAuthorizedPublisher[T any](next Publisher[T], authorizer Authorizer) (*AuthorizedPublisher[T], error) {
	if next == nil {
		return nil, fmt.Errorf("%w: publisher", ErrNilDependency)
	}
	if authorizer == nil {
		return nil, fmt.Errorf("%w: authorizer", ErrNilDependency)
	}
	return &AuthorizedPublisher[T]{next: next, authorizer: authorizer}, nil
}

// Publish validates the envelope, asks the authorizer for action "publish"
// with a copy of the headers as attributes, and forwards a clone of the
// envelope. A denial returns an error wrapping both [ErrUnauthorized] and the
// authorizer's error; the wrapped publisher is not called.
func (publisher *AuthorizedPublisher[T]) Publish(ctx context.Context, envelope Envelope[T]) error {
	if err := envelope.Validate(); err != nil {
		return err
	}
	request := AuthorizationRequest{
		Action:     "publish",
		Subject:    envelope.Subject,
		Attributes: cloneHeaders(envelope.Headers),
	}
	if err := publisher.authorizer.Authorize(ctx, request); err != nil {
		return fmt.Errorf("%w: %w", ErrUnauthorized, err)
	}
	return publisher.next.Publish(ctx, envelope.Clone())
}

// AuthorizedSubscriber is a [Subscriber] decorator that authorizes every
// subscription before forwarding it. It is safe for concurrent use if the
// wrapped subscriber and authorizer are.
type AuthorizedSubscriber[T any] struct {
	next       Subscriber[T]
	authorizer Authorizer
}

// NewAuthorizedSubscriber wraps next with authorizer. It returns
// [ErrNilDependency] if either is nil.
func NewAuthorizedSubscriber[T any](next Subscriber[T], authorizer Authorizer) (*AuthorizedSubscriber[T], error) {
	if next == nil {
		return nil, fmt.Errorf("%w: subscriber", ErrNilDependency)
	}
	if authorizer == nil {
		return nil, fmt.Errorf("%w: authorizer", ErrNilDependency)
	}
	return &AuthorizedSubscriber[T]{next: next, authorizer: authorizer}, nil
}

// Subscribe validates the subscription and handler, asks the authorizer for
// action "subscribe" with the consumer name as the "consumer" attribute, and
// forwards the call. A denial returns an error wrapping both
// [ErrUnauthorized] and the authorizer's error; the wrapped subscriber is not
// called.
func (subscriber *AuthorizedSubscriber[T]) Subscribe(ctx context.Context, subscription Subscription, handler Handler[T]) error {
	if err := subscription.Validate(); err != nil {
		return err
	}
	if handler == nil {
		return fmt.Errorf("%w: handler", ErrNilDependency)
	}
	request := AuthorizationRequest{
		Action:     "subscribe",
		Subject:    subscription.Subject,
		Attributes: map[string]string{"consumer": subscription.Consumer},
	}
	if err := subscriber.authorizer.Authorize(ctx, request); err != nil {
		return fmt.Errorf("%w: %w", ErrUnauthorized, err)
	}
	return subscriber.next.Subscribe(ctx, subscription, handler)
}
