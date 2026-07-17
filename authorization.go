package messaging

import (
	"context"
	"fmt"
)

type AuthorizationRequest struct {
	Action     string
	Subject    string
	Attributes map[string]string
}

type Authorizer interface {
	Authorize(context.Context, AuthorizationRequest) error
}

type AuthorizedPublisher[T any] struct {
	next       Publisher[T]
	authorizer Authorizer
}

func NewAuthorizedPublisher[T any](next Publisher[T], authorizer Authorizer) (*AuthorizedPublisher[T], error) {
	if next == nil {
		return nil, fmt.Errorf("%w: publisher", ErrNilDependency)
	}
	if authorizer == nil {
		return nil, fmt.Errorf("%w: authorizer", ErrNilDependency)
	}
	return &AuthorizedPublisher[T]{next: next, authorizer: authorizer}, nil
}

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

type AuthorizedSubscriber[T any] struct {
	next       Subscriber[T]
	authorizer Authorizer
}

func NewAuthorizedSubscriber[T any](next Subscriber[T], authorizer Authorizer) (*AuthorizedSubscriber[T], error) {
	if next == nil {
		return nil, fmt.Errorf("%w: subscriber", ErrNilDependency)
	}
	if authorizer == nil {
		return nil, fmt.Errorf("%w: authorizer", ErrNilDependency)
	}
	return &AuthorizedSubscriber[T]{next: next, authorizer: authorizer}, nil
}

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
