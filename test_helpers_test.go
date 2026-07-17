package messaging

import "context"

type publisherFunc[T any] func(context.Context, Envelope[T]) error

func (fn publisherFunc[T]) Publish(ctx context.Context, envelope Envelope[T]) error {
	return fn(ctx, envelope)
}

type subscriberFunc[T any] func(context.Context, Subscription, Handler[T]) error

func (fn subscriberFunc[T]) Subscribe(ctx context.Context, subscription Subscription, handler Handler[T]) error {
	return fn(ctx, subscription, handler)
}

type authorizerFunc func(context.Context, AuthorizationRequest) error

func (fn authorizerFunc) Authorize(ctx context.Context, request AuthorizationRequest) error {
	return fn(ctx, request)
}
