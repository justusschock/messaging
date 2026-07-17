package messaging

import (
	"context"
	"errors"
	"fmt"

	"github.com/justusschock/go-concurrency"
)

type FanoutPublisher[T any] struct {
	publishers  []Publisher[T]
	maxParallel int
}

func NewFanoutPublisher[T any](publishers []Publisher[T], maxParallel int) (*FanoutPublisher[T], error) {
	if len(publishers) == 0 {
		return nil, fmt.Errorf("%w: publishers", ErrNilDependency)
	}
	if maxParallel <= 0 {
		return nil, fmt.Errorf("%w: max parallel", concurrency.ErrInvalidLimit)
	}
	copyOfPublishers := make([]Publisher[T], len(publishers))
	copy(copyOfPublishers, publishers)
	for index, publisher := range copyOfPublishers {
		if publisher == nil {
			return nil, fmt.Errorf("%w: publisher %d", ErrNilDependency, index)
		}
	}
	return &FanoutPublisher[T]{publishers: copyOfPublishers, maxParallel: maxParallel}, nil
}

func (publisher *FanoutPublisher[T]) Publish(ctx context.Context, envelope Envelope[T]) error {
	if err := envelope.Validate(); err != nil {
		return err
	}
	results, err := concurrency.ParallelMap(ctx, publisher.publishers, publisher.maxParallel, func(ctx context.Context, next Publisher[T]) (error, error) {
		return next.Publish(ctx, envelope.Clone()), nil
	})
	if err != nil {
		return err
	}

	joined := make([]error, 0, len(results))
	for index, result := range results {
		if result != nil {
			joined = append(joined, fmt.Errorf("publisher %d: %w", index, result))
		}
	}
	return errors.Join(joined...)
}
