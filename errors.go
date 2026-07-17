package messaging

import "errors"

var (
	ErrInvalidEnvelope     = errors.New("messaging: invalid envelope")
	ErrInvalidSubscription = errors.New("messaging: invalid subscription")
	ErrUnauthorized        = errors.New("messaging: unauthorized")
	ErrDeliverySettled     = errors.New("messaging: delivery settled")
	ErrConsumerExists      = errors.New("messaging: consumer exists")
	ErrClosed              = errors.New("messaging: closed")
	ErrNilDependency       = errors.New("messaging: nil dependency")
)
