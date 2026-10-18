package messaging

import "errors"

var (
	// ErrInvalidEnvelope reports incomplete or malformed envelope metadata.
	ErrInvalidEnvelope = errors.New("messaging: invalid envelope")
	// ErrInvalidSubscription reports an invalid subscription identity,
	// semantics or capacity.
	ErrInvalidSubscription = errors.New("messaging: invalid subscription")
	// ErrUnauthorized reports that an authorization decorator denied the
	// operation. The authorizer's own error is wrapped alongside it.
	ErrUnauthorized = errors.New("messaging: unauthorized")
	// ErrDeliverySettled reports that a delivery attempt was already
	// acknowledged or negatively acknowledged.
	ErrDeliverySettled = errors.New("messaging: delivery settled")
	// ErrConsumerExists reports that the subject already has an active
	// consumer with the same name.
	ErrConsumerExists = errors.New("messaging: consumer exists")
	// ErrClosed reports that a transport, or an active subscription on it, has
	// been closed.
	ErrClosed = errors.New("messaging: closed")
	// ErrNilDependency reports that a required publisher, subscriber,
	// authorizer or handler is nil.
	ErrNilDependency = errors.New("messaging: nil dependency")
)
