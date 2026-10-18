package messaging

import (
	"context"
	"fmt"
)

// HandlerRequester adapts an in-process [RequestHandler] to [Requester]. It is
// safe for concurrent use if the handler is.
type HandlerRequester[Req, Resp any] struct {
	handler RequestHandler[Req, Resp]
}

// NewHandlerRequester returns a requester that calls handler. It returns
// [ErrNilDependency] if handler is nil.
func NewHandlerRequester[Req, Resp any](handler RequestHandler[Req, Resp]) (*HandlerRequester[Req, Resp], error) {
	if handler == nil {
		return nil, fmt.Errorf("%w: request handler", ErrNilDependency)
	}
	return &HandlerRequester[Req, Resp]{handler: handler}, nil
}

// Request validates the request, passes a clone to the handler, validates the
// response and returns a clone of it. Handler errors are returned unchanged.
func (requester *HandlerRequester[Req, Resp]) Request(ctx context.Context, request Envelope[Req]) (Envelope[Resp], error) {
	if err := request.Validate(); err != nil {
		return Envelope[Resp]{}, err
	}
	response, err := requester.handler(ctx, request.Clone())
	if err != nil {
		return Envelope[Resp]{}, err
	}
	if err := response.Validate(); err != nil {
		return Envelope[Resp]{}, err
	}
	return response.Clone(), nil
}
