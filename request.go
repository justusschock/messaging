package messaging

import (
	"context"
	"fmt"
)

type HandlerRequester[Req, Resp any] struct {
	handler RequestHandler[Req, Resp]
}

func NewHandlerRequester[Req, Resp any](handler RequestHandler[Req, Resp]) (*HandlerRequester[Req, Resp], error) {
	if handler == nil {
		return nil, fmt.Errorf("%w: request handler", ErrNilDependency)
	}
	return &HandlerRequester[Req, Resp]{handler: handler}, nil
}

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
