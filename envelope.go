package messaging

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
	"unicode"
)

type MessageID string

func NewMessageID() (MessageID, error) {
	bytes := make([]byte, 16)
	if _, err := rand.Read(bytes); err != nil {
		return "", fmt.Errorf("generate message ID: %w", err)
	}
	return MessageID(hex.EncodeToString(bytes)), nil
}

type Envelope[T any] struct {
	ID      MessageID
	Subject string
	SentAt  time.Time
	Headers map[string]string
	Payload T
}

func NewEnvelope[T any](subject string, payload T) (Envelope[T], error) {
	id, err := NewMessageID()
	if err != nil {
		return Envelope[T]{}, err
	}
	envelope := Envelope[T]{
		ID:      id,
		Subject: subject,
		SentAt:  time.Now().UTC(),
		Headers: make(map[string]string),
		Payload: payload,
	}
	if err := envelope.Validate(); err != nil {
		return Envelope[T]{}, err
	}
	return envelope, nil
}

func (envelope Envelope[T]) WithHeader(key, value string) (Envelope[T], error) {
	with := envelope.Clone()
	with.Headers[key] = value
	if err := with.Validate(); err != nil {
		return Envelope[T]{}, err
	}
	return with, nil
}

func (envelope Envelope[T]) Clone() Envelope[T] {
	envelope.Headers = cloneHeaders(envelope.Headers)
	return envelope
}

func (envelope Envelope[T]) Validate() error {
	switch {
	case envelope.ID == "":
		return fmt.Errorf("%w: empty ID", ErrInvalidEnvelope)
	case envelope.Subject == "":
		return fmt.Errorf("%w: empty subject", ErrInvalidEnvelope)
	case strings.IndexFunc(envelope.Subject, unicode.IsSpace) >= 0:
		return fmt.Errorf("%w: subject contains whitespace", ErrInvalidEnvelope)
	case envelope.SentAt.IsZero():
		return fmt.Errorf("%w: zero sent time", ErrInvalidEnvelope)
	}
	for key := range envelope.Headers {
		if key == "" {
			return fmt.Errorf("%w: empty header key", ErrInvalidEnvelope)
		}
	}
	return nil
}

func cloneHeaders(headers map[string]string) map[string]string {
	clone := make(map[string]string, len(headers))
	for key, value := range headers {
		clone[key] = value
	}
	return clone
}
