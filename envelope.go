package messaging

import (
	"crypto/rand"
	"fmt"
	"strings"
	"time"
	"unicode"
)

type MessageID string

func NewMessageID() (MessageID, error) {
	var uuid [16]byte
	milliseconds := uint64(time.Now().UnixMilli())
	uuid[0] = byte(milliseconds >> 40)
	uuid[1] = byte(milliseconds >> 32)
	uuid[2] = byte(milliseconds >> 24)
	uuid[3] = byte(milliseconds >> 16)
	uuid[4] = byte(milliseconds >> 8)
	uuid[5] = byte(milliseconds)
	if _, err := rand.Read(uuid[6:]); err != nil {
		return "", fmt.Errorf("generate message ID: %w", err)
	}
	uuid[6] = uuid[6]&0x0f | 0x70
	uuid[8] = uuid[8]&0x3f | 0x80
	return MessageID(fmt.Sprintf("%x-%x-%x-%x-%x", uuid[0:4], uuid[4:6], uuid[6:8], uuid[8:10], uuid[10:16])), nil
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
