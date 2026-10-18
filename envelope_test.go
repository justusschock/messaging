package messaging

import (
	"errors"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"testing/synctest"
	"time"
)

// FR-MSG-03, FR-MSG-04
func TestEnvelopeCopiesHeadersAndValidates(t *testing.T) {
	envelope, err := NewEnvelope("events.created", 1)
	if err != nil {
		t.Fatal(err)
	}

	with, err := envelope.WithHeader("trace", "abc")
	if err != nil {
		t.Fatal(err)
	}
	clone := with.Clone()
	clone.Headers["trace"] = "changed"

	if with.Headers["trace"] != "abc" {
		t.Fatal("headers aliased")
	}
	if _, exists := envelope.Headers["trace"]; exists {
		t.Fatal("WithHeader changed the original envelope")
	}
	if err := (Envelope[int]{ID: "id", Subject: "contains space"}).Validate(); !errors.Is(err, ErrInvalidEnvelope) {
		t.Fatalf("Validate=%v", err)
	}
}

// FR-MSG-02
func TestNewMessageIDIsUUIDv7String(t *testing.T) {
	// The bubble's fake clock makes the embedded timestamp exact.
	synctest.Test(t, func(t *testing.T) {
		now := time.Now().UnixMilli()
		first, err := NewMessageID()
		if err != nil {
			t.Fatal(err)
		}
		second, err := NewMessageID()
		if err != nil {
			t.Fatal(err)
		}
		time.Sleep(time.Millisecond)
		later, err := NewMessageID()
		if err != nil {
			t.Fatal(err)
		}

		uuidv7 := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-7[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$`)
		for _, id := range []MessageID{first, second, later} {
			if !uuidv7.MatchString(string(id)) {
				t.Fatalf("ID=%q", id)
			}
		}
		timestamp, err := strconv.ParseInt(strings.ReplaceAll(string(first), "-", "")[:12], 16, 64)
		if err != nil {
			t.Fatal(err)
		}
		if timestamp != now {
			t.Fatalf("timestamp=%d, want %d", timestamp, now)
		}
		if first == second {
			t.Fatalf("duplicate IDs: %q", first)
		}
		if later <= first || later <= second {
			t.Fatalf("ID %q from a later millisecond does not sort after %q and %q", later, first, second)
		}
	})
}

// FR-MSG-03
func TestNewEnvelopeInitializesValidMetadata(t *testing.T) {
	envelope, err := NewEnvelope("events.created", "payload")
	if err != nil {
		t.Fatal(err)
	}

	if err := envelope.Validate(); err != nil {
		t.Fatal(err)
	}
	if envelope.SentAt.Location() != time.UTC {
		t.Fatalf("SentAt location=%v", envelope.SentAt.Location())
	}
	if envelope.Headers == nil || len(envelope.Headers) != 0 {
		t.Fatalf("Headers=%v", envelope.Headers)
	}
}

// FR-MSG-03
func TestEnvelopeRejectsInvalidMetadata(t *testing.T) {
	valid := Envelope[int]{
		ID:      "0123456789abcdef0123456789abcdef",
		Subject: "events.created",
		SentAt:  time.Now().UTC(),
		Headers: map[string]string{"trace": "abc"},
	}

	tests := map[string]func(Envelope[int]) Envelope[int]{
		"empty ID": func(envelope Envelope[int]) Envelope[int] {
			envelope.ID = ""
			return envelope
		},
		"empty subject": func(envelope Envelope[int]) Envelope[int] {
			envelope.Subject = ""
			return envelope
		},
		"subject whitespace": func(envelope Envelope[int]) Envelope[int] {
			envelope.Subject = "events\tcreated"
			return envelope
		},
		"zero sent time": func(envelope Envelope[int]) Envelope[int] {
			envelope.SentAt = time.Time{}
			return envelope
		},
		"empty header key": func(envelope Envelope[int]) Envelope[int] {
			envelope.Headers = map[string]string{"": "abc"}
			return envelope
		},
	}

	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			if err := mutate(valid).Validate(); !errors.Is(err, ErrInvalidEnvelope) {
				t.Fatalf("Validate=%v", err)
			}
		})
	}
}

// FR-MSG-03
func TestEnvelopeWithHeaderRejectsEmptyKey(t *testing.T) {
	envelope, err := NewEnvelope("events.created", 1)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := envelope.WithHeader("", "value"); !errors.Is(err, ErrInvalidEnvelope) {
		t.Fatalf("WithHeader error=%v", err)
	}
}
