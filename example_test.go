package messaging_test

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"sync"

	"github.com/justusschock/messaging"
)

type recordingPublisher struct {
	name  string
	mu    *sync.Mutex
	names *[]string
}

func (publisher recordingPublisher) Publish(_ context.Context, _ messaging.Envelope[string]) error {
	publisher.mu.Lock()
	defer publisher.mu.Unlock()
	*publisher.names = append(*publisher.names, publisher.name)
	return nil
}

// FR-MSG-09
func ExampleFanoutPublisher() {
	var mu sync.Mutex
	names := []string{}
	fanout, _ := messaging.NewFanoutPublisher([]messaging.Publisher[string]{
		recordingPublisher{"audit", &mu, &names},
		recordingPublisher{"events", &mu, &names},
	}, 2)
	envelope, _ := messaging.NewEnvelope("created", "item-1")
	_ = fanout.Publish(context.Background(), envelope)
	slices.Sort(names)
	fmt.Println(strings.Join(names, " "))
	// Output: audit events
}
