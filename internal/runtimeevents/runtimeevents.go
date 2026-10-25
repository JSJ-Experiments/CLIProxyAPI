package runtimeevents

import (
	"sync"
	"sync/atomic"
	"time"
)

// Event is an out-of-band runtime notification intended for operator UIs.
// It is deliberately separate from provider response streams so emitting one
// cannot change model-visible history or downstream Responses protocol state.
type Event struct {
	ID              uint64    `json:"id"`
	Time            time.Time `json:"time"`
	Type            string    `json:"type"`
	Status          string    `json:"status,omitempty"`
	Provider        string    `json:"provider,omitempty"`
	Model           string    `json:"model,omitempty"`
	Session         string    `json:"session,omitempty"`
	Response        string    `json:"response,omitempty"`
	OccurrenceCount uint64    `json:"occurrence_count,omitempty"`
	RetryAttempt    int       `json:"retry_attempt,omitempty"`
	RetryLimit      int       `json:"retry_limit,omitempty"`
	WaitMS          int64     `json:"wait_ms,omitempty"`
}

var defaultBroker = newBroker()

type broker struct {
	mu          sync.RWMutex
	subscribers map[uint64]chan Event
	nextSubID   atomic.Uint64
	nextEventID atomic.Uint64
}

func newBroker() *broker {
	return &broker{subscribers: make(map[uint64]chan Event)}
}

// Publish broadcasts an event without blocking request execution. Slow UI
// subscribers may miss individual events rather than back-pressuring traffic.
func Publish(event Event) {
	if event.Time.IsZero() {
		event.Time = time.Now().UTC()
	}
	event.ID = defaultBroker.nextEventID.Add(1)

	defaultBroker.mu.RLock()
	defer defaultBroker.mu.RUnlock()
	for _, ch := range defaultBroker.subscribers {
		select {
		case ch <- event:
		default:
		}
	}
}

// Subscribe returns a bounded stream and an idempotent unsubscribe callback.
func Subscribe() (<-chan Event, func()) {
	id := defaultBroker.nextSubID.Add(1)
	ch := make(chan Event, 32)
	defaultBroker.mu.Lock()
	defaultBroker.subscribers[id] = ch
	defaultBroker.mu.Unlock()

	var once sync.Once
	return ch, func() {
		once.Do(func() {
			defaultBroker.mu.Lock()
			delete(defaultBroker.subscribers, id)
			close(ch)
			defaultBroker.mu.Unlock()
		})
	}
}
