package runtimeevents

import (
	"testing"
	"time"
)

func TestPublishSubscribe(t *testing.T) {
	ch, unsubscribe := Subscribe()
	defer unsubscribe()

	Publish(Event{Type: "codex.capacity", Status: "retrying", RetryAttempt: 1, RetryLimit: 3, WaitMS: 1000})
	select {
	case event := <-ch:
		if event.ID == 0 || event.Time.IsZero() {
			t.Fatalf("event metadata not populated: %#v", event)
		}
		if event.Type != "codex.capacity" || event.RetryAttempt != 1 || event.WaitMS != 1000 {
			t.Fatalf("unexpected event: %#v", event)
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for runtime event")
	}
}
