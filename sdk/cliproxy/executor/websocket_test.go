package executor

import (
	"errors"
	"fmt"
	"net/http"
	"testing"
)

func TestUpstreamWebsocketReplayRequiredError(t *testing.T) {
	err := NewUpstreamWebsocketReplayRequiredError()
	if !IsUpstreamWebsocketReplayRequired(err) {
		t.Fatal("replay error was not recognized")
	}
	if !IsUpstreamWebsocketReplayRequired(fmt.Errorf("wrapped: %w", err)) {
		t.Fatal("wrapped replay error was not recognized")
	}
	statusErr, ok := err.(interface{ StatusCode() int })
	if !ok || statusErr.StatusCode() != http.StatusUpgradeRequired {
		t.Fatalf("replay error = %T %v, want status 426", err, err)
	}
	requestErr, ok := err.(RequestScopedError)
	if !ok || !requestErr.IsRequestScoped() {
		t.Fatalf("replay error = %T, want request scoped", err)
	}
}

func TestUpstreamWebsocketReconnectPreservesCredentialFailure(t *testing.T) {
	underlying := fmt.Errorf("account quota exhausted")
	err := NewUpstreamWebsocketReconnectError(underlying)
	if !IsUpstreamWebsocketReconnect(err) || !IsUpstreamWebsocketReconnect(fmt.Errorf("wrapped: %w", err)) {
		t.Fatal("wrapped reconnect marker was not recognized")
	}
	if !errors.Is(err, underlying) || err.Error() != underlying.Error() {
		t.Fatal("reconnect did not preserve the original error")
	}
	if NewUpstreamWebsocketReconnectError(nil) != nil {
		t.Fatal("nil failure must not create a reconnect request")
	}
	if IsUpstreamWebsocketReplayRequired(err) {
		t.Fatal("quota reconnect was misclassified as HTTP replay")
	}
}
