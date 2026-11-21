package auth

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtimeevents"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

type quotaWaitExecutor struct {
	ProviderExecutor
	fail      atomic.Bool
	attempts  chan string
	bootstrap bool
}

func (*quotaWaitExecutor) Identifier() string { return "codex" }
func (e *quotaWaitExecutor) Execute(_ context.Context, a *Auth, _ cliproxyexecutor.Request, _ cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	e.attempts <- a.ID
	if e.fail.Load() {
		return cliproxyexecutor.Response{}, issue6503QuotaError{retryAfter: time.Hour}
	}
	return cliproxyexecutor.Response{Payload: []byte(`{"ok":true}`)}, nil
}
func (e *quotaWaitExecutor) CountTokens(ctx context.Context, a *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (cliproxyexecutor.Response, error) {
	return e.Execute(ctx, a, req, opts)
}
func (e *quotaWaitExecutor) ExecuteStream(ctx context.Context, a *Auth, req cliproxyexecutor.Request, opts cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	resp, err := e.Execute(ctx, a, req, opts)
	if err != nil && !e.bootstrap {
		return nil, err
	}
	chunks := make(chan cliproxyexecutor.StreamChunk, 1)
	chunks <- cliproxyexecutor.StreamChunk{Payload: resp.Payload, Err: err}
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

func quotaWaitFixture(t *testing.T) (*Manager, *quotaWaitExecutor, string, string) {
	t.Helper()
	m := NewManager(nil, nil, nil)
	m.SetConfig(&internalconfig.Config{Codex: internalconfig.CodexConfig{WaitOnQuota: true}})
	m.SetRetryConfig(0, 0, 0) // Quota waiting must not depend on ordinary retry budgets.
	e := &quotaWaitExecutor{attempts: make(chan string, 20)}
	e.fail.Store(true)
	m.RegisterExecutor(e)
	model, id := "quota-wait-model-"+t.Name(), "quota-wait-auth-"+t.Name()
	registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: model}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
	if _, err := m.Register(context.Background(), &Auth{ID: id, Provider: "codex", Status: StatusActive, Metadata: map[string]any{"access_token": "test-token"}}); err != nil {
		t.Fatal(err)
	}
	return m, e, model, id
}

func quotaWaitEvent(t *testing.T, events <-chan runtimeevents.Event, model, status string) runtimeevents.Event {
	t.Helper()
	timer := time.NewTimer(5 * time.Second)
	defer timer.Stop()
	for {
		select {
		case event := <-events:
			if event.Type == "codex.quota" && event.Model == model && event.Status == status {
				return event
			}
		case <-timer.C:
			t.Fatalf("missing %s event for %s", status, model)
			return runtimeevents.Event{}
		}
	}
}

func TestCodexQuotaWaitResumesWithoutClientRetry(t *testing.T) {
	withQuotaCooldownEnabled(t)
	for _, mode := range []string{"execute", "count", "stream", "bootstrap"} {
		t.Run(mode, func(t *testing.T) {
			m, e, model, id := quotaWaitFixture(t)
			e.bootstrap = mode == "bootstrap"
			events, unsub := runtimeevents.Subscribe()
			defer unsub()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				req := cliproxyexecutor.Request{Model: model}
				opts := cliproxyexecutor.Options{}
				if mode == "execute" {
					_, err := m.Execute(ctx, []string{"codex"}, req, opts)
					done <- err
				} else if mode == "count" {
					_, err := m.ExecuteCount(ctx, []string{"codex"}, req, opts)
					done <- err
				} else {
					stream, err := m.ExecuteStream(ctx, []string{"codex"}, req, opts)
					if err == nil {
						for chunk := range stream.Chunks {
							if chunk.Err != nil {
								err = chunk.Err
							}
						}
					}
					done <- err
				}
			}()
			warning := quotaWaitEvent(t, events, model, "waiting")
			if warning.WaitMS < time.Hour.Milliseconds()-5000 {
				t.Fatalf("lost reset timing: %+v", warning)
			}
			select {
			case err := <-done:
				t.Fatalf("quota leaked before recovery: %v", err)
			default:
			}
			if len(e.attempts) != 1 {
				t.Fatalf("unexpected retries while depleted: %d", len(e.attempts))
			}
			e.fail.Store(false)
			checked, _ := m.GetByID(id)
			if !m.restoreCodexQuotaAfterRecheck(ctx, checked, time.Now()) {
				t.Fatal("restoration failed")
			}
			quotaWaitEvent(t, events, model, "resuming")
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("request did not resume")
			}
			if len(e.attempts) != 2 {
				t.Fatalf("got %d upstream attempts, want exactly two", len(e.attempts))
			}
		})
	}
}

func TestCodexQuotaWaitCancellationAndDisable(t *testing.T) {
	withQuotaCooldownEnabled(t)
	for _, action := range []string{"cancel", "disable-wait", "remove-auth"} {
		t.Run(action, func(t *testing.T) {
			m, _, model, id := quotaWaitFixture(t)
			events, unsub := runtimeevents.Subscribe()
			defer unsub()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() {
				_, err := m.ExecuteStream(ctx, []string{"codex"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
				done <- err
			}()
			quotaWaitEvent(t, events, model, "waiting")
			switch action {
			case "cancel":
				cancel()
			case "disable-wait":
				m.SetConfig(&internalconfig.Config{})
			case "remove-auth":
				m.Remove(ctx, id)
			}
			select {
			case err := <-done:
				if action == "cancel" && !errors.Is(err, context.Canceled) {
					t.Fatalf("got %v, want cancellation", err)
				}
				if action == "remove-auth" && (err == nil || strings.Contains(err.Error(), "usage_limit_reached")) {
					t.Fatalf("stale quota replaced removal: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("waiter leaked")
			}
		})
	}
}

func TestCodexQuotaWaitSelectionAndReset(t *testing.T) {
	withQuotaCooldownEnabled(t)
	m, _, model, id := quotaWaitFixture(t)
	retry := time.Hour
	m.MarkResult(context.Background(), Result{AuthID: id, Provider: "codex", Model: model, Error: &Error{HTTPStatus: 429, Code: "usage_limit_reached", Message: "usage_limit_reached"}, RetryAfter: &retry, CredentialScope: true})
	a, _ := m.GetByID(id)
	now := time.Now()
	next, ready, changed := m.codexQuotaWaitState(context.Background(), model, cliproxyexecutor.Options{}, now)
	if ready || !next.Equal(a.Quota.NextRecoverAt) {
		t.Fatalf("wrong earliest reset: %v, ready=%v", next, ready)
	}
	_, ready, _ = m.codexQuotaWaitState(context.Background(), model, cliproxyexecutor.Options{}, next.Add(time.Second))
	if !ready {
		t.Fatal("known reset must immediately make credential eligible")
	}
	next, ready, _ = m.codexQuotaWaitState(context.Background(), "unsupported-model", cliproxyexecutor.Options{}, now)
	if ready || !next.IsZero() {
		t.Fatal("unrelated model must not wait on this credential")
	}
	opts := cliproxyexecutor.Options{Metadata: map[string]any{cliproxyexecutor.PinnedAuthMetadataKey: "different-auth"}}
	next, ready, _ = m.codexQuotaWaitState(context.Background(), model, opts, now)
	if ready || !next.IsZero() {
		t.Fatal("pinned request must not use a different credential")
	}
	m.notifyQuotaAvailabilityChange()
	select {
	case <-changed:
	default:
		t.Fatal("availability broadcast missed")
	}
}

func TestCodexQuotaWaitDoesNotMaskOtherErrors(t *testing.T) {
	m, _, model, _ := quotaWaitFixture(t)
	for _, err := range []error{context.Canceled, &Error{HTTPStatus: 401, Message: "invalid token"}, &Error{HTTPStatus: 403, Message: "policy rejection"}, &Error{HTTPStatus: 429, Message: "rate_limit_error"}, &Error{HTTPStatus: 503, Message: "overloaded"}} {
		retry, errWait := m.waitForCodexQuota(context.Background(), []string{"codex"}, model, cliproxyexecutor.Options{}, err)
		if retry || errWait != nil {
			t.Fatalf("unrelated error intercepted: %v", err)
		}
	}
	cfg := &internalconfig.Config{Codex: internalconfig.CodexConfig{WaitOnQuota: true}}
	cfg.Home.Enabled = true
	m.SetConfig(cfg)
	if m.codexQuotaWaitEnabled() {
		t.Fatal("Home routing must remain authoritative")
	}
}

func TestCodexQuotaWaitRestorationBeforeSubscription(t *testing.T) {
	withQuotaCooldownEnabled(t)
	m, _, model, _ := quotaWaitFixture(t)
	retry, errWait := m.waitForCodexQuota(context.Background(), []string{"codex"}, model, cliproxyexecutor.Options{}, issue6503QuotaError{retryAfter: time.Hour})
	if !retry || errWait != nil {
		t.Fatalf("restoration before subscription must retry immediately: %v, %v", retry, errWait)
	}
}

func TestCodexQuotaPartialStreamUsesRetryableErrorAfterAccounting(t *testing.T) {
	withQuotaCooldownEnabled(t)
	m, _, model, id := quotaWaitFixture(t)
	a, _ := m.GetByID(id)
	chunks := make(chan cliproxyexecutor.StreamChunk, 1)
	chunks <- cliproxyexecutor.StreamChunk{Err: issue6503QuotaError{retryAfter: time.Hour}}
	close(chunks)
	result := m.wrapStreamResult(context.Background(), a, "codex", model, model, nil, []cliproxyexecutor.StreamChunk{{Payload: []byte("already delivered")}}, chunks, OAuthModelAliasResult{}, false, cliproxyexecutor.Options{}, 0)
	var got error
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			got = chunk.Err
		}
	}
	if got == nil || statusCodeFromError(got) != http.StatusServiceUnavailable || strings.Contains(got.Error(), "usage_limit_reached") {
		t.Fatalf("terminal quota classification leaked: %v", got)
	}
	current, _ := m.GetByID(id)
	if !current.Quota.Exceeded || current.Quota.Reason != "credential_quota" || current.LastError == nil || !strings.Contains(current.LastError.Message, "usage_limit_reached") {
		t.Fatalf("lost internal quota accounting: %+v", current.Quota)
	}
	reconnect := cliproxyexecutor.NewUpstreamWebsocketReconnectError(issue6503QuotaError{retryAfter: time.Hour})
	if m.quotaStreamClientError("codex", model, cliproxyexecutor.Options{}, reconnect) != reconnect {
		t.Fatal("typed websocket reconnect must survive")
	}
}

func TestCodexQuotaAlreadyDepletedRequestWakesForNewCredential(t *testing.T) {
	withQuotaCooldownEnabled(t)
	m, e, model, id := quotaWaitFixture(t)
	retry := time.Hour
	m.MarkResult(context.Background(), Result{AuthID: id, Provider: "codex", Model: model, Error: &Error{HTTPStatus: 429, Message: "usage_limit_reached"}, RetryAfter: &retry, CredentialScope: true})
	events, unsub := runtimeevents.Subscribe()
	defer unsub()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := m.Execute(ctx, []string{"codex"}, cliproxyexecutor.Request{Model: model}, cliproxyexecutor.Options{})
		done <- err
	}()
	quotaWaitEvent(t, events, model, "waiting")
	if len(e.attempts) != 0 {
		t.Fatal("already exhausted credentials must not receive generation probes")
	}
	e.fail.Store(false)
	newID := id + "-new"
	registry.GetGlobalRegistry().RegisterClient(newID, "codex", []*registry.ModelInfo{{ID: model}})
	defer registry.GetGlobalRegistry().UnregisterClient(newID)
	if _, err := m.Register(ctx, &Auth{ID: newID, Provider: "codex", Status: StatusActive}); err != nil {
		t.Fatal(err)
	}
	quotaWaitEvent(t, events, model, "resuming")
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("new credential did not unblock waiting request")
	}
	if got := <-e.attempts; got != newID {
		t.Fatalf("used %q, want new credential", got)
	}
}

func TestCodexQuotaModelScopedWaitDoesNotBlockHealthySibling(t *testing.T) {
	withQuotaCooldownEnabled(t)
	m, _, model, id := quotaWaitFixture(t)
	sibling := model + "-sibling"
	registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: model}, {ID: sibling}})
	retry := time.Hour
	m.MarkResult(context.Background(), Result{AuthID: id, Provider: "codex", Model: model, Error: &Error{HTTPStatus: 429, Message: "usage_limit_reached"}, RetryAfter: &retry})
	next, ready, _ := m.codexQuotaWaitState(context.Background(), model, cliproxyexecutor.Options{}, time.Now())
	if ready || next.IsZero() {
		t.Fatal("model-scoped quota must wait")
	}
	next, ready, _ = m.codexQuotaWaitState(context.Background(), sibling, cliproxyexecutor.Options{}, time.Now())
	if !ready || !next.IsZero() {
		t.Fatal("unrelated sibling must remain available")
	}
}
