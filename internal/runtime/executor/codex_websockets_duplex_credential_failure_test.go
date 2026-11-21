package executor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/MoeForge/moego/ojson"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	auth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	translator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
)

func TestCodexDuplexLaterCredentialFailure(t *testing.T) {
	for _, kind := range []string{"error", "response.failed"} {
		for _, status := range []int{401, 403, 429} {
			for _, queued := range []bool{false, true} {
				t.Run(fmt.Sprintf("%s/%d/queued=%t", kind, status, queued), func(t *testing.T) {
					var attempts atomic.Int32
					done := make(chan struct{})
					upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
						defer close(done)
						attempts.Add(1)
						c, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
						if err != nil {
							t.Error(err)
							return
						}
						defer func() { _ = c.Close() }()
						_ = c.SetReadDeadline(time.Now().Add(8 * time.Second))
						if _, _, err = c.ReadMessage(); err != nil {
							t.Error(err)
							return
						}
						write := func(p string) {
							if e := c.WriteMessage(websocket.TextMessage, []byte(p)); e != nil {
								t.Error(e)
							}
						}
						write(`{"type":"response.created","response":{"id":"started","output":[]}}`)
						if queued {
							if _, _, err = c.ReadMessage(); err != nil {
								t.Error(err)
								return
							}
						} else {
							write(`{"type":"response.completed","response":{"id":"started","output":[]}}`)
						}
						errorType := "authentication_error"
						if status == 403 {
							errorType = "permission_error"
						}
						if status == 429 {
							errorType = "usage_limit_reached"
						}
						errorBody := fmt.Sprintf(`{"type":%q,"status":%d,"message":"credential rejected","resets_in_seconds":3600}`, errorType, status)
						if kind == "error" {
							write(fmt.Sprintf(`{"type":"error","status":%d,"headers":{"X-Request-Id":"later-rejection"},"error":%s}`, status, errorBody))
						} else {
							write(fmt.Sprintf(`{"type":"response.failed","response":{"id":"started","error":%s}}`, errorBody))
						}
						// The proxy must terminate this still-open socket on its own.
						_, _, _ = c.ReadMessage()
					}))
					defer upstream.Close()
					ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
					defer cancel()
					input := make(chan core.WebsocketInput, 1)
					ctx = core.WithWebsocketInput(core.WithDownstreamWebsocket(ctx), input)
					cfg := &config.Config{}
					cfg.Codex.ResponseSteering = true
					cfg.CodexResponseSteering = true
					executor := NewCodexWebsocketsExecutor(cfg)
					executor.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
					manager := auth.NewManager(nil, &auth.FillFirstSelector{}, nil)
					manager.SetConfig(cfg)
					manager.SetRetryConfig(3, 30*time.Second, 0)
					manager.RegisterExecutor(executor)
					model := "later-credential-model"
					bad := &auth.Auth{ID: t.Name() + "-bad", Provider: "codex", Status: auth.StatusActive, Attributes: map[string]string{"api_key": "test", "base_url": upstream.URL, "websockets": "true", "priority": "4"}}
					good := &auth.Auth{ID: t.Name() + "-good", Provider: "codex", Status: auth.StatusActive, Attributes: map[string]string{"api_key": "unused", "base_url": upstream.URL, "websockets": "true", "priority": "3"}}
					for _, candidate := range []*auth.Auth{bad, good} {
						registry.GetGlobalRegistry().RegisterClient(candidate.ID, "codex", []*registry.ModelInfo{{ID: model}})
						t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(candidate.ID) })
						if _, err := manager.Register(ctx, candidate); err != nil {
							t.Fatal(err)
						}
					}
					req := core.Request{Model: model, Payload: []byte(`{"model":"later-credential-model","input":[]}`)}
					opts := core.Options{SourceFormat: translator.FromString("codex"), Metadata: map[string]any{core.ExecutionSessionMetadataKey: t.Name()}}
					before := time.Now()
					result, err := manager.ExecuteStream(ctx, []string{"codex"}, req, opts)
					if err != nil {
						t.Fatal(err)
					}
					payloadSeen, terminalSeen := false, false
					for chunk := range result.Chunks {
						if chunk.Err != nil {
							terminalSeen = true
							var coded interface{ StatusCode() int }
							if !errors.As(chunk.Err, &coded) || coded.StatusCode() != status {
								t.Errorf("terminal classification lost: %v", chunk.Err)
							}
							var scoped interface{ IsRequestScoped() bool }
							if errors.As(chunk.Err, &scoped) && scoped.IsRequestScoped() {
								t.Error("credential failure became request-scoped")
							}
							if got, want := core.IsUpstreamWebsocketReconnect(chunk.Err), status != http.StatusForbidden; got != want {
								t.Errorf("reconnect=%t, want %t", got, want)
							}
							if got, want := payloadSeen, status == http.StatusForbidden; got != want {
								t.Errorf("raw credential failure forwarded=%t, want %t", got, want)
							}
							continue
						}
						event := ojson.GetBytes(chunk.Payload, "type").String()
						if event == "response.created" && queued {
							input <- core.WebsocketInput{Payload: []byte(`{"type":"response.create","input":[]}`)}
						}
						if event == kind {
							payloadSeen = true
						}
					}
					if payloadSeen != (status == http.StatusForbidden) || !terminalSeen {
						t.Errorf("failure payload=%t terminal=%t", payloadSeen, terminalSeen)
					}
					current, _ := manager.GetByID(bad.ID)
					state := current.ModelStates[model]
					if state == nil || state.LastError == nil || state.LastError.HTTPStatus != status || !state.Unavailable {
						t.Errorf("account failure not recorded: %+v", state)
					}
					if status == 429 && (current.Quota.Reason != "credential_quota" || current.Quota.NextRecoverAt.Before(before.Add(time.Hour))) {
						t.Errorf("quota scope or retry delay lost: %+v", current.Quota)
					}
					healthy, _ := manager.GetByID(good.ID)
					if healthy.Unavailable || healthy.LastError != nil {
						t.Error("unrelated healthy account changed")
					}
					select {
					case <-done:
					case <-time.After(3 * time.Second):
						t.Fatal("socket cleanup stalled")
					}
					if attempts.Load() != 1 {
						t.Fatalf("started response replayed across %d attempts", attempts.Load())
					}
				})
			}
		}
	}
}

// Buffering response.created must not hide an established duplex session from
// the manager: its writer may already have consumed steering or tool input.
func TestCodexDuplexBufferedQuotaReconnectPreservesContext(t *testing.T) {
	for _, event := range []string{"error", "response.failed"} {
		t.Run(event, func(t *testing.T) {
			testCodexDuplexBufferedQuotaReconnectPreservesContext(t, event)
		})
	}
}

func testCodexDuplexBufferedQuotaReconnectPreservesContext(t *testing.T, event string) {
	var rejected, healthy atomic.Int32
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, errUpgrade := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if errUpgrade != nil {
			t.Error(errUpgrade)
			return
		}
		defer func() { _ = conn.Close() }()
		_ = conn.SetReadDeadline(time.Now().Add(8 * time.Second))
		_, payload, errRead := conn.ReadMessage()
		if errRead != nil {
			t.Error(errRead)
			return
		}
		write := func(payload string) bool {
			if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(payload)); errWrite != nil {
				t.Error(errWrite)
				return false
			}
			return true
		}
		if r.Header.Get("Authorization") == "Bearer depleted" {
			rejected.Add(1)
			if !write(`{"type":"response.created","response":{"id":"depleted-response","output":[]}}`) {
				return
			}
			_, steer, errSteer := conn.ReadMessage()
			if errSteer != nil || ojson.GetBytesNoCopy(steer, "type").String() != "response.steer" {
				t.Errorf("missing queued steering: %s (%v)", steer, errSteer)
				return
			}
			failure := `{"type":"error","status_code":429,"error":{"type":"usage_limit_reached","message":"quota exhausted","resets_in_seconds":3600}}`
			if event == "response.failed" {
				failure = `{"type":"response.failed","response":{"id":"depleted-response","error":{"type":"usage_limit_reached","message":"quota exhausted","resets_in_seconds":3600}}}`
			}
			if !write(failure) {
				return
			}
		} else {
			healthy.Add(1)
			if ojson.GetBytesNoCopy(payload, "input.1.content").String() != "preserve this steering" {
				t.Error("established request replayed without the consumed steering")
			}
			if !write(`{"type":"response.created","response":{"id":"recovered","output":[]}}`) ||
				!write(`{"type":"response.completed","response":{"id":"recovered","output":[]}}`) {
				return
			}
		}
		// Cleanup must close the upstream; it must not depend on an upstream EOF.
		_, _, _ = conn.ReadMessage()
	}))
	defer upstream.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	input := make(chan core.WebsocketInput, 1)
	input <- core.WebsocketInput{Payload: []byte(`{"type":"response.steer","input":[{"role":"user","content":"preserve this steering"}]}`)}
	duplexCtx := core.WithWebsocketInput(core.WithDownstreamWebsocket(ctx), input)
	cfg := &config.Config{Codex: config.CodexConfig{ResponseSteering: true, StreamBootstrapBuffering: true, WaitOnQuota: true}}
	exec := NewCodexWebsocketsExecutor(cfg)
	exec.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
	manager := auth.NewManager(nil, &auth.FillFirstSelector{}, nil)
	manager.SetConfig(cfg)
	manager.SetRetryConfig(3, 30*time.Second, 0)
	manager.RegisterExecutor(exec)
	model := "buffered-quota-reconnect-model"
	for i, key := range []string{"depleted", "healthy"} {
		candidate := &auth.Auth{ID: fmt.Sprintf("%s-%d", t.Name(), i), Provider: "codex", Status: auth.StatusActive, Attributes: map[string]string{
			"api_key": key, "base_url": upstream.URL, "websockets": "true", "priority": fmt.Sprint(4 - i),
		}}
		registry.GetGlobalRegistry().RegisterClient(candidate.ID, "codex", []*registry.ModelInfo{{ID: model}})
		t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(candidate.ID) })
		if _, errRegister := manager.Register(ctx, candidate); errRegister != nil {
			t.Fatal(errRegister)
		}
	}
	req := core.Request{Model: model, Payload: []byte(fmt.Sprintf(`{"model":%q,"input":[{"role":"user","content":"original task"}]}`, model))}
	opts := core.Options{SourceFormat: translator.FromString("codex"), Metadata: map[string]any{core.ExecutionSessionMetadataKey: t.Name()}}
	result, errStream := manager.ExecuteStream(duplexCtx, []string{"codex"}, req, opts)
	if errStream != nil {
		t.Fatal(errStream)
	}
	var reconnect bool
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			reconnect = core.IsUpstreamWebsocketReconnect(chunk.Err)
		} else if ojson.GetBytesNoCopy(chunk.Payload, "type").String() != "response.created" {
			t.Errorf("unexpected payload before reconnect: %s", chunk.Payload)
		}
	}
	if !reconnect || rejected.Load() != 1 || healthy.Load() != 0 {
		t.Fatalf("must reconnect before rotating: signal=%t rejected=%d healthy=%d", reconnect, rejected.Load(), healthy.Load())
	}

	// A client reconnect resends the full transcript on a fresh session. The
	// exhausted account must be skipped and the original task must complete.
	req.Payload = []byte(fmt.Sprintf(`{"model":%q,"input":[{"role":"user","content":"original task"},{"role":"user","content":"preserve this steering"}]}`, model))
	opts.Metadata = map[string]any{core.ExecutionSessionMetadataKey: t.Name() + "-reconnected"}
	reconnectedCtx, cancelReconnected := context.WithCancel(core.WithWebsocketInput(core.WithDownstreamWebsocket(ctx), make(chan core.WebsocketInput)))
	defer cancelReconnected()
	result, errStream = manager.ExecuteStream(reconnectedCtx, []string{"codex"}, req, opts)
	if errStream != nil {
		t.Fatal(errStream)
	}
	completed := false
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Error(chunk.Err)
		}
		if ojson.GetBytesNoCopy(chunk.Payload, "type").String() == "response.completed" {
			completed = true
			cancelReconnected()
		}
	}
	if !completed || rejected.Load() != 1 || healthy.Load() != 1 {
		t.Fatalf("task did not recover: completed=%t rejected=%d healthy=%d", completed, rejected.Load(), healthy.Load())
	}
}
