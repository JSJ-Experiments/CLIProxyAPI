package test

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	runtimeexecutor "github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtimeevents"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers/openai"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestCodexQuotaWebsocketWaitKeepsAliveDuringBootstrap(t *testing.T) {
	gin.SetMode(gin.TestMode)
	for _, cancelWait := range []bool{false, true} {
		name := "resume"
		if cancelWait {
			name = "cancel"
		}
		t.Run(name, func(t *testing.T) {
			model, id := "quota-websocket-"+name+"-model", "quota-websocket-"+name+"-auth"
			var recovered atomic.Bool
			var attempts atomic.Int32
			upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				attempts.Add(1)
				if !recovered.Load() {
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusTooManyRequests)
					_, _ = io.WriteString(w, `{"error":{"type":"usage_limit_reached","message":"Quota exhausted","resets_in_seconds":3600}}`)
					return
				}
				w.Header().Set("Content-Type", "text/event-stream")
				_, _ = io.WriteString(w, "data: {\"type\":\"response.completed\",\"response\":{\"id\":\"recovered\",\"status\":\"completed\",\"output\":[],\"usage\":{\"input_tokens\":1,\"output_tokens\":1,\"total_tokens\":2}}}\n\n")
			}))
			defer upstream.Close()
			cfg := &config.Config{Codex: config.CodexConfig{WaitOnQuota: true, StreamBootstrapBuffering: true}}
			// WebSocket control frames do not require the SSE pre-first-token option.
			cfg.Streaming = config.StreamingConfig{KeepAliveSeconds: 1}
			manager := cliproxyauth.NewManager(nil, nil, nil)
			manager.SetConfig(cfg)
			manager.SetRetryConfig(0, 0, 0)
			manager.RegisterExecutor(runtimeexecutor.NewCodexExecutor(cfg))
			registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: model}})
			defer registry.GetGlobalRegistry().UnregisterClient(id)
			if _, err := manager.Register(context.Background(), &cliproxyauth.Auth{ID: id, Provider: "codex", Status: cliproxyauth.StatusActive, Attributes: map[string]string{"base_url": upstream.URL, "api_key": "fixture"}, Metadata: map[string]any{"access_token": "fixture", "disable_cooling": false}}); err != nil {
				t.Fatal(err)
			}
			h := openai.NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&cfg.SDKConfig, manager))
			handlerDone := make(chan struct{})
			router := gin.New()
			router.GET("/v1/responses", func(c *gin.Context) {
				defer close(handlerDone)
				h.ResponsesWebsocket(c)
			})
			server := httptest.NewServer(router)
			defer server.Close()
			conn, _, err := websocket.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http")+"/v1/responses", nil)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = conn.Close() }()
			pings := make(chan struct{}, 2)
			conn.SetPingHandler(func(data string) error {
				select {
				case pings <- struct{}{}:
				default:
				}
				return conn.WriteControl(websocket.PongMessage, []byte(data), time.Time{})
			})
			type readResult struct {
				payload []byte
				err     error
			}
			messages := make(chan readResult, 1)
			go func() {
				_, payload, errRead := conn.ReadMessage()
				messages <- readResult{payload: payload, err: errRead}
			}()
			events, unsubscribe := runtimeevents.Subscribe()
			defer unsubscribe()
			waitEvent := func(status string) {
				t.Helper()
				deadline := time.After(5 * time.Second)
				for {
					select {
					case event := <-events:
						if event.Type == "codex.quota" && event.Model == model && event.Status == status {
							return
						}
					case <-deadline:
						t.Fatalf("missing quota %s event", status)
					}
				}
			}
			if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.create","model":"`+model+`","input":[],"stream":true}`)); errWrite != nil {
				t.Fatal(errWrite)
			}
			waitEvent("waiting")
			for range 2 {
				select {
				case <-pings:
				case message := <-messages:
					t.Fatalf("unexpected application message while waiting: %s (%v)", message.payload, message.err)
				case <-time.After(5 * time.Second):
					t.Fatal("missing WebSocket Ping during quota wait")
				}
			}
			if attempts.Load() != 1 {
				t.Fatalf("unexpected generation attempts while depleted: %d", attempts.Load())
			}
			if cancelWait {
				_ = conn.Close()
				waitEvent("cancelled")
			} else {
				recovered.Store(true)
				if _, _, errReset := manager.ResetQuota(context.Background(), id); errReset != nil {
					t.Fatal(errReset)
				}
				waitEvent("resuming")
				select {
				case message := <-messages:
					if message.err != nil || !strings.Contains(string(message.payload), "response.completed") || strings.Contains(string(message.payload), "usage_limit_reached") {
						t.Fatalf("bad resumed response: %s (%v)", message.payload, message.err)
					}
				case <-time.After(5 * time.Second):
					t.Fatal("request did not resume after quota recovery")
				}
				if attempts.Load() != 2 {
					t.Fatalf("expected one resumed generation attempt, got %d", attempts.Load())
				}
				_ = conn.Close()
			}
			select {
			case <-handlerDone:
			case <-time.After(5 * time.Second):
				t.Fatal("WebSocket handler did not exit after disconnect")
			}

		})
	}
}
