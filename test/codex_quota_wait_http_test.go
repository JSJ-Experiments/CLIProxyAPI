package test

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	runtimeexecutor "github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtimeevents"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/api/handlers/openai"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func TestCodexQuotaHTTPWaitKeepsSSEAliveAndResumesSameRequest(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const model = "codex-quota-http-wait-model"
	const id = "codex-quota-http-wait-auth"
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
	cfg.Streaming = config.StreamingConfig{KeepAliveSeconds: 15, SendKeepAliveBeforeFT: true}
	manager := cliproxyauth.NewManager(nil, nil, nil)
	manager.SetConfig(cfg)
	manager.SetRetryConfig(0, 0, 0)
	manager.RegisterExecutor(runtimeexecutor.NewCodexExecutor(cfg))
	registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: model}})
	defer registry.GetGlobalRegistry().UnregisterClient(id)
	if _, err := manager.Register(context.Background(), &cliproxyauth.Auth{ID: id, Provider: "codex", Status: cliproxyauth.StatusActive, Attributes: map[string]string{"base_url": upstream.URL, "api_key": "fixture"}, Metadata: map[string]any{"access_token": "fixture", "disable_cooling": false}}); err != nil {
		t.Fatal(err)
	}
	ticks := make(chan time.Time, 1)
	restoreTicker := handlers.SetStreamingKeepAliveTickerFactoryForTest(func(time.Duration) (<-chan time.Time, func()) { return ticks, func() {} })
	defer restoreTicker()
	h := openai.NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&cfg.SDKConfig, manager))
	router := gin.New()
	router.POST("/v1/responses", h.Responses)
	server := httptest.NewServer(router)
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, server.URL+"/v1/responses", strings.NewReader(`{"model":"`+model+`","input":"hello","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	responses := make(chan *http.Response, 1)
	failures := make(chan error, 1)
	events, unsubscribe := runtimeevents.Subscribe()
	defer unsubscribe()
	go func() {
		response, errDo := server.Client().Do(req)
		if errDo != nil {
			failures <- errDo
			return
		}
		responses <- response
	}()
	waitEvent := func(status string) {
		t.Helper()
		for {
			select {
			case event := <-events:
				if event.Type == "codex.quota" && event.Model == model && event.Status == status {
					return
				}
			case errDo := <-failures:
				t.Fatal(errDo)
			case <-time.After(5 * time.Second):
				t.Fatalf("missing %s warning", status)
			}
		}
	}
	waitEvent("waiting")
	if attempts.Load() != 1 {
		t.Fatalf("unexpected generation retries while depleted: %d", attempts.Load())
	}
	ticks <- time.Now() // Deterministic heartbeat: no platform-sensitive timer sleeps.
	var response *http.Response
	select {
	case response = <-responses:
	case errDo := <-failures:
		t.Fatal(errDo)
	case <-time.After(5 * time.Second):
		t.Fatal("no keepalive during quota wait")
	}
	defer func() {
		if errClose := response.Body.Close(); errClose != nil {
			t.Error(errClose)
		}
	}()
	if response.StatusCode != http.StatusOK || !strings.HasPrefix(response.Header.Get("Content-Type"), "text/event-stream") {
		t.Fatalf("quota escaped as HTTP error: %d", response.StatusCode)
	}
	reader := bufio.NewReader(response.Body)
	line, errRead := reader.ReadString('\n')
	if errRead != nil || !strings.HasPrefix(line, ": keep-alive") {
		t.Fatalf("expected harmless heartbeat, got %q (%v)", line, errRead)
	}
	recovered.Store(true)
	if _, _, errReset := manager.ResetQuota(ctx, id); errReset != nil {
		t.Fatal(errReset)
	}
	waitEvent("resuming")
	body, errBody := io.ReadAll(reader)
	if errBody != nil {
		t.Fatal(errBody)
	}
	if !strings.Contains(string(body), "response.completed") || strings.Contains(string(body), "usage_limit_reached") || strings.Contains(string(body), "Quota exhausted") {
		t.Fatalf("bad resumed stream: %s", body)
	}
	if attempts.Load() != 2 {
		t.Fatalf("expected exactly one resumed generation attempt, got %d", attempts.Load())
	}
}

// A started response cannot be replayed automatically without duplicating output.
type partialQuotaExecutor struct {
	cliproxyauth.ProviderExecutor
	attempts atomic.Int32
}

func (*partialQuotaExecutor) Identifier() string { return "codex" }

type partialQuotaError struct{}

func (partialQuotaError) Error() string {
	return `{"error":{"type":"usage_limit_reached","message":"Quota exhausted"}}`
}
func (partialQuotaError) StatusCode() int            { return http.StatusTooManyRequests }
func (partialQuotaError) IsCredentialScoped() bool   { return true }
func (partialQuotaError) RetryAfter() *time.Duration { wait := time.Hour; return &wait }
func (e *partialQuotaExecutor) ExecuteStream(context.Context, *cliproxyauth.Auth, cliproxyexecutor.Request, cliproxyexecutor.Options) (*cliproxyexecutor.StreamResult, error) {
	e.attempts.Add(1)
	chunks := make(chan cliproxyexecutor.StreamChunk, 3)
	chunks <- cliproxyexecutor.StreamChunk{Payload: []byte("data: {\"type\":\"response.created\",\"response\":{\"id\":\"partial\"}}\n\n")}
	chunks <- cliproxyexecutor.StreamChunk{Payload: []byte("data: {\"type\":\"response.output_text.delta\",\"delta\":\"partial text\"}\n\n")}
	chunks <- cliproxyexecutor.StreamChunk{Err: partialQuotaError{}}
	close(chunks)
	return &cliproxyexecutor.StreamResult{Chunks: chunks}, nil
}

func TestCodexQuotaPartialHTTPStreamDoesNotExposeTerminalUsageLimit(t *testing.T) {
	gin.SetMode(gin.TestMode)
	const model, id = "quota-partial-http-model", "quota-partial-http-auth"
	cfg := &config.Config{Codex: config.CodexConfig{WaitOnQuota: true}}
	manager := cliproxyauth.NewManager(nil, nil, nil)
	manager.SetConfig(cfg)
	e := &partialQuotaExecutor{}
	manager.RegisterExecutor(e)
	registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: model}})
	defer registry.GetGlobalRegistry().UnregisterClient(id)
	if _, err := manager.Register(context.Background(), &cliproxyauth.Auth{ID: id, Provider: "codex", Status: cliproxyauth.StatusActive}); err != nil {
		t.Fatal(err)
	}
	h := openai.NewOpenAIResponsesAPIHandler(handlers.NewBaseAPIHandlers(&cfg.SDKConfig, manager))
	router := gin.New()
	router.POST("/v1/responses", h.Responses)
	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/v1/responses", strings.NewReader(`{"model":"`+model+`","stream":true,"input":"hello"}`))
	req.Header.Set("User-Agent", "codex_cli_rs")
	router.ServeHTTP(recorder, req)
	body := recorder.Body.String()
	if strings.Contains(body, "usage_limit_reached") || strings.Contains(body, "Quota exhausted") || !strings.Contains(body, `"retryable":true`) || !strings.Contains(body, "partial text") || !strings.Contains(body, "upstream_temporarily_unavailable") {
		t.Fatalf("unsafe downstream error: %s", body)
	}
	if e.attempts.Load() != 1 {
		t.Fatal("partial output was automatically replayed")
	}
	a, _ := manager.GetByID(id)
	if a.LastError == nil || !strings.Contains(a.LastError.Message, "usage_limit_reached") || !a.Quota.Exceeded {
		t.Fatal("internal quota details lost")
	}
}
