package executor

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	auth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	core "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
	translator "github.com/router-for-me/CLIProxyAPI/v8/sdk/translator"
	"github.com/tidwall/gjson"
)

func TestCodexDuplexCapacityRetryIsTransparent(t *testing.T) {
	upstreamErr := make(chan error, 1)
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			upstreamErr <- err
			return
		}
		defer func() { _ = conn.Close() }()

		readCreate := func() ([]byte, bool) {
			_, payload, errRead := conn.ReadMessage()
			if errRead != nil {
				upstreamErr <- errRead
				return nil, false
			}
			return payload, true
		}
		write := func(payload string) bool {
			if errWrite := conn.WriteMessage(websocket.TextMessage, []byte(payload)); errWrite != nil {
				upstreamErr <- errWrite
				return false
			}
			return true
		}

		if _, ok := readCreate(); !ok {
			return
		}
		if !write(`{"type":"response.created","response":{"id":"first","output":[]}}`) ||
			!write(`{"type":"response.completed","response":{"id":"first","status":"completed","output":[]}}`) {
			return
		}

		second, ok := readCreate()
		if !ok {
			return
		}
		if got := gjson.GetBytes(second, "prompt_cache_key").String(); got != "retry-me" {
			upstreamErr <- fmt.Errorf("second request prompt_cache_key = %q, want retry-me", got)
			return
		}
		if !write(`{"type":"response.created","response":{"id":"failed-attempt","output":[]}}`) ||
			!write(`{"type":"response.in_progress","response":{"id":"failed-attempt"}}`) ||
			!write(`{"type":"error","error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later."},"sequence_number":2}`) ||
			!write(`{"type":"response.failed","response":{"id":"failed-attempt","status":"failed","error":{"code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later."}}}`) {
			return
		}

		retried, ok := readCreate()
		if !ok {
			return
		}
		if got := gjson.GetBytes(retried, "prompt_cache_key").String(); got != "retry-me" {
			upstreamErr <- fmt.Errorf("retry prompt_cache_key = %q, want retry-me", got)
			return
		}
		if !write(`{"type":"response.created","response":{"id":"retried","output":[]}}`) ||
			!write(`{"type":"response.completed","response":{"id":"retried","status":"completed","output":[]}}`) {
			return
		}
	}))
	defer upstream.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	input := make(chan core.WebsocketInput, 1)
	ctx = core.WithWebsocketInput(core.WithDownstreamWebsocket(ctx), input)
	cfg := &config.Config{}
	cfg.Codex.ResponseSteering = true
	cfg.Codex.StreamBootstrapBuffering = true
	cfg.Codex.CapacityRetryWait = "0s"
	exec := NewCodexWebsocketsExecutor(cfg)
	exec.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
	credential := &auth.Auth{ID: "capacity-retry", Provider: "codex", Attributes: map[string]string{"api_key": "test", "base_url": upstream.URL, "websockets": "true"}}
	request := func(key string) []byte {
		return []byte(fmt.Sprintf(`{"type":"response.create","model":"gpt-6.1-sol","prompt_cache_key":%q,"input":[]}`, key))
	}

	result, err := exec.ExecuteStream(ctx, credential, core.Request{Model: "gpt-6.1-sol", Payload: request("first")}, core.Options{
		SourceFormat: translator.FromString("codex"),
		Metadata:     map[string]any{core.ExecutionSessionMetadataKey: t.Name()},
	})
	if err != nil {
		t.Fatal(err)
	}

	var downstream []string
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			t.Fatalf("transparent capacity retry leaked an error: %v", chunk.Err)
		}
		downstream = append(downstream, string(chunk.Payload))
		if gjson.GetBytes(chunk.Payload, "type").String() == "response.completed" && gjson.GetBytes(chunk.Payload, "response.id").String() == "first" {
			input <- core.WebsocketInput{Payload: request("retry-me")}
		}
		if gjson.GetBytes(chunk.Payload, "type").String() == "response.completed" && gjson.GetBytes(chunk.Payload, "response.id").String() == "retried" {
			cancel()
		}
	}

	joined := strings.Join(downstream, "\n")
	if strings.Contains(joined, "failed-attempt") || strings.Contains(joined, "server_is_overloaded") || strings.Contains(joined, "currently overloaded") {
		t.Fatalf("failed capacity attempt leaked downstream: %s", joined)
	}
	if !strings.Contains(joined, `"id":"retried"`) {
		t.Fatalf("retried response never reached downstream: %s", joined)
	}
	select {
	case errUpstream := <-upstreamErr:
		t.Fatal(errUpstream)
	default:
	}
}

func TestCodexDuplexCapacityRetryToggleOffSurfacesFailure(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&websocket.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer func() { _ = conn.Close() }()
		if _, _, err = conn.ReadMessage(); err != nil {
			return
		}
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.created","response":{"id":"failed","output":[]}}`))
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"error","error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later."}}`))
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"response.failed","response":{"id":"failed","status":"failed","error":{"code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later."}}}`))
	}))
	defer upstream.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	input := make(chan core.WebsocketInput)
	ctx = core.WithWebsocketInput(core.WithDownstreamWebsocket(ctx), input)
	cfg := &config.Config{}
	cfg.Codex.ResponseSteering = true
	exec := NewCodexWebsocketsExecutor(cfg)
	exec.store = &codexWebsocketSessionStore{sessions: make(map[string]*codexWebsocketSession)}
	credential := &auth.Auth{ID: "capacity-no-retry", Provider: "codex", Attributes: map[string]string{"api_key": "test", "base_url": upstream.URL, "websockets": "true"}}
	payload := []byte(`{"type":"response.create","model":"gpt-6.1-sol","input":[]}`)

	result, err := exec.ExecuteStream(ctx, credential, core.Request{Model: "gpt-6.1-sol", Payload: payload}, core.Options{
		SourceFormat: translator.FromString("codex"),
		Metadata:     map[string]any{core.ExecutionSessionMetadataKey: t.Name()},
	})
	if err != nil {
		t.Fatal(err)
	}
	var downstream strings.Builder
	for chunk := range result.Chunks {
		if chunk.Err != nil {
			downstream.WriteString(chunk.Err.Error())
			continue
		}
		downstream.Write(chunk.Payload)
	}
	if got := downstream.String(); !strings.Contains(got, "server_is_overloaded") && !strings.Contains(got, "currently overloaded") {
		t.Fatalf("capacity failure was hidden while retry toggle was off: %s", got)
	}
}

func TestIsCodexCapacityOrOverloadFailure(t *testing.T) {
	cases := []struct {
		name string
		body string
		want bool
	}{
		{"overload error", `{"type":"error","error":{"type":"service_unavailable_error","code":"server_is_overloaded","message":"Our servers are currently overloaded. Please try again later."}}`, true},
		{"overload response failed", `{"type":"response.failed","response":{"error":{"code":"server_is_overloaded","message":"Our servers are currently overloaded."}}}`, true},
		{"model capacity", `{"type":"error","error":{"message":"Selected model is at capacity. Please try a different model."}}`, true},
		{"invalid request", `{"type":"error","error":{"type":"invalid_request_error","code":"invalid_value","message":"Invalid input."}}`, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := isCodexCapacityOrOverloadFailure([]byte(tc.body)); got != tc.want {
				t.Fatalf("isCodexCapacityOrOverloadFailure() = %t, want %t", got, tc.want)
			}
		})
	}
}
