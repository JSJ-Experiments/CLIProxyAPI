package executor

import (
	"context"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

type codexRecheckBody struct {
	io.Reader
	closed bool
}

func (b *codexRecheckBody) Close() error { b.closed = true; return nil }

func TestCodexRecheckQuotaReadOnlyRequest(t *testing.T) {
	for _, status := range []int{200, 401, 429, 503} {
		t.Run(http.StatusText(status), func(t *testing.T) {
			body := &codexRecheckBody{Reader: strings.NewReader(`{"rate_limit":{"allowed":true,"limit_reached":false,"primary_window":{"used_percent":1}}}`)}
			called := false
			ctx := context.WithValue(context.Background(), "cliproxy.roundtripper", roundTripperFunc(func(req *http.Request) (*http.Response, error) {
				called = true
				if req.Method != http.MethodGet || req.URL.String() != "https://chatgpt.com/backend-api/wham/usage" || req.Body != nil {
					t.Fatal("probe must be a read-only usage request")
				}
				if req.Header.Get("Authorization") != "Bearer fixture" || req.Header.Get("Chatgpt-Account-Id") != "fixture-account" || req.Header.Get("User-Agent") == "" {
					t.Fatal("missing OAuth request headers")
				}
				return &http.Response{StatusCode: status, Body: body, Header: make(http.Header)}, nil
			}))
			e := NewCodexExecutor(&config.Config{})
			available, err := e.RecheckQuota(ctx, &cliproxyauth.Auth{Metadata: map[string]any{"access_token": "fixture", "account_id": "fixture-account"}})
			if !called || !body.closed {
				t.Fatal("probe not called or body not closed")
			}
			if status == 200 {
				if err != nil || !available {
					t.Fatalf("available=%v err=%v", available, err)
				}
			} else if err == nil || available {
				t.Fatal("failed probe must not restore quota")
			}
		})
	}
	e := NewCodexExecutor(&config.Config{})
	for _, a := range []*cliproxyauth.Auth{nil, {}} {
		if available, err := e.RecheckQuota(context.Background(), a); err == nil || available {
			t.Fatal("missing token accepted")
		}
	}
}
