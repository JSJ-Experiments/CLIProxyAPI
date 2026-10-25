package management

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/usageledger"
	coreauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
)

func TestCodexQuotaObservationOnlyExistingCredential(t *testing.T) {
	t.Setenv("CLIPROXY_CODEX_USAGE_DIR", t.TempDir())
	manager := coreauth.NewManager(nil, nil, nil)
	_, err := manager.Register(context.Background(), &coreauth.Auth{ID: "codex-test", Provider: "codex", Index: "auth-one"})
	if err != nil {
		t.Fatal(err)
	}
	h := NewHandlerWithoutConfigFilePath(&config.Config{AuthDir: t.TempDir()}, manager)
	send := func(payload string) int {
		rec := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(rec)
		c.Request = httptest.NewRequest(http.MethodPost, "/v8/management/observability/usage/codex/quota", bytes.NewBufferString(payload))
		c.Request.Header.Set("Content-Type", "application/json")
		h.PostCodexQuotaObservation(c)
		return c.Writer.Status()
	}
	for _, payload := range []string{
		`{"auth_index":"missing","windows":[{"id":"five-hour","used_percent":12.5}]}`,
		`{"auth_index":"auth-one","windows":[{"id":"five-hour","used_percent":120}]}`,
	} {
		if code := send(payload); code < 400 {
			t.Fatalf("unsafe payload accepted: %s, code %d", payload, code)
		}
	}
	if code := send(`{"auth_index":"auth-one","windows":[{"id":"five-hour","used_percent":12.5}]}`); code != 204 {
		t.Fatalf("post code=%d", code)
	}
	rec := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest(http.MethodGet, "/v8/management/observability/usage/codex?hours=24", nil)
	h.GetCodexClientUsage(c)
	var data usageledger.Report
	if err := json.Unmarshal(rec.Body.Bytes(), &data); err != nil {
		t.Fatal(err)
	}
	if len(data.Quotas) != 1 || data.Quotas[0].AuthIndex != "auth-one" {
		t.Fatalf("quota missing: %+v", data.Quotas)
	}
}
