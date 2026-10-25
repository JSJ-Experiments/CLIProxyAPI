package executor

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtime/executor/helps"
	cliproxyauth "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/auth"
	log "github.com/sirupsen/logrus"
)

// RecheckQuota uses the read-only usage endpoint, not a model generation request.
func (e *CodexExecutor) RecheckQuota(ctx context.Context, auth *cliproxyauth.Auth) (bool, error) {
	if auth == nil {
		return false, fmt.Errorf("codex quota recheck: missing credential")
	}
	token, _ := auth.Metadata["access_token"].(string)
	if strings.TrimSpace(token) == "" {
		return false, fmt.Errorf("codex quota recheck: missing OAuth token")
	}
	req, errRequest := http.NewRequestWithContext(ctx, http.MethodGet, "https://chatgpt.com/backend-api/wham/usage", nil)
	if errRequest != nil {
		return false, errRequest
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("User-Agent", codexUserAgent)
	if account, _ := auth.Metadata["account_id"].(string); strings.TrimSpace(account) != "" {
		req.Header.Set("Chatgpt-Account-Id", account)
	}
	client := helps.NewUtlsHTTPClient(ctx, e.cfg, auth, 0)
	response, errDo := client.Do(req)
	if errDo != nil {
		return false, fmt.Errorf("codex quota recheck: request failed: %w", errDo)
	}
	defer func() {
		if errClose := response.Body.Close(); errClose != nil {
			log.Debug("codex quota recheck: response close failed")
		}
	}()
	if response.StatusCode != http.StatusOK {
		return false, fmt.Errorf("codex quota recheck: HTTP %d", response.StatusCode)
	}
	body, errRead := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if errRead != nil {
		return false, fmt.Errorf("codex quota recheck: read response: %w", errRead)
	}
	return helps.CodexUsageQuotaAvailable(body), nil
}

func (e *CodexAutoExecutor) RecheckQuota(ctx context.Context, auth *cliproxyauth.Auth) (bool, error) {
	if e == nil || e.httpExec == nil {
		return false, fmt.Errorf("codex quota recheck: missing executor")
	}
	return e.httpExec.RecheckQuota(ctx, auth)
}
