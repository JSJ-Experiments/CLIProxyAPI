package auth

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/runtimeevents"
	cliproxyexecutor "github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/executor"
)

func (m *Manager) codexQuotaWaitEnabled() bool {
	cfg := m.runtimeConfigSnapshot()
	return cfg != nil && cfg.Codex.WaitOnQuota && !cfg.Home.Enabled
}

func isCodexUsageLimitError(err error) bool {
	return err != nil && statusCodeFromError(err) == http.StatusTooManyRequests &&
		strings.Contains(strings.ToLower(err.Error()), "usage_limit_reached")
}

// notifyQuotaAvailabilityChange runs after scheduler/registry publication, never
// under mu. It is separate from structural auth watchers, whose contract excludes
// request result accounting.
func (m *Manager) notifyQuotaAvailabilityChange() {
	if m == nil {
		return
	}
	m.mu.Lock()
	if m.quotaAvailabilityChanged != nil {
		close(m.quotaAvailabilityChanged)
		m.quotaAvailabilityChanged = nil
	}
	m.mu.Unlock()
}

// codexQuotaWaitState subscribes and inspects the same state under one lock so
// restoration between inspection and waiting cannot be missed. Selection filters
// and aliases must match ordinary routing; unrelated credentials cannot unblock it.
func (m *Manager) codexQuotaWaitState(ctx context.Context, model string, opts cliproxyexecutor.Options, now time.Time) (time.Time, bool, <-chan struct{}) {
	eligibility := authSelectionEligibilityForRequest(ctx, opts)
	pinned := pinnedAuthIDFromMetadata(opts.Metadata)
	reg := registry.GetGlobalRegistry()
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.quotaAvailabilityChanged == nil {
		m.quotaAvailabilityChanged = make(chan struct{})
	}
	var earliest time.Time
	for _, a := range m.auths {
		if a == nil || executorKeyFromAuth(a) != "codex" || a.Disabled || a.Status == StatusDisabled ||
			m.cooldownDisabledForAuth(a) ||
			(pinned != "" && a.ID != pinned) || !eligibility.allows(a) ||
			!m.authSupportsRouteModel(reg, a, model) {
			continue
		}
		selectionModel := m.selectionModelForAuth(a, model)
		blocked, reason, next := isAuthBlockedForModel(a, selectionModel, now)
		if !blocked {
			return time.Time{}, true, m.quotaAvailabilityChanged
		}
		if reason != blockReasonCooldown || !next.After(now) {
			continue
		}
		quota := a.Quota.Exceeded && a.Quota.Reason == "credential_quota"
		if !quota {
			state := existingModelState(a, canonicalModelKey(selectionModel))
			quota = state != nil && state.Quota.Exceeded && isCodexQuotaState(state)
		}
		if quota && (earliest.IsZero() || next.Before(earliest)) {
			earliest = next
		}
	}
	return earliest, false, m.quotaAvailabilityChanged
}

// waitForCodexQuota is only used before returning a response/stream. It never
// replays already-delivered output, and quota waits do not consume retry rounds.
func (m *Manager) waitForCodexQuota(ctx context.Context, providers []string, model string, opts cliproxyexecutor.Options, err error) (bool, error) {
	if !m.codexQuotaWaitEnabled() || len(providers) != 1 || providers[0] != "codex" ||
		errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) ||
		isRequestStopError(err) || isRequestTerminatedError(err) ||
		cliproxyexecutor.IsUpstreamWebsocketReconnect(err) {
		return false, nil
	}
	// A configured continuity scheduler owns waiting and its request budget.
	if _, active := m.canRunCodexContinuity(ctx, providers); active {
		return false, nil
	}
	var cooldown *modelCooldownError
	if !isCodexUsageLimitError(err) && !errors.As(err, &cooldown) {
		return false, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	waiting := false
	status := "cancelled"
	publish := func(state string, wait time.Duration) {
		session, _ := opts.Metadata[cliproxyexecutor.ExecutionSessionMetadataKey].(string)
		runtimeevents.Publish(runtimeevents.Event{Type: "codex.quota", Status: state, Provider: "codex", Model: model, Session: session, WaitMS: wait.Milliseconds()})
	}
	defer func() {
		if waiting {
			publish(status, 0)
		}
	}()
	for {
		if errCtx := ctx.Err(); errCtx != nil {
			return false, errCtx
		}
		if !m.codexQuotaWaitEnabled() {
			return false, nil
		}
		next, ready, changed := m.codexQuotaWaitState(ctx, model, opts, time.Now())
		if ready || next.IsZero() {
			if ready {
				// A concurrent probe may restore quota before this waiter has
				// subscribed. Retry immediately instead of leaking the old error.
				status = "resuming"
				return true, nil
			}
			if waiting {
				// Retry selection even if the waiting credential was removed or
				// disabled, so stale quota errors never replace the current cause.
				status = "resuming"
				return true, nil
			}
			return false, nil
		}
		wait := time.Until(next)
		if !waiting {
			waiting = true
			publish("waiting", wait)
		}
		// This is a read-only state rescan, not an upstream retry/probe. It
		// covers registry changes too, without extending the known reset time.
		if wait > time.Second {
			wait = time.Second
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
		case <-changed:
		case <-timer.C:
		}
		timer.Stop()
	}
}

// quotaStreamClientError preserves actual quota accounting above this boundary.
// Partial output cannot be safely replayed: ask the client to retry rather than
// exposing the provider's terminal account-usage classification.
func (m *Manager) quotaStreamClientError(provider, model string, opts cliproxyexecutor.Options, err error) error {
	if provider != "codex" || !m.codexQuotaWaitEnabled() || !isCodexUsageLimitError(err) {
		return err
	}
	session, _ := opts.Metadata[cliproxyexecutor.ExecutionSessionMetadataKey].(string)
	runtimeevents.Publish(runtimeevents.Event{Type: "codex.quota", Status: "reconnect_required", Provider: provider, Model: model, Session: session})
	if cliproxyexecutor.IsUpstreamWebsocketReconnect(err) {
		return err
	}
	return &codexQuotaRetryError{}
}

// No Unwrap: quota diagnostics have already been recorded internally. Downstream
// error builders must not rediscover the provider's terminal quota classification.
type codexQuotaRetryError struct{}

func (*codexQuotaRetryError) Error() string {
	return `{"error":{"type":"server_error","code":"upstream_temporarily_unavailable","message":"Upstream temporarily unavailable; retry with full context.","retryable":true}}`
}

func (*codexQuotaRetryError) StatusCode() int { return http.StatusServiceUnavailable }
