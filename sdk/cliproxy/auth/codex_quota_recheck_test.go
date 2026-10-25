package auth

import (
	"context"
	"testing"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
)

func codexRecheckFixture(t *testing.T, scoped bool) (*Manager, *Auth, time.Time) {
	t.Helper()
	m := NewManager(nil, nil, nil)
	id := "codex-recheck-" + t.Name()
	registry.GetGlobalRegistry().RegisterClient(id, "codex", []*registry.ModelInfo{{ID: "model-a"}, {ID: "model-b"}})
	t.Cleanup(func() { registry.GetGlobalRegistry().UnregisterClient(id) })
	if _, err := m.Register(context.Background(), &Auth{ID: id, Provider: "codex", Status: StatusActive, Metadata: map[string]any{"access_token": "fixture"}}); err != nil {
		t.Fatal(err)
	}
	m.MarkResult(context.Background(), Result{AuthID: id, Provider: "codex", Model: "model-b", Success: true})
	retry := 3 * time.Hour
	m.MarkResult(context.Background(), Result{AuthID: id, Provider: "codex", Model: "model-a", Error: &Error{HTTPStatus: 429, Code: "usage_limit_reached", Message: "usage_limit_reached"}, RetryAfter: &retry, CredentialScope: scoped})
	a, _ := m.GetByID(id)
	return m, a, time.Now()
}

func TestCodexQuotaRecheckRestoresOnlyQuota(t *testing.T) {
	withQuotaCooldownEnabled(t)
	for _, scoped := range []bool{true, false} {
		t.Run(map[bool]string{true: "credential", false: "model"}[scoped], func(t *testing.T) {
			m, a, now := codexRecheckFixture(t, scoped)
			if !codexQuotaRecheckEligible(a, now) {
				t.Fatal("exhausted account not eligible")
			}
			if !m.restoreCodexQuotaAfterRecheck(context.Background(), a, now) {
				t.Fatal("not restored")
			}
			stored, _ := m.GetByID(a.ID)
			if stored.Quota.Exceeded || stored.Unavailable || stored.Status != StatusActive || !stored.NextRetryAfter.IsZero() {
				t.Fatal("credential remains blocked")
			}
			for model, state := range stored.ModelStates {
				if state.Unavailable || state.Quota.Exceeded {
					t.Fatalf("model %s remains blocked", model)
				}
			}
			if m.restoreCodexQuotaAfterRecheck(context.Background(), a, now) {
				t.Fatal("stale probe restored twice")
			}
		})
	}
}

func TestCodexQuotaRecheckRejectsStaleOrDisabled(t *testing.T) {
	withQuotaCooldownEnabled(t)
	for name, mutate := range map[string]func(*Auth){
		"disabled":                func(a *Auth) { a.Disabled = true },
		"status disabled":         func(a *Auth) { a.Status = StatusDisabled },
		"replacement":             func(a *Auth) { a.RegistrationEpoch++ },
		"token changed":           func(a *Auth) { a.CredentialVersion++ },
		"new quota deadline":      func(a *Auth) { a.Quota.NextRecoverAt = a.Quota.NextRecoverAt.Add(time.Hour) },
		"new quota same deadline": func(a *Auth) { a.cooldownRecordedSeq++ },
		"updated while probing":   func(a *Auth) { a.UpdatedAt = a.UpdatedAt.Add(time.Second) },
		"unauthorized":            func(a *Auth) { a.LastError = &Error{HTTPStatus: 401, Message: "unauthorized"} },
		"other provider":          func(a *Auth) { a.Provider = "claude" },
		"no token":                func(a *Auth) { delete(a.Metadata, "access_token") },
	} {
		t.Run(name, func(t *testing.T) {
			m, a, now := codexRecheckFixture(t, true)
			m.mu.Lock()
			mutate(m.auths[a.ID])
			m.mu.Unlock()
			if m.restoreCodexQuotaAfterRecheck(context.Background(), a, now) {
				t.Fatal("stale or disabled state restored")
			}
		})
	}
	t.Run("removed", func(t *testing.T) {
		m, a, now := codexRecheckFixture(t, true)
		m.mu.Lock()
		delete(m.auths, a.ID)
		m.mu.Unlock()
		if m.restoreCodexQuotaAfterRecheck(context.Background(), a, now) {
			t.Fatal("removed credential restored")
		}
	})
	t.Run("cancelled", func(t *testing.T) {
		m, a, now := codexRecheckFixture(t, true)
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		if m.restoreCodexQuotaAfterRecheck(ctx, a, now) {
			t.Fatal("cancelled probe restored")
		}
	})
}

func TestCodexQuotaRecheckPreservesIndependentFailures(t *testing.T) {
	withQuotaCooldownEnabled(t)
	for _, shadow := range []bool{false, true} {
		t.Run(map[bool]string{false: "new sibling error", true: "wrapped independent error"}[shadow], func(t *testing.T) {
			m, a, now := codexRecheckFixture(t, true)
			independent := &ModelState{Status: StatusError, Unavailable: true, NextRetryAfter: now.Add(time.Hour), LastError: &Error{HTTPStatus: 503, Message: "independent overload"}}
			m.mu.Lock()
			if shadow {
				m.auths[a.ID].ModelStates["model-b"].credentialQuotaShadow = independent
			} else {
				m.auths[a.ID].ModelStates["model-b"] = independent
			}
			m.mu.Unlock()
			// The probe must not delete a newer independent shadow, even if added while in flight.
			if !m.restoreCodexQuotaAfterRecheck(context.Background(), a, now) {
				t.Fatal("quota did not restore")
			}
			stored, _ := m.GetByID(a.ID)
			state := stored.ModelStates["model-b"]
			if !state.Unavailable || state.LastError == nil || state.LastError.Message != "independent overload" {
				t.Fatal("independent error lost")
			}
			if stored.ModelStates["model-a"].Unavailable {
				t.Fatal("quota model not released")
			}
		})
	}
}

func TestCodexQuotaRecheckDisabledWhileInFlight(t *testing.T) {
	withQuotaCooldownEnabled(t)
	m, a, now := codexRecheckFixture(t, true)
	cfg := &internalconfig.Config{}
	cfg.Codex.QuotaRecheckInterval = "0"
	m.runtimeConfig.Store(cfg)
	if m.restoreCodexQuotaAfterRecheck(context.Background(), a, now) {
		t.Fatal("disabled recheck must not clear quota")
	}
}

func TestCodexQuotaRecheckEligibilityAndSchedule(t *testing.T) {
	now := time.Date(2026, 10, 25, 8, 0, 0, 0, time.UTC)
	healthy := &Auth{Provider: "codex", Metadata: map[string]any{"access_token": "fixture"}}
	if codexQuotaRecheckEligible(healthy, now) {
		t.Fatal("healthy accounts must not be probed")
	}
	healthy.Quota = QuotaState{Exceeded: true, Reason: "credential_quota"}
	if !codexQuotaRecheckEligible(healthy, now) {
		t.Fatal("exhausted account not eligible")
	}
	healthy.Metadata["expired"] = now.Add(-time.Hour).Format(time.RFC3339)
	if codexQuotaRecheckEligible(healthy, now) {
		t.Fatal("expired token should be refreshed, not probed")
	}
	for _, tt := range []struct {
		last     time.Time
		interval time.Duration
		want     bool
	}{
		{time.Time{}, 10 * time.Minute, false}, {now, 10 * time.Minute, false}, {now.Add(-9 * time.Minute), 10 * time.Minute, false}, {now.Add(-10 * time.Minute), 10 * time.Minute, true}, {now.Add(-time.Hour), 0, false}, {now.Add(-time.Hour), -time.Minute, false},
	} {
		if got := codexQuotaRecheckDue(tt.last, now, tt.interval); got != tt.want {
			t.Fatalf("due=%v want %v", got, tt.want)
		}
	}
	m := NewManager(nil, nil, nil)
	if m.codexQuotaRecheckInterval() != 10*time.Minute {
		t.Fatal("default interval")
	}
	cfg := &internalconfig.Config{}
	cfg.Codex.QuotaRecheckInterval = "5m"
	m.runtimeConfig.Store(cfg)
	if m.codexQuotaRecheckInterval() != 5*time.Minute {
		t.Fatal("interval reload")
	}
	cfg2 := *cfg
	cfg2.Codex.QuotaRecheckInterval = "0"
	m.runtimeConfig.Store(&cfg2)
	if m.codexQuotaRecheckInterval() != 0 {
		t.Fatal("disabled interval")
	}
	cfg3 := *cfg
	cfg3.Home.Enabled = true
	m.runtimeConfig.Store(&cfg3)
	if m.codexQuotaRecheckInterval() != 0 {
		t.Fatal("Home owns quota polling")
	}
}
