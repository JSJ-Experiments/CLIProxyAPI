package auth

import (
	"context"
	"net/http"
	"strings"
	"time"

	internalconfig "github.com/router-for-me/CLIProxyAPI/v8/internal/config"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/registry"
	log "github.com/sirupsen/logrus"
)

// QuotaRechecker verifies quota without generating a response or consuming tokens.
type QuotaRechecker interface {
	RecheckQuota(context.Context, *Auth) (bool, error)
}

func codexQuotaRecheckEligible(a *Auth, now time.Time) bool {
	if a == nil || !strings.EqualFold(a.Provider, "codex") || a.Disabled || a.Status == StatusDisabled || hasUnauthorizedAuthFailure(a) || hasDisabledInvalidGrantFailure(a) {
		return false
	}
	if a.LastError != nil && (a.LastError.StatusCode() == http.StatusUnauthorized || strings.EqualFold(a.LastError.Code, "unauthorized") || isInvalidGrantResultError(a.LastError)) {
		return false
	}
	token, _ := a.Metadata["access_token"].(string)
	if strings.TrimSpace(token) == "" {
		return false
	}
	if expiry, ok := a.AccessTokenExpirationTime(); ok && !expiry.IsZero() && !expiry.After(now) {
		return false
	}
	if a.Quota.Exceeded && a.Quota.Reason == "credential_quota" {
		return true
	}
	for _, state := range a.ModelStates {
		if state != nil && state.Status != StatusDisabled && state.Quota.Exceeded && isCodexQuotaState(state) {
			return true
		}
	}
	return false
}

func isCodexQuotaState(state *ModelState) bool {
	return state != nil && state.LastError != nil && (strings.EqualFold(state.LastError.Code, "usage_limit_reached") || strings.Contains(strings.ToLower(state.LastError.Message), "usage_limit_reached"))
}

func (m *Manager) codexQuotaRecheckInterval() time.Duration {
	cfg, _ := m.runtimeConfig.Load().(*internalconfig.Config)
	if cfg == nil {
		return 10 * time.Minute
	}
	if cfg.Home.Enabled {
		return 0
	}
	return cfg.Codex.QuotaRecheckIntervalDuration()
}

func codexQuotaRecheckDue(last, now time.Time, interval time.Duration) bool {
	return interval > 0 && !last.IsZero() && !now.Before(last.Add(interval))
}

// runCodexQuotaRechecks owns the schedule. Only exhausted accounts are queried,
// with at most four cancellable probes in flight; healthy accounts are untouched.
func (m *Manager) runCodexQuotaRechecks(ctx context.Context) {
	last := make(map[string]time.Time)
	active := make(map[string]bool)
	completed := make(chan string, 4)
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case id := <-completed:
			delete(active, id)
			last[id] = time.Now()
		case now := <-timer.C:
			interval := m.codexQuotaRecheckInterval()
			cadence := 30 * time.Second
			if interval > 0 && interval < cadence {
				cadence = interval
			}
			if cadence < time.Second {
				cadence = time.Second
			}
			timer.Reset(cadence)
			seen := make(map[string]bool)
			for _, a := range m.List() {
				if interval <= 0 || !codexQuotaRecheckEligible(a, now) {
					continue
				}
				seen[a.ID] = true
				previous, exists := last[a.ID]
				if !exists {
					last[a.ID] = now
					continue
				}
				if active[a.ID] || !codexQuotaRecheckDue(previous, now, interval) || len(active) >= cap(completed) {
					continue
				}
				rechecker, ok := m.executorFor(executorKeyFromAuth(a)).(QuotaRechecker)
				if !ok {
					continue
				}
				active[a.ID] = true
				go func(snapshot *Auth) {
					available, errProbe := rechecker.RecheckQuota(ctx, snapshot)
					// Use the formatter's supported state field; arbitrary fields are omitted.
					if ctx.Err() == nil {
						state := "exhausted"
						if errProbe != nil {
							state = "probe_failed"
						} else if available {
							state = "available"
						}
						log.WithFields(log.Fields{"auth_index": snapshot.Index, "state": state}).Debug("codex quota recheck completed")
					}
					if errProbe == nil && available && ctx.Err() == nil && m.codexQuotaRecheckInterval() > 0 {
						if m.restoreCodexQuotaAfterRecheck(ctx, snapshot, time.Now()) {
							log.WithField("auth_index", snapshot.Index).Info("codex quota recheck verified restoration; credential is available again")
						}
					}
					select {
					case completed <- snapshot.ID:
					case <-ctx.Done():
					}
				}(a)
			}
			for id := range last {
				if !seen[id] && !active[id] {
					delete(last, id)
				}
			}
		}
	}
}

// restoreCodexQuotaAfterRecheck releases only the quota state that was probed.
// Registration/version/deadline checks prevent late probes from reviving removed,
// replaced, disabled or newly exhausted credentials. Independent failures survive.
func (m *Manager) restoreCodexQuotaAfterRecheck(ctx context.Context, checked *Auth, now time.Time) bool {
	if checked == nil {
		return false
	}
	release, errLock := m.lockAuthMutationContext(ctx, checked.ID)
	if errLock != nil {
		return false
	}
	defer release()
	m.mu.Lock()
	current := m.auths[checked.ID]
	if ctx.Err() != nil || m.codexQuotaRecheckInterval() <= 0 || !codexQuotaRecheckEligible(current, now) || current.RegistrationEpoch != checked.RegistrationEpoch || current.CredentialVersion != checked.CredentialVersion || !current.UpdatedAt.Equal(checked.UpdatedAt) || !cooldownQuotaEqual(current.Quota, checked.Quota) || current.cooldownRecordedSeq != checked.cooldownRecordedSeq {
		m.mu.Unlock()
		return false
	}
	credentialScoped := current.Quota.Reason == "credential_quota"
	changed := false
	for model, state := range current.ModelStates {
		before := checked.ModelStates[model]
		if state == nil || before == nil || state.Status == StatusDisabled || !state.Quota.Exceeded || !cooldownQuotaEqual(state.Quota, before.Quota) || state.cooldownRecordedSeq != before.cooldownRecordedSeq || !state.UpdatedAt.Equal(before.UpdatedAt) {
			continue
		}
		wrapped := credentialScoped && (state.credentialQuotaPropagated || state.Quota.Reason == "credential_quota")
		if !wrapped && !isCodexQuotaState(state) {
			continue
		}
		if shadow := state.credentialQuotaShadow; shadow != nil && isModelStateActiveCooldown(shadow, now) {
			*state = *shadow.cloneShallow()
		} else {
			resetModelState(state, now)
		}
		changed = true
	}
	if credentialScoped || changed && current.Quota.Reason == "quota" {
		applyCooldownFields(&current.Quota, QuotaState{})
		current.NextRetryAfter = time.Time{}
		current.Unavailable = false
		changed = true
	}
	if !changed {
		m.mu.Unlock()
		return false
	}
	if len(current.ModelStates) > 0 {
		updateAggregatedAvailability(current, now)
	}
	if !hasModelError(current, now) {
		current.Status = StatusActive
		current.StatusMessage = ""
		current.LastError = nil
	}
	current.Generation++
	current.UpdatedAt = now
	snapshot := current.Clone()
	errPersist := m.persistLocked(ctx, current)
	m.mu.Unlock()
	release()
	if errPersist != nil {
		log.Warn("codex quota recheck: could not persist restored credential")
	}
	if m.scheduler != nil {
		m.scheduler.upsertAuth(snapshot)
	}
	models, epoch := registry.GetGlobalRegistry().GetModelsAndEpochForClient(snapshot.ID)
	projections := make([]registry.ClientModelProjection, 0, len(models))
	for _, model := range models {
		if model != nil {
			projections = append(projections, m.clientModelProjectionForAuth(snapshot, model.ID, now))
		}
	}
	registry.GetGlobalRegistry().ApplyClientModelProjections(snapshot.ID, epoch, snapshot.Generation, projections)
	m.persistCooldownStates(ctx)
	m.hook.OnAuthUpdated(ctx, snapshot)
	return true
}
