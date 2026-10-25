package management

import (
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/router-for-me/CLIProxyAPI/v8/internal/usageledger"
)

// GetCodexClientUsage returns aggregate upstream attempts and tokens grouped by
// auth account and hashed downstream API key. Percentages remain account-only.
func (h *Handler) GetCodexClientUsage(c *gin.Context) {
	hours := 24.0
	if raw := strings.TrimSpace(c.Query("hours")); raw != "" {
		parsed, err := strconv.ParseFloat(raw, 64)
		if err != nil || math.IsNaN(parsed) || math.IsInf(parsed, 0) || parsed <= 0 || parsed > 168 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "hours must be greater than 0 and no more than 168"})
			return
		}
		hours = parsed
	}
	report, err := usageledger.Summary(time.Now(), time.Duration(hours*float64(time.Hour)))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not read Codex usage ledger"})
		return
	}
	c.JSON(http.StatusOK, report)
}

type codexQuotaObservation struct {
	AuthIndex string                    `json:"auth_index" binding:"required"`
	Windows   []usageledger.QuotaWindow `json:"windows" binding:"required"`
}

// PostCodexQuotaObservation stores server-reported absolute usage percentages.
// It accepts only an index of an existing Codex credential in this proxy.
func (h *Handler) PostCodexQuotaObservation(c *gin.Context) {
	var payload codexQuotaObservation
	if err := c.ShouldBindJSON(&payload); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid Codex quota observation"})
		return
	}
	payload.AuthIndex = strings.TrimSpace(payload.AuthIndex)
	if len(payload.Windows) == 0 || len(payload.Windows) > 32 || len(payload.AuthIndex) > 512 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid Codex quota observation"})
		return
	}
	for _, w := range payload.Windows {
		if strings.TrimSpace(w.ID) == "" || len(w.ID) > 128 || w.UsedPercent == nil || math.IsNaN(*w.UsedPercent) || math.IsInf(*w.UsedPercent, 0) || *w.UsedPercent < 0 || *w.UsedPercent > 100 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid quota window"})
			return
		}
		if w.PeriodHours != nil && (math.IsNaN(*w.PeriodHours) || math.IsInf(*w.PeriodHours, 0) || *w.PeriodHours < 0 || *w.PeriodHours > 24*365) {
			c.JSON(http.StatusBadRequest, gin.H{"error": "invalid quota window duration"})
			return
		}
	}
	h.mu.Lock()
	manager := h.authManager
	h.mu.Unlock()
	found := false
	if manager != nil {
		for _, auth := range manager.List() {
			if auth != nil && strings.EqualFold(auth.Provider, "codex") && (auth.Index == payload.AuthIndex || auth.EnsureIndex() == payload.AuthIndex) {
				found = true
				break
			}
		}
	}
	if !found {
		c.JSON(http.StatusNotFound, gin.H{"error": "Codex credential not found"})
		return
	}
	if err := usageledger.ObserveQuota(payload.AuthIndex, payload.Windows); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "could not record Codex quota observation"})
		return
	}
	c.Status(http.StatusNoContent)
}
