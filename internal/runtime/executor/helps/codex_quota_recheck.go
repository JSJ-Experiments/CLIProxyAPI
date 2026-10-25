package helps

import (
	"math"

	"github.com/MoeForge/moego/ojson"
)

// CodexUsageQuotaAvailable requires explicit, consistent proof of available quota.
// Missing/malformed windows and exhausted additional pools never clear a warning.
func CodexUsageQuotaAvailable(body []byte) bool {
	if !ojson.ValidBytes(body) {
		return false
	}
	root := ojson.ParseBytes(body)
	if !codexUsageLimitAvailable(root.Get("rate_limit")) {
		return false
	}
	additional := root.Get("additional_rate_limits")
	if additional.Exists() && additional.Type != ojson.Null {
		if !additional.IsArray() {
			return false
		}
		for _, item := range additional.Array() {
			if !codexUsageLimitAvailable(item.Get("rate_limit")) {
				return false
			}
		}
	}
	return true
}

func codexUsageLimitAvailable(limit ojson.Result) bool {
	if !limit.IsObject() || limit.Get("allowed").Type != ojson.True || limit.Get("limit_reached").Type != ojson.False {
		return false
	}
	hasWindow := false
	for _, key := range []string{"primary_window", "secondary_window"} {
		window := limit.Get(key)
		if !window.Exists() || window.Type == ojson.Null {
			continue
		}
		used := window.Get("used_percent")
		if !window.IsObject() || used.Type != ojson.Number || math.IsNaN(used.Float()) || math.IsInf(used.Float(), 0) || used.Float() < 0 || used.Float() >= 100 {
			return false
		}
		hasWindow = true
	}
	return hasWindow
}
