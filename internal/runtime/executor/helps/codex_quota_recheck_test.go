package helps

import "testing"

func TestCodexUsageQuotaAvailable(t *testing.T) {
	const available = `{"allowed":true,"limit_reached":false,"primary_window":{"used_percent":12},"secondary_window":{"used_percent":87.5}}`
	for _, tt := range []struct {
		name, body string
		want       bool
	}{
		{"available", `{"rate_limit":` + available + `}`, true},
		{"single window", `{"rate_limit":{"allowed":true,"limit_reached":false,"primary_window":{"used_percent":0},"secondary_window":null}}`, true},
		{"additional available", `{"rate_limit":` + available + `,"additional_rate_limits":[{"rate_limit":` + available + `}]}`, true},
		{"additional exhausted", `{"rate_limit":` + available + `,"additional_rate_limits":[{"rate_limit":{"allowed":false,"limit_reached":true,"primary_window":{"used_percent":100}}}]}`, false},
		{"no windows", `{"rate_limit":{"allowed":true,"limit_reached":false}}`, false},
		{"missing flags", `{"rate_limit":{"primary_window":{"used_percent":1}}}`, false},
		{"string flags", `{"rate_limit":{"allowed":"true","limit_reached":false,"primary_window":{"used_percent":1}}}`, false},
		{"contradictory flags", `{"rate_limit":{"allowed":true,"limit_reached":true,"primary_window":{"used_percent":1}}}`, false},
		{"full", `{"rate_limit":{"allowed":true,"limit_reached":false,"primary_window":{"used_percent":100}}}`, false},
		{"negative", `{"rate_limit":{"allowed":true,"limit_reached":false,"primary_window":{"used_percent":-1}}}`, false},
		{"string percent", `{"rate_limit":{"allowed":true,"limit_reached":false,"primary_window":{"used_percent":"1"}}}`, false},
		{"malformed additional", `{"rate_limit":` + available + `,"additional_rate_limits":{}}`, false},
		{"missing additional limit", `{"rate_limit":` + available + `,"additional_rate_limits":[{}]}`, false},
		{"empty", `{}`, false}, {"invalid", `{"rate_limit":`, false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if got := CodexUsageQuotaAvailable([]byte(tt.body)); got != tt.want {
				t.Fatalf("available=%v, want %v", got, tt.want)
			}
		})
	}
}
