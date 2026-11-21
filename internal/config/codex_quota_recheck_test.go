package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestCodexQuotaRecheckInterval(t *testing.T) {
	for _, tt := range []struct {
		value string
		want  time.Duration
	}{
		{"", 10 * time.Minute}, {"10m", 10 * time.Minute}, {"5m", 5 * time.Minute}, {"0", 0}, {"0s", 0}, {" 2m30s ", 150 * time.Second}, {"invalid", 10 * time.Minute}, {"-1m", 10 * time.Minute},
	} {
		c := &CodexConfig{QuotaRecheckInterval: tt.value}
		if got := c.QuotaRecheckIntervalDuration(); got != tt.want {
			t.Errorf("%q=%v, want %v", tt.value, got, tt.want)
		}
	}
	var c *CodexConfig
	if c.QuotaRecheckIntervalDuration() != 10*time.Minute {
		t.Fatal("nil must default to 10m")
	}
}

func TestCodexQuotaRecheckV8RoundTrip(t *testing.T) {
	for _, value := range []string{"0", "5m"} {
		data := []byte("config-version: 8\nserver: {port: 8317}\nupstream:\n  codex:\n    wait-on-quota: true\n    quota-recheck-interval: \"" + value + "\"\n")
		cfg, err := ParseConfigBytes(data)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Codex.QuotaRecheckInterval != value || !cfg.Codex.WaitOnQuota {
			t.Fatal("v8 interval not loaded")
		}
		path := filepath.Join(t.TempDir(), "config.yaml")
		if err = os.WriteFile(path, data, 0600); err != nil {
			t.Fatal(err)
		}
		if err = SaveConfigPreserveComments(path, cfg); err != nil {
			t.Fatal(err)
		}
		cfg, err = LoadConfig(path)
		if err != nil {
			t.Fatal(err)
		}
		if cfg.Codex.QuotaRecheckInterval != value || !cfg.Codex.WaitOnQuota {
			t.Fatal("interval not preserved")
		}
	}
}

func TestCodexQuotaWaitKeepaliveConfig(t *testing.T) {
	cfg, errParse := ParseConfigBytes([]byte("config-version: 8\nserver: {port: 8317}\nupstream:\n  codex:\n    wait-on-quota: true\n    quota-recheck-interval: 15s\nrequests:\n  streaming:\n    keepalive-seconds: 15\n    send-keepalive-before-ft: true\n  nonstream-keepalive-interval: 15\n"))
	if errParse != nil {
		t.Fatal(errParse)
	}
	if !cfg.Codex.WaitOnQuota || cfg.Streaming.KeepAliveSeconds != 15 || !cfg.Streaming.SendKeepAliveBeforeFT || cfg.NonStreamKeepAliveInterval != 15 {
		t.Fatal("quota waiting requires v8 requests keepalive settings to reach the handlers")
	}
}
