package usageledger

import (
	"context"
	"math"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
)

func TestCodexUsageGroupsByAccountAndHashedClient(t *testing.T) {
	dir := t.TempDir()
	t.Setenv("CLIPROXY_CODEX_USAGE_DIR", dir)
	scope1 := strings.Repeat("a", 64)
	scope2 := strings.Repeat("b", 64)
	plugin := &plugin{}
	for _, tc := range []struct {
		account, scope string
		in, out        int64
		fail           bool
	}{
		{"auth-one", scope1, 80, 20, false},
		{"auth-one", scope2, 60, 30, true},
		{"auth-two", scope1, 40, 10, false},
	} {
		ctx := logging.WithClientRequestMetadata(context.Background(), logging.ClientRequestMetadata{ClientKeyScope: tc.scope})
		plugin.HandleUsage(ctx, usage.Record{Provider: "codex", AuthIndex: tc.account, RequestedAt: time.Now(), Failed: tc.fail, Detail: usage.Detail{InputTokens: tc.in, OutputTokens: tc.out, TotalTokens: tc.in + tc.out}})
	}
	plugin.HandleUsage(context.Background(), usage.Record{Provider: "claude", AuthIndex: "auth-one"})
	pct := 26.25
	if err := ObserveQuota("auth-one", []QuotaWindow{{ID: "five-hour", UsedPercent: &pct}}); err != nil {
		t.Fatal(err)
	}
	report, err := Summary(time.Now().Add(time.Second), time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Attributions) != 3 {
		t.Fatalf("attributions = %+v", report.Attributions)
	}
	if report.Attributions[0].AuthIndex != "auth-one" || report.Attributions[0].ClientKeyScope != scope1 || report.Attributions[0].TotalTokens != 100 {
		t.Fatalf("first row = %+v", report.Attributions[0])
	}
	if report.Attributions[1].Failed != 1 || report.Attributions[1].TotalTokens != 90 {
		t.Fatalf("second row = %+v", report.Attributions[1])
	}
	if len(report.Quotas) != 1 || *report.Quotas[0].Windows[0].UsedPercent != pct {
		t.Fatalf("quota = %+v", report.Quotas)
	}
	files, _ := os.ReadDir(dir)
	if len(files) != 1 {
		t.Fatalf("expected daily ledger file, got %d", len(files))
	}
	info, _ := files[0].Info()
	if info.Mode().Perm() != 0600 {
		t.Fatalf("insecure file permissions: %v", info.Mode())
	}
}

func TestCodexUsageLookbackAndPercentageValidation(t *testing.T) {
	t.Setenv("CLIPROXY_CODEX_USAGE_DIR", t.TempDir())
	if _, err := Summary(time.Now(), MaxLookback+time.Second); err == nil {
		t.Fatal("expected lookback error")
	}
	if err := ObserveQuota("", []QuotaWindow{{ID: "five-hour"}}); err == nil {
		t.Fatal("expected auth index error")
	}
	pct := math.NaN()
	// Append stores only structurally valid input; the authenticated API validates
	// non-finite and out-of-range percentages before forwarding it here.
	if err := ObserveQuota("test", []QuotaWindow{{ID: "five-hour", UsedPercent: &pct}}); err == nil {
		t.Fatal("NaN must not be encodable as JSON")
	}
}

func TestCodexLedgerDoesNotWriteWithoutExplicitDirectoryOrGeneration(t *testing.T) {
	t.Setenv("CLIPROXY_CODEX_USAGE_DIR", "")
	(&plugin{}).HandleUsage(context.Background(), usage.Record{Provider: "codex", AuthIndex: "test"})
	if _, err := Summary(time.Now(), time.Hour); err == nil {
		t.Fatal("expected disabled ledger to reject reads")
	}
	dir := t.TempDir()
	t.Setenv("CLIPROXY_CODEX_USAGE_DIR", dir)
	(&plugin{}).HandleUsage(context.Background(), usage.Record{
		Provider: "codex", AuthIndex: "test", Generate: usage.GenerateFlag(false),
	})
	files, err := os.ReadDir(dir)
	if err != nil || len(files) != 0 {
		t.Fatalf("result-only attempts must not be billed: files=%v, err=%v", files, err)
	}
}
