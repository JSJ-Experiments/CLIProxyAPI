// Package usageledger records Codex provider usage by upstream account and
// irreversible downstream client-key scope. It deliberately does NOT translate
// token counts into quota percentages: Codex does not publish that conversion.
package usageledger

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/router-for-me/CLIProxyAPI/v8/internal/logging"
	"github.com/router-for-me/CLIProxyAPI/v8/sdk/cliproxy/usage"
	log "github.com/sirupsen/logrus"
)

const MaxLookback = 7 * 24 * time.Hour

// QuotaWindow records a server-provided absolute used percentage observation.
type QuotaWindow struct {
	ID          string   `json:"id"`
	UsedPercent *float64 `json:"used_percent"`
	ResetAtMs   *int64   `json:"reset_at_ms,omitempty"`
	PeriodHours *float64 `json:"period_hours,omitempty"`
}

type Event struct {
	Kind           string        `json:"kind"`
	Time           time.Time     `json:"time"`
	AuthIndex      string        `json:"auth_index"`
	ClientKeyScope string        `json:"client_key_scope,omitempty"`
	RequestID      string        `json:"request_id,omitempty"`
	Model          string        `json:"model,omitempty"`
	Failed         bool          `json:"failed,omitempty"`
	InputTokens    int64         `json:"input_tokens,omitempty"`
	OutputTokens   int64         `json:"output_tokens,omitempty"`
	TotalTokens    int64         `json:"total_tokens,omitempty"`
	Windows        []QuotaWindow `json:"windows,omitempty"`
}

type Attribution struct {
	AuthIndex      string `json:"auth_index"`
	ClientKeyScope string `json:"client_key_scope"`
	Attempts       int64  `json:"attempts"`
	Failed         int64  `json:"failed"`
	InputTokens    int64  `json:"input_tokens"`
	OutputTokens   int64  `json:"output_tokens"`
	TotalTokens    int64  `json:"total_tokens"`
}

type QuotaSnapshot struct {
	AuthIndex  string        `json:"auth_index"`
	ObservedAt time.Time     `json:"observed_at"`
	Windows    []QuotaWindow `json:"windows"`
}

type Report struct {
	Since        time.Time       `json:"since"`
	Through      time.Time       `json:"through"`
	Attributions []Attribution   `json:"attributions"`
	Quotas       []QuotaSnapshot `json:"quotas"`
}

var fileMu sync.Mutex

// Each UTC calendar day is a separate private JSONL file, so reads and writes
// remain bounded and old logs can be archived or removed without rewriting data.
func ledgerDir() (string, error) {
	if custom := strings.TrimSpace(os.Getenv("CLIPROXY_CODEX_USAGE_DIR")); custom != "" {
		return custom, nil
	}
	// An explicit directory is mandatory: SDK users, unit tests, and standalone
	// tools must never accidentally write into a real proxy's accounting log.
	return "", errors.New("Codex usage ledger is not configured")
}

func appendEvent(event Event) error {
	if event.Time.IsZero() {
		event.Time = time.Now().UTC()
	}
	event.Time = event.Time.UTC()
	data, err := json.Marshal(event)
	if err != nil {
		return err
	}
	dir, err := ledgerDir()
	if err != nil {
		return err
	}
	fileMu.Lock()
	defer fileMu.Unlock()
	if err = os.MkdirAll(dir, 0700); err != nil {
		return err
	}
	path := filepath.Join(dir, "codex-"+event.Time.Format("2006-01-02")+".jsonl")
	f, err := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	_, err = f.Write(append(data, '\n'))
	closeErr := f.Close()
	if err != nil {
		return err
	}
	return closeErr
}

// ObserveQuota accepts only normalized windows, not tokens or auth credentials.
func ObserveQuota(authIndex string, windows []QuotaWindow) error {
	authIndex = strings.TrimSpace(authIndex)
	if authIndex == "" || len(authIndex) > 512 {
		return errors.New("invalid auth index")
	}
	if len(windows) == 0 || len(windows) > 32 {
		return errors.New("invalid quota windows")
	}
	return appendEvent(Event{Kind: "quota", Time: time.Now().UTC(), AuthIndex: authIndex, Windows: windows})
}

func (p *plugin) HandleUsage(ctx context.Context, record usage.Record) {
	if !strings.EqualFold(record.Provider, "codex") {
		return
	}
	if strings.TrimSpace(os.Getenv("CLIPROXY_CODEX_USAGE_DIR")) == "" ||
		!usage.GenerateEnabled(record.Generate) {
		return
	}
	authIndex := strings.TrimSpace(record.AuthIndex)
	if authIndex == "" {
		authIndex = "auth_id:" + strings.TrimSpace(record.AuthID)
	}
	if authIndex == "auth_id:" {
		return
	}
	scope := strings.TrimSpace(logging.GetClientRequestMetadata(ctx).ClientKeyScope)
	if scope == "" {
		scope = "unattributed"
	}
	d := usage.EnsureTokenBreakdownForProvider(record.Detail, record.Provider, record.ExecutorType)
	requestedAt := record.RequestedAt
	if requestedAt.IsZero() {
		requestedAt = time.Now().UTC()
	}
	err := appendEvent(Event{
		Kind: "request", Time: requestedAt, AuthIndex: authIndex, ClientKeyScope: scope,
		RequestID: record.RequestID, Model: record.Model, Failed: record.Failed,
		InputTokens: d.InputTokens, OutputTokens: d.OutputTokens, TotalTokens: d.TotalTokens,
	})
	if err != nil {
		log.WithError(err).Warn("codex usage ledger write failed")
	}
}

type plugin struct{}

func init() { usage.RegisterNamedPlugin("codex-client-usage-ledger", &plugin{}) }

// Summary returns one week's bounded account/key request totals and latest
// quota observations. They are intentionally separate; quota percentages are
// account-level, never assigned to a client without causal evidence.
func Summary(now time.Time, lookback time.Duration) (Report, error) {
	if lookback <= 0 || lookback > MaxLookback {
		return Report{}, errors.New("lookback must be between 1 second and 7 days")
	}
	now = now.UTC()
	since := now.Add(-lookback)
	out := Report{Since: since, Through: now, Attributions: []Attribution{}, Quotas: []QuotaSnapshot{}}
	dir, err := ledgerDir()
	if err != nil {
		return out, err
	}
	sums := map[string]*Attribution{}
	snapshots := map[string]QuotaSnapshot{}
	fileMu.Lock()
	defer fileMu.Unlock()
	for day := time.Date(since.Year(), since.Month(), since.Day(), 0, 0, 0, 0, time.UTC); !day.After(now); day = day.AddDate(0, 0, 1) {
		path := filepath.Join(dir, "codex-"+day.Format("2006-01-02")+".jsonl")
		f, openErr := os.Open(path)
		if os.IsNotExist(openErr) {
			continue
		}
		if openErr != nil {
			return out, openErr
		}
		decoder := json.NewDecoder(bufio.NewReader(f))
		for {
			var ev Event
			e := decoder.Decode(&ev)
			if errors.Is(e, io.EOF) {
				break
			}
			if e != nil {
				_ = f.Close()
				return out, fmt.Errorf("decode codex usage ledger: %w", e)
			}
			if ev.Time.Before(since) || ev.Time.After(now) {
				continue
			}
			if ev.Kind == "quota" {
				prior, ok := snapshots[ev.AuthIndex]
				if !ok || ev.Time.After(prior.ObservedAt) {
					snapshots[ev.AuthIndex] = QuotaSnapshot{AuthIndex: ev.AuthIndex, ObservedAt: ev.Time, Windows: ev.Windows}
				}
				continue
			}
			if ev.Kind != "request" {
				continue
			}
			id := ev.AuthIndex + "\x00" + ev.ClientKeyScope
			item := sums[id]
			if item == nil {
				item = &Attribution{AuthIndex: ev.AuthIndex, ClientKeyScope: ev.ClientKeyScope}
				sums[id] = item
			}
			item.Attempts++
			if ev.Failed {
				item.Failed++
			}
			item.InputTokens += ev.InputTokens
			item.OutputTokens += ev.OutputTokens
			item.TotalTokens += ev.TotalTokens
		}
		if err = f.Close(); err != nil {
			return out, err
		}
	}
	for _, s := range sums {
		out.Attributions = append(out.Attributions, *s)
	}
	for _, s := range snapshots {
		out.Quotas = append(out.Quotas, s)
	}
	sort.Slice(out.Attributions, func(i, j int) bool {
		a, b := out.Attributions[i], out.Attributions[j]
		if a.AuthIndex != b.AuthIndex {
			return a.AuthIndex < b.AuthIndex
		}
		return a.ClientKeyScope < b.ClientKeyScope
	})
	sort.Slice(out.Quotas, func(i, j int) bool { return out.Quotas[i].AuthIndex < out.Quotas[j].AuthIndex })
	return out, nil
}
