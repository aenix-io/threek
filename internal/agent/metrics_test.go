package agent

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/aenix-io/threek/internal/config"
)

func TestMetricsAfterTransfer(t *testing.T) {
	h := newHarness(fastConfig(config.ModeEnforce))
	h.ticks(2)
	m := h.agent.metrics
	if got := testutil.ToFloat64(m.isLeader); got != 1 {
		t.Errorf("is_leader = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.linkUp.WithLabelValues("1", "2")); got != 1 {
		t.Errorf("link_up{1,2} = %v, want 1", got)
	}
	h.fake.setCut(1, 2)
	h.ticks(5)
	if got := testutil.ToFloat64(m.linkUp.WithLabelValues("1", "2")); got != 0 {
		t.Errorf("link_up{1,2} = %v, want 0", got)
	}
	if got := testutil.ToFloat64(m.transfers.WithLabelValues("partition", "success")); got != 1 {
		t.Errorf("transfers{partition,success} = %v, want 1", got)
	}
	if got := testutil.ToFloat64(m.isLeader); got != 0 {
		t.Errorf("is_leader = %v after the transfer, want 0", got)
	}
	if got := testutil.ToFloat64(m.records); got != 3 {
		t.Errorf("member_records = %v, want 3", got)
	}
}

func TestMetricsFailedTransfer(t *testing.T) {
	cfg := fastConfig(config.ModeEnforce)
	cfg.MoveCooldown = 30 * time.Second
	h := newHarness(cfg)
	h.fake.moveErr = errors.New("transfer timed out")
	h.ticks(40)
	h.fake.setCut(1, 2)
	h.ticks(10)
	if got := testutil.ToFloat64(h.agent.metrics.transfers.WithLabelValues("partition", "failure")); got != 1 {
		t.Errorf("transfers{partition,failure} = %v, want 1", got)
	}
}

func TestMetricsDryRun(t *testing.T) {
	h := newHarness(fastConfig(config.ModeDryRun))
	h.ticks(2)
	h.fake.setCut(1, 2)
	h.ticks(20)
	if got := testutil.ToFloat64(h.agent.metrics.dryRunTransfers.WithLabelValues("partition")); got != 1 {
		t.Errorf("dry_run_transfers{partition} = %v, want 1", got)
	}
}

func TestMetricsPollFailures(t *testing.T) {
	h := newHarness(fastConfig(config.ModeEnforce))
	h.fake.localErr = errors.New("connection refused")
	h.ticks(3)
	if got := testutil.ToFloat64(h.agent.metrics.pollFailures); got != 3 {
		t.Errorf("poll_failures_total = %v, want 3", got)
	}
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func TestMetricsEndpoint(t *testing.T) {
	cfg := fastConfig(config.ModeDryRun)
	cfg.PollInterval = 10 * time.Millisecond
	cfg.MetricsListen = freeAddr(t)
	h := newHarness(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.agent.Run(ctx) }()
	defer func() {
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run() error = %v", err)
		}
	}()

	deadline := time.Now().Add(5 * time.Second)
	for {
		body, err := get("http://" + cfg.MetricsListen + "/metrics")
		if err == nil && strings.Contains(body, "threek_is_leader 1") {
			if !strings.Contains(body, "go_goroutines") {
				t.Error("metrics lack the Go runtime collector")
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("metrics endpoint: %v\n%s", err, body)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func get(url string) (string, error) {
	resp, err := http.Get(url) //nolint:noctx // test helper with a deadline loop around it
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	return string(body), err
}

func TestMetricsListenError(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	cfg := fastConfig(config.ModeDryRun)
	cfg.MetricsListen = l.Addr().String()
	h := newHarness(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := h.agent.Run(ctx); err == nil {
		t.Error("Run() error = nil with the metrics address in use")
	}
}
