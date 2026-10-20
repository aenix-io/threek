package agent

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/aenix-io/threek/internal/config"
)

func TestWatchdogExitsStalledAgent(t *testing.T) {
	cfg := fastConfig(config.ModeDryRun)
	cfg.PollInterval = 10 * time.Millisecond
	h := newHarness(cfg)
	h.agent.stallTimeout = 100 * time.Millisecond
	exited := make(chan int, 1)
	h.agent.exit = func(code int) { exited <- code }
	h.fake.hang = make(chan struct{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.agent.Run(ctx) }()
	defer func() {
		close(h.fake.hang)
		cancel()
		if err := <-done; err != nil {
			t.Errorf("Run() error = %v", err)
		}
	}()

	select {
	case code := <-exited:
		if code != 1 {
			t.Errorf("exit code = %d, want 1", code)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the watchdog did not end a stalled agent")
	}
	if !strings.Contains(h.logs.String(), "agent stalled") {
		t.Errorf("logs do not explain the exit:\n%s", h.logs)
	}
}

func TestWatchdogQuietWhileTheLoopRuns(t *testing.T) {
	cfg := fastConfig(config.ModeDryRun)
	cfg.PollInterval = 10 * time.Millisecond
	h := newHarness(cfg)
	h.agent.stallTimeout = 200 * time.Millisecond
	h.agent.exit = func(code int) { t.Errorf("exit(%d) while the loop runs", code) }
	// Failing polls still complete, so the loop is not stalled.
	h.fake.localErr = errors.New("connection refused")

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := h.agent.Run(ctx); err != nil {
		t.Errorf("Run() error = %v", err)
	}
}

func TestLastPollTimestamp(t *testing.T) {
	h := newHarness(fastConfig(config.ModeDryRun))
	h.ticks(1)
	succeeded := h.now.Add(-time.Second)
	if got := testutil.ToFloat64(h.agent.metrics.lastPoll); got != float64(succeeded.Unix()) {
		t.Errorf("last_poll_timestamp_seconds = %v, want %d", got, succeeded.Unix())
	}
	h.fake.localErr = errors.New("connection refused")
	h.ticks(3)
	if got := testutil.ToFloat64(h.agent.metrics.lastPoll); got != float64(succeeded.Unix()) {
		t.Errorf("last_poll_timestamp_seconds = %v after failed polls, want %d", got, succeeded.Unix())
	}
}
