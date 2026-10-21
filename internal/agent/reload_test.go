package agent

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/aenix-io/threek/internal/config"
)

// configSource stands in for a mounted ConfigMap that changes under the agent.
type configSource struct {
	mu  sync.Mutex
	cfg config.Config
	err error
}

func (s *configSource) set(cfg config.Config, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cfg, s.err = cfg, err
}

func (s *configSource) load() (config.Config, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.cfg, s.err
}

func watched(h *harness, cfg config.Config) *configSource {
	src := &configSource{cfg: cfg}
	h.agent.WatchConfig(src.load)
	return src
}

func TestReloadedModeAppliesWithoutRestart(t *testing.T) {
	h := newHarness(fastConfig(config.ModeDryRun))
	src := watched(h, fastConfig(config.ModeDryRun))
	h.fake.setCut(1, 2)
	h.ticks(5)
	if moves := h.fake.movesSoFar(); len(moves) != 0 {
		t.Fatalf("dry-run moved the leader: %v", moves)
	}
	src.set(fastConfig(config.ModeEnforce), nil)
	h.ticks(1)
	if moves := h.fake.movesSoFar(); len(moves) != 1 || moves[0] != 3 {
		t.Errorf("moves = %v, want one move to member 3 right after the switch", moves)
	}
	if !strings.Contains(h.logs.String(), "configuration changed") {
		t.Errorf("the switch is not logged:\n%s", h.logs)
	}
}

func TestInvalidConfigurationKeepsThePreviousOne(t *testing.T) {
	h := newHarness(fastConfig(config.ModeEnforce))
	src := watched(h, fastConfig(config.ModeEnforce))
	h.ticks(1)
	src.set(config.Config{}, errors.New("mode: must be one of off, dry-run, enforce"))
	h.fake.setCut(1, 2)
	h.ticks(5)
	if moves := h.fake.movesSoFar(); len(moves) != 1 {
		t.Errorf("moves = %v, want the partition handled with the previous configuration", moves)
	}
	if n := strings.Count(h.logs.String(), "invalid configuration"); n != 1 {
		t.Errorf("invalid configuration logged %d times, want once:\n%s", n, h.logs)
	}
	if got := testutil.ToFloat64(h.agent.metrics.configValid); got != 0 {
		t.Errorf("threek_config_valid = %v with an invalid configuration, want 0", got)
	}
	src.set(fastConfig(config.ModeEnforce), nil)
	h.ticks(1)
	if got := testutil.ToFloat64(h.agent.metrics.configValid); got != 1 {
		t.Errorf("threek_config_valid = %v after a fix, want 1", got)
	}
}

func TestStartLogShowsTheLoadedMode(t *testing.T) {
	h := newHarness(fastConfig(config.ModeOff))
	watched(h, fastConfig(config.ModeEnforce))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	if err := h.agent.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	for line := range strings.Lines(h.logs.String()) {
		if strings.Contains(line, `"msg":"agent started"`) {
			if !strings.Contains(line, `"mode":"enforce"`) {
				t.Errorf("start log = %s, want the mode from the configuration", line)
			}
			return
		}
	}
	t.Errorf("no start log:\n%s", h.logs)
}

func TestOffWithdrawsTheRecordAndStopsPolling(t *testing.T) {
	h := newHarness(fastConfig(config.ModeDryRun))
	src := watched(h, fastConfig(config.ModeDryRun))
	h.ticks(2)
	if _, ok := h.fake.records[1]; !ok {
		t.Fatal("no record published in dry-run")
	}
	src.set(fastConfig(config.ModeOff), nil)
	h.ticks(1)
	if _, ok := h.fake.records[1]; ok {
		t.Error("record is still present after switching off")
	}
	calls := len(h.fake.statusCalls)
	h.ticks(3)
	if len(h.fake.statusCalls) != calls {
		t.Errorf("status called %d times while off", len(h.fake.statusCalls)-calls)
	}

	src.set(fastConfig(config.ModeDryRun), nil)
	h.ticks(1)
	if _, ok := h.fake.records[1]; !ok {
		t.Error("no record published after switching back on")
	}
}

// A link seen down before the agent was switched off must not count toward
// MoveAfter after it is switched back on.
func TestOffForgetsLinkHistory(t *testing.T) {
	cfg := fastConfig(config.ModeEnforce)
	cfg.DownAfter = 3
	cfg.MoveAfter = 10 * time.Second
	h := newHarness(cfg)
	src := watched(h, cfg)
	h.ticks(2)
	h.fake.setCut(1, 2)
	h.ticks(4)
	if m := h.fake.movesSoFar(); len(m) != 0 {
		t.Fatalf("moved too early: %v", m)
	}
	off := cfg
	off.Mode = config.ModeOff
	src.set(off, nil)
	h.ticks(1)
	h.now = h.now.Add(time.Hour)
	src.set(cfg, nil)
	h.ticks(1)
	if m := h.fake.movesSoFar(); len(m) != 0 {
		t.Errorf("moved on the first poll after switching back on, from stale link history: %v", m)
	}
}

func TestOffClearsTheView(t *testing.T) {
	h := newHarness(fastConfig(config.ModeDryRun))
	src := watched(h, fastConfig(config.ModeDryRun))
	h.ticks(3)
	if testutil.CollectAndCount(h.agent.metrics.linkUp) == 0 {
		t.Fatal("no link metrics while on")
	}
	src.set(fastConfig(config.ModeOff), nil)
	h.ticks(1)
	if n := testutil.CollectAndCount(h.agent.metrics.linkUp); n != 0 {
		t.Errorf("%d link metrics left while off", n)
	}
	if got := testutil.ToFloat64(h.agent.metrics.records); got != 0 {
		t.Errorf("member records = %v while off, want 0", got)
	}
	for mode, want := range map[string]float64{"off": 1, "dry-run": 0, "enforce": 0} {
		if got := testutil.ToFloat64(h.agent.metrics.mode.WithLabelValues(mode)); got != want {
			t.Errorf("threek_mode{mode=%q} = %v, want %v", mode, got, want)
		}
	}
}

func TestStartsOffWithoutTouchingTheCluster(t *testing.T) {
	h := newHarness(fastConfig(config.ModeOff))
	watched(h, fastConfig(config.ModeOff))
	h.ticks(3)
	if len(h.fake.statusCalls) != 0 {
		t.Errorf("an agent that is off called status %d times", len(h.fake.statusCalls))
	}
	if _, ok := h.fake.records[1]; ok {
		t.Error("an agent that is off published a record")
	}
}

func TestReloadedPollInterval(t *testing.T) {
	slow := fastConfig(config.ModeDryRun)
	slow.PollInterval = time.Hour
	h := newHarness(slow)
	fast := slow
	fast.PollInterval = 10 * time.Millisecond
	watched(h, fast)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if err := h.agent.Run(ctx); err != nil {
		t.Fatalf("Run() error = %v", err)
	}
	if n := len(h.fake.statusCalls); n < 5 {
		t.Errorf("status called %d times in 300ms, want the reloaded 10ms interval", n)
	}
}

// Without a peer CA every probe of an https peer fails, which would read as a
// member that reaches nobody.
func TestPeerTLSWithoutCAFailsThePoll(t *testing.T) {
	h := newHarness(fastConfig(config.ModeEnforce))
	h.fake.noPeerCA = true
	h.ticks(3)
	if _, ok := h.fake.records[1]; ok {
		t.Error("a record was published from probes that cannot succeed")
	}
	if got := testutil.ToFloat64(h.agent.metrics.pollFailures); got != 3 {
		t.Errorf("poll failures = %v, want 3", got)
	}
	if n := strings.Count(h.logs.String(), "--peer-trusted-ca-file"); n != 1 {
		t.Errorf("the missing peer CA is logged %d times, want once:\n%s", n, h.logs)
	}
}
