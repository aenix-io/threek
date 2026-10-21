// Package agent watches the local etcd member and transfers leadership.
package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aenix-io/threek/internal/cluster"
	"github.com/aenix-io/threek/internal/config"
	"github.com/aenix-io/threek/internal/engine"
	"github.com/aenix-io/threek/internal/etcdconf"
	"github.com/aenix-io/threek/internal/probe"
)

// Version is the agent version published in member records, set at build time.
var Version = "dev"

const (
	// remoteTimeout bounds every call to another member, so an unreachable
	// one cannot use up the time the poll needs for local calls.
	remoteTimeout = time.Second
	pollTimeout   = 3 * time.Second
	// recordTTL bounds how long a stalled agent's view stays visible: the
	// lease is renewed by the poll loop itself.
	recordTTL       = 10 * time.Second
	transferTimeout = 30 * time.Second
	// stallTimeout is how long the poll loop may go without completing a poll
	// before the agent exits for the supervisor to restart it. Every call in
	// a poll has a timeout, so only a bug can get near it.
	stallTimeout  = 30 * time.Second
	revokeTimeout = 2 * time.Second
	// maxAppliedLag is how far behind the leader a member may be and still be
	// a transfer target. It is far above the lag of a member keeping up over
	// a WAN link, and far below that of a member stalled by defragmentation.
	maxAppliedLag = 1000
)

// Cluster is the part of cluster.Client the agent uses.
type Cluster interface {
	LocalURL() string
	Status(ctx context.Context, url string) (cluster.Status, error)
	Members(ctx context.Context) ([]cluster.Member, error)
	Records(ctx context.Context) (map[uint64]cluster.Record, error)
	MoveLeader(ctx context.Context, target uint64) error
	GrantLease(ctx context.Context, ttl time.Duration) (int64, error)
	PutRecord(ctx context.Context, id uint64, r cluster.Record, lease int64) error
	RenewLease(ctx context.Context, lease int64) error
	RevokeLease(ctx context.Context, lease int64) error
}

type Prober interface {
	Reachable(ctx context.Context, peerURLs []string) bool
}

// Run connects to the local member and runs the agent until ctx is done.
// With a non-nil load, the configuration is reloaded before every poll.
func Run(ctx context.Context, cfg config.Config, load func() (config.Config, error), settings etcdconf.Settings, logger *slog.Logger) error {
	c, err := cluster.New(settings)
	if err != nil {
		return err
	}
	defer c.Close()
	p, err := probe.New(settings, remoteTimeout)
	if err != nil {
		return err
	}
	defer p.Close()
	a := New(cfg, c, p, logger, time.Now)
	if load != nil {
		a.WatchConfig(load)
	}
	return a.Run(ctx)
}

type Agent struct {
	cfg     config.Config
	load    func() (config.Config, error)
	c       Cluster
	prober  Prober
	logger  *slog.Logger
	now     func() time.Time
	engine  *engine.Engine
	metrics *metrics

	remoteTimeout time.Duration
	pollTimeout   time.Duration
	stallTimeout  time.Duration
	exit          func(code int)
	started       time.Time
	lastLoop      atomic.Int64

	self      uint64
	lease     int64
	published *cluster.Record
	lastLog   map[string]string
}

func New(cfg config.Config, c Cluster, p Prober, logger *slog.Logger, now func() time.Time) *Agent {
	a := &Agent{
		cfg:           cfg,
		c:             c,
		prober:        p,
		logger:        logger,
		now:           now,
		engine:        engine.New(engineParams(cfg)),
		metrics:       newMetrics(),
		remoteTimeout: remoteTimeout,
		pollTimeout:   pollTimeout,
		stallTimeout:  stallTimeout,
		exit:          os.Exit,
		started:       time.Now(),
		lastLog:       map[string]string{},
	}
	a.setMode()
	return a
}

// Run polls every PollInterval until ctx is done, then withdraws the
// member's record.
func (a *Agent) Run(ctx context.Context) error {
	if a.cfg.MetricsListen != "" {
		stop, err := a.metrics.serve(ctx, a.cfg.MetricsListen)
		if err != nil {
			return err
		}
		defer stop()
	}
	a.logger.Info("agent started", "mode", a.cfg.Mode, "version", Version)
	a.markLoop()
	watchdogCtx, stopWatchdog := context.WithCancel(ctx)
	var watchdog sync.WaitGroup
	watchdog.Go(func() { a.watchdog(watchdogCtx) })
	defer watchdog.Wait()
	defer stopWatchdog()

	interval := a.cfg.PollInterval
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		a.Tick(ctx)
		a.markLoop()
		if a.cfg.PollInterval != interval {
			interval = a.cfg.PollInterval
			ticker.Reset(interval)
		}
		select {
		case <-ctx.Done():
			a.withdraw(ctx)
			return nil
		case <-ticker.C:
		}
	}
}

func engineParams(cfg config.Config) engine.Params {
	return engine.Params{
		DownAfter:       cfg.DownAfter,
		UpAfter:         cfg.UpAfter,
		MoveAfter:       cfg.MoveAfter,
		MoveCooldown:    cfg.MoveCooldown,
		PreferAfter:     cfg.PreferAfter,
		Identity:        cfg.Identity,
		PreferredLeader: cfg.PreferredLeader,
	}
}

// WatchConfig makes the agent read its configuration from load before every
// poll. An invalid configuration is logged and the previous one stays.
func (a *Agent) WatchConfig(load func() (config.Config, error)) {
	a.load = load
}

func (a *Agent) reload() {
	if a.load == nil {
		return
	}
	cfg, err := a.load()
	if err != nil {
		a.logChange("config", slog.LevelError, "invalid configuration, keeping the previous one", err.Error(), "error", err)
		return
	}
	delete(a.lastLog, "config")
	if cfg == a.cfg {
		return
	}
	a.logger.Info("configuration changed", "mode", cfg.Mode, "preferredLeader", cfg.PreferredLeader)
	// Link history gathered before the agent was off says nothing about the
	// links now, so switching on starts over.
	if (a.cfg.Mode == config.ModeOff) != (cfg.Mode == config.ModeOff) {
		a.engine = engine.New(engineParams(cfg))
	} else {
		a.engine.SetParams(engineParams(cfg))
	}
	a.cfg = cfg
	a.setMode()
}

func (a *Agent) setMode() {
	for _, m := range []config.Mode{config.ModeOff, config.ModeDryRun, config.ModeEnforce} {
		v := 0.0
		if m == a.cfg.Mode {
			v = 1
		}
		a.metrics.mode.WithLabelValues(string(m)).Set(v)
	}
}

func (a *Agent) markLoop() {
	a.lastLoop.Store(int64(time.Since(a.started)))
}

// watchdog ends the process when the poll loop stops making progress. A
// stalled agent never touches etcd, but it silently stops protecting it; the
// supervisor restarts an agent that exits.
func (a *Agent) watchdog(ctx context.Context) {
	ticker := time.NewTicker(a.stallTimeout / 4)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			stalled := time.Since(a.started) - time.Duration(a.lastLoop.Load())
			if stalled > a.stallTimeout {
				a.logger.Error("agent stalled, exiting", "stalled", stalled.String())
				a.exit(1)
				return
			}
		}
	}
}

// Tick polls the cluster once, publishes the local view and acts on the
// decision.
func (a *Agent) Tick(ctx context.Context) {
	a.reload()
	if a.cfg.Mode == config.ModeOff {
		a.withdraw(ctx)
		a.metrics.linkUp.Reset()
		a.metrics.records.Set(0)
		a.logChange("decision", slog.LevelInfo, "threek is off", "")
		return
	}
	pollCtx, cancel := context.WithTimeout(ctx, a.pollTimeout)
	obs, err := a.observe(pollCtx)
	cancel()
	if err != nil {
		a.metrics.pollFailures.Inc()
		a.logChange("poll", slog.LevelWarn, "poll failed", err.Error(), "error", err)
		return
	}
	delete(a.lastLog, "poll")
	a.metrics.lastPoll.Set(float64(obs.Now.Unix()))
	d := a.engine.Observe(obs)
	a.metrics.linkUp.Reset()
	for _, l := range a.engine.Links() {
		up := 0.0
		if l.State == engine.LinkUp {
			up = 1
		}
		a.metrics.linkUp.WithLabelValues(hex(uint64(l.From)), hex(uint64(l.To))).Set(up)
	}
	a.act(ctx, d)
}

func (a *Agent) observe(ctx context.Context) (engine.Observation, error) {
	local, err := a.c.Status(ctx, a.c.LocalURL())
	if err != nil {
		return engine.Observation{}, err
	}
	a.self = local.MemberID
	members, err := a.c.Members(ctx)
	if err != nil {
		return engine.Observation{}, err
	}
	var voters []cluster.Member
	for _, m := range members {
		if !m.IsLearner {
			voters = append(voters, m)
		}
	}

	leading := local.Leader == local.MemberID
	if leading {
		a.metrics.isLeader.Set(1)
	} else {
		a.metrics.isLeader.Set(0)
	}
	sees := map[uint64]bool{}
	healthy := map[uint64]bool{local.MemberID: true}
	remoteCtx, cancel := context.WithTimeout(ctx, a.remoteTimeout)
	defer cancel()
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, m := range voters {
		if m.ID == local.MemberID {
			continue
		}
		wg.Go(func() {
			reachable := a.prober.Reachable(remoteCtx, m.PeerURLs)
			mu.Lock()
			defer mu.Unlock()
			sees[m.ID] = reachable
		})
		if leading {
			wg.Go(func() {
				ok := a.healthy(remoteCtx, m, local)
				mu.Lock()
				defer mu.Unlock()
				healthy[m.ID] = ok
			})
		}
	}
	wg.Wait()

	a.publish(ctx, sees)
	records, err := a.c.Records(ctx)
	if err != nil {
		return engine.Observation{}, err
	}
	a.metrics.records.Set(float64(len(records)))

	obs := engine.Observation{
		Now:    a.now(),
		Self:   engine.MemberID(local.MemberID),
		Leader: engine.MemberID(local.Leader),
		Term:   local.Term,
		Active: map[engine.MemberID]map[engine.MemberID]bool{engine.MemberID(local.MemberID): row(sees)},
	}
	for _, m := range voters {
		r := records[m.ID]
		obs.Members = append(obs.Members, engine.Member{ID: engine.MemberID(m.ID), Identity: r.Identity, Healthy: healthy[m.ID]})
		if _, ok := records[m.ID]; ok && m.ID != local.MemberID {
			obs.Active[engine.MemberID(m.ID)] = recordRow(r)
		}
	}
	return obs, nil
}

// healthy reports whether member m belongs to the local cluster and can take
// over leadership.
func (a *Agent) healthy(ctx context.Context, m cluster.Member, local cluster.Status) bool {
	for _, url := range m.ClientURLs {
		st, err := a.c.Status(ctx, url)
		if err != nil {
			a.logger.Debug("member status", "member", hex(m.ID), "url", url, "error", err)
			continue
		}
		if st.ClusterID != local.ClusterID || st.MemberID != m.ID {
			a.logger.Debug("URL belongs to another member", "member", hex(m.ID), "url", url,
				"cluster", hex(st.ClusterID), "answered", hex(st.MemberID))
			continue
		}
		lag := local.AppliedIndex - min(local.AppliedIndex, st.AppliedIndex)
		return len(st.Errors) == 0 && lag <= maxAppliedLag
	}
	return false
}

// publish keeps the member's record current. The lease is renewed on every
// poll, so the record expires when the agent stops polling; the record is
// rewritten only when the view changes.
func (a *Agent) publish(ctx context.Context, sees map[uint64]bool) {
	r := cluster.Record{
		SchemaVersion: cluster.RecordSchemaVersion,
		Identity:      a.cfg.Identity,
		AgentVersion:  Version,
		Sees:          map[string]bool{},
	}
	for id, ok := range sees {
		r.Sees[hex(id)] = ok
	}

	if a.lease != 0 {
		err := a.c.RenewLease(ctx, a.lease)
		switch {
		case errors.Is(err, cluster.ErrLeaseExpired):
			a.lease, a.published = 0, nil
		case err != nil:
			a.logChange("publish", slog.LevelWarn, "renew member record", err.Error(), "error", err)
			return
		}
	}
	if a.lease == 0 {
		lease, err := a.c.GrantLease(ctx, recordTTL)
		if err != nil {
			a.logChange("publish", slog.LevelWarn, "publish member record", err.Error(), "error", err)
			return
		}
		a.lease = lease
	}
	if a.published != nil && maps.Equal(a.published.Sees, r.Sees) {
		return
	}
	if err := a.c.PutRecord(ctx, a.self, r, a.lease); err != nil {
		a.logChange("publish", slog.LevelWarn, "publish member record", err.Error(), "error", err)
		return
	}
	a.published = &r
	a.logChange("publish", slog.LevelInfo, "member record published", fmt.Sprint(r.Sees), "sees", r.Sees)
}

func (a *Agent) withdraw(ctx context.Context) {
	if a.lease == 0 {
		return
	}
	revokeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), revokeTimeout)
	defer cancel()
	if err := a.c.RevokeLease(revokeCtx, a.lease); err != nil {
		a.logger.Warn("withdraw member record", "error", err)
	}
	a.lease, a.published = 0, nil
}

func row(sees map[uint64]bool) map[engine.MemberID]bool {
	r := make(map[engine.MemberID]bool, len(sees))
	for id, ok := range sees {
		r[engine.MemberID(id)] = ok
	}
	return r
}

func recordRow(r cluster.Record) map[engine.MemberID]bool {
	out := make(map[engine.MemberID]bool, len(r.Sees))
	for key, ok := range r.Sees {
		if id, err := strconv.ParseUint(key, 16, 64); err == nil {
			out[engine.MemberID(id)] = ok
		}
	}
	return out
}

func (a *Agent) act(ctx context.Context, d engine.Decision) {
	attrs := []any{"note", d.Note, "view", a.engine.View()}
	if d.Wait > 0 {
		attrs = append(attrs, "wait", d.Wait.String())
	}
	if !d.Transfer {
		a.logChange("decision", slog.LevelInfo, "decision", d.Note, attrs...)
		return
	}
	attrs = append(attrs, "target", hex(uint64(d.Target)), "reason", d.Reason)
	if a.cfg.Mode != config.ModeEnforce {
		if a.logChange("decision", slog.LevelInfo, "would transfer leadership", d.Note, attrs...) {
			a.metrics.dryRunTransfers.WithLabelValues(string(d.Reason)).Inc()
		}
		return
	}
	delete(a.lastLog, "decision")
	a.logger.Info("transferring leadership", attrs...)
	moveCtx, cancel := context.WithTimeout(ctx, transferTimeout)
	defer cancel()
	if err := a.c.MoveLeader(moveCtx, uint64(d.Target)); err != nil {
		a.engine.TransferFailed(a.now())
		a.metrics.transfers.WithLabelValues(string(d.Reason), "failure").Inc()
		a.logger.Error("leadership transfer failed", append(attrs, "error", err)...)
		return
	}
	a.metrics.transfers.WithLabelValues(string(d.Reason), "success").Inc()
	a.logger.Info("leadership transferred", attrs...)
}

// logChange logs msg only when key differs from the previous key logged in
// the same category, so a situation that lasts is logged once. It reports
// whether it logged.
func (a *Agent) logChange(category string, level slog.Level, msg, key string, attrs ...any) bool {
	key = msg + "\x00" + key
	if a.lastLog[category] == key {
		return false
	}
	a.lastLog[category] = key
	a.logger.Log(context.Background(), level, msg, attrs...)
	return true
}

func hex(id uint64) string {
	return fmt.Sprintf("%x", id)
}
