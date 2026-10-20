// Package agent watches the local etcd member and transfers leadership.
package agent

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/aenix-io/threek/internal/cluster"
	"github.com/aenix-io/threek/internal/config"
	"github.com/aenix-io/threek/internal/engine"
	"github.com/aenix-io/threek/internal/etcdconf"
	"github.com/aenix-io/threek/internal/peermetrics"
)

// Version is the agent version published in member records, set at build time.
var Version = "dev"

const (
	recordTTL       = 30 * time.Second
	recordRetry     = 5 * time.Second
	transferTimeout = 30 * time.Second
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
	Metrics(ctx context.Context, url string, id uint64) (peermetrics.Peers, error)
	Records(ctx context.Context) (map[uint64]cluster.Record, error)
	MoveLeader(ctx context.Context, target uint64) error
	KeepRecord(ctx context.Context, id uint64, r cluster.Record, ttl time.Duration) error
}

// Run connects to the local member and runs the agent until ctx is done.
func Run(ctx context.Context, cfg config.Config, settings etcdconf.Settings, logger *slog.Logger) error {
	c, err := cluster.New(settings)
	if err != nil {
		return err
	}
	defer c.Close()
	return New(cfg, c, logger, time.Now).Run(ctx)
}

type Agent struct {
	cfg    config.Config
	c      Cluster
	logger *slog.Logger
	now    func() time.Time
	engine *engine.Engine

	self    uint64
	prev    map[uint64]peermetrics.Peers
	lastLog string
}

func New(cfg config.Config, c Cluster, logger *slog.Logger, now func() time.Time) *Agent {
	return &Agent{
		cfg:    cfg,
		c:      c,
		logger: logger,
		now:    now,
		engine: engine.New(engine.Params{
			DownAfter:       cfg.DownAfter,
			UpAfter:         cfg.UpAfter,
			MoveAfter:       cfg.MoveAfter,
			MoveCooldown:    cfg.MoveCooldown,
			PreferAfter:     cfg.PreferAfter,
			Identity:        cfg.Identity,
			PreferredLeader: cfg.PreferredLeader,
		}),
		prev: map[uint64]peermetrics.Peers{},
	}
}

// Run polls every PollInterval until ctx is done.
func (a *Agent) Run(ctx context.Context) error {
	a.logger.Info("agent started", "mode", a.cfg.Mode, "version", Version)
	ticker := time.NewTicker(a.cfg.PollInterval)
	defer ticker.Stop()
	var records sync.WaitGroup
	defer records.Wait()
	recordStarted := false
	for {
		a.Tick(ctx)
		if !recordStarted && a.self != 0 && a.cfg.Identity != "" {
			recordStarted = true
			records.Go(func() { a.keepRecord(ctx, a.self) })
		}
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
		}
	}
}

// Tick polls the cluster once and acts on the decision.
func (a *Agent) Tick(ctx context.Context) {
	pollCtx, cancel := context.WithTimeout(ctx, max(a.cfg.PollInterval, time.Second))
	obs, err := a.observe(pollCtx)
	cancel()
	if err != nil {
		a.logChange(slog.LevelWarn, "poll failed", err.Error(), "error", err)
		return
	}
	a.act(ctx, a.engine.Observe(obs))
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
	records := map[uint64]cluster.Record{}
	if a.cfg.PreferredLeader != "" {
		if records, err = a.c.Records(ctx); err != nil {
			return engine.Observation{}, err
		}
	}

	obs := engine.Observation{
		Now:    a.now(),
		Self:   engine.MemberID(local.MemberID),
		Leader: engine.MemberID(local.Leader),
		Term:   local.Term,
		Active: map[engine.MemberID]map[engine.MemberID]bool{},
	}
	var voters []cluster.Member
	for _, m := range members {
		if !m.IsLearner {
			voters = append(voters, m)
			obs.Members = append(obs.Members, engine.Member{ID: engine.MemberID(m.ID), Identity: records[m.ID].Identity})
		}
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	leading := local.Leader == local.MemberID
	for i, m := range voters {
		if m.ID != local.MemberID && !leading {
			continue
		}
		wg.Go(func() {
			peers, healthy, ok := a.inspect(ctx, m, local)
			mu.Lock()
			defer mu.Unlock()
			obs.Members[i].Healthy = healthy
			if ok {
				obs.Active[engine.MemberID(m.ID)] = a.row(m.ID, peers)
			}
		})
	}
	wg.Wait()
	return obs, nil
}

// inspect reads member m's view of its peers and whether it is a healthy
// transfer target. ok is false when the view could not be read.
func (a *Agent) inspect(ctx context.Context, m cluster.Member, local cluster.Status) (peers peermetrics.Peers, healthy, ok bool) {
	if m.ID == local.MemberID {
		peers, err := a.c.Metrics(ctx, a.c.LocalURL(), m.ID)
		if err != nil {
			a.logger.Debug("read own metrics", "error", err)
			return peermetrics.Peers{}, true, false
		}
		return peers, true, true
	}
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
		peers, err := a.c.Metrics(ctx, url, m.ID)
		if err != nil {
			a.logger.Debug("member metrics", "member", hex(m.ID), "url", url, "error", err)
			continue
		}
		lag := local.AppliedIndex - min(local.AppliedIndex, st.AppliedIndex)
		return peers, len(st.Errors) == 0 && lag <= maxAppliedLag, true
	}
	return peermetrics.Peers{}, false, false
}

func (a *Agent) row(id uint64, peers peermetrics.Peers) map[engine.MemberID]bool {
	active := peers.ActiveSince(a.prev[id])
	a.prev[id] = peers
	row := make(map[engine.MemberID]bool, len(active))
	for peer, up := range active {
		row[engine.MemberID(peer)] = up
	}
	return row
}

func (a *Agent) act(ctx context.Context, d engine.Decision) {
	attrs := []any{"note", d.Note, "view", a.engine.View()}
	if d.Wait > 0 {
		attrs = append(attrs, "wait", d.Wait.String())
	}
	if !d.Transfer {
		a.logChange(slog.LevelInfo, "decision", d.Note, attrs...)
		return
	}
	attrs = append(attrs, "target", hex(uint64(d.Target)), "reason", d.Reason)
	if a.cfg.Mode != config.ModeEnforce {
		a.logChange(slog.LevelInfo, "would transfer leadership", d.Note, attrs...)
		return
	}
	a.lastLog = ""
	a.logger.Info("transferring leadership", attrs...)
	moveCtx, cancel := context.WithTimeout(ctx, transferTimeout)
	defer cancel()
	if err := a.c.MoveLeader(moveCtx, uint64(d.Target)); err != nil {
		a.engine.TransferFailed(a.now())
		a.logger.Error("leadership transfer failed", append(attrs, "error", err)...)
		return
	}
	a.logger.Info("leadership transferred", attrs...)
}

// logChange logs only when key differs from the previous call's, so a
// situation that lasts is logged once.
func (a *Agent) logChange(level slog.Level, msg, key string, attrs ...any) {
	key = msg + "\x00" + key
	if key == a.lastLog {
		return
	}
	a.lastLog = key
	a.logger.Log(context.Background(), level, msg, attrs...)
}

func (a *Agent) keepRecord(ctx context.Context, self uint64) {
	r := cluster.Record{SchemaVersion: cluster.RecordSchemaVersion, Identity: a.cfg.Identity, AgentVersion: Version}
	for {
		err := a.c.KeepRecord(ctx, self, r, recordTTL)
		if errors.Is(err, context.Canceled) || ctx.Err() != nil {
			return
		}
		a.logger.Warn("member record", "error", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(recordRetry):
		}
	}
}

func hex(id uint64) string {
	return fmt.Sprintf("%x", id)
}
