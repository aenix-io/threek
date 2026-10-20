package agent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aenix-io/threek/internal/cluster"
	"github.com/aenix-io/threek/internal/config"
	"github.com/aenix-io/threek/internal/peermetrics"
)

const clusterID = 0xc1

func url(id uint64) string {
	return fmt.Sprintf("https://m%x:2379", id)
}

// fakeCluster is a three-member cluster seen from member 1.
type fakeCluster struct {
	mu        sync.Mutex
	self      uint64
	leader    uint64
	cut       map[[2]uint64]bool
	clusterOf map[uint64]uint64
	errorsOf  map[uint64][]string
	appliedOf map[uint64]uint64
	learners  map[uint64]bool
	records   map[uint64]cluster.Record
	moveErr   error
	moves     []uint64
	fetched   []string
	kept      chan cluster.Record
}

func newFakeCluster() *fakeCluster {
	return &fakeCluster{
		self:      1,
		leader:    1,
		cut:       map[[2]uint64]bool{},
		clusterOf: map[uint64]uint64{},
		errorsOf:  map[uint64][]string{},
		appliedOf: map[uint64]uint64{},
		learners:  map[uint64]bool{},
		records:   map[uint64]cluster.Record{},
		kept:      make(chan cluster.Record, 10),
	}
}

func (f *fakeCluster) setCut(x, y uint64) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.cut[[2]uint64{x, y}] = true
	f.cut[[2]uint64{y, x}] = true
}

func (f *fakeCluster) ids() []uint64 {
	ids := []uint64{1, 2, 3}
	for id := range f.learners {
		ids = append(ids, id)
	}
	return ids
}

func (f *fakeCluster) idOf(u string) uint64 {
	for _, id := range f.ids() {
		if url(id) == u {
			return id
		}
	}
	panic("unknown URL " + u)
}

func (f *fakeCluster) reachable(id uint64) error {
	if id != f.self && f.cut[[2]uint64{f.self, id}] {
		return errors.New("unreachable")
	}
	return nil
}

func (f *fakeCluster) LocalURL() string { return url(f.self) }

func (f *fakeCluster) Status(_ context.Context, u string) (cluster.Status, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	id := f.idOf(u)
	if err := f.reachable(id); err != nil {
		return cluster.Status{}, err
	}
	cid, ok := f.clusterOf[id]
	if !ok {
		cid = clusterID
	}
	applied, ok := f.appliedOf[id]
	if !ok {
		applied = 5000
	}
	return cluster.Status{
		ClusterID: cid, MemberID: id, Leader: f.leader, Term: 1,
		AppliedIndex: applied, Errors: f.errorsOf[id],
	}, nil
}

func (f *fakeCluster) Members(context.Context) ([]cluster.Member, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	var members []cluster.Member
	for _, id := range f.ids() {
		members = append(members, cluster.Member{ID: id, ClientURLs: []string{url(id)}, IsLearner: f.learners[id]})
	}
	return members, nil
}

func (f *fakeCluster) Metrics(_ context.Context, u string, id uint64) (peermetrics.Peers, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fetched = append(f.fetched, u)
	if err := f.reachable(id); err != nil {
		return peermetrics.Peers{}, err
	}
	p := peermetrics.Peers{Active: map[uint64]bool{}, Disconnects: map[uint64]float64{}}
	for _, peer := range []uint64{1, 2, 3} {
		if peer != id {
			p.Active[peer] = !f.cut[[2]uint64{id, peer}]
		}
	}
	return p, nil
}

func (f *fakeCluster) Records(context.Context) (map[uint64]cluster.Record, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.records, nil
}

func (f *fakeCluster) MoveLeader(_ context.Context, target uint64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.moves = append(f.moves, target)
	if f.moveErr != nil {
		return f.moveErr
	}
	f.leader = target
	return nil
}

func (f *fakeCluster) KeepRecord(ctx context.Context, _ uint64, r cluster.Record, _ time.Duration) error {
	f.kept <- r
	<-ctx.Done()
	return ctx.Err()
}

func (f *fakeCluster) movesSoFar() []uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.moves)
}

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func fastConfig(mode config.Mode) config.Config {
	cfg := config.Default()
	cfg.Mode = mode
	cfg.DownAfter = 1
	cfg.UpAfter = 1
	cfg.MoveAfter = 2 * time.Second
	cfg.MoveCooldown = 0
	return cfg
}

type harness struct {
	agent *Agent
	fake  *fakeCluster
	logs  *syncBuffer
	now   time.Time
}

func newHarness(cfg config.Config) *harness {
	h := &harness{fake: newFakeCluster(), logs: &syncBuffer{}, now: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)}
	logger := slog.New(slog.NewJSONHandler(h.logs, nil))
	h.agent = New(cfg, h.fake, logger, func() time.Time { return h.now })
	return h
}

func (h *harness) ticks(n int) {
	for range n {
		h.agent.Tick(context.Background())
		h.now = h.now.Add(time.Second)
	}
}

func TestFollowerReadsOnlyItself(t *testing.T) {
	h := newHarness(fastConfig(config.ModeEnforce))
	h.fake.leader = 2
	h.ticks(3)
	for _, u := range h.fake.fetched {
		if u != url(1) {
			t.Errorf("follower fetched metrics of %s", u)
		}
	}
}

func TestLeaderReadsEveryVoter(t *testing.T) {
	h := newHarness(fastConfig(config.ModeEnforce))
	h.fake.learners[4] = true
	h.ticks(1)
	slices.Sort(h.fake.fetched)
	if want := []string{url(1), url(2), url(3)}; !slices.Equal(h.fake.fetched, want) {
		t.Errorf("fetched = %v, want %v", h.fake.fetched, want)
	}
}

func TestEnforceTransfersOnPartition(t *testing.T) {
	h := newHarness(fastConfig(config.ModeEnforce))
	h.ticks(2)
	h.fake.setCut(1, 2)
	h.ticks(5)
	if moves := h.fake.movesSoFar(); !slices.Equal(moves, []uint64{3}) {
		t.Errorf("moves = %v, want [3]", moves)
	}
	if !strings.Contains(h.logs.String(), "leadership transferred") {
		t.Errorf("logs do not report the transfer:\n%s", h.logs)
	}
}

func TestFollowerPartitionDoesNothing(t *testing.T) {
	h := newHarness(fastConfig(config.ModeEnforce))
	h.ticks(2)
	h.fake.setCut(2, 3)
	h.ticks(20)
	if moves := h.fake.movesSoFar(); len(moves) != 0 {
		t.Errorf("moves = %v, want none: the leader reaches both followers", moves)
	}
}

func TestDryRunNeverTransfers(t *testing.T) {
	h := newHarness(fastConfig(config.ModeDryRun))
	h.ticks(2)
	h.fake.setCut(1, 2)
	h.ticks(20)
	if moves := h.fake.movesSoFar(); len(moves) != 0 {
		t.Errorf("moves = %v, want none", moves)
	}
	if n := strings.Count(h.logs.String(), "would transfer leadership"); n != 1 {
		t.Errorf("dry-run decision logged %d times, want once:\n%s", n, h.logs)
	}
}

func TestDecisionsAreLoggedOnChange(t *testing.T) {
	h := newHarness(fastConfig(config.ModeEnforce))
	h.ticks(30)
	if n := strings.Count(h.logs.String(), `"note":"full connectivity"`); n != 1 {
		t.Errorf("unchanged decision logged %d times, want once:\n%s", n, h.logs)
	}
}

func TestMemberOfAnotherClusterIsUnavailable(t *testing.T) {
	h := newHarness(fastConfig(config.ModeEnforce))
	h.fake.clusterOf[3] = 0xbad
	h.ticks(2)
	h.fake.setCut(1, 2)
	h.ticks(20)
	if moves := h.fake.movesSoFar(); len(moves) != 0 {
		t.Errorf("moves = %v, want none", moves)
	}
}

func TestUnhealthyCandidate(t *testing.T) {
	for name, setup := range map[string]func(*fakeCluster){
		"alarm":   func(f *fakeCluster) { f.errorsOf[3] = []string{"memberID:3 alarm:NOSPACE"} },
		"lagging": func(f *fakeCluster) { f.appliedOf[3] = 10 },
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(fastConfig(config.ModeEnforce))
			setup(h.fake)
			h.ticks(2)
			h.fake.setCut(1, 2)
			h.ticks(20)
			if moves := h.fake.movesSoFar(); len(moves) != 0 {
				t.Errorf("moves = %v, want none", moves)
			}
		})
	}
}

func TestFailedTransferBacksOff(t *testing.T) {
	cfg := fastConfig(config.ModeEnforce)
	cfg.MoveCooldown = 30 * time.Second
	h := newHarness(cfg)
	h.fake.moveErr = errors.New("transfer timed out")
	h.ticks(40)
	h.fake.setCut(1, 2)
	h.ticks(20)
	if moves := h.fake.movesSoFar(); len(moves) != 1 {
		t.Errorf("moves = %v, want a single attempt within the cooldown", moves)
	}
	if !strings.Contains(h.logs.String(), "leadership transfer failed") {
		t.Errorf("logs do not report the failure:\n%s", h.logs)
	}
}

func TestPreferredLeaderUsesRecords(t *testing.T) {
	cfg := fastConfig(config.ModeEnforce)
	cfg.Identity = "zone-a"
	cfg.PreferredLeader = "zone-c"
	cfg.PreferAfter = 5 * time.Second
	h := newHarness(cfg)
	h.fake.records[2] = cluster.Record{SchemaVersion: 1, Identity: "zone-b"}
	h.fake.records[3] = cluster.Record{SchemaVersion: 1, Identity: "zone-c"}
	h.ticks(10)
	if moves := h.fake.movesSoFar(); !slices.Equal(moves, []uint64{3}) {
		t.Errorf("moves = %v, want [3]", moves)
	}
}

func TestRunPublishesRecordWithIdentity(t *testing.T) {
	cfg := fastConfig(config.ModeDryRun)
	cfg.PollInterval = 10 * time.Millisecond
	cfg.Identity = "zone-a"
	h := newHarness(cfg)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- h.agent.Run(ctx) }()
	select {
	case r := <-h.fake.kept:
		want := cluster.Record{SchemaVersion: cluster.RecordSchemaVersion, Identity: "zone-a", AgentVersion: Version}
		if r != want {
			t.Errorf("record = %+v, want %+v", r, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("record was not published")
	}
	cancel()
	if err := <-done; err != nil {
		t.Errorf("Run() error = %v", err)
	}
}

func TestRunWithoutIdentityPublishesNothing(t *testing.T) {
	cfg := fastConfig(config.ModeDryRun)
	cfg.PollInterval = 10 * time.Millisecond
	h := newHarness(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := h.agent.Run(ctx); err != nil {
		t.Errorf("Run() error = %v", err)
	}
	select {
	case r := <-h.fake.kept:
		t.Errorf("published %+v without an identity", r)
	default:
	}
}
