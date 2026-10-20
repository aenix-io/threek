package agent

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aenix-io/threek/internal/cluster"
	"github.com/aenix-io/threek/internal/config"
)

const clusterID = 0xc1

func clientURL(id uint64) string { return fmt.Sprintf("https://m%x:2379", id) }
func peerURL(id uint64) string   { return fmt.Sprintf("https://m%x:2380", id) }

// fakeCluster is a three-member cluster seen from member 1. The agents of
// members 2 and 3 are simulated: their records follow the cut links.
type fakeCluster struct {
	mu          sync.Mutex
	self        uint64
	leader      uint64
	cut         map[[2]uint64]bool
	clusterOf   map[uint64]uint64
	errorsOf    map[uint64][]string
	appliedOf   map[uint64]uint64
	learners    map[uint64]bool
	identities  map[uint64]string
	silent      map[uint64]bool
	records     map[uint64]cluster.Record
	leases      map[int64]bool
	nextLease   int64
	puts        int
	revoked     []int64
	moveErr     error
	moves       []uint64
	statusCalls []string
}

func newFakeCluster() *fakeCluster {
	return &fakeCluster{
		self:       1,
		leader:     1,
		cut:        map[[2]uint64]bool{},
		clusterOf:  map[uint64]uint64{},
		errorsOf:   map[uint64][]string{},
		appliedOf:  map[uint64]uint64{},
		learners:   map[uint64]bool{},
		identities: map[uint64]string{},
		silent:     map[uint64]bool{},
		records:    map[uint64]cluster.Record{},
		leases:     map[int64]bool{},
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
		if clientURL(id) == u || peerURL(id) == u {
			return id
		}
	}
	panic("unknown URL " + u)
}

// simulateOthers publishes the records the agents of members 2 and 3 would.
func (f *fakeCluster) simulateOthers() {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, id := range []uint64{2, 3} {
		if f.silent[id] || f.cut[[2]uint64{f.self, id}] {
			continue
		}
		sees := map[string]bool{}
		for _, peer := range []uint64{1, 2, 3} {
			if peer != id {
				sees[strconv.FormatUint(peer, 16)] = !f.cut[[2]uint64{id, peer}]
			}
		}
		f.records[id] = cluster.Record{SchemaVersion: 1, Identity: f.identities[id], Sees: sees}
	}
}

func (f *fakeCluster) LocalURL() string { return clientURL(f.self) }

// Status of an unreachable member hangs until ctx is done, as a gRPC call
// to a connection stuck in CONNECTING does.
func (f *fakeCluster) Status(ctx context.Context, u string) (cluster.Status, error) {
	f.mu.Lock()
	f.statusCalls = append(f.statusCalls, u)
	id := f.idOf(u)
	if id != f.self && f.cut[[2]uint64{f.self, id}] {
		f.mu.Unlock()
		<-ctx.Done()
		return cluster.Status{}, ctx.Err()
	}
	defer f.mu.Unlock()
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
		members = append(members, cluster.Member{
			ID: id, ClientURLs: []string{clientURL(id)}, PeerURLs: []string{peerURL(id)}, IsLearner: f.learners[id],
		})
	}
	return members, nil
}

func (f *fakeCluster) Records(ctx context.Context) (map[uint64]cluster.Record, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	return maps.Clone(f.records), nil
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

func (f *fakeCluster) GrantLease(context.Context, time.Duration) (int64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.nextLease++
	f.leases[f.nextLease] = true
	return f.nextLease, nil
}

func (f *fakeCluster) PutRecord(_ context.Context, id uint64, r cluster.Record, lease int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.leases[lease] {
		return errors.New("lease not found")
	}
	f.puts++
	f.records[id] = r
	return nil
}

func (f *fakeCluster) RenewLease(_ context.Context, lease int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if !f.leases[lease] {
		return cluster.ErrLeaseExpired
	}
	return nil
}

func (f *fakeCluster) RevokeLease(_ context.Context, lease int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.revoked = append(f.revoked, lease)
	delete(f.leases, lease)
	delete(f.records, f.self)
	return nil
}

// expireLeases drops every lease and the local record, as etcd does when the
// agent stops renewing.
func (f *fakeCluster) expireLeases() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.leases = map[int64]bool{}
	delete(f.records, f.self)
}

func (f *fakeCluster) movesSoFar() []uint64 {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.moves)
}

// fakeProber probes from member 1 through fakeCluster's links.
type fakeProber struct{ f *fakeCluster }

func (p fakeProber) Reachable(_ context.Context, peerURLs []string) bool {
	p.f.mu.Lock()
	defer p.f.mu.Unlock()
	return !p.f.cut[[2]uint64{p.f.self, p.f.idOf(peerURLs[0])}]
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
	h.agent = New(cfg, h.fake, fakeProber{h.fake}, logger, func() time.Time { return h.now })
	h.agent.remoteTimeout = 20 * time.Millisecond
	h.agent.pollTimeout = 100 * time.Millisecond
	return h
}

func (h *harness) ticks(n int) {
	for range n {
		h.fake.simulateOthers()
		h.agent.Tick(context.Background())
		h.now = h.now.Add(time.Second)
	}
}

func TestEveryAgentPublishesItsView(t *testing.T) {
	h := newHarness(fastConfig(config.ModeDryRun))
	h.fake.leader = 2
	h.ticks(3)
	if got, want := h.fake.records[1].Sees, map[string]bool{"2": true, "3": true}; !maps.Equal(got, want) {
		t.Errorf("published view = %v, want %v", got, want)
	}
	h.fake.setCut(1, 2)
	h.ticks(3)
	if got, want := h.fake.records[1].Sees, map[string]bool{"2": false, "3": true}; !maps.Equal(got, want) {
		t.Errorf("published view = %v, want %v", got, want)
	}
	if h.fake.puts != 2 {
		t.Errorf("records written %d times, want 2: only when the view changes", h.fake.puts)
	}
}

func TestExpiredLeaseIsGrantedAgain(t *testing.T) {
	h := newHarness(fastConfig(config.ModeDryRun))
	h.ticks(2)
	h.fake.expireLeases()
	h.ticks(1)
	if _, ok := h.fake.records[1]; !ok {
		t.Error("record was not published again after the lease expired")
	}
}

func TestFollowerDoesNotInspectOthers(t *testing.T) {
	h := newHarness(fastConfig(config.ModeEnforce))
	h.fake.leader = 2
	h.ticks(3)
	for _, u := range h.fake.statusCalls {
		if u != clientURL(1) {
			t.Errorf("follower called status of %s", u)
		}
	}
}

func TestLeaderInspectsEveryVoter(t *testing.T) {
	h := newHarness(fastConfig(config.ModeEnforce))
	h.fake.learners[4] = true
	h.ticks(1)
	calls := slices.Clone(h.fake.statusCalls)
	slices.Sort(calls)
	calls = slices.Compact(calls)
	if want := []string{clientURL(1), clientURL(2), clientURL(3)}; !slices.Equal(calls, want) {
		t.Errorf("status calls = %v, want %v", calls, want)
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

// A member that does not answer must not use up the time the agent needs to
// publish its own view and read the others'.
func TestHangingMemberDoesNotStallThePoll(t *testing.T) {
	h := newHarness(fastConfig(config.ModeEnforce))
	h.ticks(2)
	h.fake.setCut(1, 2)
	h.ticks(5)
	if strings.Contains(h.logs.String(), "poll failed") {
		t.Errorf("poll failed while a member hangs:\n%s", h.logs)
	}
	if moves := h.fake.movesSoFar(); !slices.Equal(moves, []uint64{3}) {
		t.Errorf("moves = %v, want [3]", moves)
	}
}

func TestMissingCandidateRecordMeansUnknownView(t *testing.T) {
	h := newHarness(fastConfig(config.ModeEnforce))
	h.ticks(2)
	h.fake.silent[3] = true
	delete(h.fake.records, 3)
	h.fake.setCut(1, 2)
	h.ticks(20)
	if moves := h.fake.movesSoFar(); len(moves) != 0 {
		t.Errorf("moves = %v, want none without the candidate's view", moves)
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
	if n := strings.Count(h.logs.String(), `"msg":"decision"`); n != 1 {
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
	h.fake.identities[2] = "zone-b"
	h.fake.identities[3] = "zone-c"
	h.ticks(10)
	if moves := h.fake.movesSoFar(); !slices.Equal(moves, []uint64{3}) {
		t.Errorf("moves = %v, want [3]", moves)
	}
	if got := h.fake.records[1]; got.Identity != "zone-a" || got.AgentVersion != Version {
		t.Errorf("published record = %+v, want identity zone-a and version %s", got, Version)
	}
}

func TestRunRevokesRecordOnStop(t *testing.T) {
	cfg := fastConfig(config.ModeDryRun)
	cfg.PollInterval = 10 * time.Millisecond
	h := newHarness(cfg)
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	if err := h.agent.Run(ctx); err != nil {
		t.Errorf("Run() error = %v", err)
	}
	if len(h.fake.revoked) != 1 {
		t.Errorf("revoked leases = %v, want the record's lease", h.fake.revoked)
	}
	if _, ok := h.fake.records[1]; ok {
		t.Error("record is still present after the agent stopped")
	}
}
