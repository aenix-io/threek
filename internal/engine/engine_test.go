package engine

import (
	"testing"
	"time"
)

const (
	a MemberID = 1
	b MemberID = 2
	c MemberID = 3
)

var start = time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

func params() Params {
	return Params{
		DownAfter:    3,
		UpAfter:      2,
		MoveAfter:    15 * time.Second,
		MoveCooldown: 0,
		PreferAfter:  time.Minute,
		Identity:     "zone-a",
	}
}

// cluster drives an Engine running on member a, polled once per second.
type cluster struct {
	t       *testing.T
	e       *Engine
	now     time.Time
	leader  MemberID
	term    uint64
	members []Member
	cut     map[[2]MemberID]bool
	missing map[MemberID]bool
	// absent hides a's metric series for b while the link is still up.
	absent bool
}

func newCluster(t *testing.T, p Params) *cluster {
	return &cluster{
		t:      t,
		e:      New(p),
		now:    start,
		leader: a,
		term:   1,
		members: []Member{
			{ID: a, Identity: "zone-a", Healthy: true},
			{ID: b, Identity: "zone-b", Healthy: true},
			{ID: c, Identity: "zone-c", Healthy: true},
		},
		cut:     map[[2]MemberID]bool{},
		missing: map[MemberID]bool{},
	}
}

func (cl *cluster) setCut(x, y MemberID, cut bool) {
	cl.cut[[2]MemberID{x, y}] = cut
	cl.cut[[2]MemberID{y, x}] = cut
}

func (cl *cluster) member(id MemberID) *Member {
	for i := range cl.members {
		if cl.members[i].ID == id {
			return &cl.members[i]
		}
	}
	cl.t.Fatalf("no member %d", id)
	return nil
}

func (cl *cluster) observation() Observation {
	active := map[MemberID]map[MemberID]bool{}
	for _, m := range cl.members {
		if cl.missing[m.ID] || (m.ID != a && cl.cut[[2]MemberID{a, m.ID}]) {
			continue
		}
		row := map[MemberID]bool{}
		for _, peer := range cl.members {
			if peer.ID == m.ID {
				continue
			}
			if m.ID == a && peer.ID == b && cl.absent {
				continue
			}
			row[peer.ID] = !cl.cut[[2]MemberID{m.ID, peer.ID}]
		}
		active[m.ID] = row
	}
	return Observation{
		Now:     cl.now,
		Self:    a,
		Leader:  cl.leader,
		Term:    cl.term,
		Members: append([]Member(nil), cl.members...),
		Active:  active,
	}
}

func (cl *cluster) poll() Decision {
	d := cl.e.Observe(cl.observation())
	cl.now = cl.now.Add(time.Second)
	return d
}

// pollUntil polls up to n times and returns the first transfer decision and
// the number of polls it took, or a zero Decision and n.
func (cl *cluster) pollUntil(n int) (Decision, int) {
	for i := 1; i <= n; i++ {
		if d := cl.poll(); d.Transfer {
			return d, i
		}
	}
	return Decision{}, n
}

func (cl *cluster) expectNoTransfer(n int) {
	cl.t.Helper()
	if d, i := cl.pollUntil(n); d.Transfer {
		cl.t.Fatalf("poll %d: unexpected transfer to %d (%s)", i, d.Target, d.Reason)
	}
}

func (cl *cluster) expectTransferAt(n int, target MemberID, reason Reason) {
	cl.t.Helper()
	cl.expectNoTransfer(n - 1)
	d := cl.poll()
	if !d.Transfer {
		cl.t.Fatalf("poll %d: no transfer (%s), want transfer to %d", n, d.Note, target)
	}
	if d.Target != target || d.Reason != reason {
		cl.t.Fatalf("poll %d: transfer to %d (%s), want %d (%s)", n, d.Target, d.Reason, target, reason)
	}
}

// warmUp brings every link up.
func (cl *cluster) warmUp() {
	cl.expectNoTransfer(params().UpAfter)
}

// Partition timing with params(): the third inactive sample declares the link
// down, and MoveAfter (15 polls) later the leader transfers: poll 3+15 = 18.
const partitionTransferPoll = 18

func TestFullConnectivityDoesNothing(t *testing.T) {
	cl := newCluster(t, params())
	cl.expectNoTransfer(600)
}

func TestPartialPartitionMovesLeaderToMemberThatSeesEveryone(t *testing.T) {
	cl := newCluster(t, params())
	cl.warmUp()
	cl.setCut(a, b, true)
	cl.expectTransferAt(partitionTransferPoll, c, ReasonPartition)
}

func TestPartialPartitionOtherSide(t *testing.T) {
	cl := newCluster(t, params())
	cl.warmUp()
	cl.setCut(a, c, true)
	cl.expectTransferAt(partitionTransferPoll, b, ReasonPartition)
}

func TestPartitionWithoutLeaderSideMember(t *testing.T) {
	// b <-> c broken while the leader a reaches both: Raft is fine.
	cl := newCluster(t, params())
	cl.warmUp()
	cl.setCut(b, c, true)
	cl.expectNoTransfer(120)
}

func TestSiteDownDoesNothing(t *testing.T) {
	cl := newCluster(t, params())
	cl.warmUp()
	cl.setCut(a, b, true)
	cl.setCut(c, b, true)
	cl.expectNoTransfer(120)
}

func TestCandidateViewUnavailable(t *testing.T) {
	cl := newCluster(t, params())
	cl.warmUp()
	cl.setCut(a, b, true)
	cl.missing[c] = true
	cl.expectNoTransfer(120)
}

func TestCandidateUnhealthy(t *testing.T) {
	cl := newCluster(t, params())
	cl.warmUp()
	cl.setCut(a, b, true)
	cl.member(c).Healthy = false
	cl.expectNoTransfer(120)
}

func TestOwnViewUnavailable(t *testing.T) {
	// The link is already down when the local metrics stop being readable;
	// the stale state must not trigger a transfer.
	cl := newCluster(t, params())
	cl.warmUp()
	cl.setCut(a, b, true)
	cl.expectNoTransfer(params().DownAfter)
	cl.missing[a] = true
	cl.expectNoTransfer(120)
}

func TestNotLeaderDoesNothing(t *testing.T) {
	cl := newCluster(t, params())
	cl.leader = b
	cl.warmUp()
	cl.setCut(a, b, true)
	cl.expectNoTransfer(120)
}

func TestMissingSeriesCountsAsInactive(t *testing.T) {
	cl := newCluster(t, params())
	cl.warmUp()
	cl.absent = true
	cl.expectTransferAt(partitionTransferPoll, c, ReasonPartition)
}

func TestFlappingLinkDoesNothing(t *testing.T) {
	cl := newCluster(t, params())
	cl.warmUp()
	for i := range 120 {
		cl.setCut(a, b, i%3 != 2)
		if d := cl.poll(); d.Transfer {
			t.Fatalf("poll %d: unexpected transfer to %d", i, d.Target)
		}
	}
}

func TestLinkRecoveryCancelsTransfer(t *testing.T) {
	cl := newCluster(t, params())
	cl.warmUp()
	cl.setCut(a, b, true)
	cl.expectNoTransfer(10)
	cl.setCut(a, b, false)
	cl.expectNoTransfer(120)
}

func TestUnknownLinksDoNothing(t *testing.T) {
	p := params()
	p.UpAfter = 1000
	cl := newCluster(t, p)
	cl.expectNoTransfer(5)
	cl.setCut(a, b, true)
	// a -> b goes down, but c's links never become known to be up.
	cl.expectNoTransfer(120)
}

func TestUnsupportedMemberCount(t *testing.T) {
	for _, n := range []int{2, 4} {
		cl := newCluster(t, params())
		if n == 2 {
			cl.members = cl.members[:2]
		} else {
			cl.members = append(cl.members, Member{ID: 4, Identity: "zone-d", Healthy: true})
		}
		cl.warmUp()
		cl.setCut(a, b, true)
		cl.expectNoTransfer(120)
	}
}

func TestCooldownDelaysTransfer(t *testing.T) {
	p := params()
	p.MoveCooldown = 60 * time.Second
	cl := newCluster(t, p)
	cl.warmUp()
	cl.setCut(a, b, true)
	// Leadership was observed at poll 1; the cooldown ends 60 polls later.
	cl.expectTransferAt(60-p.UpAfter+1, c, ReasonPartition)
}

func TestTermChangeRestartsCooldown(t *testing.T) {
	p := params()
	p.MoveCooldown = 60 * time.Second
	cl := newCluster(t, p)
	cl.expectNoTransfer(50)
	cl.term = 2
	cl.setCut(a, b, true)
	cl.expectTransferAt(61, c, ReasonPartition)
}

func TestRegainedLeadershipRestartsCooldown(t *testing.T) {
	p := params()
	p.MoveCooldown = 60 * time.Second
	cl := newCluster(t, p)
	cl.expectNoTransfer(50)
	cl.leader = b
	cl.poll()
	cl.leader = a
	cl.setCut(a, b, true)
	cl.expectTransferAt(61, c, ReasonPartition)
}

func preferred(target string) Params {
	p := params()
	p.PreferredLeader = target
	return p
}

// With full connectivity from the first poll, links are up after UpAfter
// polls and PreferAfter (60 polls) later the leader returns: poll 2+60 = 62.
const preferredTransferPoll = 62

func TestPreferredLeader(t *testing.T) {
	cl := newCluster(t, preferred("zone-c"))
	cl.expectTransferAt(preferredTransferPoll, c, ReasonPreferred)
}

func TestPreferredLeaderIsSelf(t *testing.T) {
	cl := newCluster(t, preferred("zone-a"))
	cl.expectNoTransfer(600)
}

func TestPreferredLeaderUnknownIdentity(t *testing.T) {
	cl := newCluster(t, preferred("zone-x"))
	cl.expectNoTransfer(600)
}

func TestPreferredLeaderDuplicateIdentity(t *testing.T) {
	cl := newCluster(t, preferred("zone-c"))
	cl.member(b).Identity = "zone-c"
	cl.expectNoTransfer(600)
}

func TestPreferredLeaderUsesLocalIdentityFromParams(t *testing.T) {
	// A stale record of the local member must not count as a duplicate.
	cl := newCluster(t, preferred("zone-c"))
	cl.member(a).Identity = "zone-c"
	cl.expectTransferAt(preferredTransferPoll, c, ReasonPreferred)
}

func TestPreferredLeaderUnhealthy(t *testing.T) {
	cl := newCluster(t, preferred("zone-c"))
	cl.member(c).Healthy = false
	cl.expectNoTransfer(600)
}

func TestPreferredLeaderWaitsForStableConnectivity(t *testing.T) {
	cl := newCluster(t, preferred("zone-c"))
	cl.expectNoTransfer(30)
	cl.setCut(b, c, true)
	cl.expectNoTransfer(5)
	cl.setCut(b, c, false)
	// b <-> c needs UpAfter polls to come back, then PreferAfter starts over.
	cl.expectTransferAt(params().UpAfter+60, c, ReasonPreferred)
}

func TestPreferredLeaderWaitsForEveryRow(t *testing.T) {
	cl := newCluster(t, preferred("zone-c"))
	cl.missing[b] = true
	cl.expectNoTransfer(600)
}

// Link history survives new parameters: a preferred leader set after a long
// stretch of full connectivity applies at once.
func TestSetParamsKeepsLinkHistory(t *testing.T) {
	cl := newCluster(t, params())
	cl.expectNoTransfer(preferredTransferPoll)
	cl.e.SetParams(preferred("zone-c"))
	cl.expectTransferAt(1, c, ReasonPreferred)
}

func TestPartitionWinsOverPreferredLeader(t *testing.T) {
	cl := newCluster(t, preferred("zone-b"))
	cl.warmUp()
	cl.setCut(a, b, true)
	cl.expectTransferAt(partitionTransferPoll, c, ReasonPartition)
}

func TestRemovedMemberLinksAreForgotten(t *testing.T) {
	cl := newCluster(t, params())
	cl.warmUp()
	cl.members = cl.members[:2]
	cl.poll()
	if got := len(cl.e.links); got != 2 {
		t.Errorf("tracked links = %d, want 2", got)
	}
}

func TestNotesAreStableWhileWaiting(t *testing.T) {
	cl := newCluster(t, params())
	cl.warmUp()
	cl.setCut(a, b, true)
	cl.expectNoTransfer(params().DownAfter)
	first := cl.poll()
	second := cl.poll()
	if first.Note != second.Note {
		t.Errorf("notes differ while waiting: %q, %q", first.Note, second.Note)
	}
	if first.Wait != second.Wait+time.Second {
		t.Errorf("wait = %s then %s, want it to count down by the poll interval", first.Wait, second.Wait)
	}
}

func TestFailedTransferWaitsForCooldown(t *testing.T) {
	p := params()
	p.MoveCooldown = 30 * time.Second
	cl := newCluster(t, p)
	cl.expectNoTransfer(40)
	cl.setCut(a, b, true)
	cl.expectTransferAt(partitionTransferPoll, c, ReasonPartition)
	cl.e.TransferFailed(cl.now.Add(-time.Second))
	cl.expectTransferAt(30, c, ReasonPartition)
}

func TestView(t *testing.T) {
	cl := newCluster(t, params())
	cl.warmUp()
	cl.setCut(a, b, true)
	cl.expectNoTransfer(params().DownAfter)
	view := cl.e.View()
	want := map[string]string{
		"1->2": "down", "1->3": "up",
		"2->1": "up", "2->3": "up",
		"3->1": "up", "3->2": "up",
	}
	if len(view) != len(want) {
		t.Fatalf("View() = %v, want %v", view, want)
	}
	for k, v := range want {
		if view[k] != v {
			t.Errorf("View()[%s] = %q, want %q", k, view[k], v)
		}
	}
}

func TestLinks(t *testing.T) {
	cl := newCluster(t, params())
	cl.warmUp()
	cl.setCut(a, b, true)
	cl.expectNoTransfer(params().DownAfter)
	got := map[[2]MemberID]LinkState{}
	for _, l := range cl.e.Links() {
		got[[2]MemberID{l.From, l.To}] = l.State
	}
	want := map[[2]MemberID]LinkState{
		{a, b}: LinkDown, {a, c}: LinkUp,
		{b, a}: LinkUp, {b, c}: LinkUp,
		{c, a}: LinkUp, {c, b}: LinkUp,
	}
	if len(got) != len(want) {
		t.Fatalf("Links() = %v, want %v", got, want)
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("link %v = %v, want %v", k, got[k], v)
		}
	}
}
