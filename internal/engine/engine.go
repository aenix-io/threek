// Package engine decides where the etcd leader should be. It is a pure state
// machine: the agent feeds it observations and acts on its decisions.
package engine

import (
	"fmt"
	"time"
)

// MemberID is an etcd member ID.
type MemberID uint64

// Member is a voting member of the cluster.
type Member struct {
	ID MemberID
	// Identity comes from the member's record; empty when there is none.
	Identity string
	// Healthy reports whether the member answered its status call and is
	// not lagging behind.
	Healthy bool
}

// Observation is one poll of the cluster by the local agent.
type Observation struct {
	Now    time.Time
	Self   MemberID
	Leader MemberID
	Term   uint64
	// Members are the voting members; learners are excluded.
	Members []Member
	// Active[x][y] reports whether member x's transport to member y is active.
	// A missing row means x's view could not be read in this poll; a missing
	// entry in a row means inactive.
	Active map[MemberID]map[MemberID]bool
}

type Params struct {
	DownAfter    int
	UpAfter      int
	MoveAfter    time.Duration
	MoveCooldown time.Duration
	PreferAfter  time.Duration
	// Identity is the local member's identity.
	Identity        string
	PreferredLeader string
}

type Reason string

const (
	ReasonPartition Reason = "partition"
	ReasonPreferred Reason = "preferred-leader"
)

// Decision is either a leadership transfer or no action, with a note
// explaining the outcome for logs.
type Decision struct {
	Transfer bool
	Target   MemberID
	Reason   Reason
	Note     string
}

func none(format string, args ...any) Decision {
	return Decision{Note: fmt.Sprintf(format, args...)}
}

type linkState int

const (
	linkUnknown linkState = iota
	linkUp
	linkDown
)

type link struct{ from, to MemberID }

type tracker struct {
	state      linkState
	since      time.Time
	lastActive bool
	streak     int
}

func (t *tracker) sample(active bool, now time.Time, p Params) {
	if active == t.lastActive {
		t.streak++
	} else {
		t.lastActive, t.streak = active, 1
	}
	switch {
	case active && t.state != linkUp && t.streak >= p.UpAfter:
		t.state, t.since = linkUp, now
	case !active && t.state != linkDown && t.streak >= p.DownAfter:
		t.state, t.since = linkDown, now
	}
}

type Engine struct {
	p           Params
	links       map[link]*tracker
	leading     bool
	leaderTerm  uint64
	leaderSince time.Time
	fullSince   time.Time
}

func New(p Params) *Engine {
	return &Engine{p: p, links: make(map[link]*tracker)}
}

// Observe updates the state with o and decides what to do.
func (e *Engine) Observe(o Observation) Decision {
	e.trackLeadership(o)
	e.trackLinks(o)

	if !e.leading {
		return none("not the leader")
	}
	if len(o.Members) != 3 {
		return none("%d voting members, only 3 are supported", len(o.Members))
	}
	if _, ok := o.Active[o.Self]; !ok {
		return none("own view is unavailable")
	}
	if d, decided := e.partition(o); decided {
		return d
	}
	return e.preferred(o)
}

func (e *Engine) trackLeadership(o Observation) {
	leading := o.Self != 0 && o.Leader == o.Self
	if leading && (!e.leading || o.Term != e.leaderTerm) {
		e.leaderSince, e.leaderTerm = o.Now, o.Term
	}
	e.leading = leading
}

func (e *Engine) trackLinks(o Observation) {
	members := make(map[MemberID]bool, len(o.Members))
	for _, m := range o.Members {
		members[m.ID] = true
	}
	for l := range e.links {
		if !members[l.from] || !members[l.to] {
			delete(e.links, l)
		}
	}

	full := true
	for _, from := range o.Members {
		row, known := o.Active[from.ID]
		for _, to := range o.Members {
			if from.ID == to.ID {
				continue
			}
			t := e.tracker(from.ID, to.ID)
			if known {
				t.sample(row[to.ID], o.Now, e.p)
			}
			full = full && known && t.state == linkUp
		}
	}
	switch {
	case !full:
		e.fullSince = time.Time{}
	case e.fullSince.IsZero():
		e.fullSince = o.Now
	}
}

func (e *Engine) tracker(from, to MemberID) *tracker {
	l := link{from, to}
	t, ok := e.links[l]
	if !ok {
		t = &tracker{}
		e.links[l] = t
	}
	return t
}

func (e *Engine) up(from, to MemberID) bool {
	return e.tracker(from, to).state == linkUp
}

// partition applies the partial partition rule. decided is false when the
// leader reaches every member, so lower-priority rules may apply.
func (e *Engine) partition(o Observation) (d Decision, decided bool) {
	var lost MemberID
	for _, m := range o.Members {
		if m.ID != o.Self && e.tracker(o.Self, m.ID).state == linkDown {
			lost = m.ID
			break
		}
	}
	if lost == 0 {
		return Decision{}, false
	}
	if wait := e.p.MoveAfter - o.Now.Sub(e.tracker(o.Self, lost).since); wait > 0 {
		return none("member %d unreachable, transfer in %s", lost, wait), true
	}

	var candidate Member
	for _, m := range o.Members {
		if m.ID != o.Self && m.ID != lost {
			candidate = m
		}
	}
	_, fresh := o.Active[candidate.ID]
	switch {
	case !fresh || !candidate.Healthy:
		return none("member %d unreachable, member %d is unavailable", lost, candidate.ID), true
	case !e.up(o.Self, candidate.ID) || !e.up(candidate.ID, o.Self) || !e.up(candidate.ID, lost):
		return none("member %d unreachable, no member reaches everyone", lost), true
	}
	return e.transfer(o, candidate.ID, ReasonPartition), true
}

func (e *Engine) preferred(o Observation) Decision {
	if e.p.PreferredLeader == "" {
		return none("full connectivity")
	}
	if e.p.Identity == e.p.PreferredLeader {
		return none("already on the preferred leader")
	}
	var matches []Member
	for _, m := range o.Members {
		identity := m.Identity
		if m.ID == o.Self {
			identity = e.p.Identity
		}
		if identity == e.p.PreferredLeader {
			matches = append(matches, m)
		}
	}
	if len(matches) != 1 {
		return none("preferred identity %q matches %d members", e.p.PreferredLeader, len(matches))
	}
	target := matches[0]
	if !target.Healthy {
		return none("preferred member %d is unhealthy", target.ID)
	}
	if e.fullSince.IsZero() {
		return none("waiting for full connectivity")
	}
	if wait := e.p.PreferAfter - o.Now.Sub(e.fullSince); wait > 0 {
		return none("returning to the preferred member in %s", wait)
	}
	return e.transfer(o, target.ID, ReasonPreferred)
}

func (e *Engine) transfer(o Observation, target MemberID, reason Reason) Decision {
	if wait := e.p.MoveCooldown - o.Now.Sub(e.leaderSince); wait > 0 {
		return none("%s transfer to member %d in %s, after cooldown", reason, target, wait)
	}
	return Decision{Transfer: true, Target: target, Reason: reason, Note: fmt.Sprintf("%s: transfer to member %d", reason, target)}
}
