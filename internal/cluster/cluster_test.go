package cluster

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/aenix-io/threek/internal/etcdconf"
	"github.com/aenix-io/threek/internal/etcdtest"
)

func newClient(t *testing.T, m *etcdtest.Member) *Client {
	t.Helper()
	c, err := New(m.Settings)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

func timeout(t *testing.T) context.Context {
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func TestNewRequiresCAForHTTPS(t *testing.T) {
	_, err := New(etcdconf.Settings{ListenClientURLs: []string{"https://127.0.0.1:2379"}})
	if err == nil {
		t.Fatal("New() error = nil, want an error about the missing CA")
	}
}

func TestStatusAndMembers(t *testing.T) {
	cl := etcdtest.Start(t, 3)
	ctx := timeout(t)
	c := newClient(t, cl.Members[0])

	local, err := c.Status(ctx, c.LocalURL())
	if err != nil {
		t.Fatalf("Status(local) error = %v", err)
	}
	if local.MemberID != cl.ID(t, cl.Members[0]) {
		t.Errorf("local member ID = %x, want %x", local.MemberID, cl.ID(t, cl.Members[0]))
	}
	if want := cl.ID(t, cl.Leader(t)); local.Leader != want {
		t.Errorf("leader = %x, want %x", local.Leader, want)
	}
	if local.Term == 0 || local.AppliedIndex == 0 {
		t.Errorf("status = %+v, want term and applied index", local)
	}

	members, err := c.Members(ctx)
	if err != nil {
		t.Fatalf("Members() error = %v", err)
	}
	if len(members) != 3 {
		t.Fatalf("members = %+v, want 3", members)
	}
	for _, m := range members {
		if m.IsLearner || len(m.ClientURLs) == 0 {
			t.Errorf("member %+v, want a voter with client URLs", m)
		}
		remote, err := c.Status(ctx, m.ClientURLs[0])
		if err != nil {
			t.Fatalf("Status(%s) error = %v", m.ClientURLs[0], err)
		}
		if remote.MemberID != m.ID || remote.ClusterID != local.ClusterID {
			t.Errorf("remote status = %+v, want member %x of cluster %x", remote, m.ID, local.ClusterID)
		}
	}
}

func TestMembersIncludeLearners(t *testing.T) {
	cl := etcdtest.Start(t, 3)
	ctx := timeout(t)
	// etcd refuses to add members until every member has been connected for
	// its health interval.
	for {
		_, err := cl.Client(t, cl.Members[0]).MemberAddAsLearner(ctx, []string{"https://127.0.0.1:1"})
		if err == nil {
			break
		}
		if ctx.Err() != nil {
			t.Fatalf("add learner: %v", err)
		}
		time.Sleep(500 * time.Millisecond)
	}
	members, err := newClient(t, cl.Members[0]).Members(ctx)
	if err != nil {
		t.Fatalf("Members() error = %v", err)
	}
	learners := 0
	for _, m := range members {
		if m.IsLearner {
			learners++
		}
	}
	if len(members) != 4 || learners != 1 {
		t.Errorf("members = %+v, want 3 voters and 1 learner", members)
	}
}

func TestMetrics(t *testing.T) {
	cl := etcdtest.Start(t, 3)
	ctx := timeout(t)
	c := newClient(t, cl.Members[0])
	members, err := c.Members(ctx)
	if err != nil {
		t.Fatal(err)
	}
	// Streams come up shortly after the leader is elected.
	deadline := time.Now().Add(15 * time.Second)
	for {
		allActive := true
		for _, m := range members {
			peers, err := c.Metrics(ctx, m.ClientURLs[0], m.ID)
			if err != nil {
				t.Fatalf("Metrics(%s) error = %v", m.ClientURLs[0], err)
			}
			for _, other := range members {
				if other.ID != m.ID && !peers.Active[other.ID] {
					allActive = false
				}
			}
		}
		if allActive {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("not every member reports every peer as active")
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func TestMoveLeader(t *testing.T) {
	cl := etcdtest.Start(t, 3)
	ctx := timeout(t)
	leader := cl.Leader(t)
	var target *etcdtest.Member
	for _, m := range cl.Members {
		if m != leader {
			target = m
			break
		}
	}

	if err := newClient(t, target).MoveLeader(ctx, cl.ID(t, leader)); !errors.Is(err, ErrNotLeader) {
		t.Errorf("MoveLeader() from a follower error = %v, want ErrNotLeader", err)
	}
	if err := newClient(t, leader).MoveLeader(ctx, cl.ID(t, target)); err != nil {
		t.Fatalf("MoveLeader() error = %v", err)
	}
	if got := cl.Leader(t); got != target {
		t.Errorf("leader = %s, want %s", got.Name, target.Name)
	}
}

func TestRecords(t *testing.T) {
	cl := etcdtest.Start(t, 3)
	ctx := timeout(t)
	c := newClient(t, cl.Members[0])
	id := cl.ID(t, cl.Members[0])

	keepCtx, stop := context.WithCancel(ctx)
	done := make(chan error, 1)
	want := Record{SchemaVersion: RecordSchemaVersion, Identity: "zone-a", AgentVersion: "test"}
	go func() { done <- c.KeepRecord(keepCtx, id, want, 5*time.Second) }()

	reader := newClient(t, cl.Members[1])
	deadline := time.Now().Add(10 * time.Second)
	for {
		records, err := reader.Records(ctx)
		if err != nil {
			t.Fatalf("Records() error = %v", err)
		}
		if got, ok := records[id]; ok {
			if got != want {
				t.Errorf("record = %+v, want %+v", got, want)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("record did not appear")
		}
		time.Sleep(100 * time.Millisecond)
	}

	stop()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Errorf("KeepRecord() error = %v, want context.Canceled", err)
	}
	records, err := reader.Records(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := records[id]; ok {
		t.Error("record is still present after KeepRecord returned")
	}
}

func TestRecordsSkipInvalidValues(t *testing.T) {
	cl := etcdtest.Start(t, 3)
	ctx := timeout(t)
	kv := cl.Client(t, cl.Members[0])
	if _, err := kv.Put(ctx, RecordPrefix+"not-hex", `{"identity":"x"}`); err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Put(ctx, RecordPrefix+"1", `not json`); err != nil {
		t.Fatal(err)
	}
	if _, err := kv.Put(ctx, RecordPrefix+"2", `{"schemaVersion":1,"identity":"zone-b","futureField":true}`); err != nil {
		t.Fatal(err)
	}
	records, err := newClient(t, cl.Members[0]).Records(ctx)
	if err != nil {
		t.Fatalf("Records() error = %v", err)
	}
	want := map[uint64]Record{2: {SchemaVersion: 1, Identity: "zone-b"}}
	if len(records) != 1 || records[2] != want[2] {
		t.Errorf("records = %+v, want %+v", records, want)
	}
}
