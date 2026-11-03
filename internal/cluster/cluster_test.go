package cluster

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"sync"
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
	_, err := New(etcdconf.Settings{Endpoint: "https://127.0.0.1:2379"})
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
		if m.IsLearner || len(m.ClientURLs) == 0 || len(m.PeerURLs) == 0 {
			t.Errorf("member %+v, want a voter with client and peer URLs", m)
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

// The client must follow its certificate files when they are rotated in
// place: here they start out issued by a CA the cluster does not trust.
func TestRotatedFilesTakeEffect(t *testing.T) {
	cl := etcdtest.Start(t, 3)
	ctx := timeout(t)
	m := cl.Members[0]
	dir := t.TempDir()
	other := etcdtest.NewCA(t, dir)
	certFile, keyFile := other.Issue(t, dir, "agent")
	caFile := filepath.Join(dir, "trusted-ca.crt")
	etcdtest.Replace(t, caFile, other.CertFile)
	c, err := New(etcdconf.Settings{Endpoint: m.ClientURL, CertFile: certFile, KeyFile: keyFile, TrustedCAFile: caFile})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })

	short, cancel := context.WithTimeout(ctx, time.Second)
	defer cancel()
	if _, err := c.Status(short, m.ClientURL); err == nil {
		t.Fatal("Status() error = nil with certificates of another CA")
	}
	etcdtest.Replace(t, certFile, m.Settings.CertFile)
	etcdtest.Replace(t, keyFile, m.Settings.KeyFile)
	etcdtest.Replace(t, caFile, m.Settings.TrustedCAFile)
	if changed, err := c.ReloadTLS(); !changed || err != nil {
		t.Fatalf("ReloadTLS() = %v, %v; want a change", changed, err)
	}
	if _, err := c.Status(ctx, m.ClientURL); err != nil {
		t.Errorf("Status() after the rotation error = %v", err)
	}
}

// ownFiles copies m's files, so a test can break them without breaking etcd.
func ownFiles(t *testing.T, m *etcdtest.Member) etcdconf.Settings {
	t.Helper()
	dir := t.TempDir()
	s := etcdconf.Settings{
		Endpoint:      m.ClientURL,
		CertFile:      filepath.Join(dir, "tls.crt"),
		KeyFile:       filepath.Join(dir, "tls.key"),
		TrustedCAFile: filepath.Join(dir, "ca.crt"),
	}
	etcdtest.Replace(t, s.CertFile, m.Settings.CertFile)
	etcdtest.Replace(t, s.KeyFile, m.Settings.KeyFile)
	etcdtest.Replace(t, s.TrustedCAFile, m.Settings.TrustedCAFile)
	return s
}

// Status calls to members without a cached connection make new handshakes,
// which must use the files read before they broke.
func TestBrokenFilesKeepThePreviousConfig(t *testing.T) {
	cl := etcdtest.Start(t, 3)
	ctx := timeout(t)
	for name, breakFiles := range map[string]func(t *testing.T, s etcdconf.Settings){
		"empty CA": func(t *testing.T, s etcdconf.Settings) {
			if err := os.WriteFile(s.TrustedCAFile, nil, 0o600); err != nil {
				t.Fatal(err)
			}
		},
		"key of another certificate": func(t *testing.T, s etcdconf.Settings) {
			etcdtest.Replace(t, s.KeyFile, cl.Members[1].Settings.KeyFile)
		},
	} {
		t.Run(name, func(t *testing.T) {
			s := ownFiles(t, cl.Members[0])
			c, err := New(s)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = c.Close() })
			if _, err := c.Status(ctx, c.LocalURL()); err != nil {
				t.Fatal(err)
			}
			breakFiles(t, s)
			if changed, err := c.ReloadTLS(); changed || err == nil {
				t.Errorf("ReloadTLS() = %v, %v with broken files; want an error and no change", changed, err)
			}
			for _, m := range cl.Members {
				if _, err := c.Status(ctx, m.ClientURL); err != nil {
					t.Errorf("Status(%s) with the previous config error = %v", m.Name, err)
				}
			}
		})
	}
}

// Rotated files take effect only on ReloadTLS, so status calls in flight
// while the files change keep their connections.
func TestStatusCallsDuringRotation(t *testing.T) {
	cl := etcdtest.Start(t, 3)
	ctx := timeout(t)
	s := ownFiles(t, cl.Members[0])
	c, err := New(s)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })

	errs := make(chan error, len(cl.Members))
	var wg sync.WaitGroup
	for _, m := range cl.Members {
		wg.Go(func() {
			for range 20 {
				if _, err := c.Status(ctx, m.ClientURL); err != nil {
					errs <- err
					return
				}
			}
		})
	}
	other := cl.Members[1].Settings
	etcdtest.Replace(t, s.CertFile, other.CertFile)
	etcdtest.Replace(t, s.KeyFile, other.KeyFile)
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Errorf("Status() while the files changed error = %v", err)
	}
	if changed, err := c.ReloadTLS(); !changed || err != nil {
		t.Fatalf("ReloadTLS() = %v, %v; want a change", changed, err)
	}
	if _, err := c.Status(ctx, c.LocalURL()); err != nil {
		t.Errorf("Status() after ReloadTLS error = %v", err)
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

// eventuallyRecord waits until reader's view of member id's record satisfies
// want. Serializable reads on a member other than the writer may lag.
func eventuallyRecord(t *testing.T, reader *Client, id uint64, want func(Record, bool) bool) {
	t.Helper()
	ctx := timeout(t)
	deadline := time.Now().Add(10 * time.Second)
	for {
		records, err := reader.Records(ctx)
		if err != nil {
			t.Fatalf("Records() error = %v", err)
		}
		r, ok := records[id]
		if want(r, ok) {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("record of %x = %+v (present: %v), not as expected", id, r, ok)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestRecordLifecycle(t *testing.T) {
	cl := etcdtest.Start(t, 3)
	ctx := timeout(t)
	c := newClient(t, cl.Members[0])
	reader := newClient(t, cl.Members[1])
	id := cl.ID(t, cl.Members[0])

	lease, err := c.GrantLease(ctx, 5*time.Second)
	if err != nil {
		t.Fatalf("GrantLease() error = %v", err)
	}
	first := Record{SchemaVersion: RecordSchemaVersion, Identity: "zone-a", AgentVersion: "test", Sees: map[string]bool{"b": true, "c": true}}
	if err := c.PutRecord(ctx, id, first, lease); err != nil {
		t.Fatalf("PutRecord() error = %v", err)
	}
	eventuallyRecord(t, reader, id, func(r Record, ok bool) bool { return ok && reflect.DeepEqual(r, first) })

	second := first
	second.Sees = map[string]bool{"b": false, "c": true}
	if err := c.PutRecord(ctx, id, second, lease); err != nil {
		t.Fatalf("PutRecord() error = %v", err)
	}
	eventuallyRecord(t, reader, id, func(r Record, ok bool) bool { return ok && reflect.DeepEqual(r, second) })

	if err := c.RenewLease(ctx, lease); err != nil {
		t.Fatalf("RenewLease() error = %v", err)
	}
	if err := c.RevokeLease(ctx, lease); err != nil {
		t.Fatalf("RevokeLease() error = %v", err)
	}
	eventuallyRecord(t, reader, id, func(_ Record, ok bool) bool { return !ok })
	if err := c.RenewLease(ctx, lease); !errors.Is(err, ErrLeaseExpired) {
		t.Errorf("RenewLease() after revoke error = %v, want ErrLeaseExpired", err)
	}
}

func TestRecordExpiresWithoutRenewal(t *testing.T) {
	cl := etcdtest.Start(t, 3)
	ctx := timeout(t)
	c := newClient(t, cl.Members[0])
	id := cl.ID(t, cl.Members[0])
	lease, err := c.GrantLease(ctx, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := c.PutRecord(ctx, id, Record{SchemaVersion: RecordSchemaVersion}, lease); err != nil {
		t.Fatal(err)
	}
	eventuallyRecord(t, c, id, func(_ Record, ok bool) bool { return !ok })
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
	if !reflect.DeepEqual(records, want) {
		t.Errorf("records = %+v, want %+v", records, want)
	}
}
