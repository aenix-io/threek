package probe

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"net"
	"net/http"
	"net/http/httptest"
	"net/http/httptrace"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aenix-io/threek/internal/etcdconf"
	"github.com/aenix-io/threek/internal/etcdtest"
)

const timeout = 300 * time.Millisecond

// peer starts a TLS server that requires a client certificate from ca, like
// etcd's peer listener, and returns its URL.
func peer(t *testing.T, ca *etcdtest.CA, dir string, handler http.HandlerFunc) string {
	t.Helper()
	return peerServer(t, ca, dir, handler, nil).URL
}

func peerServer(t *testing.T, ca *etcdtest.CA, dir string, handler http.HandlerFunc, connState func(net.Conn, http.ConnState)) *httptest.Server {
	t.Helper()
	certFile, keyFile := ca.Issue(t, dir, "server")
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pem, err := os.ReadFile(ca.CertFile)
	if err != nil {
		t.Fatal(err)
	}
	pool.AppendCertsFromPEM(pem)
	srv := httptest.NewUnstartedServer(handler)
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert}
	srv.Config.ConnState = connState
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv
}

func newProber(t *testing.T) (*Prober, *etcdtest.CA, string) {
	t.Helper()
	dir := t.TempDir()
	ca := etcdtest.NewCA(t, dir)
	certFile, keyFile := ca.Issue(t, dir, "client")
	p, err := New(etcdconf.Settings{PeerCertFile: certFile, PeerKeyFile: keyFile, PeerTrustedCAFile: ca.CertFile}, timeout)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	t.Cleanup(p.Close)
	return p, ca, dir
}

func probing(t *testing.T) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/raft/probing" {
			t.Errorf("probe path = %s, want /raft/probing", r.URL.Path)
		}
		_, _ = w.Write([]byte(`{"Hash":"","Now":"2026-01-01T00:00:00Z"}`))
	}
}

func TestReachable(t *testing.T) {
	p, ca, dir := newProber(t)
	url := peer(t, ca, dir, probing(t))
	for range 3 {
		if !p.Reachable(context.Background(), []string{url}) {
			t.Fatal("Reachable() = false, want true")
		}
	}
}

func TestUnresponsivePeer(t *testing.T) {
	p, ca, dir := newProber(t)
	release := make(chan struct{})
	url := peer(t, ca, dir, func(http.ResponseWriter, *http.Request) { <-release })
	defer close(release)
	start := time.Now()
	if p.Reachable(context.Background(), []string{url}) {
		t.Error("Reachable() = true for a peer that never answers")
	}
	if elapsed := time.Since(start); elapsed > 3*timeout {
		t.Errorf("probe took %s, want about %s", elapsed, timeout)
	}
}

func TestBadStatus(t *testing.T) {
	p, ca, dir := newProber(t)
	url := peer(t, ca, dir, func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusServiceUnavailable) })
	if p.Reachable(context.Background(), []string{url}) {
		t.Error("Reachable() = true for a failing peer")
	}
}

func TestClosedPort(t *testing.T) {
	p, _, _ := newProber(t)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	if p.Reachable(context.Background(), []string{"https://" + addr}) {
		t.Error("Reachable() = true for a closed port")
	}
}

func TestAnyURLIsEnough(t *testing.T) {
	p, ca, dir := newProber(t)
	url := peer(t, ca, dir, probing(t))
	if !p.Reachable(context.Background(), []string{"https://127.0.0.1:1", url}) {
		t.Error("Reachable() = false although the second URL answers")
	}
}

func TestUntrustedPeer(t *testing.T) {
	p, _, _ := newProber(t)
	otherDir := t.TempDir()
	url := peer(t, etcdtest.NewCA(t, otherDir), otherDir, probing(t))
	if p.Reachable(context.Background(), []string{url}) {
		t.Error("Reachable() = true for a peer signed by another CA")
	}
}

func TestRealEtcdPeer(t *testing.T) {
	cl := etcdtest.Start(t, 3)
	p, err := New(cl.Members[0].Settings, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	for _, m := range cl.Members[1:] {
		if !p.Reachable(context.Background(), []string{m.PeerURL}) {
			t.Errorf("peer %s is not reachable", m.Name)
		}
	}
}

func TestRealEtcdPeerKeepsTheConnection(t *testing.T) {
	cl := etcdtest.Start(t, 3)
	p, err := New(cl.Members[0].Settings, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer p.Close()
	var reused atomic.Int32
	ctx := httptrace.WithClientTrace(context.Background(), &httptrace.ClientTrace{
		GotConn: func(info httptrace.GotConnInfo) {
			if info.Reused {
				reused.Add(1)
			}
		},
	})
	for range 5 {
		if !p.Reachable(ctx, []string{cl.Members[1].PeerURL}) {
			t.Fatal("Reachable() = false, want true")
		}
	}
	if n := reused.Load(); n != 4 {
		t.Errorf("%d of 5 probes reused the connection, want 4", n)
	}
}

func TestVerifiesTLS(t *testing.T) {
	plain, err := New(etcdconf.Settings{}, timeout)
	if err != nil {
		t.Fatal(err)
	}
	if plain.VerifiesTLS() {
		t.Error("VerifiesTLS() = true without a peer CA")
	}
	dir := t.TempDir()
	ca := etcdtest.NewCA(t, dir)
	cert, key := ca.Issue(t, dir, "agent")
	p, err := New(etcdconf.Settings{PeerCertFile: cert, PeerKeyFile: key, PeerTrustedCAFile: ca.CertFile}, timeout)
	if err != nil {
		t.Fatal(err)
	}
	if !p.VerifiesTLS() {
		t.Error("VerifiesTLS() = false with a peer CA")
	}
}

// Every new connection costs a TLS handshake, and probes run every second
// against every peer: they must reuse one connection per peer.
func TestProbesReuseTheConnection(t *testing.T) {
	p, ca, dir := newProber(t)
	var conns atomic.Int32
	srv := peerServer(t, ca, dir, probing(t), func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			conns.Add(1)
		}
	})
	for range 5 {
		if !p.Reachable(context.Background(), []string{srv.URL}) {
			t.Fatal("Reachable() = false, want true")
		}
	}
	if n := conns.Load(); n != 1 {
		t.Errorf("5 probes opened %d connections, want 1", n)
	}
}
