// Package probe checks whether etcd peers are reachable, with the same
// request etcd's own prober sends to the peer port.
package probe

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/aenix-io/threek/internal/etcdconf"
	"github.com/aenix-io/threek/internal/tlsfiles"
)

// path is etcd's rafthttp.ProbingPrefix. A bare TCP connect would not do:
// etcd logs every peer connection that closes before the TLS handshake.
const path = "/raft/probing"

// maxBody bounds how much of a probe response is read; etcd answers with a
// few dozen bytes.
const maxBody = 64 << 10

// idleTimeout closes a connection that no probe has used for a while. It
// also bounds the connection of a probe still in flight during ReloadTLS,
// which returns to a transport that is never closed again.
const idleTimeout = 90 * time.Second

type Prober struct {
	files   *tlsfiles.Source
	timeout time.Duration

	mu     sync.Mutex
	client *http.Client
}

// New returns a prober that authenticates with the member's peer certificate
// and gives up on a peer after timeout.
func New(s etcdconf.Settings, timeout time.Duration) (*Prober, error) {
	p := &Prober{timeout: timeout}
	var tlsConfig *tls.Config
	if s.PeerTrustedCAFile != "" {
		var err error
		if p.files, err = tlsfiles.New(s.PeerCertFile, s.PeerKeyFile, s.PeerTrustedCAFile); err != nil {
			return nil, fmt.Errorf("peer TLS: %w", err)
		}
		tlsConfig = p.files.Config()
	}
	p.client = newHTTPClient(tlsConfig)
	return p, nil
}

func newHTTPClient(tlsConfig *tls.Config) *http.Client {
	return &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig, IdleConnTimeout: idleTimeout}}
}

// VerifiesTLS reports whether a peer CA was given, without which no peer
// serving TLS can be verified.
func (p *Prober) VerifiesTLS() bool {
	return p.files != nil
}

func (p *Prober) Close() {
	p.httpClient().CloseIdleConnections()
}

// ReloadTLS reads the peer certificate files again. When they changed, the
// idle connections made with the old files are closed; a probe in flight
// keeps its connection until idleTimeout. When the files are broken, the
// previous files stay in use.
func (p *Prober) ReloadTLS() (changed bool, err error) {
	if p.files == nil {
		return false, nil
	}
	changed, err = p.files.Reload()
	if err != nil {
		return false, fmt.Errorf("peer TLS: %w", err)
	}
	if changed {
		p.mu.Lock()
		old := p.client
		p.client = newHTTPClient(p.files.Config())
		p.mu.Unlock()
		old.CloseIdleConnections()
	}
	return changed, nil
}

func (p *Prober) httpClient() *http.Client {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.client
}

// Reachable reports whether any of a member's peer URLs answers in time.
func (p *Prober) Reachable(ctx context.Context, peerURLs []string) bool {
	for _, u := range peerURLs {
		if p.probe(ctx, strings.TrimSuffix(u, "/")+path) {
			return true
		}
	}
	return false
}

func (p *Prober) probe(ctx context.Context, url string) bool {
	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, http.NoBody)
	if err != nil {
		return false
	}
	resp, err := p.httpClient().Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	// An unread body keeps the connection from being reused, and a new
	// connection every probe costs a TLS handshake.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxBody))
	return resp.StatusCode == http.StatusOK
}
