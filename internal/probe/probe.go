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
	"time"

	"go.etcd.io/etcd/client/pkg/v3/transport"

	"github.com/aenix-io/threek/internal/etcdconf"
)

// path is etcd's rafthttp.ProbingPrefix. A bare TCP connect would not do:
// etcd logs every peer connection that closes before the TLS handshake.
const path = "/raft/probing"

// maxBody bounds how much of a probe response is read; etcd answers with a
// few dozen bytes.
const maxBody = 64 << 10

type Prober struct {
	client      *http.Client
	timeout     time.Duration
	verifiesTLS bool
}

// New returns a prober that authenticates with the member's peer certificate
// and gives up on a peer after timeout.
func New(s etcdconf.Settings, timeout time.Duration) (*Prober, error) {
	var tlsConfig *tls.Config
	if s.PeerTrustedCAFile != "" {
		info := transport.TLSInfo{CertFile: s.PeerCertFile, KeyFile: s.PeerKeyFile, TrustedCAFile: s.PeerTrustedCAFile}
		var err error
		if tlsConfig, err = info.ClientConfig(); err != nil {
			return nil, fmt.Errorf("peer TLS: %w", err)
		}
	}
	return &Prober{
		client:      &http.Client{Transport: &http.Transport{TLSClientConfig: tlsConfig}},
		timeout:     timeout,
		verifiesTLS: tlsConfig != nil,
	}, nil
}

// VerifiesTLS reports whether a peer CA was given, without which no peer
// serving TLS can be verified.
func (p *Prober) VerifiesTLS() bool {
	return p.verifiesTLS
}

func (p *Prober) Close() {
	p.client.CloseIdleConnections()
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
	resp, err := p.client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	// An unread body keeps the connection from being reused, and a new
	// connection every probe costs a TLS handshake.
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, maxBody))
	return resp.StatusCode == http.StatusOK
}
