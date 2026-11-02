// Package tlsfiles builds client TLS configs from certificate files and
// rebuilds them when the files change, so a rotation in place takes effect
// without a restart.
package tlsfiles

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"os"
	"sync"

	"go.etcd.io/etcd/client/pkg/v3/transport"
)

// Source is a client TLS config backed by a certificate, its key and the CA
// that verifies servers. Any of them may be empty.
//
// The config is rebuilt rather than verified by hand on every handshake:
// etcd already reloads the certificate itself per handshake, but the CA pool
// cannot be swapped without turning off Go's own server verification.
type Source struct {
	info transport.TLSInfo

	mu      sync.Mutex
	content []byte
	config  *tls.Config
}

// New reads the files and fails when they do not make a valid config.
func New(certFile, keyFile, caFile string) (*Source, error) {
	s := &Source{
		info: transport.TLSInfo{CertFile: certFile, KeyFile: keyFile, TrustedCAFile: caFile},
	}
	if _, err := s.Reload(); err != nil {
		return nil, err
	}
	return s, nil
}

// Config returns the config of the files as last loaded.
func (s *Source) Config() *tls.Config {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.config
}

// Reload reads the files again and reports whether the config changed.
// Connections made with the previous config keep presenting the old
// certificate and trusting the old CA. When the files cannot be read or do
// not make a valid config, the previous config stays.
func (s *Source) Reload() (changed bool, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	content, err := s.read()
	if err != nil {
		return false, err
	}
	if s.config != nil && bytes.Equal(content, s.content) {
		return false, nil
	}
	// ClientConfig reads the files once more; a change in between only makes
	// the next Reload rebuild the config again.
	cfg, err := s.info.ClientConfig()
	if err != nil {
		return false, err
	}
	changed = s.config != nil
	s.config, s.content = cfg, content
	return changed, nil
}

// read returns the content of every file. It rejects a CA file without
// certificates, which etcd would turn into a pool that trusts no server.
func (s *Source) read() ([]byte, error) {
	var all []byte
	for _, f := range []string{s.info.CertFile, s.info.KeyFile, s.info.TrustedCAFile} {
		if f == "" {
			continue
		}
		content, err := os.ReadFile(f) //nolint:gosec // the paths are the agent's own flags
		if err != nil {
			return nil, err
		}
		if f == s.info.TrustedCAFile && !x509.NewCertPool().AppendCertsFromPEM(content) {
			return nil, fmt.Errorf("no certificate in %s", f)
		}
		all = append(all, content...)
		all = append(all, 0)
	}
	return all, nil
}
