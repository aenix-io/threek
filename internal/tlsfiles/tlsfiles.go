// Package tlsfiles builds client TLS configs from certificate files and
// rebuilds them when the files change, so a rotation in place takes effect
// without a restart.
package tlsfiles

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"sync"
)

// Source is a client TLS config backed by a certificate, its key and the CA
// that verifies servers. Any of them may be empty, but not the certificate
// without its key.
//
// The config holds the certificate and CA as read by the last successful
// Reload and never reads the files on a handshake, so files rewritten one
// after another cannot break new connections halfway through a rotation.
type Source struct {
	certFile, keyFile, caFile string

	mu      sync.Mutex
	content []byte
	config  *tls.Config
}

// New reads the files and fails when they do not make a valid config.
func New(certFile, keyFile, caFile string) (*Source, error) {
	if (certFile == "") != (keyFile == "") {
		return nil, errors.New("a certificate file needs a key file and the other way round")
	}
	s := &Source{certFile: certFile, keyFile: keyFile, caFile: caFile}
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
	var cert, key, ca []byte
	for _, f := range []struct {
		path    string
		content *[]byte
	}{{s.certFile, &cert}, {s.keyFile, &key}, {s.caFile, &ca}} {
		if f.path == "" {
			continue
		}
		if *f.content, err = os.ReadFile(f.path); err != nil {
			return false, err
		}
	}
	content := bytes.Join([][]byte{cert, key, ca}, []byte{0})
	if s.config != nil && bytes.Equal(content, s.content) {
		return false, nil
	}
	cfg, err := s.build(cert, key, ca)
	if err != nil {
		return false, err
	}
	changed = s.config != nil
	s.config, s.content = cfg, content
	return changed, nil
}

func (s *Source) build(cert, key, ca []byte) (*tls.Config, error) {
	cfg := &tls.Config{MinVersion: tls.VersionTLS12}
	if s.certFile != "" {
		pair, err := tls.X509KeyPair(cert, key)
		if err != nil {
			return nil, fmt.Errorf("%s and %s: %w", s.certFile, s.keyFile, err)
		}
		// Certificates would be offered only to servers whose accepted CAs
		// include the issuer; like etcd, always present the certificate and
		// let the server decide.
		cfg.GetClientCertificate = func(*tls.CertificateRequestInfo) (*tls.Certificate, error) {
			return &pair, nil
		}
	}
	if s.caFile != "" {
		cfg.RootCAs = x509.NewCertPool()
		// An empty pool would trust no server.
		if !cfg.RootCAs.AppendCertsFromPEM(ca) {
			return nil, fmt.Errorf("no certificate in %s", s.caFile)
		}
	}
	return cfg, nil
}
