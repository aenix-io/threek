package tlsfiles

import (
	"crypto/tls"
	"crypto/x509"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"

	"github.com/aenix-io/threek/internal/etcdtest"
)

type files struct {
	cert, key, ca string
}

func issue(t *testing.T) files {
	t.Helper()
	dir := t.TempDir()
	ca := etcdtest.NewCA(t, dir)
	cert, key := ca.Issue(t, dir, "agent")
	return files{cert: cert, key: key, ca: ca.CertFile}
}

// rotate replaces dst's files with src's the way kubelet updates a mounted
// Secret: the new content appears at once.
func rotate(t *testing.T, dst, src files) {
	t.Helper()
	for _, p := range [][2]string{{src.cert, dst.cert}, {src.key, dst.key}, {src.ca, dst.ca}} {
		content, err := os.ReadFile(p[0])
		if err != nil {
			t.Fatal(err)
		}
		tmp := filepath.Join(filepath.Dir(p[1]), ".tmp")
		if err := os.WriteFile(tmp, content, 0o600); err != nil {
			t.Fatal(err)
		}
		if err := os.Rename(tmp, p[1]); err != nil {
			t.Fatal(err)
		}
	}
}

// server starts a TLS server that presents a certificate from f and
// requires a client certificate signed by f's CA.
func server(t *testing.T, f files) string {
	t.Helper()
	cert, err := tls.LoadX509KeyPair(f.cert, f.key)
	if err != nil {
		t.Fatal(err)
	}
	pem, err := os.ReadFile(f.ca)
	if err != nil {
		t.Fatal(err)
	}
	pool := x509.NewCertPool()
	pool.AppendCertsFromPEM(pem)
	srv := httptest.NewUnstartedServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	srv.TLS = &tls.Config{Certificates: []tls.Certificate{cert}, ClientCAs: pool, ClientAuth: tls.RequireAndVerifyClientCert}
	srv.StartTLS()
	t.Cleanup(srv.Close)
	return srv.URL
}

func get(t *testing.T, cfg *tls.Config, url string) error {
	t.Helper()
	transport := &http.Transport{TLSClientConfig: cfg}
	defer transport.CloseIdleConnections()
	resp, err := (&http.Client{Transport: transport}).Get(url)
	if err != nil {
		return err
	}
	return resp.Body.Close()
}

func newSource(t *testing.T, f files) *Source {
	t.Helper()
	s, err := New(f.cert, f.key, f.ca)
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return s
}

func TestNewRejectsMissingFiles(t *testing.T) {
	f := issue(t)
	if _, err := New(f.cert, f.key, filepath.Join(t.TempDir(), "missing.crt")); err == nil {
		t.Error("New() error = nil with a missing CA file")
	}
	if _, err := New(filepath.Join(t.TempDir(), "missing.crt"), f.key, f.ca); err == nil {
		t.Error("New() error = nil with a missing certificate file")
	}
}

func TestConfigIsKeptWhileTheFilesStay(t *testing.T) {
	s := newSource(t, issue(t))
	first, _, err := s.Config()
	if err != nil {
		t.Fatal(err)
	}
	second, changed, err := s.Config()
	if err != nil {
		t.Fatal(err)
	}
	if changed || first != second {
		t.Errorf("Config() changed = %v, same config = %v; want the same config for unchanged files", changed, first == second)
	}
}

func TestRotationTakesEffect(t *testing.T) {
	old, rotated := issue(t), issue(t)
	url := server(t, rotated)
	s := newSource(t, old)
	cfg, _, err := s.Config()
	if err != nil {
		t.Fatal(err)
	}
	if err := get(t, cfg, url); err == nil {
		t.Fatal("a server of another CA accepted the agent before the rotation")
	}

	rotate(t, old, rotated)
	cfg, changed, err := s.Config()
	if err != nil {
		t.Fatalf("Config() after rotation error = %v", err)
	}
	if !changed {
		t.Error("Config() changed = false after rotation")
	}
	if err := get(t, cfg, url); err != nil {
		t.Errorf("handshake with the rotated files: %v", err)
	}
}

func TestBrokenFilesKeepThePreviousConfig(t *testing.T) {
	f := issue(t)
	s := newSource(t, f)
	before, _, err := s.Config()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(f.ca, []byte("not a certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	after, changed, err := s.Config()
	if err == nil {
		t.Error("Config() error = nil with a broken CA file")
	}
	if changed || after != before {
		t.Errorf("Config() changed = %v, same config = %v; want the previous config", changed, after == before)
	}
}

func TestServerVerificationStaysOn(t *testing.T) {
	s := newSource(t, issue(t))
	cfg, _, err := s.Config()
	if err != nil {
		t.Fatal(err)
	}
	if cfg.InsecureSkipVerify {
		t.Error("InsecureSkipVerify = true")
	}
}
