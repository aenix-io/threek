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

func rotate(t *testing.T, dst, src files) {
	t.Helper()
	etcdtest.Replace(t, dst.cert, src.cert)
	etcdtest.Replace(t, dst.key, src.key)
	etcdtest.Replace(t, dst.ca, src.ca)
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
	if _, err := New(f.cert, "", f.ca); err == nil {
		t.Error("New() error = nil with a certificate file but no key file")
	}
}

func TestUnchangedFilesKeepTheConfig(t *testing.T) {
	s := newSource(t, issue(t))
	before := s.Config()
	changed, err := s.Reload()
	if err != nil {
		t.Fatal(err)
	}
	if changed || s.Config() != before {
		t.Errorf("Reload() changed = %v, same config = %v; want the same config for unchanged files", changed, s.Config() == before)
	}
}

func TestRotationTakesEffect(t *testing.T) {
	old, rotated := issue(t), issue(t)
	url := server(t, rotated)
	s := newSource(t, old)
	if err := get(t, s.Config(), url); err == nil {
		t.Fatal("a server of another CA accepted the agent before the rotation")
	}

	rotate(t, old, rotated)
	changed, err := s.Reload()
	if err != nil {
		t.Fatalf("Reload() after rotation error = %v", err)
	}
	if !changed {
		t.Error("Reload() changed = false after rotation")
	}
	if err := get(t, s.Config(), url); err != nil {
		t.Errorf("handshake with the rotated files: %v", err)
	}
}

func write(t *testing.T, path string, content []byte) {
	t.Helper()
	if err := os.WriteFile(path, content, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestBrokenFilesKeepThePreviousConfig(t *testing.T) {
	for name, breakFiles := range map[string]func(t *testing.T, f files){
		"empty CA": func(t *testing.T, f files) { write(t, f.ca, nil) },
		"not a CA": func(t *testing.T, f files) { write(t, f.ca, []byte("not a certificate")) },
		"missing CA": func(t *testing.T, f files) {
			if err := os.Remove(f.ca); err != nil {
				t.Fatal(err)
			}
		},
		// Talos rewrites the files in place, one after another.
		"key of another certificate":  func(t *testing.T, f files) { etcdtest.Replace(t, f.key, issue(t).key) },
		"certificate of another key":  func(t *testing.T, f files) { etcdtest.Replace(t, f.cert, issue(t).cert) },
		"certificate without its key": func(t *testing.T, f files) { write(t, f.key, nil) },
	} {
		t.Run(name, func(t *testing.T) {
			f := issue(t)
			url := server(t, f)
			s := newSource(t, f)
			before := s.Config()
			breakFiles(t, f)
			for range 2 {
				changed, err := s.Reload()
				if err == nil {
					t.Error("Reload() error = nil with broken files")
				}
				if changed || s.Config() != before {
					t.Errorf("Reload() changed = %v, same config = %v; want the previous config", changed, s.Config() == before)
				}
			}
			if err := get(t, s.Config(), url); err != nil {
				t.Errorf("handshake with the previous config: %v", err)
			}
		})
	}
}

func TestServerVerificationStaysOn(t *testing.T) {
	if newSource(t, issue(t)).Config().InsecureSkipVerify {
		t.Error("InsecureSkipVerify = true")
	}
}
