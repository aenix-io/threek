// Package etcdtest starts real etcd clusters for tests. The etcd binary is
// taken from TEST_ETCD_BINARY; tests are skipped when it is not set.
package etcdtest

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"fmt"
	"math/big"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"go.etcd.io/etcd/client/pkg/v3/transport"
	clientv3 "go.etcd.io/etcd/client/v3"
	"go.uber.org/zap"

	"github.com/aenix-io/threek/internal/etcdconf"
)

const binaryEnv = "TEST_ETCD_BINARY"

// Member is one etcd process of a test cluster.
type Member struct {
	Name      string
	ClientURL string
	PeerURL   string
	// Settings are the client settings the agent would resolve for this member.
	Settings etcdconf.Settings
	cmd      *exec.Cmd
	log      string
}

type Cluster struct {
	Members []*Member
}

// Binary returns the etcd binary or skips the test.
func Binary(t *testing.T) string {
	t.Helper()
	path := os.Getenv(binaryEnv)
	if path == "" {
		t.Skipf("%s is not set", binaryEnv)
	}
	return path
}

// Start starts an n-member cluster with client and peer mTLS and waits until
// it has a leader. The cluster is stopped when the test ends.
func Start(t *testing.T, n int) *Cluster {
	t.Helper()
	binary := Binary(t)
	dir := t.TempDir()
	ca := newCA(t, dir)

	cl := &Cluster{}
	var initial []string
	for i := range n {
		m := &Member{
			Name:      fmt.Sprintf("m%d", i),
			ClientURL: "https://" + freeAddr(t),
			PeerURL:   "https://" + freeAddr(t),
		}
		cert, key := ca.issue(t, dir, m.Name)
		m.Settings = etcdconf.Settings{
			CertFile:            cert,
			KeyFile:             key,
			TrustedCAFile:       ca.certFile,
			ListenClientURLs:    []string{m.ClientURL},
			AdvertiseClientURLs: []string{m.ClientURL},
		}
		cl.Members = append(cl.Members, m)
		initial = append(initial, m.Name+"="+m.PeerURL)
	}
	for _, m := range cl.Members {
		m.log = filepath.Join(dir, m.Name+".log")
		logFile, err := os.Create(m.log)
		if err != nil {
			t.Fatal(err)
		}
		m.cmd = exec.Command(binary,
			"--name="+m.Name,
			"--data-dir="+filepath.Join(dir, m.Name+".etcd"),
			"--listen-client-urls="+m.ClientURL,
			"--advertise-client-urls="+m.ClientURL,
			"--listen-peer-urls="+m.PeerURL,
			"--initial-advertise-peer-urls="+m.PeerURL,
			"--initial-cluster="+strings.Join(initial, ","),
			"--initial-cluster-token=etcdtest",
			"--cert-file="+m.Settings.CertFile,
			"--key-file="+m.Settings.KeyFile,
			"--trusted-ca-file="+ca.certFile,
			"--client-cert-auth",
			"--peer-cert-file="+m.Settings.CertFile,
			"--peer-key-file="+m.Settings.KeyFile,
			"--peer-trusted-ca-file="+ca.certFile,
			"--peer-client-cert-auth",
		)
		m.cmd.Stdout, m.cmd.Stderr = logFile, logFile
		if err := m.cmd.Start(); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			_ = m.cmd.Process.Signal(syscall.SIGTERM)
			_ = m.cmd.Wait()
			_ = logFile.Close()
			if t.Failed() {
				out, _ := os.ReadFile(m.log)
				t.Logf("%s log:\n%s", m.Name, tail(string(out), 40))
			}
		})
	}
	cl.waitForLeader(t)
	return cl
}

// Client returns a client connected to member m only.
func (cl *Cluster) Client(t *testing.T, m *Member) *clientv3.Client {
	t.Helper()
	tlsInfo := transport.TLSInfo{
		CertFile:      m.Settings.CertFile,
		KeyFile:       m.Settings.KeyFile,
		TrustedCAFile: m.Settings.TrustedCAFile,
	}
	tlsConfig, err := tlsInfo.ClientConfig()
	if err != nil {
		t.Fatal(err)
	}
	c, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{m.ClientURL},
		TLS:         tlsConfig,
		DialTimeout: 5 * time.Second,
		Logger:      zap.NewNop(),
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}

// Leader returns the member that leads, once every member agrees on it.
// Members briefly disagree while leadership moves.
func (cl *Cluster) Leader(t *testing.T) *Member {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for {
		if leader, ok := cl.agreedLeader(t); ok {
			return cl.ByID(t, leader)
		}
		if time.Now().After(deadline) {
			t.Fatal("members do not agree on the leader")
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (cl *Cluster) agreedLeader(t *testing.T) (uint64, bool) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var leader uint64
	for _, m := range cl.Members {
		st, err := cl.Client(t, m).Status(ctx, m.ClientURL)
		if err != nil {
			t.Fatalf("status of %s: %v", m.Name, err)
		}
		if st.Leader == 0 || leader != 0 && st.Leader != leader {
			return 0, false
		}
		leader = st.Leader
	}
	return leader, true
}

// ID returns the member ID of m.
func (cl *Cluster) ID(t *testing.T, m *Member) uint64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	st, err := cl.Client(t, m).Status(ctx, m.ClientURL)
	if err != nil {
		t.Fatal(err)
	}
	return st.Header.MemberId
}

// ByID returns the member with the given ID.
func (cl *Cluster) ByID(t *testing.T, id uint64) *Member {
	t.Helper()
	for _, m := range cl.Members {
		if cl.ID(t, m) == id {
			return m
		}
	}
	t.Fatalf("no member with ID %x", id)
	return nil
}

func (cl *Cluster) waitForLeader(t *testing.T) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		if cl.allHaveLeader(t) {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatal("cluster did not elect a leader")
}

func (cl *Cluster) allHaveLeader(t *testing.T) bool {
	for _, m := range cl.Members {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		st, err := cl.Client(t, m).Status(ctx, m.ClientURL)
		cancel()
		if err != nil || st.Leader == 0 {
			return false
		}
	}
	return true
}

func freeAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	return l.Addr().String()
}

func tail(s string, lines int) string {
	all := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(all) > lines {
		all = all[len(all)-lines:]
	}
	return strings.Join(all, "\n")
}

type ca struct {
	cert     *x509.Certificate
	key      *ecdsa.PrivateKey
	certFile string
}

func newCA(t *testing.T, dir string) *ca {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "etcdtest-ca"},
		NotBefore:             time.Now().Add(-time.Hour),
		NotAfter:              time.Now().Add(24 * time.Hour),
		IsCA:                  true,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true,
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatal(err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatal(err)
	}
	c := &ca{cert: cert, key: key, certFile: filepath.Join(dir, "ca.crt")}
	writePEM(t, c.certFile, "CERTIFICATE", der)
	return c
}

// issue creates a certificate usable as etcd server, peer and client
// certificate, like the ones Talos issues.
func (c *ca) issue(t *testing.T, dir, name string) (certFile, keyFile string) {
	t.Helper()
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	serial, err := rand.Int(rand.Reader, big.NewInt(1<<62))
	if err != nil {
		t.Fatal(err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: serial,
		Subject:      pkix.Name{CommonName: name},
		NotBefore:    time.Now().Add(-time.Hour),
		NotAfter:     time.Now().Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth, x509.ExtKeyUsageClientAuth},
		DNSNames:     []string{"localhost"},
		IPAddresses:  []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, c.cert, &key.PublicKey, c.key)
	if err != nil {
		t.Fatal(err)
	}
	keyDER, err := x509.MarshalECPrivateKey(key)
	if err != nil {
		t.Fatal(err)
	}
	certFile = filepath.Join(dir, name+".crt")
	keyFile = filepath.Join(dir, name+".key")
	writePEM(t, certFile, "CERTIFICATE", der)
	writePEM(t, keyFile, "EC PRIVATE KEY", keyDER)
	return certFile, keyFile
}

func writePEM(t *testing.T, path, typ string, der []byte) {
	t.Helper()
	if err := os.WriteFile(path, pem.EncodeToMemory(&pem.Block{Type: typ, Bytes: der}), 0o600); err != nil {
		t.Fatal(err)
	}
}
