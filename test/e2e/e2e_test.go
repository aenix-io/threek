//go:build e2e

// Package e2e runs three-member clusters of the threek image in containers
// and breaks the network between them. E2E_IMAGE names the image under test.
//
// Containers run the way Talos runs etcd: an explicit command line, a
// non-root user, no capabilities, client and peer mTLS.
package e2e

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aenix-io/threek/internal/etcdtest"
)

const (
	nettoolsBase = "docker.io/library/alpine:3.24.2@sha256:294b683cb724975bec92580e1e685676bd4b50bda910ddb8c51d4cabeaec77e6"
	nettools     = "threek-e2e-nettools"
	subnet       = "172.31.250.0/24"
	pki          = "/etc/etcd/pki"
)

var image string

func TestMain(m *testing.M) {
	image = os.Getenv("E2E_IMAGE")
	if image == "" {
		fmt.Println("E2E_IMAGE is not set, skipping end-to-end tests")
		os.Exit(0)
	}
	build := exec.Command("docker", "build", "--quiet", "--tag", nettools, "-")
	build.Stdin = strings.NewReader("FROM " + nettoolsBase + "\nRUN apk add --no-cache curl iptables\n")
	build.Stderr = os.Stderr
	if err := build.Run(); err != nil {
		fmt.Println("build network tools image:", err)
		os.Exit(1)
	}
	os.Exit(m.Run())
}

func docker(args ...string) (string, error) {
	var stdout, stderr bytes.Buffer
	cmd := exec.Command("docker", args...)
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("docker %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(stdout.String()), nil
}

func mustDocker(t *testing.T, args ...string) string {
	t.Helper()
	out, err := docker(args...)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

type member struct {
	name      string
	ip        string
	container string
	id        uint64
}

type cluster struct {
	t       *testing.T
	pkiDir  string
	members []*member
}

func (c *cluster) member(name string) *member {
	for _, m := range c.members {
		if m.name == name {
			return m
		}
	}
	c.t.Fatalf("no member %s", name)
	return nil
}

// start runs members a, b and c with extra threek flags and waits for a leader.
func start(t *testing.T, threekFlags ...string) *cluster {
	t.Helper()
	suffix := make([]byte, 3)
	_, _ = rand.Read(suffix)
	prefix := "threek-e2e-" + hex.EncodeToString(suffix)
	dir := t.TempDir()
	ca := etcdtest.NewCA(t, dir)

	network := mustDocker(t, "network", "create", "--subnet", subnet, prefix)
	t.Cleanup(func() { _, _ = docker("network", "rm", network) })

	c := &cluster{t: t, pkiDir: dir}
	var initial []string
	for i, name := range []string{"a", "b", "c"} {
		m := &member{name: name, ip: fmt.Sprintf("172.31.250.%d", 11+i), container: prefix + "-" + name}
		ca.Issue(t, dir, name, net.ParseIP(m.ip))
		c.members = append(c.members, m)
		initial = append(initial, fmt.Sprintf("%s=https://%s:2380", name, m.ip))
	}
	for _, m := range c.members {
		args := []string{
			"run", "--detach", "--name", m.container,
			"--network", network, "--ip", m.ip,
			"--user", "60:60", "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
			"--read-only", "--tmpfs", "/var/lib/etcd:uid=60,gid=60,mode=0700",
			"--volume", dir + ":" + pki + ":ro",
			image, "/usr/local/bin/etcd",
			"--name=" + m.name,
			"--data-dir=/var/lib/etcd",
			"--listen-client-urls=https://0.0.0.0:2379",
			"--advertise-client-urls=https://" + m.ip + ":2379",
			"--listen-peer-urls=https://0.0.0.0:2380",
			"--initial-advertise-peer-urls=https://" + m.ip + ":2380",
			"--initial-cluster=" + strings.Join(initial, ","),
			"--initial-cluster-state=new",
			"--initial-cluster-token=" + prefix,
			"--client-cert-auth=true",
			"--cert-file=" + pki + "/" + m.name + ".crt",
			"--key-file=" + pki + "/" + m.name + ".key",
			"--trusted-ca-file=" + pki + "/ca.crt",
			"--peer-client-cert-auth=true",
			"--peer-cert-file=" + pki + "/" + m.name + ".crt",
			"--peer-key-file=" + pki + "/" + m.name + ".key",
			"--peer-trusted-ca-file=" + pki + "/ca.crt",
			"--threek-identity=zone-" + m.name,
		}
		mustDocker(t, append(args, threekFlags...)...)
		t.Cleanup(func() {
			if t.Failed() {
				logs, _ := exec.Command("docker", "logs", "--tail", "60", m.container).CombinedOutput()
				t.Logf("logs of %s:\n%s", m.name, logs)
			}
			_, _ = docker("rm", "--force", m.container)
		})
	}

	eventually(t, time.Minute, "cluster elects a leader", func() (bool, string) {
		leader, ok := c.agreedLeader()
		return ok && leader != 0, ""
	})
	for _, m := range c.members {
		st, err := c.status(m)
		if err != nil {
			t.Fatal(err)
		}
		m.id = st.Header.MemberID
	}
	return c
}

func (c *cluster) etcdctl(m *member, args ...string) (string, error) {
	base := []string{
		"exec", m.container, "/usr/local/bin/etcdctl",
		"--endpoints=https://127.0.0.1:2379",
		"--cacert=" + pki + "/ca.crt",
		"--cert=" + pki + "/" + m.name + ".crt",
		"--key=" + pki + "/" + m.name + ".key",
		"--command-timeout=3s",
	}
	return docker(append(base, args...)...)
}

type status struct {
	Header struct {
		MemberID uint64 `json:"member_id"`
	} `json:"header"`
	Leader uint64 `json:"leader"`
}

func (c *cluster) status(m *member) (status, error) {
	out, err := c.etcdctl(m, "endpoint", "status", "--write-out=json")
	if err != nil {
		return status{}, err
	}
	var resp []struct {
		Status status `json:"Status"`
	}
	if err := json.Unmarshal([]byte(out), &resp); err != nil {
		return status{}, fmt.Errorf("parse status of %s: %w: %s", m.name, err, out)
	}
	if len(resp) != 1 {
		return status{}, fmt.Errorf("status of %s has %d entries: %s", m.name, len(resp), out)
	}
	return resp[0].Status, nil
}

// leaderSeenBy returns the leader as member m sees it; 0 means none.
func (c *cluster) leaderSeenBy(m *member) uint64 {
	st, err := c.status(m)
	if err != nil {
		return 0
	}
	return st.Leader
}

func (c *cluster) agreedLeader() (uint64, bool) {
	var leader uint64
	for _, m := range c.members {
		l := c.leaderSeenBy(m)
		if l == 0 || leader != 0 && l != leader {
			return 0, false
		}
		leader = l
	}
	return leader, true
}

func (c *cluster) moveLeaderTo(m *member) {
	c.t.Helper()
	leader, ok := c.agreedLeader()
	if !ok {
		c.t.Fatal("members do not agree on the leader")
	}
	if leader != m.id {
		var current *member
		for _, x := range c.members {
			if x.id == leader {
				current = x
			}
		}
		if _, err := c.etcdctl(current, "move-leader", strconv.FormatUint(m.id, 16)); err != nil {
			c.t.Fatal(err)
		}
	}
	eventually(c.t, 30*time.Second, "leader moves to "+m.name, func() (bool, string) {
		l, ok := c.agreedLeader()
		return ok && l == m.id, ""
	})
}

// metrics returns the agent metrics of member m.
func (c *cluster) metrics(m *member) string {
	c.t.Helper()
	return mustDocker(c.t, "run", "--rm", "--network", "container:"+m.container, nettools,
		"curl", "--silent", "--fail", "http://127.0.0.1:2390/metrics")
}

// partition drops all traffic between x and y in both directions, without
// resets, like a broken WAN link.
func (c *cluster) partition(x, y *member) {
	c.t.Helper()
	for _, pair := range [][2]*member{{x, y}, {y, x}} {
		mustDocker(c.t, "run", "--rm", "--network", "container:"+pair[0].container, "--cap-add", "NET_ADMIN", nettools,
			"sh", "-c", fmt.Sprintf("iptables -I INPUT -s %[1]s -j DROP && iptables -I OUTPUT -d %[1]s -j DROP", pair[1].ip))
	}
}

// heal removes the rules partition added between x and y.
func (c *cluster) heal(x, y *member) {
	c.t.Helper()
	for _, pair := range [][2]*member{{x, y}, {y, x}} {
		mustDocker(c.t, "run", "--rm", "--network", "container:"+pair[0].container, "--cap-add", "NET_ADMIN", nettools,
			"sh", "-c", fmt.Sprintf("iptables -D INPUT -s %[1]s -j DROP && iptables -D OUTPUT -d %[1]s -j DROP", pair[1].ip))
	}
}

// flap breaks the link between x and y for down every period, until the test
// ends. Rules in x's network namespace alone cut the link both ways.
func (c *cluster) flap(x, y *member, down, period time.Duration) {
	c.t.Helper()
	name := x.container + "-flap"
	script := fmt.Sprintf("while true; do "+
		"iptables -I INPUT -s %[1]s -j DROP; iptables -I OUTPUT -d %[1]s -j DROP; sleep %[2]g; "+
		"iptables -D INPUT -s %[1]s -j DROP; iptables -D OUTPUT -d %[1]s -j DROP; sleep %[3]g; done",
		y.ip, down.Seconds(), (period - down).Seconds())
	mustDocker(c.t, "run", "--detach", "--name", name, "--network", "container:"+x.container, "--cap-add", "NET_ADMIN", nettools, "sh", "-c", script)
	c.t.Cleanup(func() { _, _ = docker("rm", "--force", name) })
}

// leaderIs reports whether every member in members sees m as the leader.
func (c *cluster) leaderIs(m *member, members ...*member) (bool, string) {
	var seen []string
	ok := true
	for _, x := range members {
		l := c.leaderSeenBy(x)
		seen = append(seen, fmt.Sprintf("%s sees %x", x.name, l))
		ok = ok && l == m.id
	}
	return ok, "(" + strings.Join(seen, ", ") + ")"
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for {
		ok, detail := cond()
		if ok {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting: %s %s", what, detail)
		}
		time.Sleep(time.Second)
	}
}

func always(t *testing.T, period time.Duration, what string, cond func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(period)
	for time.Now().Before(deadline) {
		if ok, detail := cond(); !ok {
			t.Fatalf("violated: %s %s", what, detail)
		}
		time.Sleep(2 * time.Second)
	}
}

func TestPartialPartitionMovesLeaderToMemberThatSeesEveryone(t *testing.T) {
	c := start(t, "--threek-mode=enforce", "--threek-move-cooldown=10s", "--threek-metrics-listen=127.0.0.1:2390")
	a, b, w := c.member("a"), c.member("b"), c.member("c")
	c.moveLeaderTo(a)

	c.partition(a, b)
	broken := time.Now()
	eventually(t, 90*time.Second, "leader moves to c and b follows it", func() (bool, string) {
		seenByC, seenByB := c.leaderSeenBy(w), c.leaderSeenBy(b)
		return seenByC == w.id && seenByB == w.id, fmt.Sprintf("(c sees %x, b sees %x)", seenByC, seenByB)
	})
	t.Logf("leader moved to c %s after the partition", time.Since(broken).Round(time.Second))

	if _, err := c.etcdctl(b, "put", "e2e", "written-through-b"); err != nil {
		t.Errorf("write through b after the transfer: %v", err)
	}
	if m := c.metrics(a); !strings.Contains(m, `threek_transfers_total{reason="partition",result="success"} 1`) {
		t.Errorf("metrics of a do not count the transfer:\n%s", m)
	}
}

func TestSiteFailureKeepsLeader(t *testing.T) {
	c := start(t, "--threek-mode=enforce", "--threek-move-cooldown=10s")
	a, b, w := c.member("a"), c.member("b"), c.member("c")
	c.moveLeaderTo(a)

	mustDocker(t, "stop", b.container)
	always(t, 45*time.Second, "leader stays on a", func() (bool, string) {
		seenByA, seenByC := c.leaderSeenBy(a), c.leaderSeenBy(w)
		return seenByA == a.id && seenByC == a.id, fmt.Sprintf("(a sees %x, c sees %x)", seenByA, seenByC)
	})
}

func TestDryRunLeavesTheProblemInPlace(t *testing.T) {
	c := start(t, "--threek-mode=dry-run", "--threek-move-cooldown=10s")
	a, b, w := c.member("a"), c.member("b"), c.member("c")
	c.moveLeaderTo(a)

	c.partition(a, b)
	eventually(t, 30*time.Second, "b loses the leader", func() (bool, string) {
		return c.leaderSeenBy(b) == 0, ""
	})
	always(t, 45*time.Second, "leader stays on a and b stays leaderless", func() (bool, string) {
		seenByC, seenByB := c.leaderSeenBy(w), c.leaderSeenBy(b)
		return seenByC == a.id && seenByB == 0, fmt.Sprintf("(c sees %x, b sees %x)", seenByC, seenByB)
	})
	logs, err := exec.Command("docker", "logs", a.container).CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(logs, []byte("would transfer leadership")) {
		t.Error("the leader's agent did not log the transfer it would make")
	}
}

func TestStopEndsLikeUpstreamEtcd(t *testing.T) {
	c := start(t, "--threek-mode=enforce")
	a := c.member("a")
	mustDocker(t, "stop", a.container)
	if code := mustDocker(t, "inspect", "--format", "{{.State.ExitCode}}", a.container); code != "0" {
		t.Errorf("exit code = %s, want 0 as upstream etcd exits as PID 1", code)
	}
}

func TestPreferredLeaderYieldsToPartitionAndReturns(t *testing.T) {
	c := start(t, "--threek-mode=enforce", "--threek-move-cooldown=10s",
		"--threek-preferred-leader=zone-c", "--threek-prefer-after=10s")
	a, b, w := c.member("a"), c.member("b"), c.member("c")

	eventually(t, 90*time.Second, "leader settles on the preferred member c", func() (bool, string) {
		return c.leaderIs(w, a, b, w)
	})

	c.partition(a, w)
	eventually(t, 90*time.Second, "leader leaves c for b, which reaches everyone", func() (bool, string) {
		return c.leaderIs(b, a, b, w)
	})

	c.heal(a, w)
	healed := time.Now()
	eventually(t, 90*time.Second, "leader returns to c after the partition heals", func() (bool, string) {
		return c.leaderIs(w, a, b, w)
	})
	t.Logf("leader returned to c %s after the partition healed", time.Since(healed).Round(time.Second))
}

func TestFlappingLinkKeepsLeader(t *testing.T) {
	c := start(t, "--threek-mode=enforce", "--threek-move-cooldown=10s")
	a, b, w := c.member("a"), c.member("b"), c.member("c")
	c.moveLeaderTo(a)

	c.flap(a, b, 1500*time.Millisecond, 5*time.Second)
	always(t, 60*time.Second, "leader stays on a", func() (bool, string) {
		return c.leaderIs(a, a, w)
	})

	// The flaps must have been seen, or the test proves nothing.
	logs, err := exec.Command("docker", "logs", a.container).CombinedOutput()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Contains(logs, fmt.Appendf(nil, `"%x":false`, b.id)) {
		t.Error("the agent on a never saw b unreachable")
	}
}

func TestFollowerPartitionKeepsLeader(t *testing.T) {
	c := start(t, "--threek-mode=enforce", "--threek-move-cooldown=10s")
	a, b, w := c.member("a"), c.member("b"), c.member("c")
	c.moveLeaderTo(a)

	c.partition(b, w)
	always(t, 45*time.Second, "leader stays on a and both followers follow it", func() (bool, string) {
		return c.leaderIs(a, a, b, w)
	})
}
