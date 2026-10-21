//go:build kubernetes

// Package kubernetes runs etcd-operator clusters with the threek sidecar on a
// kind cluster prepared by setup.sh, named by K8S_E2E_CONTEXT, and breaks the
// network between members on the kind nodes.
package kubernetes

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"
	"time"
)

const (
	cluster   = "etcd"
	configMap = cluster + "-threek"
)

var kubeContext string

func TestMain(m *testing.M) {
	kubeContext = os.Getenv("K8S_E2E_CONTEXT")
	if kubeContext == "" {
		fmt.Println("K8S_E2E_CONTEXT is not set, skipping Kubernetes end-to-end tests")
		os.Exit(0)
	}
	os.Exit(m.Run())
}

func run(name string, args ...string) (string, error) {
	var stdout, stderr bytes.Buffer
	cmd := exec.Command(name, args...)
	cmd.Stdout, cmd.Stderr = &stdout, &stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(stderr.String()))
	}
	return strings.TrimSpace(stdout.String()), nil
}

type env struct {
	t         *testing.T
	namespace string
}

// with returns the environment for subtest t, so failures and cleanups
// belong to it.
func (e *env) with(t *testing.T) *env {
	return &env{t: t, namespace: e.namespace}
}

func (e *env) kubectl(args ...string) (string, error) {
	return run("kubectl", append([]string{"--context", kubeContext, "--namespace", e.namespace}, args...)...)
}

func (e *env) mustKubectl(args ...string) string {
	e.t.Helper()
	out, err := e.kubectl(args...)
	if err != nil {
		e.t.Fatal(err)
	}
	return out
}

type containerStatus struct {
	Name         string `json:"name"`
	Ready        bool   `json:"ready"`
	RestartCount int    `json:"restartCount"`
}

type pod struct {
	Metadata struct {
		Name string `json:"name"`
		UID  string `json:"uid"`
	} `json:"metadata"`
	Spec struct {
		NodeName string `json:"nodeName"`
	} `json:"spec"`
	Status struct {
		PodIP                 string            `json:"podIP"`
		ContainerStatuses     []containerStatus `json:"containerStatuses"`
		InitContainerStatuses []containerStatus `json:"initContainerStatuses"`
	} `json:"status"`
}

func (p pod) status(name string) (containerStatus, bool) {
	for _, s := range slices.Concat(p.Status.ContainerStatuses, p.Status.InitContainerStatuses) {
		if s.Name == name {
			return s, true
		}
	}
	return containerStatus{}, false
}

func (e *env) pods() []pod {
	e.t.Helper()
	var list struct {
		Items []pod `json:"items"`
	}
	out := e.mustKubectl("get", "pods", "--selector", "etcd-operator.cozystack.io/cluster="+cluster, "--output", "json")
	if err := json.Unmarshal([]byte(out), &list); err != nil {
		e.t.Fatal(err)
	}
	return list.Items
}

func (e *env) pod(name string) pod {
	e.t.Helper()
	for _, p := range e.pods() {
		if p.Metadata.Name == name {
			return p
		}
	}
	e.t.Fatalf("no pod %s", name)
	return pod{}
}

func (e *env) etcdctl(podName string, args ...string) (string, error) {
	return e.kubectl(append([]string{
		"exec", podName, "--container", "etcd", "--",
		"etcdctl", "--endpoints=https://127.0.0.1:2379", "--command-timeout=3s",
		"--cacert=/etc/etcd/tls/client/ca.crt",
		"--cert=/etc/etcd/tls/client/tls.crt",
		"--key=/etc/etcd/tls/client/tls.key",
	}, args...)...)
}

// leaderSeenBy returns the pod name of the leader as the member in podName
// sees it.
func (e *env) leaderSeenBy(podName string) (string, error) {
	out, err := e.etcdctl(podName, "endpoint", "status", "--write-out=json")
	if err != nil {
		return "", err
	}
	var statuses []struct {
		Status struct {
			Header struct {
				MemberID uint64 `json:"member_id"`
			} `json:"header"`
			Leader uint64 `json:"leader"`
		} `json:"Status"`
	}
	if err := json.Unmarshal([]byte(out), &statuses); err != nil || len(statuses) != 1 {
		return "", fmt.Errorf("endpoint status %q: %w", out, err)
	}
	members, err := e.etcdctl(podName, "member", "list", "--write-out=json")
	if err != nil {
		return "", err
	}
	var list struct {
		Members []struct {
			ID   uint64 `json:"ID"`
			Name string `json:"name"`
		} `json:"members"`
	}
	if err := json.Unmarshal([]byte(members), &list); err != nil {
		return "", err
	}
	for _, m := range list.Members {
		if m.ID == statuses[0].Status.Leader {
			return m.Name, nil
		}
	}
	return "", fmt.Errorf("no leader seen by %s", podName)
}

func (e *env) agentLogs(podName string) string {
	out, err := e.kubectl("logs", podName, "--container", "threek-agent")
	if err != nil {
		return err.Error()
	}
	return out
}

// logged reports whether the agent in podName logged msg with the given
// attribute values.
func (e *env) logged(podName, msg string, attrs map[string]string) bool {
	for line := range strings.Lines(e.agentLogs(podName)) {
		var entry map[string]any
		if json.Unmarshal([]byte(line), &entry) != nil || entry["msg"] != msg {
			continue
		}
		matches := true
		for k, v := range attrs {
			matches = matches && entry[k] == v
		}
		if matches {
			return true
		}
	}
	return false
}

func (e *env) setConfig(data map[string]string) {
	e.t.Helper()
	manifest, err := json.Marshal(map[string]any{
		"apiVersion": "v1",
		"kind":       "ConfigMap",
		"metadata":   map[string]any{"name": configMap, "namespace": e.namespace},
		"data":       data,
	})
	if err != nil {
		e.t.Fatal(err)
	}
	cmd := exec.Command("kubectl", "--context", kubeContext, "apply", "--filename", "-")
	cmd.Stdin = bytes.NewReader(manifest)
	if out, err := cmd.CombinedOutput(); err != nil {
		e.t.Fatalf("apply ConfigMap: %v: %s", err, out)
	}
}

// partition drops traffic between two pods on the kind node of the first:
// both directions pass through that node's forwarding path.
func (e *env) partition(x, y pod) {
	e.t.Helper()
	rules := [][]string{
		{"--source", x.Status.PodIP, "--destination", y.Status.PodIP, "--jump", "DROP"},
		{"--source", y.Status.PodIP, "--destination", x.Status.PodIP, "--jump", "DROP"},
	}
	for _, r := range rules {
		if _, err := run("docker", append([]string{"exec", x.Spec.NodeName, "iptables", "--insert", "FORWARD", "1"}, r...)...); err != nil {
			e.t.Fatal(err)
		}
		e.t.Cleanup(func() {
			_, _ = run("docker", append([]string{"exec", x.Spec.NodeName, "iptables", "--delete", "FORWARD"}, r...)...)
		})
	}
}

func eventually(t *testing.T, timeout time.Duration, what string, cond func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		ok, detail := cond()
		if ok {
			return
		}
		last = detail
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("timed out after %s waiting for %s: %s", timeout, what, last)
}

func newEnv(t *testing.T) *env {
	t.Helper()
	suffix := make([]byte, 3)
	_, _ = rand.Read(suffix)
	e := &env{t: t, namespace: "threek-e2e-" + hex.EncodeToString(suffix)}
	if _, err := run("kubectl", "--context", kubeContext, "create", "namespace", e.namespace); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if t.Failed() {
			for _, p := range e.pods() {
				t.Logf("agent logs of %s:\n%s", p.Metadata.Name, e.agentLogs(p.Metadata.Name))
			}
		}
		_, _ = run("kubectl", "--context", kubeContext, "delete", "namespace", e.namespace, "--wait=false")
	})
	e.mustKubectl("apply", "--filename", "testdata/cluster.yaml")
	eventually(t, 10*time.Minute, "three ready members with the agent", func() (bool, string) {
		pods := e.pods()
		if len(pods) != 3 {
			return false, fmt.Sprintf("%d pods", len(pods))
		}
		for _, p := range pods {
			etcd, _ := p.status("etcd")
			agent, ok := p.status("threek-agent")
			if !ok {
				return false, p.Metadata.Name + " has no agent"
			}
			if !etcd.Ready || !agent.Ready {
				return false, p.Metadata.Name + " is not ready"
			}
		}
		return true, ""
	})
	return e
}

func TestOperatorClusterWithAgent(t *testing.T) {
	e := newEnv(t)
	pods := e.pods()

	t.Run("the agent starts with the member's certificates", func(t *testing.T) {
		e := e.with(t)
		for _, p := range pods {
			if !e.logged(p.Metadata.Name, "agent started", nil) || e.logged(p.Metadata.Name, "agent stopped, restarting", nil) {
				t.Errorf("agent of %s did not start cleanly:\n%s", p.Metadata.Name, e.agentLogs(p.Metadata.Name))
			}
		}
	})

	t.Run("the mode changes without restarting etcd", func(t *testing.T) {
		e := e.with(t)
		e.setConfig(map[string]string{"mode": "dry-run"})
		eventually(t, 4*time.Minute, "every agent in dry-run with its zone published", func() (bool, string) {
			for _, p := range pods {
				if !e.logged(p.Metadata.Name, "configuration changed", map[string]string{"mode": "dry-run"}) {
					return false, p.Metadata.Name + " is not in dry-run"
				}
			}
			out, err := e.etcdctl(pods[0].Metadata.Name, "get", "--prefix", "/threek/members/", "--print-value-only")
			if err != nil {
				return false, err.Error()
			}
			for _, zone := range []string{"zone-a", "zone-b", "zone-c"} {
				if !strings.Contains(out, `"identity":"`+zone+`"`) {
					return false, "records: " + out
				}
			}
			return true, ""
		})
		for _, before := range pods {
			after := e.pod(before.Metadata.Name)
			b, _ := before.status("etcd")
			a, _ := after.status("etcd")
			if after.Metadata.UID != before.Metadata.UID || a.RestartCount != b.RestartCount {
				t.Errorf("%s was recreated or etcd restarted", before.Metadata.Name)
			}
		}
	})

	t.Run("enforce moves the leader away from a partial partition", func(t *testing.T) {
		e := e.with(t)
		e.setConfig(map[string]string{"mode": "enforce", "move-cooldown": "0s", "move-after": "10s"})
		eventually(t, 4*time.Minute, "every agent in enforce", func() (bool, string) {
			for _, p := range pods {
				if !e.logged(p.Metadata.Name, "configuration changed", map[string]string{"mode": "enforce"}) {
					return false, p.Metadata.Name + " is not in enforce"
				}
			}
			return true, ""
		})
		leaderName, err := e.leaderSeenBy(pods[0].Metadata.Name)
		if err != nil {
			t.Fatal(err)
		}
		var leader pod
		var others []pod
		for _, p := range pods {
			if p.Metadata.Name == leaderName {
				leader = p
			} else {
				others = append(others, p)
			}
		}
		cut, witness := others[0], others[1]
		e.partition(leader, cut)
		eventually(t, 2*time.Minute, "the leader on "+witness.Metadata.Name, func() (bool, string) {
			got, err := e.leaderSeenBy(witness.Metadata.Name)
			if err != nil {
				return false, err.Error()
			}
			return got == witness.Metadata.Name, "leader " + got
		})
		eventually(t, time.Minute, "the cut-off member to follow the new leader", func() (bool, string) {
			got, err := e.leaderSeenBy(cut.Metadata.Name)
			if err != nil {
				return false, err.Error()
			}
			return got == witness.Metadata.Name, "leader " + got
		})
		if !e.logged(leader.Metadata.Name, "leadership transferred", map[string]string{"reason": "partition"}) {
			t.Errorf("the agent of %s did not log the transfer:\n%s", leader.Metadata.Name, e.agentLogs(leader.Metadata.Name))
		}
	})
}
