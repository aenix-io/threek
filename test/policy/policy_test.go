//go:build policy

// Package policy tests deploy/threek-agent.yaml against a real API server.
// It needs a cluster with the policy applied, named by POLICY_KUBE_CONTEXT.
// Pods are created with --dry-run=server, so nothing is left behind.
package policy

import (
	"bytes"
	"encoding/json"
	"maps"
	"os"
	"os/exec"
	"slices"
	"testing"
)

type container struct {
	Name          string   `json:"name"`
	Image         string   `json:"image"`
	RestartPolicy string   `json:"restartPolicy"`
	Args          []string `json:"args"`
	Env           []struct {
		Name      string `json:"name"`
		ValueFrom struct {
			FieldRef struct {
				FieldPath string `json:"fieldPath"`
			} `json:"fieldRef"`
		} `json:"valueFrom"`
	} `json:"env"`
	VolumeMounts []struct {
		Name      string `json:"name"`
		MountPath string `json:"mountPath"`
	} `json:"volumeMounts"`
}

type pod struct {
	Spec struct {
		InitContainers []container `json:"initContainers"`
		Volumes        []struct {
			Name      string `json:"name"`
			ConfigMap *struct {
				Name     string `json:"name"`
				Optional bool   `json:"optional"`
			} `json:"configMap"`
		} `json:"volumes"`
	} `json:"spec"`
}

// memberPod is an etcd-operator member Pod as the operator builds it.
func memberPod(labels map[string]string, tls, restore bool) map[string]any {
	volumes := []any{map[string]any{"name": "data", "emptyDir": map[string]any{}}}
	if tls {
		volumes = append(volumes,
			map[string]any{"name": "tls-client", "secret": map[string]any{"secretName": "etcd-client-tls"}},
			map[string]any{"name": "tls-peer", "secret": map[string]any{"secretName": "etcd-peer-tls"}},
		)
	}
	spec := map[string]any{
		"automountServiceAccountToken": false,
		"containers":                   []any{map[string]any{"name": "etcd", "image": "registry.k8s.io/etcd:3.6.4-0"}},
		"volumes":                      volumes,
	}
	if restore {
		spec["initContainers"] = []any{map[string]any{"name": "restore", "image": "registry.k8s.io/etcd:3.6.4-0"}}
	}
	all := map[string]string{
		"app.kubernetes.io/name":             "etcd",
		"app.kubernetes.io/instance":         "etcd",
		"app.kubernetes.io/managed-by":       "etcd-operator",
		"etcd-operator.cozystack.io/cluster": "etcd",
		"threek.aenix.io/inject":             "true",
	}
	for k, v := range labels {
		if v == "" {
			delete(all, k)
		} else {
			all[k] = v
		}
	}
	return map[string]any{
		"apiVersion": "v1",
		"kind":       "Pod",
		"metadata":   map[string]any{"name": "etcd-abcde", "namespace": "default", "labels": all},
		"spec":       spec,
	}
}

func admit(t *testing.T, manifest map[string]any) pod {
	t.Helper()
	kubeContext := os.Getenv("POLICY_KUBE_CONTEXT")
	if kubeContext == "" {
		t.Skip("POLICY_KUBE_CONTEXT not set")
	}
	in, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("kubectl", "--context", kubeContext, "create", "--dry-run=server", "--output=json", "--filename=-")
	cmd.Stdin = bytes.NewReader(in)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("kubectl create: %v: %s", err, stderr.String())
	}
	var p pod
	if err := json.Unmarshal(out, &p); err != nil {
		t.Fatal(err)
	}
	return p
}

func agentOf(p pod) (container, bool) {
	for _, c := range p.Spec.InitContainers {
		if c.Name == "threek-agent" {
			return c, true
		}
	}
	return container{}, false
}

func names(p pod) []string {
	var n []string
	for _, c := range p.Spec.InitContainers {
		n = append(n, c.Name)
	}
	return n
}

func mounts(c container) map[string]string {
	m := map[string]string{}
	for _, vm := range c.VolumeMounts {
		m[vm.Name] = vm.MountPath
	}
	return m
}

func TestInjectsIntoTLSMember(t *testing.T) {
	p := admit(t, memberPod(nil, true, true))
	if got := names(p); !slices.Equal(got, []string{"restore", "threek-agent"}) {
		t.Fatalf("init containers = %v, want the agent after restore", got)
	}
	agent, _ := agentOf(p)
	if agent.RestartPolicy != "Always" {
		t.Errorf("restartPolicy = %q, want a native sidecar", agent.RestartPolicy)
	}
	for _, arg := range []string{
		"--endpoint=https://127.0.0.1:2379",
		"--cert-file=/etc/etcd/tls/client/tls.crt",
		"--trusted-ca-file=/etc/etcd/tls/client/ca.crt",
		"--peer-cert-file=/etc/etcd/tls/peer/tls.crt",
		"--peer-trusted-ca-file=/etc/etcd/tls/peer/ca.crt",
		"--config-dir=/etc/threek",
	} {
		if !slices.Contains(agent.Args, arg) {
			t.Errorf("args lack %s: %v", arg, agent.Args)
		}
	}
	want := map[string]string{"threek-config": "/etc/threek", "tls-client": "/etc/etcd/tls/client", "tls-peer": "/etc/etcd/tls/peer"}
	if got := mounts(agent); !maps.Equal(got, want) {
		t.Errorf("mounts = %v, want %v", got, want)
	}
	if len(agent.Env) != 1 || agent.Env[0].Name != "THREEK_IDENTITY" ||
		agent.Env[0].ValueFrom.FieldRef.FieldPath != "metadata.labels['topology.kubernetes.io/zone']" {
		t.Errorf("env = %+v, want the zone label as identity", agent.Env)
	}
	last := p.Spec.Volumes[len(p.Spec.Volumes)-1]
	if last.Name != "threek-config" || last.ConfigMap == nil || last.ConfigMap.Name != "etcd-threek" || !last.ConfigMap.Optional {
		t.Errorf("config volume = %+v, want the optional ConfigMap etcd-threek", last)
	}
}

func TestInjectsIntoPlaintextMember(t *testing.T) {
	p := admit(t, memberPod(nil, false, false))
	agent, ok := agentOf(p)
	if !ok {
		t.Fatalf("no agent in %v", names(p))
	}
	if !slices.Contains(agent.Args, "--endpoint=http://127.0.0.1:2379") {
		t.Errorf("args = %v, want a plain HTTP endpoint", agent.Args)
	}
	for _, arg := range agent.Args {
		if arg != "--endpoint=http://127.0.0.1:2379" && arg != "--config-dir=/etc/threek" && arg != "--metrics-listen=:2390" {
			t.Errorf("unexpected argument %s without TLS", arg)
		}
	}
	if got := mounts(agent); len(got) != 1 {
		t.Errorf("mounts = %v, want only the ConfigMap", got)
	}
}

func TestInjectsIntoPodWithoutVolumes(t *testing.T) {
	manifest := memberPod(nil, false, false)
	delete(manifest["spec"].(map[string]any), "volumes")
	p := admit(t, manifest)
	if _, ok := agentOf(p); !ok {
		t.Fatalf("no agent in %v", names(p))
	}
	if len(p.Spec.Volumes) != 1 || p.Spec.Volumes[0].Name != "threek-config" {
		t.Errorf("volumes = %+v, want only the ConfigMap", p.Spec.Volumes)
	}
}

func TestNodeIdentity(t *testing.T) {
	p := admit(t, memberPod(map[string]string{"threek.aenix.io/identity": "node"}, true, false))
	agent, ok := agentOf(p)
	if !ok {
		t.Fatalf("no agent in %v", names(p))
	}
	if got := agent.Env[0].ValueFrom.FieldRef.FieldPath; got != "spec.nodeName" {
		t.Errorf("identity field = %q, want spec.nodeName", got)
	}
}

func TestLeavesOtherPodsAlone(t *testing.T) {
	cases := map[string]map[string]any{
		"without the opt-in label":  memberPod(map[string]string{"threek.aenix.io/inject": ""}, true, true),
		"opt-in label is not true":  memberPod(map[string]string{"threek.aenix.io/inject": "false"}, true, true),
		"without the cluster label": memberPod(map[string]string{"etcd-operator.cozystack.io/cluster": ""}, true, true),
	}
	notEtcd := memberPod(nil, true, true)
	notEtcd["spec"].(map[string]any)["containers"] = []any{map[string]any{"name": "snapshot", "image": "registry.k8s.io/etcd:3.6.4-0"}}
	cases["without an etcd container"] = notEtcd
	for name, manifest := range cases {
		t.Run(name, func(t *testing.T) {
			if _, ok := agentOf(admit(t, manifest)); ok {
				t.Error("agent injected")
			}
		})
	}
}
