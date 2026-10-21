# threek

A drop-in etcd supervisor that keeps the Raft leader on a member every other member can reach.

> **Status:** early development, not released yet. The agent, both ways to run it and the container images are implemented; the Talos image is tested end to end against blackhole partitions. See [docs/design.md](docs/design.md).

## The problem

Take a three-member etcd cluster across three sites, A, B and C.
The link between A and B breaks, while C still reaches both.

If the leader is in A, etcd never recovers on its own.
The member in B loses the leader and asks for an election, but C refuses to vote because it still hears the leader in A.
This is how PreVote and CheckQuorum are supposed to work: they protect Raft from disruptive elections.
The member in B stays leaderless for as long as the partition lasts, and every client attached to it fails.

```mermaid
sequenceDiagram
    participant CL as Clients in site B
    participant B as etcd in site B
    participant C as etcd in site C
    participant A as Leader in site A

    Note over B,A: The link between A and B breaks
    A->>C: heartbeat
    A-xB: heartbeat is lost
    loop every election timeout
        B->>C: pre-vote request
        C-->>B: rejected: leader A is alive
    end
    Note over B: Stays leaderless while the partition lasts
    CL->>B: request
    B-->>CL: error: no leader
```

The fix is a manual `etcdctl move-leader` to the member in C. threek does it automatically.

## How it works

A small agent runs next to every etcd member and talks to etcd only through its public APIs.
It runs in one of two ways:

- **Kubernetes with etcd-operator:** a sidecar container, injected into member Pods by a MutatingAdmissionPolicy. The operator and the etcd image stay as they are.
- **Talos:** an etcd image in which threek replaces the `etcd` binary. It starts upstream etcd with the original arguments and runs the agent in a separate process.

The agent follows one rule:

> If the leader cannot reach some member, and another member reaches everyone, the leader transfers leadership to that member.

- Every agent probes its peers with the same request etcd's own prober sends to the peer port, and publishes what it sees in a small record in the cluster itself.
- Only the leader acts, using the standard `MoveLeader` API. No consensus between agents, no etcd fork, no new ports.
- When nobody reaches everyone (a site is down, the network is fully split), threek does nothing and Raft behaves as usual.
- Optionally, a preferred member holds the leader in normal operation, and the leader returns to it after a partition heals.

With the defaults the leader moves about 20 seconds after the link fails.

## Requirements and limitations

- Exactly three voting members. Learners are ignored.
- The client port (2379) must be reachable between members, not only the peer port (2380): the leader checks the other members' status through it.
- Client certificate authentication (mTLS) is supported and expected.
  etcd authentication (`etcdctl auth enable`, `spec.auth` in etcd-operator) is not supported: `MoveLeader` requires the root role.
- Client and peer TLS as configured by Talos or etcd-operator: the agent reuses etcd's own certificates. The client certificate must also have the `ClientAuth` extended key usage, and its secret must hold the CA as `ca.crt`. `--peer-auto-tls` is not supported.
- Synchronized clocks on all members, as etcd itself expects.
- Clients must not be routed across the partition. threek restores a leader for every member, but a client in one site that is load-balanced to a member in an unreachable site still fails. Use topology-aware routing (for example `trafficDistribution: PreferSameZone`) or list every member in the client configuration.
- Each agent keeps one small record under the `/threek/members/` prefix of the cluster's own keyspace. It is rewritten only when what the member sees changes.

### etcd-operator

- Kubernetes 1.36 or later. On 1.35, enable the `MutatingAdmissionPolicy` feature gate and the `admissionregistration.k8s.io/v1beta1` API, and apply the policy with that API version.
- [cozystack/etcd-operator](https://github.com/cozystack/etcd-operator) needs no changes. The policy relies on its member Pod layout (the `etcd` container, the `tls-client` and `tls-peer` volumes), which is not part of the operator's API.
- The agent is a native sidecar: it starts before etcd and counts toward the Pod's readiness. If the agent image cannot be pulled, the member does not start, so mirror the image wherever etcd's own image is mirrored. A crashed agent makes the Pod not ready until it restarts.

Install the policy once per cluster, from the release:

```bash
kubectl apply --filename https://github.com/aenix-io/threek/releases/download/<threek version>/threek-agent.yaml
```

Opt a cluster in with a label. The operator applies it to new Pods only, so roll the members once:

```yaml
apiVersion: etcd-operator.cozystack.io/v1alpha2
kind: EtcdCluster
metadata:
  name: etcd
spec:
  additionalMetadata:
    labels:
      threek.aenix.io/inject: "true"
      # threek.aenix.io/identity: node   # identity from the node name instead of the zone
```

The agent stays off until the ConfigMap `<cluster name>-threek` exists in the cluster's namespace.
Changes reach running agents within a minute or two, without restarting etcd:

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: etcd-threek
data:
  mode: dry-run
  preferred-leader: zone-c   # optional
```

The identity is the node's `topology.kubernetes.io/zone` label, copied to the Pod by Kubernetes, or the node name with `threek.aenix.io/identity: node`.
The agent serves its metrics on port 2390 of the Pod (`threek-metrics`).

### Talos

- Talos runs `/usr/local/bin/etcd` from the image set in `cluster.etcd.image`, and threek's settings go to `cluster.etcd.extraArgs`.
- With `cluster.etcd.image` set explicitly, Talos upgrades no longer upgrade etcd. Upgrade etcd by changing the threek image.
- Changing a setting restarts etcd: roll the nodes one at a time.

```yaml
cluster:
  etcd:
    image: ghcr.io/aenix-io/threek:<threek version>-etcd<etcd version>
    extraArgs:
      threek-mode: dry-run
      threek-identity: zone-c          # unique per member
      threek-preferred-leader: zone-c  # optional, same on every member
```

## Configuration

| Setting | Default | Meaning |
| --- | --- | --- |
| `mode` | `off` | `off`: no agent activity. `dry-run`: decide and log only. `enforce`: transfer leadership. |
| `identity` | empty | label of this member, such as a zone or a node name |
| `preferred-leader` | empty | identity that should hold the leader in normal operation |
| `metrics-listen` | empty | address for the agent's Prometheus metrics, such as `127.0.0.1:2390` |

Timing settings and the full reference are in [docs/design.md](docs/design.md#configuration).

- On Kubernetes, `identity` and `metrics-listen` are set by the policy, and every other setting is a key of the ConfigMap.
- On Talos, every setting is an etcd flag with the `threek-` prefix (`--threek-mode`), or an environment variable (`THREEK_MODE`). The flag wins.
  The supervisor removes `--threek-*` flags and `THREEK_*` variables before starting etcd.
  With `--threek-mode=off` the image behaves exactly like upstream etcd, so rolling back is a flag change.

## Releases

Every release publishes:

```text
ghcr.io/aenix-io/threek-agent:<threek version>              # sidecar agent for Kubernetes
ghcr.io/aenix-io/threek:<threek version>-etcd<etcd version> # etcd with threek, for Talos
```

Images are built for `linux/amd64` and `linux/arm64`.
The release also carries `threek-agent.yaml`, the admission policy with the agent image pinned by digest.

Tags are never moved. Images carry provenance and an SBOM, and are signed with cosign keyless signing:

```bash
cosign verify <image> \
  --certificate-identity-regexp '^https://github.com/aenix-io/threek/' \
  --certificate-oidc-issuer https://token.actions.githubusercontent.com
```

## Development

```bash
make test   # unit and integration tests; set TEST_ETCD_BINARY to an etcd binary to run the tests against real clusters
make lint   # golangci-lint
make build  # bin/threek and bin/threek-agent
make image IMAGE=threek:dev ETCD_VERSION=v3.6.15
make agent-image AGENT_IMAGE=threek-agent:dev

# admission policy tests: a cluster with deploy/threek-agent.yaml applied
POLICY_KUBE_CONTEXT=kind-kind go test -tags policy ./test/policy/

# end-to-end tests: containers with blackhole partitions, needs Docker
E2E_IMAGE=threek:dev go test -tags e2e -timeout 20m ./test/e2e/
```

## License

Apache License 2.0, see [LICENSE](LICENSE).
