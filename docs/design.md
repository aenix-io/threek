# threek design

Status: draft. Everything below is implemented. Nothing is released yet.

## Problem

A three-member etcd cluster is spread across three sites: A, B and C, one member per site.
Site C is often a witness that holds no workload of its own.

When the link between A and B breaks while C still reaches both, etcd can end up in a state it never leaves on its own:

1. The leader is in A. It still has a quorum with C, so the cluster accepts writes.
2. The member in B loses the leader. It tries to start an election, but C refuses to vote because C still hears a live leader.
3. The member in B stays leaderless for as long as the partition lasts. Every client attached to B fails.

With several etcd clusters on the same three sites (for example, a Kubernetes control plane plus several application clusters) every cluster stays wherever its leader happened to be.
Some clusters keep working from site B, others do not, and the result depends on chance.

A manual `etcdctl move-leader` to the member in C fixes it, because C reaches every member.
threek automates exactly that.

## Why etcd does not fix this itself

The behavior comes from PreVote and CheckQuorum.
They protect Raft from disruptive elections: a member that cannot hear the leader is not allowed to depose a leader that the rest of the quorum still hears.
This is intended behavior, not a bug.

Upstream has no mechanism for this case:

- leader priority was proposed and closed as stale ([etcd-io/etcd#10201](https://github.com/etcd-io/etcd/issues/10201));
- etcd has no connectivity-aware leader transfer, only manual `move-leader`;
- `ForgetLeader` exists in the raft library ([etcd-io/raft#78](https://github.com/etcd-io/raft/pull/78)), but etcd server neither uses nor exposes it.

## Goals and non-goals

Goals:

- move the leader to a member that reaches every other member when a partial partition occurs;
- optionally keep the leader on a preferred member during normal operation;
- run upstream etcd unmodified, with unmodified arguments;
- need no changes in the systems that launch etcd (Talos, etcd-operator);
- one mechanism for every etcd cluster, so all clusters on the same sites converge to the same member during a partition.

Non-goals:

- clusters with more or fewer than three voting members;
- full site failures and full partitions, which Raft already handles;
- routing etcd clients around a partition (see [Client routing](#client-routing));
- etcd authentication (`etcdctl auth enable`, `spec.auth` in etcd-operator): `MoveLeader` requires the root role, and the agent connects with a certificate that maps to no etcd user. Client certificate authentication (mTLS) alone is supported.

## The rule

> If the leader cannot reach some member, and another member reaches everyone, the leader transfers leadership to that member.

Properties:

- Only the current leader decides, and only about its own cluster. Agents do not need consensus between themselves.
- The transfer goes through the current leader, which is reachable by definition, using the standard `MoveLeader` API. No etcd fork is needed.
- etcd accepts `MoveLeader` only on the actual leader (`ErrNotLeader` otherwise). Two agents that both believe they are the leader cannot both transfer leadership.
- If nobody reaches everyone (a full site failure, a full partition), threek does nothing and Raft handles the situation as usual.

## Architecture

The agent is one program with two ways to run:

| Platform | Delivery | Settings |
| --- | --- | --- |
| Kubernetes with etcd-operator | `threek-agent` sidecar, injected by a MutatingAdmissionPolicy | flags set by the policy, plus a ConfigMap reloaded on every poll |
| Talos | etcd image whose `etcd` binary is the threek supervisor | `--threek-*` flags among etcd's own |

Talos has no Pod to add a container to, and its machine config only chooses the etcd image and its arguments, so there the agent has to come with etcd.
On Kubernetes a sidecar keeps etcd's image, version and arguments under the operator's control, and the mode changes without restarting etcd.

### Kubernetes sidecar

`deploy/threek-agent.yaml` holds the policy. It acts on the creation of Pods that carry `threek.aenix.io/inject: "true"`, the operator's `etcd-operator.cozystack.io/cluster` label and an `etcd` container.
etcd-operator copies `spec.additionalMetadata.labels` to member Pods, so the operator needs no changes.

The policy appends a native sidecar (an init container with `restartPolicy: Always`) that:

- shares the Pod's network namespace, so its probes take the same path as etcd's traffic;
- mounts the member's `tls-client` and `tls-peer` volumes when they exist, and connects over plain HTTP when they do not;
- takes `THREEK_IDENTITY` from the downward API: the Pod's `topology.kubernetes.io/zone` label, which Kubernetes copies from the node at binding ([KEP-4742](https://github.com/kubernetes/enhancements/issues/4742), on by default since 1.35), or `spec.nodeName` with `threek.aenix.io/identity: node`;
- mounts the ConfigMap `<cluster>-threek` as an optional volume;
- requests as much CPU and memory as its limits, so a Guaranteed member stays Guaranteed.

Members started with `--peer-auto-tls` are left alone: their self-signed peer certificates cannot be verified, see [Peer TLS](#peer-tls).

Volume names and mount paths are internals of etcd-operator, not its API. The policy tests pin the layout the policy expects.

The policy fails open (`failurePolicy: Ignore`): an error in it creates the member without the agent.
The sidecar itself is part of the Pod, and that has two consequences:

- etcd starts after the sidecar has started, so a member waits while the agent image cannot be pulled. The agent image belongs in the same mirror as etcd's.
- The sidecar counts toward the Pod's readiness. While the agent is not running, the Pod is not ready: the member leaves the client Service and counts as disrupted for the PodDisruptionBudget. Raft is not affected.

The agent therefore never exits on its own errors. Invalid settings, missing certificates or an unreachable etcd are logged and retried while it keeps running.
Only the watchdog ends it, when the poll loop stops making progress, and kubelet restarts it.

Settings shared by the members come from the ConfigMap, one key per setting, and are read again before every poll.
kubelet updates the mounted files within a minute or two of a change, without restarting any container.

- A missing ConfigMap or a missing `mode` key means `off`: the agent publishes nothing and makes no calls to etcd.
- An invalid ConfigMap is logged once, and the previous settings stay.
- Switching on after `off` starts with an empty link history, because links seen before say nothing about the links now.

### Talos image

threek ships as an etcd image in which the `etcd` binary is replaced:

| Path | Content |
| --- | --- |
| `/usr/local/bin/etcd` | threek supervisor, first `etcd` in `PATH` |
| `/usr/local/bin/etcd-upstream` | upstream etcd binary, unmodified |
| `/usr/local/bin/etcdctl`, `/usr/local/bin/etcdutl` | upstream tools of the same etcd version |

The image uses the same distroless base, user, command, working directory and ports as the upstream etcd image.
Like etcd, threek runs as a non-root user without capabilities when the platform starts it that way, as Talos does.

### Process model

```text
/usr/local/bin/etcd (supervisor)
├── etcd-upstream <original argv without --threek-*>
└── threek-agent  (same binary, started with argv[0] = threek-agent)
```

The supervisor is kept minimal, because a crash of it is a crash of the etcd member.
All logic lives in the agent, a separate process. A panic, a fatal runtime error or a leak in the agent never touches etcd.

Supervisor behavior:

1. Invalid threek settings: exit with code 2 without starting etcd, as etcd does for invalid flags.
2. `--threek-mode=off`, or a non-server invocation (`--version`, `--help`, the `gateway` and `grpc-proxy` subcommands): `exec` the upstream binary directly. The process becomes plain etcd.
3. Otherwise start upstream etcd with the original argv, minus `--threek-*` flags, and the original environment, minus `THREEK_*` variables.
4. Forward signals to etcd. When etcd exits, stop the agent and end the way etcd ended: with its exit code, or by the signal that killed it. Restarting is left to whoever restarts etcd today (Talos, kubelet).
   After a graceful shutdown on SIGINT or SIGTERM etcd kills itself with the same signal, except as PID 1, where it exits with code 0. The supervisor is PID 1 in a container, so it applies that rule in etcd's place.
5. When the agent exits, restart it with exponential backoff. etcd keeps running.
6. Write nothing to stdout. Supervisor and agent log JSON to stderr with a `"logger":"threek"` field, so they can be told apart from etcd's own log lines.

The agent also exits when its poll loop completes no poll for 30 s, so the supervisor restarts an agent that hangs instead of crashing.
Every call in a poll has a timeout, so only a bug can trigger this.

In the image the agent has no liveness or readiness probe of its own.
Talos probes act on the whole container, so failing one for the agent would restart etcd.
The container's probes keep checking etcd, which the agent never affects.

### Reading etcd settings

The agent needs a few etcd settings: client and peer TLS files and client URLs.
The sidecar receives them as flags from the policy. In the Talos image the agent reads them from etcd's own configuration:
etcd accepts settings from flags (`-flag`, `--flag`, `--flag=value`, `--flag value`), from `ETCD_*` environment variables and from `--config-file`.
The agent resolves its subset using etcd's own precedence rules and never rewrites them.

Connections to the local member go to `https://127.0.0.1:2379` (or the first local client URL).
Both Talos and etcd-operator include `localhost` and `127.0.0.1` in the etcd server certificate SANs and give it the `ClientAuth` extended key usage.
The agent therefore uses `--cert-file`/`--key-file` as its client certificate and `--trusted-ca-file` to verify the server.
For probes to the peer port it uses `--peer-cert-file`, `--peer-key-file` and `--peer-trusted-ca-file`, as etcd itself does.
No additional credentials are needed.

## Connectivity signal

Every agent probes the other voting members once per poll interval with the request etcd's own prober sends: `GET /raft/probing` on the member's peer URL, with the member's peer certificate and a one second timeout.
A probe succeeds when any peer URL of the member answers `200`.

This is the same traffic etcd already exchanges between peers, so it needs no new firewall rules and makes etcd log nothing.
A bare TCP connect would not do: etcd logs every peer connection that closes before the TLS handshake.

Link state uses hysteresis:

- a link is **down** after `--threek-down-after` consecutive failed probes;
- a link is **up** again after `--threek-up-after` consecutive successful probes.

### Why not etcd's own transport state

etcd exports `etcd_network_active_peers` per peer, and reading it would have avoided probing.
It does not notice a blackholed peer: etcd sets the read timeout of its peer streams on a dialer that `net/http` ignores, so a stream to a peer that stopped answering stays "active" until TCP gives up on it, which takes minutes.
The end-to-end tests showed exactly that, so threek measures reachability itself.
The fix is proposed upstream in [etcd-io/etcd#22639](https://github.com/etcd-io/etcd/pull/22639); released etcd versions keep the bug.

### Sharing views

The leader needs every member's view, not only its own.
Each agent publishes its view in its member record (see [Member records](#member-records)), and the leader reads the records of the others from its local member:

- its own row comes from its own probes in the current poll;
- every other voting member's row comes from that member's record;
- a member without a record has an unknown row and is never a transfer target.

Targets come only from the local member's `MemberList`, learners excluded.
Only the leader also calls `Status` on the other members' client URLs, to check their health (see [Priorities](#priorities)) and that the cluster ID and member ID match.
Every call to another member has its own one second timeout, so an unreachable member cannot use up the time the poll needs for local calls.

The member cut off from the leader never has to report anything: the leader learns that it cannot reach that member from its own probes.
The candidate reaches everyone, the leader included, so its record stays current.
A member cut off from the quorum cannot renew its record, which expires; that member is never a candidate anyway.

## Member records

Each agent keeps one record in its own cluster:

```text
key:   /threek/members/<member id, hex>
value: {"schemaVersion": 1, "identity": "zone-c", "agentVersion": "v0.1.0",
        "sees": {"<peer id, hex>": true, "<peer id, hex>": false}}
lease: 10 s, renewed by every poll
```

- The record is rewritten only when the view or the identity changes. Lease renewals are not Raft proposals, so a stable cluster adds no writes.
- The lease is renewed by the poll loop itself, not by a background keep-alive, so the view of an agent that stopped polling expires within 10 s instead of staying stale.
- The agent revokes the lease when it stops, which removes the record at once.
- Kubernetes API servers only read keys under their `--etcd-prefix` (`/registry` by default), so the records are invisible to them. They do appear in snapshots and in `etcdctl get --prefix`.

## Decisions

The decision engine is a pure function of the reachability view, member records, local leadership state and timers.
It returns either "no action" or "transfer to member X, reason R".

### Priorities

1. **Partial partition.** The leader has a link to some member that has been down for at least `--threek-move-after` (counted from the moment the link was declared down), and the third member is healthy and has every link up, both in its own row and in the leader's. Transfer to that member, even if it is not the preferred leader.
2. **Preferred leader.** Every link has been up for at least `--threek-prefer-after`, the local member is not the preferred one, exactly one voting member carries the preferred identity, and that member is healthy. Transfer to the preferred member.
3. Otherwise, do nothing.

A transfer target, under either rule, must be healthy: its `Status` call answers within one second, it reports no alarms, and its applied index is at most 1000 entries behind the leader's.
A member busy with defragmentation blocks its backend, fails this check and is not chosen as a target.

If several members share the preferred identity, the agent logs a warning and ignores the preference.

### Rate limit

An agent may not transfer leadership away until `--threek-move-cooldown` has passed since its member became the leader.
Leadership acquisition is observed locally (through `Status`), so the limit holds across leader changes, and no shared state is needed.
The trade-off: a partition that starts shortly after any leadership change, including a manual or operator-initiated one, is handled only after the cooldown expires.

Every transfer attempt pauses writes until it completes or times out, so a failed attempt is retried only after `--threek-move-cooldown`.

Without a preferred leader the leader stays wherever it moved after the partition heals.

### Scenarios

| Scenario | Member that reaches everyone | threek action |
| --- | --- | --- |
| Full connectivity | all | nothing, or return to the preferred leader |
| A ↔ B broken | C | transfer to C |
| A ↔ C broken | B | transfer to B |
| Site A fully down | none | nothing, Raft re-elects between B and C |
| All links broken | none | nothing, no quorum anywhere |
| Flapping link | changes | hysteresis and cooldown keep the leader in place |
| Partition healed, preferred leader set | all | after `--threek-prefer-after`, transfer to the preferred member |

### Expected timeline with defaults

A ↔ B breaks at T, the leader is in A, C reaches both:

| Time | Event |
| --- | --- |
| T + 3 s | the agent on A declares the A ↔ B link down (3 failed probes, one per second) |
| T + 18 s | `--threek-move-after` passes, A transfers leadership to C |
| T + 20 s | B receives heartbeats from the new leader and serves again |

The end-to-end test measures about 20 s from the partition until B follows the new leader.
All defaults are starting points and are to be tuned on a test bench.

## Configuration

| Setting | Default | Meaning |
| --- | --- | --- |
| `mode` | `off` | `off`: no agent activity. `dry-run`: decide and log, never transfer. `enforce`: transfer. |
| `poll-interval` | `1s` | how often the agent probes its peers and polls etcd |
| `down-after` | `3` | consecutive failed probes before a link is down |
| `up-after` | `10` | consecutive successful probes before a link is up |
| `move-after` | `15s` | how long a link stays down before a partition transfer |
| `move-cooldown` | `2m` | minimum leadership tenure before this agent transfers leadership away |
| `identity` | empty | label of this member: a zone or a node name, unique within the cluster |
| `preferred-leader` | empty | identity that holds the leader in normal operation; empty means no preference |
| `prefer-after` | `5m` | how long connectivity must be full before returning to the preferred leader |
| `metrics-listen` | empty | address for the agent's Prometheus endpoint; empty disables it |

`off` is the default, so threek does nothing until it is enabled explicitly.

**Sidecar.** `identity` and `metrics-listen` differ per member and are flags of `threek-agent`, set by the policy (`--identity` falls back to `THREEK_IDENTITY`).
Every other setting is a key of the ConfigMap; the ConfigMap may not set the per-member ones.
The remaining flags of `threek-agent` point at etcd: `--endpoint`, `--cert-file`, `--key-file`, `--trusted-ca-file`, `--peer-cert-file`, `--peer-key-file`, `--peer-trusted-ca-file` and `--config-dir`.

**Talos image.** Every setting is an extra etcd flag with the `threek-` prefix, such as `--threek-mode`. The supervisor removes them before starting etcd, otherwise etcd would reject the unknown flags.
Every flag has an environment fallback with the same name (`--threek-mode` ↔ `THREEK_MODE`). The flag wins.
With `--threek-mode=off` the image is a plain etcd, so rolling back is a flag change, not an image change.

### Choosing the identity

| Identity | Preferred leader | When |
| --- | --- | --- |
| zone | one value for every cluster, e.g. `zone-c` | one member per zone; a member moving to another node of the same zone changes nothing |
| node name | a value per cluster | the leader must stay on a specific node |

etcd member names are not suitable: etcd-operator generates them randomly.

## Deployment

### Talos

- Set `cluster.etcd.image` to the threek image. Talos always runs `/usr/local/bin/etcd <args>` and ignores the image entrypoint.
- `cluster.etcd.extraArgs` uses a denylist: keys that Talos manages cannot be overridden, any other key is passed as `--key=value`. `--threek-*` flags pass through.
- The machine config differs per node anyway, so `threek-identity` is set there directly.
- Talos only computes the default etcd image when `cluster.etcd.image` is empty. With an explicit image a Talos upgrade no longer upgrades etcd, and etcd has to be upgraded by changing the image.

```yaml
cluster:
  etcd:
    image: ghcr.io/aenix-io/threek:<threek version>-etcd<etcd version>
    extraArgs:
      threek-mode: dry-run
      threek-identity: zone-c
      threek-preferred-leader: zone-c
```

### etcd-operator (cozystack/etcd-operator)

- Apply the policy from the release, which pins the agent image by digest. It needs Kubernetes 1.36, or 1.35 with the `MutatingAdmissionPolicy` feature gate and the `admissionregistration.k8s.io/v1beta1` API.
- Label the cluster through `spec.additionalMetadata.labels`. The operator labels new Pods only, so the members are rolled once to get the sidecar.
- Create the ConfigMap `<cluster>-threek`. Switching `dry-run` to `enforce` afterwards is a ConfigMap edit.

```yaml
apiVersion: etcd-operator.cozystack.io/v1alpha2
kind: EtcdCluster
metadata:
  name: etcd
spec:
  additionalMetadata:
    labels:
      threek.aenix.io/inject: "true"
---
apiVersion: v1
kind: ConfigMap
metadata:
  name: etcd-threek
data:
  mode: dry-run
  preferred-leader: zone-c
```

## Security

- The agent listens on nothing but its optional metrics endpoint, which the policy enables on port 2390 of the Pod. It only connects to the client and peer URLs of its own cluster's members.
- It authenticates with the member's own certificates and verifies other members with the member's own CAs. A member of another cluster signed by a different CA is rejected by TLS. Clusters that share a CA are told apart by the cluster ID check described in [Sharing views](#sharing-views).
- The agent writes only its own record under `/threek/members/`.
- The metrics endpoint is plain HTTP and exposes no secrets. Bind it to a trusted address.
- The sidecar runs as a non-root user with a read-only root filesystem, no capabilities and the `RuntimeDefault` seccomp profile, and needs no Kubernetes API access.

## Observability

Logs: every decision, including "no action" after a state change, is logged once with its reason and the reachability view that led to it.

Metrics, when `metrics-listen` is set:

| Metric | Meaning |
| --- | --- |
| `threek_mode{mode}` | 1 for the active mode; the metrics below describe only a mode other than `off` |
| `threek_link_up{from,to}` | link state after hysteresis, as seen by this agent: 1 up, 0 down or unknown |
| `threek_is_leader` | whether the local member is the leader |
| `threek_transfers_total{reason,result}` | transfers attempted, by reason and result |
| `threek_dry_run_transfers_total{reason}` | transfers that would have happened in `dry-run` |
| `threek_member_records` | member records visible to this agent |
| `threek_poll_failures_total` | polls that could not read the local member |
| `threek_last_poll_timestamp_seconds` | Unix time of the last successful poll, for alerting on an agent that stopped working |

The endpoint also serves the Go runtime and process metrics of the agent.
The agent serves it, not the supervisor, so it is unavailable while the agent restarts.
The supervisor logs every agent restart; on Kubernetes, kubelet counts the sidecar's restarts.

## Versioning and upgrades

threek follows semantic versioning. The image tag carries both versions: `<threek version>-etcd<etcd version>`.

Upgrade rules:

- Supported upgrades go between adjacent minor versions only (N → N+1), never skipping a minor.
- A cluster with members on N and N+1 must work. This covers member records and any other data shared between agents.
- Within a minor version the member record schema is backward-compatible: readers ignore unknown fields, and new fields are optional.
- A field may be removed only in N+2, after N+1 stopped relying on it.
- Patch releases never change the schema.

Every supported etcd minor version is covered by the integration and end-to-end tests.

## Testing

Every pull request runs:

- **Unit tests** for the decision engine. It is a pure function, so each scenario from the table above is a table-driven case, including flapping, unknown rows, duplicate identities, learners and cooldown.
- **Unit tests** for argument handling: every etcd flag syntax, `ETCD_*` environment, `--config-file`, removal of `--threek-*`, and the ConfigMap layout kubelet mounts.
- **Unit tests** for reloading: mode switches without restart, invalid settings keep the previous ones, `off` withdraws the record and forgets the link history.
- **Supervisor tests**: signal forwarding, exit codes, `exec` for non-server invocations, agent crash isolation (etcd must survive).
- **Integration tests** against real etcd processes with mTLS: the etcd client calls, the probes and the return to the preferred leader with three agents.
- **End-to-end tests**: three members of the threek image in containers, run as Talos runs etcd, with links broken by `iptables` drop rules. They assert that:
  - the leader moves after a partial partition, the leader's agent counts the transfer, and clients of the cut-off member can write again;
  - the leader settles on the preferred member, leaves it when the preferred member is the one cut off, and returns after the partition heals;
  - nothing moves on a site failure, on a partition between followers, on a link that flaps for shorter than `--threek-down-after` probes, or in `dry-run`;
  - the image exits like upstream etcd on stop.

- **Policy tests**: `deploy/threek-agent.yaml` on a kind cluster of every supported Kubernetes minor version, with Pods laid out as etcd-operator builds them, through server-side dry runs: the sidecar, its arguments, mounts and identity, and Pods the policy must leave alone.

Integration and end-to-end tests run against every supported etcd minor version.

After the first release, a version skew test runs agents of N and N+1 in one cluster.

## Limitations and known interactions

### Client routing

threek restores a leader for every member. It does not change how clients reach members.
A client in site B that connects through a load balancer or a Kubernetes Service may still be routed to the member in site A across the broken link.
Clients must reach members without crossing the partition, for example:

- a Service with `trafficDistribution: PreferSameZone`;
- every member listed in the client configuration, so the client fails over on its own.

### Leader transfers by other tools

etcd-operator transfers leadership away from a member before defragmenting it.
With a preferred leader configured, threek does not return the leader to a member that fails the health check, and the cooldown delays any transfer after a leadership change.

### Peer TLS

Peers must use TLS with a CA (`--peer-trusted-ca-file`) or plain HTTP.
`--peer-auto-tls` is not supported: self-signed peer certificates cannot be verified, and the agent does not start.

### What a probe checks

A probe proves that the peer port accepts a TLS connection and answers HTTP within one second.
It does not prove that Raft messages flow, for example through a path with a broken MTU that only drops large packets.

## Rejected alternatives

| Alternative | Why not |
| --- | --- |
| A central process that aligns leaders of all clusters | it can be on the wrong side of the partition itself, needs credentials for every cluster, and must call `move-leader` through a leader it may not reach |
| A separate mechanism that pins leaders to the witness | covered by the preferred leader, with automatic departure when the witness is the one cut off |
| etcd with PreVote disabled | endless elections between the two sides of the partition |
| etcd fork with `ForgetLeader` | the same effect as an external `move-leader`, at the cost of maintaining a fork |
| The etcd image on Kubernetes too | etcd-operator would need free-form etcd flags, a per-cluster image and per-member environment; the image would tie etcd's security fixes to threek releases, a mode change would restart every member, and the agent would share etcd's memory limit |
| Mutating webhook instead of the policy | a service with its own certificates to run, and a failure mode in the admission path of every member Pod |
| Agent-to-agent reports over a separate port | a new port in every firewall and network policy and a listening agent; records in etcd carry the same reports over existing ports |
| etcd's transport metrics (`etcd_network_active_peers`) | blind to blackholed peers for minutes, see [Why not etcd's own transport state](#why-not-etcds-own-transport-state) |
| memberlist (SWIM) | its indirect probes deliberately hide partial partitions, which is exactly what threek has to detect |
| Embedding etcd as a library | every etcd flag would have to be parsed exactly as etcd does, and etcd would have to be rebuilt for every release |

## Open questions

1. Default cooldown: a long cooldown delays the reaction to a partition that starts right after a leadership change, a short one allows more transfers on flapping links.
2. How long to run `dry-run` before switching to `enforce`, and which metrics decide it.
3. The minimum set of supported etcd minor versions for the first release.
