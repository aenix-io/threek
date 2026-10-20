# threek design

Status: draft. The supervisor and the decision engine are implemented; the agent's etcd I/O is not.

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
- be invisible to the systems that launch etcd (Talos, etcd-operator): same binary path, same arguments, same exit codes;
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

### Image layout

threek ships as an etcd image in which the `etcd` binary is replaced:

| Path | Content |
| --- | --- |
| `/usr/local/bin/etcd` | threek supervisor, first `etcd` in `PATH` |
| `/usr/local/bin/etcd-upstream` | upstream etcd binary, unmodified |
| `/usr/local/bin/etcdctl`, `/usr/local/bin/etcdutl` | upstream tools of the same etcd version |

The image runs as an unprivileged user and needs no capabilities.

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

### Reading etcd settings

The agent needs a few etcd settings: client TLS files and client URLs.
etcd accepts settings from flags (`-flag`, `--flag`, `--flag=value`, `--flag value`), from `ETCD_*` environment variables and from `--config-file`.
The agent resolves its subset using etcd's own precedence rules and never rewrites them.

Connections to the local member go to `https://127.0.0.1:2379` (or the first local client URL).
Both Talos and etcd-operator include `localhost` and `127.0.0.1` in the etcd server certificate SANs and give it the `ClientAuth` extended key usage.
The agent therefore uses `--cert-file`/`--key-file` as its client certificate and `--trusted-ca-file` to verify the server.
No additional credentials are needed.

## Connectivity signal

threek does not probe the network itself.
It reads what etcd's own Raft transport reports:

```text
etcd_network_active_peers{Local="<member id>", Remote="<peer id>"}
```

etcd keeps transport streams between every pair of members, followers included, and sends a link heartbeat over idle streams about every 1.7 s.
A stream without traffic for 5 s (etcd's fixed connection read timeout) marks the peer inactive.
This captures what matters (whether Raft messages flow) rather than whether a TCP connect to port 2380 succeeds.

Every poll interval the leader's agent builds a reachability view:

- its own row from the local member's `/metrics`;
- every other voting member's row from that member's `/metrics` on its advertised client URL (port 2379, already open between members, same certificate as above).

Targets come only from the local member's `MemberList`, learners excluded.
Before using a member's row, the agent calls `Status` on the same URL and checks that the cluster ID and member ID in the response match.

Rules for reading the metric:

- A missing series means inactive: etcd creates the series only after the first successful activation.
- Series for members that are no longer in `MemberList` are ignored: etcd never deletes them.
- An increase of `etcd_network_disconnected_peers_total` between two polls counts as an inactive sample, so short flaps between polls are not lost.
- If a member's `/metrics` cannot be fetched in the current poll, its row is unknown. A member with an unknown row is never a transfer target.

Link state uses hysteresis on top of etcd's own 5 s timeout:

- a link is **down** after `--threek-down-after` consecutive inactive samples;
- a link is **up** again after `--threek-up-after` consecutive active samples.

Because every row is fetched by the leader's own agent at a locally known moment, view freshness never compares clocks of different machines.

## Member records

For the preferred leader feature the leader must know the identity of every member.
Each agent with an identity configured keeps a record in its own cluster:

```text
key:   /threek/members/<member id, hex>
value: {"schemaVersion": 1, "identity": "zone-c", "agentVersion": "v0.1.0"}
lease: 30 s, kept alive by the agent
```

- The record is written once when the agent starts or when its lease is lost. Keep-alives are not Raft proposals, so steady state adds no writes.
- A member cut off from the leader cannot keep its lease alive, and its record expires. This does not matter: the partition rule does not use identities.
- Without `--threek-identity` the agent writes nothing.
- Kubernetes API servers only read keys under their `--etcd-prefix` (`/registry` by default), so the records are invisible to them. They do appear in snapshots and in `etcdctl get --prefix`.

## Decisions

The decision engine is a pure function of the reachability view, member records, local leadership state and timers.
It returns either "no action" or "transfer to member X, reason R".

### Priorities

1. **Partial partition.** The leader has a link to some member that has been down for at least `--threek-move-after` (counted from the moment the link was declared down), and the third member has every link up, both in its own row and in the leader's. Transfer to that member, even if it is not the preferred leader.
2. **Preferred leader.** Every link has been up for at least `--threek-prefer-after`, the local member is not the preferred one, exactly one voting member carries the preferred identity, and that member is healthy. Transfer to the preferred member.
3. Otherwise, do nothing.

"Healthy" for the preferred member means its `Status` call answers within the poll timeout and its applied index is not lagging behind.
A member busy with defragmentation blocks its backend, fails this check and is not chosen as a target.

If several members share the preferred identity, the agent logs a warning and ignores the preference.

### Rate limit

An agent may not transfer leadership away until `--threek-move-cooldown` has passed since its member became the leader.
Leadership acquisition is observed locally (through `Status`), so the limit holds across leader changes, and no shared state is needed.
The trade-off: a partition that starts shortly after any leadership change, including a manual or operator-initiated one, is handled only after the cooldown expires.

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
| T + 5 s | etcd on A and B marks the peer inactive |
| T + 8 s | the agent on A declares the A ↔ B link down (3 samples at 1 s) |
| T + 23 s | `--threek-move-after` passes, A transfers leadership to C |
| T + 24 s | B receives heartbeats from the new leader and serves again |

All defaults are starting points and are to be tuned on a test bench.

## Configuration

Settings are extra etcd flags. The supervisor removes them before starting etcd, otherwise etcd would reject the unknown flags.
Every flag has an environment fallback with the same name (`--threek-mode` ↔ `THREEK_MODE`). The flag wins.
The environment is meant for values that differ per member and come from the Kubernetes downward API.

| Flag | Default | Meaning |
| --- | --- | --- |
| `--threek-mode` | `off` | `off`: plain etcd, no agent. `dry-run`: decide and log, never transfer. `enforce`: transfer. |
| `--threek-poll-interval` | `1s` | how often the agent samples metrics and status |
| `--threek-down-after` | `3` | consecutive inactive samples before a link is down |
| `--threek-up-after` | `10` | consecutive active samples before a link is up |
| `--threek-move-after` | `15s` | how long a link stays down before a partition transfer |
| `--threek-move-cooldown` | `2m` | minimum leadership tenure before this agent transfers leadership away |
| `--threek-identity` | empty | label of this member: a zone or a node name, unique within the cluster |
| `--threek-preferred-leader` | empty | identity that holds the leader in normal operation; empty means no preference |
| `--threek-prefer-after` | `5m` | how long connectivity must be full before returning to the preferred leader |
| `--threek-metrics-listen` | empty | address for the agent's Prometheus endpoint; empty disables it |

`off` is the default, so the image is a plain etcd until threek is enabled explicitly.
Rolling back is a flag change, not an image change.

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

Requires support in the operator:

- `spec.extraArgs` with a denylist of operator-managed flags;
- a `THREEK_IDENTITY` environment variable on the etcd container from the downward API, because `extraArgs` is shared by every member while the identity is not:
  - node name: `fieldRef: spec.nodeName`, always available;
  - zone: `fieldRef: metadata.labels['topology.kubernetes.io/zone']`, only when the cluster copies node topology labels to pods ([KEP-4742](https://github.com/kubernetes/enhancements/issues/4742));
- the threek image set explicitly in the operator configuration.

The operator applies extra arguments only to new members, so switching `dry-run` to `enforce` means rolling the members one by one.

```yaml
apiVersion: etcd-operator.cozystack.io/v1alpha2
kind: EtcdCluster
spec:
  version: "3.6.5"
  extraArgs:
    threek-mode: dry-run
    threek-preferred-leader: zone-c
```

The operator's restore init container runs from the same image, so the image must keep `etcdutl`.

## Security

- The agent listens on nothing by default. It only connects to client URLs of its own cluster's members.
- It authenticates with the member's own server certificate and verifies peers with the member's own client CA. A member of another cluster signed by a different CA is rejected by TLS. Clusters that share a CA are told apart by the cluster ID check described in [Connectivity signal](#connectivity-signal).
- The agent writes only under `/threek/members/`, and only when an identity is configured.
- The optional metrics endpoint is plain HTTP and exposes no secrets. Bind it to a trusted address.

## Observability

Logs: every decision, including "no action" after a state change, is logged once with its reason and the reachability view that led to it.

Metrics, when `--threek-metrics-listen` is set:

| Metric | Meaning |
| --- | --- |
| `threek_link_up{from,to}` | link state after hysteresis, as seen by this agent |
| `threek_is_leader` | whether the local member is the leader |
| `threek_transfers_total{reason,result}` | transfers attempted, by reason and result |
| `threek_dry_run_transfers_total{reason}` | transfers that would have happened in `dry-run` |
| `threek_member_records` | member records visible to this agent |

The supervisor logs every agent restart.

## Versioning and upgrades

threek follows semantic versioning. The image tag carries both versions: `<threek version>-etcd<etcd version>`.

Upgrade rules:

- Supported upgrades go between adjacent minor versions only (N → N+1), never skipping a minor.
- A cluster with members on N and N+1 must work. This covers member records and any other data shared between agents.
- Within a minor version the member record schema is backward-compatible: readers ignore unknown fields, and new fields are optional.
- A field may be removed only in N+2, after N+1 stopped relying on it.
- Patch releases never change the schema.

threek also depends on etcd's metric names. Every supported etcd minor version is covered by the end-to-end tests.

## Testing

Every pull request runs:

- **Unit tests** for the decision engine. It is a pure function, so each scenario from the table above is a table-driven case, including flapping, unknown rows, duplicate identities, learners and cooldown.
- **Unit tests** for argument handling: every etcd flag syntax, `ETCD_*` environment, `--config-file`, removal of `--threek-*`.
- **Supervisor tests**: signal forwarding, exit codes, `exec` for non-server invocations, agent crash isolation (etcd must survive).
- **End-to-end tests**: three real etcd members in containers, partial partitions made with `iptables` drop rules. They assert that the leader moves within the expected time, that nothing moves on a full site failure, in `dry-run`, or on flapping, and that the preferred leader comes back after the partition heals. The suite runs against every supported etcd minor version.

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

### etcd versions

The connectivity signal relies on `etcd_network_active_peers` and `etcd_network_disconnected_peers_total`, verified against etcd v3.6.
Every other supported version must pass the end-to-end suite.

## Rejected alternatives

| Alternative | Why not |
| --- | --- |
| A central process that aligns leaders of all clusters | it can be on the wrong side of the partition itself, needs credentials for every cluster, and must call `move-leader` through a leader it may not reach |
| A separate mechanism that pins leaders to the witness | covered by the preferred leader, with automatic departure when the witness is the one cut off |
| etcd with PreVote disabled | endless elections between the two sides of the partition |
| etcd fork with `ForgetLeader` | the same effect as an external `move-leader`, at the cost of maintaining a fork |
| Sidecar or static pod next to etcd | a different mechanism per platform, separate credentials, changes to pod specs and network policies |
| Agent-to-agent reports over a separate port | a new port in every firewall and network policy, a listening agent, and a TCP probe instead of the Raft-level signal |
| memberlist (SWIM) | its indirect probes deliberately hide partial partitions, which is exactly what threek has to detect |
| Embedding etcd as a library | every etcd flag would have to be parsed exactly as etcd does, and etcd would have to be rebuilt for every release |

## Open questions

1. Default cooldown: a long cooldown delays the reaction to a partition that starts right after a leadership change, a short one allows more transfers on flapping links.
2. How long to run `dry-run` before switching to `enforce`, and which metrics decide it.
3. The minimum set of supported etcd minor versions for the first release.
