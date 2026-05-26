# Skein

> **Working title.** A C# library for low-latency distributed state, built on FASTER. Four-tier model: replicated reference data, sharded persistent state, followers for cross-node reads, and session-bound hot state with active warm-replica failover.

> **Status: learning project / pre-alpha.** This is a deliberately scoped library for exploring distributed systems primitives — replication protocols, consensus, placement, and failover — in the .NET ecosystem. It is not a Redis replacement. Read the [Non-goals](#non-goals) section before deciding whether to use it.

---

## What this is

Skein is an **embedded distributed cache library** for .NET 8+ that lets an application's processes form a cluster, share state across nodes, and survive node failures without losing the hot working set. It is designed for workloads where the network round-trip to a separate cache service (Redis, Garnet, etc.) is too expensive — typically sub-millisecond p99 budgets.

The library is built on top of [FASTER](https://github.com/microsoft/FASTER) (Microsoft's high-performance KV storage engine) and uses gRPC for inter-node communication.

### A note on FASTER vs. Tsavorite

Tsavorite is Microsoft's active fork of FASTER, used internally by [Garnet](https://github.com/microsoft/garnet). It is the architectural successor and is where Microsoft's investment goes. **However, as of this writing, Tsavorite has no standalone NuGet package** — it ships only as part of `Microsoft.Garnet`, which pulls in Lua, Azure libraries, the RESP server, and other dependencies inappropriate for an embedded library. There is an [open issue](https://github.com/microsoft/garnet/issues/691) requesting a standalone Tsavorite package; until it ships, Skein uses `Microsoft.FASTER.Core`.

The storage layer is wrapped behind a small internal interface so the migration to Tsavorite is a localized change when its packaging situation improves. The wrapper exists explicitly for this reason and should not grow features that depend on FASTER-specific behavior.

## What this is not

- **Not a Redis replacement.** If you want a high-performance .NET cache *server*, use [Garnet](https://github.com/microsoft/garnet). It is more mature, RESP-compatible, and works with existing Redis clients.
- **Not a database.** No SQL, no secondary indexes, no transactions across arbitrary keys. The replicated and sharded tiers are key-value only.
- **Not an actor framework.** No supervision, no message protocols, no workflows. Compose with Orleans or Akka.NET for those concerns.
- **Not for every workload.** If your access patterns are uniformly random across keys, this library buys you nothing over Redis. See [When to use](#when-to-use-skein).

---

## The architectural thesis

Most "distributed cache" libraries treat all data the same way. A real low-latency workload has at least three different kinds of state, each with different access patterns and different replication needs:

| Tier | Examples | Access pattern | Replication model |
|---|---|---|---|
| **Replicated** | Configs, feature flags, currency rates, instrument metadata | Read constantly, written rarely. Small (fits in RAM per node). | Mirrored to every node. Eventually consistent. Updates propagated via gossip. |
| **Sharded** | User profiles, account state, large persistent entities | Read/write per entity. Large (does not fit in one node). | Partitioned by hash; RF=2 (primary + replica). |
| **Followers** | Auth sessions, per-tenant permissions, hot reference slices | Read from many nodes, written by one owner | Owner authoritative; opt-in read-only replicas on nodes that read the key. Invalidation- or TTL-based coherency. |
| **Session** | Game rooms, trading sessions, RTB user-session state, multiplayer matches | Ephemeral, hot, written constantly during a session's lifetime | Active warm replica streaming operations from primary. Sub-second failover with no journal replay. |

The four tiers exist because real low-latency workloads have at least four distinct data shapes, and forcing them all into one replication model is the failure mode of most "distributed cache" libraries.

The **followers tier** is the answer to "this data has a clear owner, but other nodes need to read it frequently enough that an RPC per read is too expensive." Auth sessions are the canonical example: owned by the node that handled login, but read by every node that subsequently handles a request from that user. Selective, dynamic replication — not the all-to-all of the replicated tier, not the deterministic RF=2 of the sharded tier.

The **session tier** is the distinguishing feature. Orleans and Akka.NET cluster sharding can do "single active entity with persistent journal" well, but on failure they reactivate the entity *from scratch* by replaying the journal. For long-lived sessions with thousands or millions of events, that recovery time is unacceptable in a low-latency context. Skein's session tier maintains a warm in-memory replica that can promote in milliseconds.

---

## When to use Skein

**Good fit:**

- Hot-path latency budget is tight (RTB, game backends, financial routing, real-time analytics).
- Most requests have a natural shard key (user, tenant, room, instrument) that aligns with the load balancer.
- Working set per request is mostly reference data plus one or a few entities.
- The application controls its deployment and can co-locate the cache with the service process.

**Bad fit — use Redis or Garnet:**

- Cache is shared by many independent services.
- No natural shard key in the request.
- Cache lifecycle must be decoupled from app lifecycle.
- Workload is uniformly random across keys (no locality to exploit).

**Use Skein as an *acceleration layer* on top of Redis** if your hot working set has locality but your cold tail does not. Skein handles the local + sharded path; Redis handles the long tail.

---

## Architecture

### Node model

Each Skein node is an application process embedding the library. Nodes discover each other through a configurable membership provider (static config, DNS, Consul, Kubernetes service discovery — pluggable). Once formed, the cluster maintains a topology view that includes:

- Membership (who is alive).
- Hash ring (which node owns which shard).
- Session directory (where each active session lives + replica location).

The topology is observable and is the integration point for framework adapters (see [Framework integration](#framework-integration)).

### Storage per node

Each node embeds a FASTER KV instance for the sharded and replicated tiers and uses FasterLog as the replication WAL. FASTER provides:

- Lock-free concurrent reads and writes.
- Atomic single-key RMW (read-modify-write).
- Hybrid memory + disk storage with configurable size tiers.
- Checkpointing and recovery.

These primitives are accessed through a thin internal abstraction (`IStorageEngine`) so migration to Tsavorite — once it has a standalone package — is a swap of one implementation rather than a rewrite. Features that exist in Tsavorite but not in FASTER (e.g., `LockableContext` for in-process multi-key locking) are deferred until the migration; the design does not depend on them in Phase 1.

### Networking

- **Inter-node:** gRPC over HTTP/2. HTTP/3 (QUIC) is on the roadmap once .NET's msquic story is more stable across deployment targets.
- **Client-facing:** the library is embedded in the application, so there is no public "client protocol." Application code calls the library directly; the library routes internally to the correct node.

### Sharding

- Consistent hash ring with virtual nodes (default: 256 vnodes per physical node, configurable).
- Replication factor 2: primary + 1 replica, with the replica deterministically placed at the next physical node in the ring.
- Rebalancing on node join/leave moves shards atomically: streaming snapshot + log tail + cutover with version-fenced routing.

### Replication protocols

Four different replication mechanisms for the four tiers:

**Replicated tier:** updates broadcast to all nodes via gossip (epidemic protocol). Eventually consistent, last-write-wins by timestamp. Suitable for small, infrequently-written data.

**Sharded tier:** primary holds the authoritative copy and ships operations to the replica via FasterLog streaming. Replication mode is configurable per dataset:

- `Async` — primary acks before replica ack. Best write latency. Loses up to a few ms of writes on primary failure.
- `SyncToMemory` — primary acks after replica has the operation in memory (microseconds). Zero loss on single-node failure. Recommended default for most workloads.
- `SyncToDurable` — primary acks after replica fsyncs. Strongest durability, highest latency.

**Followers tier:** owner-authoritative. Each owner maintains a registry of nodes that currently follow each key. On write, the owner notifies followers (invalidate or push-update, configurable). Followers are created on-demand (first read on a non-owner node) or via explicit pinning. Followers are evicted by LRU pressure, TTL expiry, or owner request. The follower registry is itself replicated to the sharded-tier replica so that promotion preserves the follower set.

For auth and other security-sensitive datasets, prefer `WriteThroughInvalidate` over TTL-based coherency — staleness windows on auth records are a real risk on logout/revocation.

**Session tier:** continuous operation-log streaming primary → replica, with the replica applying operations in order to maintain an identical in-memory state. On primary failure, the replica is fenced-promoted (see [Failover](#failover)).

### Failover

Failure detection uses SWIM-style gossip with φ-accrual scoring. A node is considered failed when its score exceeds the configured threshold *and* a quorum of observers concur.

Promotion of a session-tier replica to primary uses fencing tokens to prevent split-brain:

1. The cluster's coordinator (Raft-elected, see [Coordinator](#coordinator)) assigns a monotonically increasing epoch to each promotion.
2. The new primary advertises its epoch in every response.
3. Clients reject responses from primaries with lower epochs (handles the case where an old primary returns from a partition).
4. The old primary, on rejoining the cluster, sees the higher epoch and steps down rather than serving stale writes.

This is enough to guarantee no acknowledged write is lost as long as one of the two replicas survives. With both lost simultaneously, data loss is bounded by the replication mode (see above).

### Coordinator

A small Raft-replicated coordinator service runs across (typically) 3 or 5 nodes in the cluster. Its responsibilities are minimal:

- Membership decisions (admit/eject nodes).
- Epoch assignment for promotion.
- Topology version vector (for client routing consistency).

The coordinator is *not* on the data path. Its load is proportional to topology change frequency, not request rate. A small coordinator quorum can serve a large data cluster.

---

## Public API sketch

```csharp
// Cluster bootstrap
var cluster = await SkeinCluster.JoinAsync(new SkeinOptions
{
    NodeId = "node-a",
    BindAddress = "0.0.0.0:7777",
    Seeds = ["node-a:7777", "node-b:7777", "node-c:7777"],
    DataPath = "/var/lib/skein",
});

// Replicated tier — declared at startup, mirrored to every node
var permissions = cluster.Replicated<string, Permissions>("permissions");

await permissions.SetAsync("user:42", new Permissions(...));
var p = await permissions.GetAsync("user:42");  // local read, always

// Sharded tier — partitioned by hash, RF=2
var profiles = cluster.Sharded<string, UserProfile>("profiles", new ShardedOptions
{
    ReplicationMode = ReplicationMode.SyncToMemory,
});

await profiles.SetAsync("user:42", profile);  // routed to primary
var prof = await profiles.GetAsync("user:42"); // local if owner, RPC otherwise

// Followers tier — owner-authoritative with opt-in read replicas
var sessions = cluster.WithFollowers<string, AuthSession>("auth-sessions", new FollowerOptions
{
    CoherencyMode = CoherencyMode.WriteThroughInvalidate,
    ReplicaTtl = TimeSpan.FromMinutes(15),
    MaxFollowersPerKey = 16,
});

var session = await sessions.GetAsync(sessionId);
// First read on this node: RPC to owner, become a follower locally
// Subsequent reads: local, ~2-5μs
// On owner: writes invalidate all current followers

// Session tier — open a session with warm replica
await using var session = await cluster.OpenSessionAsync<GameRoomState>("room:42");

await session.ApplyAsync(new PlayerJoined(playerId));
await session.ApplyAsync(new MoveMade(...));
var state = session.State;  // local in-memory access

// Session closes on dispose; warm replica is released
```

### Topology API (for framework integration and observability)

```csharp
public interface ITopology
{
    NodeId GetPrimaryFor(string shardKey);
    NodeId GetReplicaFor(string shardKey);
    Task<NodeId> GetSessionPrimaryAsync(string sessionKey, CancellationToken ct);
    Task AwaitSessionReadyAsync(string sessionKey, CancellationToken ct);
    IObservable<TopologyChange> Changes { get; }
}
```

This is the surface that Orleans/Akka adapters consume.

---

## Framework integration

Skein is usable standalone, but composes cleanly with actor frameworks when grain/entity placement should follow the data.

### Orleans

A custom `PlacementDirector` consults Skein's topology and places grains on the node that owns the session:

```csharp
[CoLocatedWith("game-rooms")]
public interface IGameRoomGrain : IGrainWithStringKey { ... }
```

On grain activation, `OnActivateAsync` awaits session readiness before serving requests. On primary failure, Orleans reactivates the grain — Skein's warm replica has already been promoted, so activation completes in milliseconds rather than waiting for a journal replay.

### Akka.NET

A custom `IShardAllocationStrategy` consults Skein's topology to place shards on the owning node. Akka.Persistence journal interface can be implemented on top of FasterLog as a separate (and useful) integration even without active-replica failover.

Both adapters are thin shims (~200 lines each) over Skein's public topology API. They live in separate NuGet packages so the core library doesn't depend on either framework.

---

## Deployment

Skein is designed to run in containers and orchestrated environments from day one. The architectural choices below exist to keep the library cleanly deployable on Docker, Kubernetes, and similar platforms without retrofitting.

### Node identity and addressing

- **Bind and advertise addresses are separate config values.** Inside a pod, the library binds to `0.0.0.0`, but advertises its pod IP or DNS name to peers. Hardcoding either address would break in containerized environments.
- **Node IDs are stable and persistent.** Generated on first boot and persisted to the data directory (or derived from a stable identity such as a StatefulSet ordinal). Identification by IP address is explicitly rejected — pods get new IPs on restart, and identifying by IP makes "node restarted" indistinguishable from "different node joined."
- **All ports, addresses, data paths, and cluster names come from configuration.** No `const int Port = 7777` anywhere in the codebase.

### Configuration

Uses `Microsoft.Extensions.Configuration` with layered sources:

1. JSON files (for local development).
2. Environment variables (the canonical Kubernetes mechanism).
3. Command-line arguments (for debugging and overrides).

Environment variables override files; arguments override env vars. The same image is used in every environment; configuration varies.

### Persistent storage

Two supported deployment modes:

| Mode | Survives container restart? | Survives pod replacement? | Trade-off |
|---|---|---|---|
| **PersistentVolumeClaim (recommended default)** | ✅ Yes | ✅ Yes | Faster rejoin after restart; less network traffic on rolling deploys; survives node loss. |
| **EmptyDir** | ✅ Yes | ❌ No (data lost) | Lower latency on local I/O; no PV management; lower cost. Returning nodes treated as fresh joins. |

The cluster's catch-up protocol (used for both genuine failures and EmptyDir restarts) is designed to be fast: parallel chunk streaming, hot-shard prioritization, peers serving reads during catch-up.

### Lifecycle

- **Graceful shutdown** (SIGTERM with grace period): node announces intent-to-leave to the cluster, flushes in-flight replication to peers, checkpoints storage state, drains in-flight requests with a timeout, exits cleanly. This avoids the 1–3 second failure-detection delay per pod during rolling deploys.
- **Sudden crash** (OOMKill, segfault, SIGKILL): cluster detects via gossip and φ-accrual failure detector. Replicas promote where applicable. Data recoverable from storage checkpoints if storage persisted.

### Health probes

Two separate endpoints on a dedicated HTTP port (not the gRPC port):

- **`/healthz/live`** — returns OK as long as the process is responsive. Should *not* fail during rebalancing or peer degradation; failing liveness causes Kubernetes to restart the pod, which is usually the wrong action for a transient cluster condition.
- **`/healthz/ready`** — returns OK only when the node is a full cluster member with caught-up replication. New pods correctly fail readiness during join/catch-up, preventing premature traffic.

### Membership discovery

Pluggable `IMembershipProvider` interface with these implementations planned:

- **Static config** — for local development and Docker Compose.
- **DNS-based** — points at a Kubernetes headless service; resolves to all pod IPs. Default for K8s deployments.
- **Kubernetes API** — uses the K8s API for richer pod metadata and event subscriptions (later phase).

### Observability

Built in from the start, not retrofitted:

- **Structured logging** via `Microsoft.Extensions.Logging` with JSON output, configured at startup via `LogLevel` config.
- **Prometheus metrics** via `System.Diagnostics.Metrics` exposed at `/metrics`.
- **OpenTelemetry tracing** — gRPC calls participate in trace context propagated by clients.

### Clocks

Distributed systems are sensitive to clock skew between hosts:

- **Monotonic clocks** (`Stopwatch.GetTimestamp()`) for measuring durations within a process.
- **Logical clocks** (Lamport timestamps) for ordering events across nodes. Wall-clock timestamps are never used for replication ordering or conflict resolution.
- **Wall clocks** only where civil time is genuinely required (TTL expiry, audit logging).

### Container artifact

- Single binary, produced by `dotnet publish -c Release`.
- Runs on the official `mcr.microsoft.com/dotnet/aspnet:8.0` runtime image.
- Runs as non-root (`USER 1000`).
- Configuration via environment variables; no config baked into the image.

### Kubernetes deployment shape

The expected (not yet shipped) deployment:

- **`StatefulSet`** for cluster nodes — stable network identities and ordered pod creation.
- **Headless `Service`** for inter-pod DNS discovery.
- **`PodDisruptionBudget`** to prevent excessive simultaneous evictions.
- **`PersistentVolumeClaim`** template per pod (when running in PVC mode).

A reference Helm chart will be provided once the cluster is functional (post-Phase 5).

---

## Non-goals

Stating these explicitly to keep the project from drifting:

- **No RESP wire protocol.** This is an embedded library, not a server.
- **No multi-region replication.** Single-cluster only. Multi-region is a different design (active-active with conflict resolution, or active-passive with WAN replication) and a different project.
- **No full Redis data-structure surface.** KV plus the session model is the scope. No streams, no pub/sub, no sorted sets. (FasterLog can be exposed for users who want an append-only log primitive, but it's not a Streams equivalent.)
- **No security model beyond TLS for inter-node traffic.** Auth/authz of application-level requests is the caller's problem.
- **No automatic key-level data movement based on access patterns.** Placement happens at shard boundaries on cluster topology changes, and at session boundaries on session open. The library does not infer placement from observed traffic.
- **Native AOT compilation: deferred, not rejected.** AOT compatibility will be revisited once the core architecture is stable and the storage engine's AOT behavior is well understood. The design (byte-array payloads, source-generated logging, no runtime reflection in hot paths) aims to keep AOT viable as a later phase.

---

## Implementation roadmap

Sequenced to make each step independently usable and to surface design problems early.

### Phase 1 — Single-node foundation
- FASTER-backed KV with a clean .NET-idiomatic API (`Get`, `Set`, `Delete`, `Update`/RMW).
- Storage abstraction (`IStorageEngine`) so Tsavorite migration is localized.
- FasterLog wrapper for the append-only log primitive.
- Benchmarks vs. `MemoryCache` and Redis-via-StackExchange.

### Phase 2 — Two-node cluster, no replication
- gRPC service definition for inter-node RPC.
- Static cluster config; nodes discover each other from the config file.
- Consistent hash ring; requests for non-local keys are RPC'd to the owner.
- No replication, no failover yet.

### Phase 3 — Async replication, no failover
- RF=2 placement (primary + next-in-ring as replica).
- FasterLog as the replication WAL; primary streams to replica.
- Replica catch-up protocol after disconnect (offset-based resume).
- Replication mode configurable: `Async` only at this phase.

### Phase 4 — Membership and failure detection
- SWIM gossip for membership.
- φ-accrual failure detector.
- Nodes can join/leave the cluster dynamically (without rebalance yet — fixed-size cluster).
- Membership events exposed on the topology API.

### Phase 5 — Failover with fencing
- Small Raft implementation for the coordinator (or adopt DotNext.Net.Cluster).
- Epoch-based promotion of replicas.
- Client-side topology cache with epoch verification.
- `SyncToMemory` replication mode.

### Phase 6 — Replicated tier
- Gossip-based broadcast for small replicated datasets.
- Last-write-wins by timestamp (logical clocks, not wall clock).
- Bulk-load from an authoritative source (e.g., DB) on node startup.

### Phase 7 — Followers tier
- Owner-side follower registry per key.
- On-demand follower creation on first non-owner read.
- Explicit pinning API for known working sets.
- Invalidation broadcast on owner write (`WriteThroughInvalidate`).
- TTL-based coherency mode as alternative (`TtlVersioned`).
- LRU eviction of followers under memory pressure.
- Follower registry replicated to sharded-tier replica.

### Phase 8 — Session tier
- Session open/close protocol.
- Operation-log streaming primary → replica.
- Promotion on primary failure: warm replica becomes primary, new replica spun up on a third node.
- `OnSessionReady` hook for application code.
- Optional follower support on sessions (spectator-style remote reads).

### Phase 9 — Repartitioning
- Add/remove nodes triggers shard movement.
- Live migration protocol: snapshot + log tail + cutover with version fence.
- Rebalance budget to prevent thrashing.
- Catch-up protocol prioritizing hot shards; serve reads from peers during catch-up.

### Phase 10 — Framework adapters
- `Skein.Orleans` package: placement director, activation hooks.
- `Skein.Akka` package: shard allocation strategy, journal adapter.

### Phase 11 — Hardening
- Chaos test harness (process kills, network partitions, latency injection, packet loss).
- Property-based testing of invariants (no acknowledged write lost when ≥1 replica survives).
- Long-running soak tests.
- Performance benchmarking against Redis and Garnet baselines.

This is a multi-month project, intentionally. Each phase teaches something specific and produces working, testable code.

---

## Comparison with adjacent systems

| | Skein | Redis / Garnet | Orleans / Akka.NET | Cassandra |
|---|---|---|---|---|
| Deployment | Embedded library | Separate server | Embedded framework | Separate cluster |
| Data model | KV + session | Rich (KV, streams, pub/sub, ...) | Actor state | Wide-column |
| Hot-path latency | Sub-100μs (local) | 200μs–1ms (network) | 50–200μs (in-process actor) | 1–10ms |
| Failover with hot state | Yes (warm replica) | Yes (Sentinel/Cluster) | No (journal replay) | Yes (RF=N quorum) |
| Sharding | Consistent hash, RF=2 | Cluster mode | Cluster sharding | Token ring, RF=N |
| Co-location with app | Yes | No | Yes (single active) | No |
| Maturity | Pre-alpha | Production | Production | Production |

The honest position: Skein is targeting a narrow gap — embedded distributed state with warm-replica failover for low-latency .NET workloads — that none of the others fill exactly. For most workloads, one of the others is the right answer.

---

## Building and contributing

Project is in early design. The roadmap above is the contribution guide for now; phase boundaries are natural points for PRs. Chaos testing infrastructure is welcome before correctness-sensitive phases.

License: MIT (planned).

---

## Acknowledgements

This design owes a lot to:
- **FASTER** and **Tsavorite** (Microsoft Research) — for the storage engine and for proving the architectural shape. FASTER is the dependency today; Tsavorite is the architectural successor and the migration target.
- **FoundationDB** — for the model of layered, deterministic state machines.
- **Akka cluster sharding** and **Orleans** — for the placement and lifecycle ideas, and for the gap that motivated the session tier.
- **Cassandra** and **DynamoDB** — for consistent hashing with virtual nodes.
- **Jepsen** — for the standard of correctness testing distributed systems should be held to.
