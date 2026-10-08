# Design: Shared always-on index service (`k4a-index`)

**Status:** shipped — Phases 0-4 are implemented. Phase 5 (per-caller authorization) is the remaining
follow-up; it gates following *sensitive* topics, not the deployment.
**Depends on:** [`local-index.md`](local-index.md) — this promotes that
doc's per-process Tier-3 indexer into a shared network service, and
picks up its two explicitly-parked items ("Background daemon (`k4a
indexd`)" and "Shared/networked indexes"). [`state-management.md`](state-management.md)
— the cardinal rule ("the cluster is the source of truth; state is a
deletable optimization") is preserved, not weakened.
**Depends on:** [`deep-search.md`](deep-search.md) — the client keeps the
scan path as the correctness floor; the service never replaces it.
**Code:** `cmd/k4a-index/` (the daemon: `main.go`, `server.go`,
`manager.go`, `worker.go`, `policy.go`) reusing `internal/index` +
`internal/kafka` unchanged; `internal/remoteindex` (the client backend);
`internal/indexwire` (+ `proto/`) for the gRPC contract; `charts/k4a-index`
for the deploy. The MCP frontend is `cmd/k4a-mcp` + `charts/k4a-mcp`.

## Purpose

The local index (`local-index.md`) makes search instant — but per
process. It carries two structural limits, both of which fall out of the
"single binary, local only" premise:

1. **Single-writer per topic (flock).** A second k4a on the same machine
   reading the same topic enters read-only mode; across machines, every
   user cold-starts and re-indexes the same topic independently. N users
   do N× the indexing work and pay N× the disk.
2. **Catch-up gap on open.** The indexer only runs while k4a runs. Open a
   cold topic and the first interactive search still falls to scan while
   the backfill catches up.

For several people searching the *same* clusters — repeatedly, and
during incidents when everyone piles onto the same hot topic at once —
both limits bite. The honest fix is to run the indexer **once, centrally,
always on**: a shared `k4a-index` that continuously tails a set of
topics and answers queries out of a warm index for every k4a pointed at
it, no matter how many are searching or how heavy the load.

⚠ **Latency reality check.** An early version of this doc promised
"milliseconds". On a production-sized index a query can cost **~2–4s
server-side**, and it scales with how much the index holds, not with the
time window asked for — every search is compiled to an unanchored regex
walked across the term dictionary.

This is not a replacement for the local index — it's the shared
deployment of the same Tier-3 machinery. A k4a with no service
configured behaves exactly as `local-index.md` describes.

## Non-goals (what this is not)

- **Not a query engine over cold data.** The service indexes a rolling
  recent window per followed topic, same as the local index. Archive
  search (Athena/S3) stays the Tier-6 idea in `local-index.md`.
- **Not a hard dependency.** If `k4a-index` is unreachable, k4a runs
  its existing in-process Tier 1/2/3. The service is an accelerator.
- **Not multi-cluster fan-in.** One daemon serves one Kafka cluster
  (one MSK). A second cluster is a second daemon. No cross-cluster
  meta-index.

## Relationship to the local index and the state tiers

`state-management.md` defines Tier 3 as the local Bleve index, rebuildable
from the broker. The shared service does not add a new *client* tier — to
the client it is simply an **alternate coverage source** for the existing
`SearchDispatcher`. The dispatcher already decides index-vs-scan from
per-partition offset coverage (`Bookkeeper`); here that coverage comes
over the wire instead of from `~/.k4a/index/`.

The cardinal rule holds unchanged: the broker is truth, the shared index
is a deletable optimization. `rm -rf` the daemon's storage → clients fall
back to scan → the daemon re-warms from the broker. Nothing observable
changes except latency.

## User model

**No new mode, and ideally no new config beyond one endpoint.** `s`
still opens search, results still stream in newest-first. What changes is
who answered and how fast. The search header gains a shared-source
annotation:

```
search: /foo · example.ingest.prices.v1
indexed (shared) · 47 matches · 9ms
```

vs. the local-index render (`indexed · 12ms`) vs. scan
(`scanned · 47 matches · 9.4s · 167 MB`).

**Warm-on-first-search.** The very first search of a topic nobody has
searched yet cannot be instant — nothing is indexed. That search falls
back to a normal scan (the correctness floor), and *as a side effect*
registers the topic with the daemon, which begins following it. Every
subsequent search of that topic — by anyone — is served from the index
rather than a scan (seconds, not tens of seconds; see the latency note
above). The header tells the honest story:

```
search: /foo · some.newly.searched.topic
scanned · 12 matches · 6.1s   (now following — next search is indexed)
```

Config is a single block; absence = today's local-only behavior:

```yaml
contexts:
  staging:
    # …the usual connection fields…
    index-endpoint: k4a-index.example.com:9500  # unset → local-only
    index-insecure: true    # plaintext gRPC; the port is network-gated
```

The endpoint is a **per-context** field, not a nested `index.service` block
— a context already names one cluster, and the daemon serves exactly one
cluster, so they pair one-to-one. `--index-endpoint` / `K4A_INDEX_ENDPOINT`
and `--index-insecure` override it per invocation. Port 9500 is the gRPC
port; a TLS-terminating load balancer in front can expose :443 (it must
negotiate ALPN `h2`; see [`guides/deploy.md`](../guides/deploy.md#networking)).

## Architecture

```
   ┌────────────┐   ┌────────────┐   ┌────────────┐        many k4a
   │  k4a (dev) │   │  k4a (dev) │   │  k4a (dev) │  …      clients
   └─────┬──────┘   └─────┬──────┘   └─────┬──────┘
         │  gRPC (Search + Follow, streaming)  │
         └──────────────┬──────────────────────┘
                        ▼
        ┌──────────────────────────────────────────┐
        │                k4a-index                 │
        │                                           │
        │  ┌───────────────┐    ┌────────────────┐  │
        │  │ Follow-set    │    │ gRPC server    │  │
        │  │ manager       │◀──▶│ (indexwire)    │  │
        │  │ (allowlist,   │    └───────┬────────┘  │
        │  │  LRU, persist)│            │           │
        │  └──────┬────────┘            │ query     │
        │         │ follow/evict        ▼           │
        │  ┌──────▼────────┐   ┌─────────────────┐  │
        │  │ tail workers  │──▶│ internal/index  │  │
        │  │ (1 per topic) │   │ (Bleve + Book-  │  │
        │  └──────┬────────┘   │  keeper, reused)│  │
        │         │            └────────┬────────┘  │
        │         │ Consume             │ emptyDir  │
        └─────────┼─────────────────────┼───────────┘
                  ▼                     ▼
           ┌────────────┐        (rebuildable —
           │  MSK / Kafka│         index loss = re-warm)
           └────────────┘
```

Four components; only the first two are genuinely new code.

### 1. The follow-set manager (`cmd/k4a-index`, new)

Owns the set of topics currently being followed. Responsibilities:

- **Registration.** On a `Search` for an unknown topic, check the
  allow/deny policy (below); if allowed, add it to the follow-set and
  start a tail worker + a bounded backfill. Registration is idempotent.
- **Persistence.** The follow-set (topic names + last-queried timestamps)
  is written to the daemon's storage so a restart re-follows the same
  set. This is metadata only — the *index* itself is disposable.
- **LRU eviction.** A topic not searched within `evictAfter` (default
  **7 days**) is dropped: tail worker stopped, index directory deleted
  (index dirs are independently deletable by `state-management.md`),
  follow-set entry removed. Demand-driven registration only grows the
  set; eviction is what bounds it.
- **Caps.** A hard ceiling on concurrently-followed topics
  (`maxFollowed`, default **256**) bounds consumer connections and disk.
  At the cap, registration evicts the coldest topic first, or refuses
  (and the client stays on scan) if all are hot.

### 2. The tail workers (`cmd/k4a-index`, new)

One goroutine per followed topic, each a thin wrapper around the existing
`internal/index` indexer:

- **Live tail** from latest via `internal/kafka` Consume — steady state,
  always warm, zero extra machinery beyond what the local index already
  runs, just never stopped.
- **Bounded backfill** on registration: index the recent window (last
  `backfillWindow`, default **24h or 10 GB, whichever smaller**) using
  the same catch-up scan the local index uses. While backfilling,
  coverage reports the gap so clients scan across it.
- Writes the same `Bookkeeper` state (`newest/oldest_offset_per_partition`,
  gaps) that the dispatcher already reads.

### 3. `internal/index` — reused verbatim

The Bleve index, document shape, Bookkeeper, retention trimmer, and
schema-versioning all come straight from `local-index.md`. **This is the
point of keeping the daemon in the k4a repo:** the daemon and the local
index share exactly one indexer implementation, so "indexed (shared)" and
"indexed (local)" and "scanned" can never diverge in what they match.
`internal/index` is an `internal/` package — a separate repo could not
import it, which settles the repo question.

### 4. The client remote backend (`internal/remoteindex`)

The dispatcher gains a remote coverage source. Given a `SearchRequest`:

1. If a service endpoint is configured **and reachable**, call the
   daemon's `Search`. The daemon returns a leading **coverage frame**,
   then streams matches.
2. Apply the *same* three-way coverage logic the dispatcher already uses
   locally (`local-index.md` §Dispatcher): fully covered → done, ms;
   partially covered → merge the daemon's matches with a client-side
   scan of the uncovered slice; not covered → the client scans and fires
   a fire-and-forget `Follow(topic)` so the next search is warm.
3. If the endpoint is unreachable, fall through to today's in-process
   Tier 1/2/3. The service is never a hard dependency.

The `Source` field on progress events (`SourceIndex`/`SourceScan`/
`SourceHybrid` from `local-index.md`) gains a `shared bool` so the header
can distinguish shared from local.

## The query + coverage contract (`internal/indexwire`)

One gRPC service, server-streaming, mirroring the channels SearchView
already consumes (`matchCh`, `progCh`, `errCh`):

```proto
service Index {
  // Search streams a leading Coverage frame, then Match/Progress frames.
  // If the topic is unfollowed and allowed, the server begins following
  // it as a side effect (coverage comes back NONE for this call).
  rpc Search(SearchRequest) returns (stream SearchEvent);

  // Follow registers a topic without querying (used by the client's
  // fire-and-forget warm-up when it scans a cold topic itself).
  rpc Follow(FollowRequest) returns (FollowResponse);

  // Coverage reports per-partition indexed ranges without a query
  // (used by the Topics view to render an "indexed?" column).
  rpc Coverage(CoverageRequest) returns (CoverageResponse);
}
```

The `Coverage` frame carries, per partition: `offsetLo`, `offsetHi`,
`timestampLo`, `timestampHi`, and any `gaps`. That is exactly what the
client `Bookkeeper` exposes locally — the client makes the identical
index-vs-scan decision, just fed remotely.

**Tail lag is explicit on the wire, never silent.** If the daemon's live
tail falls behind the broker high watermark, `offsetHi` is behind HWM and
the client scans `offsetHi..HWM` for "now" queries. A stale index can
never masquerade as current results — this is the "staleness worse than
latency" instinct built into the contract, not bolted on as an alert.

**Truncation is explicit on the wire too.** The same rule applies to the
match *limit*: without an explicit signal, a stream that caps at `limit`
and closes with a bare `done=true` makes a truncated answer
indistinguishable from a complete one. The terminal `Progress`
frame carries `capped`, sourced from bleve's total hit count rather than
the returned page (the page understates, because unparseable hits are
dropped). Note this is a *different* question from
coverage: `CanServe` reasons about offset/time range and gaps, so a topic
can be fully covered, gap-free, and still answer capped.

Because an older daemon leaves that field at its proto default, the
client must not trust the flag alone — `remoteindex` also infers capping
from receiving exactly `limit` matches, and sends a resolved (never zero)
limit so that inference has something to compare against. Correctness
therefore does not wait on the daemon redeploy.

**Matching parity is load-bearing.** The daemon must apply the identical
regex/scope semantics as `deep_search` — it does, because it runs the
same `internal/index` query translation. Never let the daemon answer with
a bleve-analyzer approximation that diverges from a scan; that would make
users distrust the fast path, and distrust collapses the whole feature
back to "just scan it." An early version had exactly this failure: Bleve regexp
queries are term-anchored, so a plain-word query silently returned zero
where a scan matched. Parity now comes from the candidate-rewrite +
exact post-filter in `internal/index` (see local-index.md "Regex
semantics"), and `Bookkeeper.CanServe` additionally rejects patterns the
tokenized index cannot answer with full recall — so both the local
dispatcher and `remoteindex.Client.covers()` route those to a scan. The
`k4a-mcp` frontend has no scan to fall back to; it surfaces the same
decision as `pattern_recall: full | best_effort` so an agent knows when a
zero-match answer is not authoritative.

## MCP frontend (`k4a-mcp`)

The same warm index is also reachable by LLM agents (Claude Code, Cursor)
over the Model Context Protocol, so "did this ever happen on topic X"
becomes a tool call against the shared index instead of each agent
shelling out to a local scan. This ships as a **separate deployment**,
`cmd/k4a-mcp` (own image + `charts/k4a-mcp`),
*not* a second port on the daemon:

- **A thin gRPC client of `k4a-index`.** `k4a-mcp` holds no Kafka client,
  no Bleve index, and no AWS creds — it dials the daemon's in-cluster gRPC
  (`k4a-index.k4a.svc:9500`) and re-exposes `Search`/`Coverage`
  as MCP tools. Keeping it separate means the index daemon stays a pure
  index server and the MCP surface scales/deploys independently (it is
  stateless; add replicas freely).
- **Transport: streamable-HTTP** on `:9501` behind its own internal load
  balancer, network-gated the same way as the gRPC port (private network /
  VPN). Register
  with `claude mcp add --transport http k4a-index https://k4a-mcp.example.com/mcp`.
- **Coverage is surfaced, not hidden.** The `search` tool returns
  `served_from_index` and the full coverage frame, so an agent knows when
  the index is authoritative and when to fall back to a direct scan — the
  same honesty the client dispatcher enforces. `served_from_index: false`
  (unfollowed topic) tells the agent to `k4a search` instead.
- **Parity by construction.** The tool input mirrors the stdio `k4a mcp`
  schema (regex/scope/time semantics), and matching runs in the daemon via
  the one `internal/index` translation — the MCP path can't diverge from a
  scan any more than the local index can.

Auth is deliberately *not* oauth2-proxy (remote MCP clients do
OAuth2/bearer, not cookie-SSO); v1 is network-gated, with MCP OAuth or a
bearer token a later add if needed.

## Security — the shared-index surface

This is the decision that gates production use, and it is a policy decision
more than a technical one. A shared index **persists decrypted message
payloads to disk and makes them searchable to anyone who can reach the
daemon** — the exact multi-tenant concern `local-index.md`'s open
questions parked ("if k4a ever becomes multi-tenant, the index has to
gate by auth context"). A shared service *is* multi-tenant, so it can no
longer be parked.

The v1 stance:

- **Auto-follow is allow/deny-gated, not unconditional.** Demand-driven
  registration is the sharp edge: one user searching a sensitive topic
  would otherwise silently persist its payloads for everyone. The daemon
  checks a policy before following. Default-deny for anything matching a
  compliance/PII denylist (orders, fills, PII-bearing topics), explicit
  or pattern allowlist for the rest.
- **Reaching the daemon ≈ broad read over the followed set.** v1 does
  *not* per-caller authorize by mirroring each user's MSK ACLs (real
  engineering, deferred). Instead it treats daemon access as a single
  trust boundary: locked behind a `NetworkPolicy` (ingress only from k4a
  clients), authenticated (mTLS / the same auth references k4a already
  resolves), and — critically — only ever following allowlisted,
  non-sensitive topics. The allowlist *is* the authorization model in v1.
- **Same model in every environment.** Staging and production run the same
  v1 model — network-gated plus a non-sensitive topic allowlist as the
  authorization boundary — so production is in scope for **non-sensitive**
  topics (telemetry, reference data, etc.). Treat every widening of the
  allowlist as a deliberate, reviewed change. What Phase 5 (per-caller
  authorization) gates is following **sensitive** topics (orders/fills/PII):
  those must not be added to the allowlist until it lands, because reaching
  the daemon still grants broad read over whatever it follows.

Per-caller topic authorization (the daemon enforces each caller's own
Kafka read grants before serving a topic's matches) is the follow-up that
would let the service safely follow sensitive topics. Called out in
Open questions.

## Storage & deployment

**Storage: `emptyDir`, not a PVC.** The index is a pure optimization that
rebuilds from the broker, so index loss is a non-event — on restart the
daemon re-follows the persisted follow-set and re-backfills, with a
transient scan-fallback window while it warms. This deliberately sidesteps
PVC lifecycle pain: for a service whose local state is authoritative,
emptyDir loss is a correctness incident; here it is by-design disposable.
The one durable bit — the follow-set metadata — is tiny and lives *outside*
the pod (leaning a compacted Kafka topic so it both persists and converges
across replicas; see Open questions), so a pod restart re-follows the set
even on empty local storage. Worst case, even that is lost: the daemon
forgets what to follow and re-learns on the next search.

**Sizing the volume — on-disk is ~3x the cap** (measured).
`config.topicMaxBytes` bounds *stored* bytes, which is what the trimmer can
account for; the directory on disk carries unreclaimed scorch segments on top,
so a topic under a 1 GiB cap was observed at **~2.8 GB on disk** after 15h. Budget
`~3 x cap` per concurrently-hot topic against `storage.sizeLimit`, not the cap
itself. At the current 512 MiB cap and a 10 Gi emptyDir that is roughly six hot
topics; past that the kubelet evicts the pod at the sizeLimit — self-clearing
(a new pod gets a fresh volume) but abrupt. Note the node's *allocatable*
ephemeral storage is the real ceiling: raising `sizeLimit` past it is a node
sizing change, not a values edit.

**The disk budget is what actually bounds the volume** (`config.maxTotalBytes`
→ `--max-total-bytes`, default off; the chart sets 7GiB against a 10Gi
`sizeLimit`). Every other knob bounds something else — `topicMaxBytes` is
per-topic and counts *stored* bytes, `maxFollowed` counts topics, `evictAfter`
counts idle time — so before it existed the only backstop was the kubelet
evicting the pod at `sizeLimit`. The budget sweep runs every minute (a
high-volume topic can add hundreds of MB between hourly LRU sweeps) and
reclaims cheapest-first:

1. **Orphaned directories** — no worker owns them, so deleting one costs
   nothing. They accumulate because a watchdog restart re-follows only the
   *pinned* set, abandoning every on-demand topic's directory where the
   follow-set-walking LRU sweeper can never see it again. Guarded by an
   age check so a directory that a registration just created is never reaped.
2. **The coldest non-pinned topics**, in the same LRU order eviction already
   uses, until the root fits.

**Orphans also age out without disk pressure.** The budget only reclaims when
the volume is threatened, which bounds growth but would leave an abandoned
index holding disk indefinitely whenever there is headroom. So the hourly idle
sweep reaps orphans older than `evictAfter` too — the same clock a *followed*
topic is evicted on. The two reapers differ only in patience, and deliberately:
an orphan is not garbage but a **warm index** (a re-follow reuses the directory,
so the covered window survives and costs no re-warm), which is worth keeping
while disk is plentiful and not worth keeping when it is scarce. Under pressure
the budget takes anything safe to delete; with headroom, `evictAfter` decides —
because that setting already means "we no longer care about this topic's
index", and a followed topic aging out while an unfollowed one outlives it
would be an inconsistency rather than a policy.

Pinned topics are never evicted this way — they were pinned to stay warm, and
`topicMaxBytes` already bounds them. If they alone exceed the budget the daemon
says so rather than thrashing.

The design property this preserves: **coverage is explicit, so losing the
coldest window is honest, while a kubelet-evicted pod is a silent
whole-service outage.** Degrading along the axis clients already understand is
strictly better than falling over.

One consequence worth designing around, still open:

- **Cap ⇒ merge size.** Worst-case merge size scales with `topicMaxBytes`, so a
  smaller cap bounds how much work a single merge represents. Note this is a
  disk-and-latency argument only: it does **not** prevent the drain wedge
  (see *Failure containment*), which is a merger goroutine that has died
  rather than one that is slow.

**Deployment (`charts/k4a-index`; see [`guides/deploy.md`](../guides/deploy.md)):**

- Helm chart: Deployment, ClusterIP Service, ServiceAccount. With MSK IAM
  auth (EKS Pod Identity or IRSA on the ServiceAccount) the daemon carries
  no SASL password or signing key, so there is no Secret to sync.
- **The consumer is GROUPLESS.** The daemon tails by manual partition
  assignment (`internal/kafka` Consume → `kgo.ConsumePartitions`), not a
  consumer group: it commits no offsets and reconstructs coverage from the
  Bleve index. So there is no group to join, no group ARN, and **no group
  ACL to grant**. Its Kafka grant needs cluster describe (DescribeCluster +
  the DescribeTopic the admin watermark reads use) and read on the
  allowlisted topics only; that topic grant is the outer half of the
  security boundary.
- **NetworkPolicy** (if you use one): ingress on 9500 from the clients'
  networks. A load balancer that targets pod IPs reaches the pod from
  addresses that are not pods, so admit it with an `ipBlock`, not a
  namespaceSelector — a selector-only rule fails target health. Egress
  needs the brokers plus, for IAM auth, STS and the node-local credential
  agent.
- **1 replica day one, fan-out for HA** (see *Availability model* below).
  Bleve is embedded and single-process; concurrent *reads* are fine (many
  searchers hit one pod happily), the *write* path is one tail-writer per
  topic. Read-scaling (shard topics across pods) is deferred — don't
  pre-build it.
- **Naming:** repo/module/binary stay `k4a` / `k4a-index`; everything
  deployed is `k4a-index`.

## Availability model — fan-out replicas, not consensus

HA here is **read-only fan-out replicas with Kafka as the replication
substrate** — deliberately *not* Raft. Run N pods, each *independently*
tailing the same topics and maintaining its own local index. Nothing
coordinates them because nothing has to: the tail is **groupless** (manual
partition assignment, no offset commits), so every replica consumes every
partition on its own with no group membership to rebalance and no
per-replica group ACL to provision:

- Any replica serves any query — there is nothing to fence. This service
  never produces or writes anywhere but its own disposable index; it is pure
  read, so there is no single-writer requirement and no leadership to elect
  for correctness.
- Lose a pod and the survivors are **already warm** — no promotion, no
  failover backfill, no election. That is the entire HA story.
- Kafka does the replication. The index inherits Kafka's durability for
  free, which is the whole reason we can treat a pod's storage as
  disposable.

**Why not Raft.** Raft earns its complexity only for *authoritative state
that cannot be reconstructed from an external source of truth* and where
followers must consistently agree on it — a sequencer or an obligation
ledger, say. This index fails
that test in both directions: it is a materialized view of a Kafka log
(fully reconstructable — the cardinal rule), and two replicas being a few
seconds apart is fine (the coverage frame makes staleness explicit and
clients top up the tail with a scan). Putting a Raft log on top of a Kafka
log to replicate a view of that Kafka log pays for replication twice and
buys a consistency guarantee we have no use for. **The test for a future
reader: can the state be rebuilt from the broker? Here, always yes → no
Raft.**

**The one cost, and the deferred fix.** Naive fan-out means every replica
independently backfills every newly-followed topic → N× backfill broker
bandwidth on registration (steady-state tailing is unaffected). If that
proves to hurt, the fix is *leader election* — a k8s `Lease` or a
single-partition Kafka fence to assign *who backfills topic
X* while all replicas still serve reads — **not** Raft state replication.
Electing a coordinator for an optimization, never for correctness. Almost
certainly premature; start with independent fan-out and measure whether
the rare failover's N× backfill is even a problem.

## What's intentionally NOT here (v1)

- **Per-caller ACL enforcement.** v1 uses the allowlist as the
  authorization boundary (above). Mirroring each caller's MSK read grants
  is the follow-up that unlocks sensitive topics, not v1.
- **Consensus/Raft replication.** HA is fan-out replicas + Kafka as the
  replication log (see *Availability model*), never a Raft-replicated
  index. Lease-based backfill coordination is the deferred optimization if
  N× failover backfill ever bites.
- **Read sharding / index replication.** One pod serves all reads until
  query load proves it needs sharding.
- **Cross-topic / cross-cluster meta-index.** One daemon per cluster,
  per-topic indexes, same as local.
- **Schema-aware field indexing.** Inherited deferral from
  `local-index.md` (its Tier 5). When it lands there, the daemon gets it
  free by reusing `internal/index`.
- **~~Pushing the follow-set from config.~~** *(Added in Phase 2.)* Alongside
  demand-driven registration, a static pre-follow list (`config.prefollow` /
  `K4A_INDEX_TOPIC`) warms and pins hot topics before the first search;
  pinned topics are exempt from LRU/cap eviction. The bulk of the follow-set is
  still demand-driven + allowlist-gated.

## Why this shape (vs alternatives)

| Decision | Chosen | Rejected alternative | Why |
|---|---|---|---|
| Daemon runs the scan for cold topics | **No — client scans, daemon only indexes** | Daemon serves scans too | Keeps the daemon a pure index server; the client already has the correctness-floor scan path; no need to replicate `deep_search` fan-out server-side |
| Repo | **`cmd/k4a-index` in k4a repo** | Separate repo importing k4a | `internal/index` can't be imported across repos, and one shared indexer impl is what guarantees match parity |
| Transport | **gRPC server-streaming** | REST request/response | Results already stream with progress; a coverage-frame-then-matches stream maps directly onto the existing channels |
| Registration | **Demand-driven + allowlist** | Static config list | Zero-ceremony ("search it, it warms"); allowlist contains the security surface |
| Storage | **emptyDir** | PVC | Index is rebuildable; disposable storage avoids all PVC lifecycle pain |
| Freshness | **Coverage frame, client tops up the tail** | Trust the daemon's index as current | Never serve stale as current; staleness worse than latency |
| HA | **Fan-out replicas, Kafka as replication log** | Raft-replicated index | Index is reconstructable from the broker → consensus buys nothing; Raft is for state that *isn't* reconstructable |

## Capacity estimates

Rough, to be replaced by measurement. Per followed topic, steady state:

| Metric | Estimate | Note |
|---|---|---|
| Tail consumer cost | ~1 kgo consumer/topic | Same as one local-index tail, just never stopped |
| Index disk/topic | bounded by `topicMaxBytes` | The daemon trims oldest records inline once a topic's index exceeds the cap |
| Query latency (covered) | 9–150 ms | Same Bleve query as local-index, no cold-start |
| Warm-up on first search | scan-speed once (6–15 s) | One-time per topic, then instant for everyone |
| `maxFollowed` × `topicMaxBytes` | disk ceiling | LRU + the per-topic byte cap keep the working set well under it |

The win over per-user local indexing: N searchers share **one** index and
**one** tail per topic instead of N — and the index is already warm, so
even the first searcher after a restart pays scan-speed only once, not
once per person.

**Per-topic retention (implemented).** Each followed topic's on-disk index is
bounded by a byte cap (`--topic-max-bytes` / `K4A_INDEX_TOPIC_MAX_BYTES` /
chart `config.topicMaxBytes`, default 1 GiB; 0 disables). The async indexer
trims the oldest records inline after each flush (`internal/index.(*Indexer).Trim`),
advancing the per-partition oldest markers so the coverage frame stays honest —
the covered window simply moves forward, older queries fall back to the client
scan. This is what makes following a high-volume topic (say ~100 GB/day)
safe on the disposable emptyDir: without it a fat topic
fills the volume and evicts the pod, taking every other topic's index with it.
It drives a logical payload-byte estimate (not on-disk bytes) so it converges in
one pass rather than over-deleting while scorch defers segment reclamation.

## Failure containment — the drain path must fail loudly

The failure this section guards against: after a few hours of byte-cap trim
churn, the async drain goroutine can wedge inside a bleve call while holding
the indexer mutex. Every surface that takes that mutex — `Search`,
`Coverage`, the stats heartbeat — hangs; the Submit buffer fills and every
record is dropped, indefinitely, with no error, no restart, and no alert.
Several defenses now hold, all
shaped by the same premise: the index is disposable, so *silence* is the
failure mode, not restarts.

- **Flush errors are logged, never swallowed.** The daemon wires
  `index.AsyncOptions.OnFlushError`, so `IndexBatch`/`Trim`/`FlushBookkeeper`
  failures log with topic, stage, and batch size. (The TUI's local indexer
  keeps its best-effort silence — the hook defaults to nil.)
- **Coverage reads never take the writer's mutex.** `BookkeeperSnapshot` reads
  an immutable bookkeeper published (under `ix.mu`) at the end of every
  mutation, so coverage, `CanServe` and the stats heartbeat cannot be starved
  by a drain holding the mutex through a long `IndexBatch` — before this, at
  ~3,300 docs/sec, coverage RPCs for a busy topic blew a 10s deadline while a
  quiet topic on the same daemon answered in ~150ms, and its own 15s
  heartbeat managed 31 lines in 40 minutes. It also means
  **coverage stays answerable while a topic is genuinely wedged**, which is
  exactly when an operator wants to ask.

  The published value may lag reality by one operation, and the direction
  matters: lagging on the NEWEST edge under-claims, so a client scans — safe.
  Lagging on the OLDEST edge would over-claim, serving a window whose earliest
  records were already deleted and silently returning less than asked for. So
  `Trim`, which deletes in chunks and only refreshes its markers at the end,
  publishes a deliberately pessimistic snapshot for the partitions it is about
  to touch (their covered window collapses to empty) and republishes the true
  floor after refreshing. Under-claiming for the length of a chunk costs a
  scan; over-claiming costs the one guarantee coverage exists to provide.
- **Scorch's background failures are logged too.** `index.OnAsyncError` is
  wired to the scorch async-error callback, because its merger, persister and
  introducer goroutines **recover their own panics and then return**. That is
  the wedge's root cause: a panicking merger leaves the index with no merger
  at all, segments pile up, the persister parks in
  `pausePersisterForMergerCatchUp` waiting for a merger that no longer exists,
  `Batch` blocks in `prepareSegment` holding `ix.mu`, and every reader hangs
  behind it. `fireAsyncError` is a no-op unless a callback is registered, so
  before this the panic that started the whole cascade left no trace.
- **Drain-stall watchdog, fail-fast — but never a livelock.** A per-topic
  watchdog samples only atomics (`LastFlush`/`Buffered` — never the indexer
  mutex, since the wedge is precisely a blocked mutex holder): if records are
  queued and the drain has made no progress for `--drain-stall-timeout`
  (default 5m, 0 disables), it dumps **all goroutine stacks** to stderr and
  exits. The dump is the only way the distroless image can complete a
  root-cause analysis (no exec, no pprof) — such a dump is what named the
  blocked call: `Batch` waiting in scorch `prepareSegment` behind a persister
  parked for merger catch-up, with the merger *runnable* inside a zapx merge.

  A naked exit is a trap, though: `/data` is an `emptyDir`, which is
  **pod**-scoped, so `exit 2` restarts the *container* onto the same wedged
  index — a restart loop that can run for hours, each restart discarding the
  in-flight merge that was the actual work. Two escapes now bound it:
  - *Merge grace*: a stall is deferred up to `--drain-stall-merge-grace`
    while a scorch file merge is **demonstrably progressing** — not merely
    begun. `Indexer.MergeProgress` samples `TotFileMergeZapBeg/End` plus
    `TotFileMergeWrittenBytes` (no indexer mutex), and grace is granted only
    when bytes move or a merge completes between ticks. "Begun" alone is not
    enough: a merger that panicked leaves `Beg > End` **forever** with no
    goroutine behind it, so the earlier begun-only test waited out the entire
    grace on a corpse before every strike.
  - *Strike escalation*: consecutive stall exits are counted in a
    `stall-strikes` file **inside the topic's index dir** (survives container
    restarts, dies with the index). On the `--drain-stall-wipe-after`-th
    strike (default 3) the index dir is quarantined (renamed aside and
    deleted) before exiting, so the next container boots onto a fresh
    directory and re-warms from Kafka — self-service for what the manual
    `kubectl rollout restart` remedy did. The index is disposable; a re-warm
    costs ~1 minute.
- **MCP deadlines.** `k4a-mcp` bounds every daemon call (30s search / 10s
  coverage), so a wedged daemon surfaces to agents as a fast error naming the
  daemon instead of the client's own opaque timeout.

The `ByteSize` estimate is also upsert-aware now (a re-indexed offset counts
once), fixing a secondary defect of the same failure: redelivered records
inflated the estimate and the trimmer over-shrank the coverage window ~3x past
its hysteresis floor. Alerting on `dropped_total`/heartbeat-stall (so the
next wedge pages someone) belongs in your monitoring stack, not this repo.

## Migration / shipping plan

Each phase is independently shippable; the client's local behavior is the
fallback at every step, so nothing regresses if the daemon is absent.

### Phase 0 — contract (`internal/indexwire`) (≈ 3 days)
- Proto + generated Go for `Search`/`Follow`/`Coverage`, coverage frame.
- Round-trips a `Bookkeeper` snapshot over the wire, unit-tested against
  the local dispatcher's coverage logic. **Ships:** the wire shape,
  provable against the existing coverage decision.

### Phase 1 — the daemon, single topic, no policy (≈ 1 week)
- `cmd/k4a-index`: gRPC server + one tail worker + `internal/index`.
- Follows exactly one hard-coded topic; serves `Search`/`Coverage`.
- Run locally against `kfake`, then against a real staging topic.
- **Ships:** proof that a networked coverage source answers a client
  query at local-index latency.

### Phase 2 — follow-set lifecycle (≈ 1 week) — **IMPLEMENTED**
- Demand-driven registration on `Search`/`Follow`, LRU eviction, caps, and a
  startup pre-follow (pinned) set. The follow-set manager (`cmd/k4a-index/manager.go`)
  spawns one tail worker per followed topic on the first search and evicts the
  coldest at the `maxFollowed` cap / any topic idle past `evictAfter`. The
  allow/deny gate (`cmd/k4a-index/policy.go`, default-deny glob allowlist)
  ships in the SAME change — auto-follow without it is the one dangerous state
  (see Pickup notes), and it mirrors the daemon's Kafka topic grant.
- **Warming is live-tail only.** A newly-followed topic indexes forward from
  registration; its coverage window starts at that moment, so clients scan
  anything older (the coverage frame keeps this honest). **Bounded HISTORICAL
  backfill on registration** (indexing the recent window that predates the first
  search) is deliberately deferred: it needs a timestamp→offset lookup
  `internal/kafka` doesn't yet expose, and `internal/index.Catchup` only fills
  gaps for partitions with prior state. A follow-up; nothing is stale
  or wrong without it, the covered window is just narrower on day one.
- **Cross-pod follow-set persistence is NOT included** (still the open question
  below). Each replica learns its follow-set independently from the searches it
  serves and self-heals; the pinned pre-follow set covers restart warmth for the
  hot topics. A compacted-topic follow-set is the convergence upgrade.
- **Ships:** "search it and it warms; idle topics fall out."

### Phase 3 — client remote backend (≈ half a week) — **IMPLEMENTED**
- `internal/remoteindex` wraps the local search: a coverage-gated remote
  source (the same `Bookkeeper.CanServe` decision, run on the daemon's wire
  coverage) → the in-process cache/index/scan. Per-context `index-endpoint`
  config (+ `--index-endpoint` flag), fire-and-forget `Follow` on a cold
  scan, and `kafka.SourceSharedIndex` so the header reads `indexed (shared)`.
  Wired into all three surfaces (`k4a search`, the TUI's `s`, `k4a mcp`).
- **Verified end-to-end** against a deployed daemon: a covered query is served
  `indexed (shared)` with zero broker scan; an uncovered/unreachable daemon
  transparently falls back to the local scan.

### Phase 4 — allowlist + deploy (≈ 1 week) — **IMPLEMENTED**
- Allow/deny policy (`cmd/k4a-index/policy.go`), network-gating via
  `NetworkPolicy`.
- `charts/k4a-index` + `charts/k4a-mcp`: groupless read on the allowlisted
  topics, emptyDir storage, exposed through an internal load balancer that
  negotiates ALPN `h2` for gRPC-over-TLS on :443. No consumer-group ACL and
  no Secret for IAM auth — see *Storage & deployment*.
- **Ships:** a shared daemon everyone points at, following allowlisted
  non-sensitive topics only. See [`../guides/deploy.md`](../guides/deploy.md).

### Phase 5 (gates sensitive topics) — per-caller authorization (≈ 2 weeks)
- Daemon enforces each caller's own Kafka read grants before serving a
  topic's matches.
- **Ships:** the safety property that lets the daemon follow sensitive
  topics without turning the index into a read-around.

## Open questions

- **Per-caller authorization mechanism.** How does the daemon learn a
  caller's Kafka read grants cheaply? Options: the client presents its own
  MSK IAM identity and the daemon does a describe-ACLs cache; or a
  coarser group→topic-allowlist mapping. This gates sensitive topics — flag
  for design.
- **Compacted topics.** Same concern as `local-index.md`: the shared index
  may retain records compaction has since removed from the broker. For
  "did this ever happen?" that's a feature; the shared render must mark
  compacted-source hits so nobody reads them as current state.
- **Follow-set persistence store — leaning compacted Kafka topic.** With
  fan-out replicas the follow-set also wants to *converge* across pods, not
  just persist. A **compacted Kafka topic** (`k4a-index.followset`) does
  both: each daemon publishes "now following X" and all replicas consume
  the union, so registration on one pod warms every pod, it survives
  restarts, and it reuses Kafka as the coordination substrate a third time
  — no ConfigMap write-races, no consensus. The fallback if that's
  over-built: each replica learns independently from the searches it serves
  and self-heals (a pod that never saw a search for X just follows X the
  first time it does; client fallback covers the gap). Both beat a
  ConfigMap; flag which for review.
- **Backfill window default.** 24h/10 GB is a guess. During incidents the
  interesting window may be longer; per-topic overrides (as in
  `local-index.md`'s retention config) probably belong here too.
- **Multiple daemons per cluster for read-scaling.** If one pod can't
  serve the query load, do we shard topics across daemons (client routes
  by topic) or replicate the index (client picks any)? Defer until load
  proves it, but the client's endpoint config should allow a set, not a
  single host, so this isn't a breaking change later.
- **Does the daemon need its own `deep_search`?** Chosen: no — the client
  scans cold windows. Revisit only if we ever want the daemon to serve
  clients that don't have broker access at all (a thin/remote k4a), which
  is not a current use case.

## Pickup notes

- **Start at Phase 0/1 with `kfake`.** The whole contract can be proven
  in-process before any deploy: stand up the daemon against `kfake`, point
  a test client at it, assert the coverage decision matches the local
  dispatcher's on the same `Bookkeeper` state. `internal/index`'s tests
  already use a `t.TempDir()` Bleve dir — the daemon tests reuse that.
- **Do not fork `internal/index`.** The instant there are two indexer
  implementations, "shared" and "local" and "scan" drift and users stop
  trusting the fast path. If the daemon needs something `internal/index`
  doesn't expose, add it there and let the local index benefit too.
- **The allowlist is the v1 security model — treat it as load-bearing.**
  Ship the deny-by-default policy in the *same* change as demand-driven
  registration (Phase 2/4), never after. Auto-follow without the gate is
  the one genuinely dangerous state this design can be in.
- **Keep the daemon-is-optional invariant honest.** Every client path that
  touches the service must have a tested "endpoint unreachable → local
  behavior" branch. The service is an accelerator; a k4a with a dead
  daemon endpoint must be indistinguishable from a k4a with none.
