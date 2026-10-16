# Design: Shared always-on index service (`k4a-index`)

**Status:** shipped — Phases 0-5 are implemented. Phase 5 (per-caller authorization, #118) shipped as
an operator-written identity→topic-glob policy (`--authz-policy`), not as a mirror of each caller's
Kafka ACLs; see *Per-caller authorization* below.
**Depends on:** [`local-index.md`](local-index.md) — this promotes that
doc's per-process Tier-3 indexer into a shared network service, and
picks up its two explicitly-parked items ("Background daemon (`k4a
indexd`)" and "Shared/networked indexes"). [`state-management.md`](state-management.md)
— the cardinal rule ("the cluster is the source of truth; state is a
deletable optimization") is preserved, not weakened.
**Depends on:** [`deep-search.md`](deep-search.md) — the client keeps the
scan path as the correctness floor; the service never replaces it.
**Code:** `cmd/k4a-index/` (the daemon: `main.go`, `server.go`,
`manager.go`, `worker.go`, `policy.go`, `followset.go`, `followstore.go`; per-caller authorization
in `internal/endpointauth/authz.go`) reusing `internal/index`
unchanged and `internal/kafka` (whose `keyedlog.go` holds the follow-set store); `internal/remoteindex` (the client backend);
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
    # or, against a daemon serving TLS with a token (see Security):
    # index-ca: /path/to/ca.crt
    # index-token: $K4A_INDEX_TOKEN
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
  is written to `k4a-index-followset.json` under `--data-dir`, beside the
  indexes, on every registration and eviction (with a one-minute sweep as
  backstop) and at shutdown. A restarted container re-follows it before serving, through
  today's allowlist and only for topics used within `evictAfter`, restoring
  each LRU clock (`cmd/k4a-index/followset.go`). It survives what the
  indexes survive — a container restart, such as the drain-stall watchdog's
  exit — and not a new pod's empty volume. With `--followset-topic` the set
  is also kept in a compacted Kafka topic that a new pod reads on startup and
  every replica tails (see *Follow-set persistence across pods*). This is
  metadata only — the *index* itself is disposable.
- **LRU eviction.** A topic not searched within `evictAfter` (default
  **7 days**) is dropped. A Search, or a Follow of a topic already followed
  (the client scanned because coverage fell short), counts as use; a
  Coverage probe does not, or a probed topic could never age out.
  Eviction stops the tail worker, deletes the index directory (index dirs
  are independently deletable by `state-management.md`) and removes the
  follow-set entry. Demand-driven registration only grows the
  set; eviction is what bounds it.
- **Caps.** A hard ceiling on concurrently-followed topics
  (`maxFollowed`, default **256**) bounds consumer connections and disk.
  At the cap, registration evicts the coldest non-pinned topic first. It
  never refuses: when every followed topic is pinned there is nothing to
  evict, and the set grows past the cap (pinned topics are an operator's
  explicit choice, so the cap bounds only what searches add).

### 2. The tail workers (`cmd/k4a-index`, new)

One goroutine per followed topic, each a thin wrapper around the existing
`internal/index` indexer:

- **One ordered read: bounded backfill, then the live tail** (#56). At
  registration the worker plans a start offset per partition
  (`planBackfill`, `cmd/k4a-index/backfill.go`) from the broker's log-start,
  high-watermark and time-indexed offsets, and reads forward from there with
  `kafka.Client.ConsumeFrom`, straight on into the live tail. A partition
  the index has not seen starts at the first record of `--backfill-window`
  (default **24h**), pulled forward to its share of
  `--backfill-max-records` (default **1,000,000** across the topic; the
  design's 10 GB bound became this record cap, and `--topic-max-bytes`
  still trims on top). A partition the index already holds resumes right
  after its newest record — the catch-up a restart used to skip, which left
  a silent hole between the old index and a tail that restarted near the
  end. If that resume is impossible (the records were deleted, the topic was
  recreated, or the gap exceeds the cap) the index is deleted and rebuilt
  from the window instead of joined across a hole.
- **Coverage stays contiguous throughout.** Because history and tail are one
  stream per partition, the indexed range never spans a hole; while the
  history is being read, the newest-offset frontier is simply behind the
  broker, so a query reaching "now" falls to a scan exactly as for a lagging
  tail. `Coverage.backfilling` is true until every partition has reached its
  registration-time high watermark (or for at most 15 minutes, for a quiet
  partition whose last offset is a transaction marker). Until then every
  record waits for indexer buffer room (`AsyncIndexer.SubmitWait`) rather
  than drop, since the backfill keeps the buffer full; afterwards the live
  tail's never-block rule applies. If planning fails (the broker cannot list
  offsets), an index that already holds records is started over before the
  plain tail runs, for the same reason. This reuses `internal/index`
  unchanged apart from that method, rather than `index.Catchup`'s separate
  range scan, because a second consumer racing the tail is what would open
  the hole the single stream avoids.
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
  // (the client's index-vs-scan check before every search).
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
zero-match answer is not authoritative. A JSON-path `path` is never sent to
the daemon (`CanServe` declines it, see deep-search.md); `k4a-mcp` filters
the daemon's matches by it with the scan's own rule (`kafka.PathMatches`) and
reports `path_recall: full` only when the daemon's answer was uncapped and the
topic followed, `best_effort` otherwise.

**Decoding is part of the contract.** Started with `--schema-registry-url`
(or `K4A_SCHEMA_REGISTRY_URL`, the chart's `schemaRegistry` values, or its
context's `schema-registry:` block), the daemon indexes registry-framed keys
and values as their decoded JSON, and `Coverage.decoder` carries the
registry's identity, a hash, never the URL. The client declines coverage
whose decoder differs from the one its own scan would use
(`Bookkeeper.CanServe`), so a daemon with no registry never serves a decoding
client and a decoding daemon never serves a raw one or one on another
registry; each scans. A daemon predating the field reports none. A client
predating it ignores it, so upgrade clients before giving the daemon a
registry. `k4a-mcp` shows the identity as `coverage.decoder`. See
[`schema-registry.md`](schema-registry.md) § *Search and the indexes*.

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
  index server and the MCP surface scales/deploys independently. It is
  stateless, including at the MCP layer: it serves the go-sdk's `Stateless`
  streamable-HTTP mode, so no `Mcp-Session-Id` lives in pod memory and any
  replica answers any request without sticky routing (#33).
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
OAuth2/bearer, not cookie-SSO). The listener takes the same opt-in TLS,
mTLS and shared bearer token as the gRPC port (#54, `internal/endpointauth`),
which Claude Code sends as a header, and the same `--authz-policy` (Phase 5,
below); MCP's own OAuth flow remains later work. Its hop to `k4a-index`
presents that daemon's credentials (`--index-tls`, `--index-token-file`, …),
so the daemon sees k4a-mcp as one caller: telling the people behind it apart
is k4a-mcp's own policy's job.

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
  clients), authenticated at the transport, and — critically — only ever
  following allowlisted, non-sensitive topics. The allowlist *is* the
  authorization model in v1.
- **Transport authentication, not authorization (#54).** The gRPC listener
  can serve TLS, require a client certificate from a CA (mTLS) and require
  a shared bearer token (`internal/endpointauth`; `--tls-cert`,
  `--tls-client-ca`, `--token-file`). All are opt-in so an upgrade changes
  nothing on the wire; an unauthenticated listener says so at startup. This
  answers "is the caller part of this deployment?", not "may this caller
  read this topic?" — that is Phase 5, below. Health checks stay open. Clients
  (`internal/remoteindex`) send a token only over TLS, and a context's
  credentials only to that context's endpoint. A server-side
  `--max-search-limit` bounds one Search, and search patterns are logged at
  debug, since a pattern is often the sensitive value being looked for.
- **Same model in every environment.** Staging and production run the same
  v1 model — network-gated plus a non-sensitive topic allowlist as the
  authorization boundary — so production is in scope for **non-sensitive**
  topics (telemetry, reference data, etc.). Treat every widening of the
  allowlist as a deliberate, reviewed change. Without a per-caller policy,
  reaching the daemon grants broad read over whatever it follows, so
  sensitive topics (orders/fills/PII) belong on the allowlist only together
  with an `--authz-policy` that grants them to the callers who may read them.

### Per-caller authorization (Phase 5, #118)

`--authz-policy FILE` (`K4A_INDEX_AUTHZ_POLICY`, `K4A_MCP_AUTHZ_POLICY`; the
charts' `listener.authz`) maps each caller to the topic globs it may
**search** and **follow**:

```yaml
callers:
  - name: payments-oncall
    cert: {cn: payments-oncall}     # or san: <DNS/email/URI/IP>, or subject: "CN=...,O=..."
    search: ["payments.*", "telemetry.*"]
    follow: ["payments.*"]
  - name: ci
    tokenSHA256: <hex SHA-256 of the bearer token>
    search: ["telemetry.*"]
```

- **Identity.** A verified client certificate, matched exactly on its
  subject CN, one SAN, or the full subject DN; or a bearer token, matched by
  its SHA-256 so the policy (a ConfigMap) never holds a token. A caller
  matching several entries gets their union. Certificate entries need a
  client CA; token entries replace the shared `--token-file` (one
  Authorization header cannot carry both), and the daemon refuses either
  contradiction at startup. Unknown fields are refused too, so a typo
  cannot silently grant nothing.
- **Grants.** Globs are `path.Match`, as for `--allow`. `search` covers
  Search and Coverage (coverage reveals whether and how far a topic is
  indexed); `follow` covers Follow and the warm-on-first-search follow a
  Search would start. Neither implies the other: a caller who may search but
  not follow a cold topic gets "not followed" and no follow is started.
  `--allow` still bounds everything — a policy grant never widens it.
- **Default-deny once configured.** A caller no entry names is refused
  `Unauthenticated` on every RPC but health checks (HTTP 401 on k4a-mcp); a
  named caller asking for an ungranted topic is refused `PermissionDenied`
  (a tool error on k4a-mcp). A k4a client treats either as "not covered" and
  scans with its own Kafka credentials, so a refusal costs the index, never
  the answer. A handler reached with a policy configured but no identified
  caller refuses rather than failing open.
- **No policy, no change.** Without `--authz-policy` every authenticated
  caller reaches every topic `--allow` admits, exactly as before; the daemon
  logs that at startup.
- **k4a-mcp.** The daemon sees k4a-mcp's own credential, so k4a-mcp
  enforces its own policy per tool call against the caller its listener
  identified. Grant k4a-mcp's identity, in the daemon's policy, the union of
  what its callers may reach.

What this is not: the grants are written by the operator, not derived from
each caller's own Kafka ACLs or IAM policy. Keeping the two in step is the
operator's job, as it already is for `--allow`. The policy is read at
startup (the chart rolls the pods when an inline policy changes).

## Storage & deployment

**Storage: `emptyDir`, not a PVC.** The index is a pure optimization that
rebuilds from the broker, so index loss is a non-event — on a container
restart the daemon re-follows its persisted follow-set and each index
resumes where it stopped; a new pod re-follows its pinned set and
re-backfills, with a transient scan-fallback window while it warms. This deliberately sidesteps
PVC lifecycle pain: for a service whose local state is authoritative,
emptyDir loss is a correctness incident; here it is by-design disposable.
The one durable bit — the follow-set metadata — is tiny. It lives on the
same volume, and with `--followset-topic` also *outside* the pod, in a
compacted Kafka topic, so a new pod re-follows the set on empty local storage
(see *Follow-set persistence across pods* below). Worst case, even that is
lost: the daemon forgets what to follow and re-learns on the next search.

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

## Follow-set persistence across pods (#123)

**Decision: a compacted Kafka topic, opt-in with `--followset-topic`
(`config.followSetTopic`), with the local file kept underneath it.** Code:
`cmd/k4a-index/followstore.go` (the store and its entry format),
`cmd/k4a-index/followset.go` (restore, publish, apply), and
`internal/kafka/keyedlog.go` (a compacted topic read as a key-value table).

The local file answers a container restart. It cannot answer the two cases
this section is about: a **brand-new pod** (a reschedule, a node drain, an
upgrade — each starts on an empty `emptyDir`) and a **replica set**, whose
pods each learned their follow-set from the searches that happened to reach
them. Before this, both fell back to `prefollow` plus re-learning, so after
every rollout the first search of each on-demand topic was a scan again.

**How it works.** Each entry is keyed by the followed topic, its value
`{"v":1,"lastUse":<unix millis>}`; a tombstone means "no longer followed".

- *Startup.* The daemon reads the topic to its current end, merges it with
  the local file by the newer clock, and restores the union exactly as the
  local file was restored before: through today's `--allow`, only topics used
  within `evictAfter`, LRU clocks carried over. Then it tails the topic from
  where the read stopped.
- *Publishing.* Every registration, eviction and one-minute sweep wakes a
  publisher that writes, off the request path, an entry for each followed
  topic whose clock moved and a tombstone for each topic this replica stopped
  following. Writes are at most one per topic per minute per replica. The
  shutdown path makes one last write while the set is still whole, and never
  writes tombstones for the shutdown itself.
- *Applying.* A replica that reads an entry for an allowed, recently used
  topic follows it too, with the entry's clock; for a topic it already
  follows it moves the clock forward (never back, never into the future).
  So registration on one replica warms every replica, and every replica ages
  a topic out on its most recent use anywhere — their idle evictions line up
  instead of one replica dropping a topic that is busy on another.
- *Tombstones are not applied.* Each replica evicts on its own LRU and cap.
  Acting on another replica's eviction would let two replicas at the cap
  evict each other's topics back and forth: A drops X for Y, B (whose
  coldest is W) re-asserts X, A follows X again and drops W, and so on. The
  cost is that the store can briefly lack a topic one replica still follows;
  that replica writes it back on its next use.
- *Stale echoes.* A replica remembers the clock each topic had when it
  dropped it, and ignores entries no newer — its own write read back after
  an eviction, or another replica's equally old one. Only a genuinely newer
  use brings a dropped topic back.
- *Narrower replicas never delete others' entries.* At restore a replica
  adopts as "already written" only the entries it now follows and the stale
  ones (past `evictAfter`, which its first sync deletes). A fresh entry it
  does not follow — another replica's wider `--allow`, or an index that
  failed to open — is left alone.

**Failure modes.** The store is an accelerator for the follow-set, which is
itself an accelerator. If the topic cannot be opened at startup the daemon
logs an error and keeps the local file only; it does not refuse to start. A
failed read or write is logged and retried (the tail reconnects every 30s,
the publisher on the next change). The worst case is what the design had
before: a pod that re-learns its set from searches.

**What it costs.** One more topic on the cluster the daemon already reads,
and three grants on it for the daemon's identity: read, write, and describe
(on MSK IAM, `kafka-cluster:ReadData`, `WriteData`, `DescribeTopic`), plus
create unless an operator creates it with `cleanup.policy=compact` first.
The producer runs with idempotence off — every write is a whole-value
overwrite, so a duplicate is harmless — so it needs no cluster-level
idempotent-write grant. **This is the one place the daemon writes to Kafka**,
and it is opt-in: without `--followset-topic` it stays read-only. The topic
holds topic names and timestamps, never record content; anyone who can
write to it can make the daemon follow a topic, but only one `--allow`
already admits, which is no more than any caller of the `Follow` RPC can do.

**Alternatives considered.**

| Option | Verdict | Why |
|---|---|---|
| **Compacted Kafka topic** | **Chosen** | Persists *and* converges: every replica writes and tails one log, so a follow on one pod warms all pods. Reuses the cluster and credentials the daemon already has — no new infrastructure, no new client library, no Kubernetes API access. Compaction bounds it to one entry per topic. |
| ConfigMap | Rejected | Needs the Kubernetes API from the pod (a client library the daemon does not carry, a Role and RoleBinding to write a ConfigMap), and read-modify-write of one object from N replicas is a write race that wants optimistic-concurrency retries. A 1 MiB object cap is fine for names but the watch-and-merge logic would duplicate what a Kafka log gives for free. It also ties the daemon to Kubernetes, while `k4a-index` runs anywhere. |
| Declarative seed in chart values | Already exists, not sufficient | That is `config.prefollow` (pinned topics). It covers the hot set an operator knows about, but not the demand-driven set, which is the whole point of on-demand follow; and it changes only with a deploy. It stays the right tool for "always warm". |
| PVC for the data dir | Rejected | The index is disposable by design (*Storage* above); a PVC brings back the lifecycle pain this design avoids, and still does not converge replicas. |
| Learn from searches only (the old behavior) | Kept as the fallback | No new grant, but every new pod and every replica re-learns from scratch; the default when `--followset-topic` is unset. |

**Why off by default.** It needs new grants on the daemon's identity, and a
chart upgrade must not turn a working install into one logging write errors.
Set `config.followSetTopic` once the grant is in place.

## Availability model — fan-out replicas, not consensus

HA here is **read-only fan-out replicas with Kafka as the replication
substrate** — deliberately *not* Raft. Run N pods, each *independently*
tailing the same topics and maintaining its own local index. Nothing
coordinates them because nothing has to: the tail is **groupless** (manual
partition assignment, no offset commits), so every replica consumes every
partition on its own with no group membership to rebalance and no
per-replica group ACL to provision:

- Any replica serves any query — there is nothing to fence. This service
  never produces or writes anywhere but its own disposable index (and, with
  `--followset-topic`, the follow-set store, whose entries every replica
  writes and merges without coordination); it is pure read for the data it
  serves, so there is no single-writer requirement and no leadership to elect
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

- **Mirroring each caller's Kafka ACLs.** Per-caller authorization
  (Phase 5) is an operator-written policy, not a describe-ACLs or IAM
  lookup per caller.
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
  still demand-driven + allowlist-gated. A tail that ends on its own (topic
  missing, metadata error, fatal fetch error) leaves the follow-set — its
  worker stopped, its directory kept warm — so Coverage reports
  `following=false`; the next Search or Follow re-registers it, pinned again
  if it was pre-followed.

## Why this shape (vs alternatives)

| Decision | Chosen | Rejected alternative | Why |
|---|---|---|---|
| Daemon runs the scan for cold topics | **No — client scans, daemon only indexes** | Daemon serves scans too | Keeps the daemon a pure index server; the client already has the correctness-floor scan path; no need to replicate `deep_search` fan-out server-side |
| Repo | **`cmd/k4a-index` in k4a repo** | Separate repo importing k4a | `internal/index` can't be imported across repos, and one shared indexer impl is what guarantees match parity |
| Transport | **gRPC server-streaming** | REST request/response | Results already stream with progress; a coverage-frame-then-matches stream maps directly onto the existing channels |
| Registration | **Demand-driven + allowlist** | Static config list | Zero-ceremony ("search it, it warms"); allowlist contains the security surface |
| Storage | **emptyDir** | PVC | Index is rebuildable; disposable storage avoids all PVC lifecycle pain |
| Follow-set across pods | **Compacted Kafka topic (opt-in)** | ConfigMap; chart-values seed | Persists and converges replicas on the cluster the daemon already reaches; a ConfigMap needs the Kubernetes API and races between replicas, and a seed cannot carry the demand-driven set |
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
- **Bounded historical backfill and restart catch-up** shipped later (#56):
  see *The tail workers* above. Before it, a new topic's coverage started at
  its first search.
- **Follow-set persistence is per container** (#56): the set is saved beside
  the indexes and restored on a container restart. **Cross-pod persistence**
  (#123) is the opt-in compacted topic of *Follow-set persistence across
  pods*; without it each replica, and each new pod, learns its follow-set
  from the searches it serves, and the pinned pre-follow set covers new-pod
  warmth for the hot topics.
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

### Observability (#55) — **IMPLEMENTED**
- `grpc.health.v1` on the gRPC port (SERVING once listening, NOT_SERVING
  from SIGTERM), and a separate plain-HTTP admin port (`--admin-listen`)
  with Prometheus `/metrics`, `/healthz` and `/readyz`. Follow-set gauges
  are read at scrape time from the live set; event counters cover searches
  by outcome, search latency and evictions by reason. Lag is sampled from
  the brokers' end offsets on the stats tick. See
  [`guides/deploy.md`](../guides/deploy.md#metrics-and-probes).

### Phase 5 (gates sensitive topics) — per-caller authorization — done (#118)
- `--authz-policy` on k4a-index and k4a-mcp maps mTLS identities and bearer
  tokens to search/follow topic globs, default-deny once configured (see
  *Per-caller authorization*). The mapping is operator-written rather than
  read from each caller's Kafka grants (the "coarser mapping" option below).
- **Ships:** the safety property that lets the daemon follow sensitive
  topics without turning the index into a read-around.

## Open questions

- **Per-caller authorization mechanism** — decided for Phase 5: the coarser
  identity→topic-glob mapping. Deriving grants from each caller's own Kafka
  read grants (the client presents its MSK IAM identity and the daemon keeps
  a describe-ACLs cache) stays open, if keeping the policy in step by hand
  proves error-prone.
- **Compacted topics.** Same concern as `local-index.md`: the shared index
  may retain records compaction has since removed from the broker. For
  "did this ever happen?" that's a feature; the shared render must mark
  compacted-source hits so nobody reads them as current state.
- ~~**Follow-set persistence store.**~~ *(Decided in #123.)* A compacted
  Kafka topic, opt-in; see *Follow-set persistence across pods*.
- **Backfill window default.** 24h / 1M records (`--backfill-window`, `--backfill-max-records`) is a guess. During incidents the
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
