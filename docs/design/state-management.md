# Design: State management

**Status:** shipped — `internal/state` exists and owns `~/.k4a`; the
session, cache, and index tiers are all live and surfaced by
`k4a state status|clear|dir`.
**Code:** `internal/state/` (`state.go`, `session.go`, `lock.go`);
`internal/searchcache/` (the Tier 2 query cache, `~/.k4a/cache/searches/`);
`internal/index/` (the Tier 3 Bleve index, `~/.k4a/index/`);
`internal/command/state.go` (the CLI surface). The `internal/config/`
refactor named below was not done — config stays its own package and is
simply the one tier `state` never clears.

## Purpose

k4a was originally modeled on k9s — single binary, stateless, the cluster
is the source of truth. That model is delightful for Kubernetes, where the
control plane is queryable in ways Kafka isn't. For Kafka, the lack of any
local state forces every interactive search, every metadata refresh, every
"did I just see that?" to round-trip the broker. The deep-search work has
already established that broker round-trips are the bottleneck for the most
important workflow we have.

The honest fix is to let k4a remember things between operations: query
results, scanned record bytes, search indexes, recent UI choices. But
adding state to a previously-stateless tool is dangerous if done casually —
state-corruption bugs, upgrade migrations, multi-instance races, "is this
file safe to delete" anxiety. This document specifies the rules under which
k4a is allowed to keep state.

The goal is to preserve everything that made the k9s-style model
attractive — single binary, zero install ritual, throw it away and restart
to fix anything — while letting state exist as a managed, deletable
optimization layer.

## The cardinal rule

> **The cluster is always the source of truth.** Local state is
> a cache. k4a must produce identical observable behavior on a fresh
> `~/.k4a/` directory as on a warm one — just slower.

If we ever find ourselves writing a code path that says "the index has
this record, the broker doesn't, so we'll show what the index says," we
have violated the rule and should reconsider. The only exceptions are
purely-local concepts (UI prefs, last-used context) that have no broker
counterpart.

This rule is what lets us be cavalier about state: corrupt → delete →
rebuild. Multi-instance race → second instance becomes read-only. Schema
upgrade → wipe and re-warm. None of these are user-visible disasters.

## State tiers

State is partitioned into tiers, each with a clear ownership model,
deletion policy, and rebuild cost.

| Tier | Lives in | Contents | Required for function? | Owner |
|---|---|---|---|---|
| **0 — Config** | `~/.k4a/config.yaml` | Contexts, auth references, UI prefs (`logoless`, default cluster, etc.) | No, but losing it loses user preferences | User |
| **1 — Session** | `~/.k4a/state.json` | Last active context, recent searches, recent topics, window prefs, splash skip | No | k4a (rebuilds trivially on next interaction) |
| **2 — Cache** | `~/.k4a/cache/` | Query results, scan byte cache, topic metadata snapshots, schema discovery | No | k4a (purgeable any time, automatic eviction) |
| **3 — Index** | `~/.k4a/index/<cluster>/<topic>/` | Full-text Bleve index for fast search (see `local-index.md`) | No | k4a (rebuildable from broker via catch-up scan) |
| **4 — External refs** | Config-only (no local data) | Athena workgroup names, ELK endpoints, S3-sink topic mappings | No | External system |

A few invariants follow from this table:

- **Every tier above 0 is independently deletable.** `rm -rf ~/.k4a/index`
  → search falls back to scan. `rm -rf ~/.k4a/cache` → next operation
  re-fetches. `rm -rf ~/.k4a/state.json` → user lands back on the context
  picker. The user can `rm -rf ~/.k4a` entirely and k4a behaves as on a
  fresh install.
- **No tier above 0 is required for any operation to succeed.** Searching
  with no index works (scan). Switching contexts with no session state
  works (picker). Viewing topics with no cache works (live fetch).
- **Tier 4 has no local data.** It's config-only — k4a knows the *name*
  of the external system but the data lives over there.

## Principles

### 1. Schema versioning per subsystem

Each state subsystem stores its own `schema_version` in its top-level
file (`state.json`, `cache/manifest.json`, `index/<topic>/state.json`,
etc.). On startup the loader checks the version:

- **Same version** → use as-is
- **Newer version** (downgrade case, e.g. user reverted to an older k4a) →
  rename the directory to `.<name>.bak.<timestamp>` and start fresh
- **Older version** → either migrate (rare, only when wipe is expensive)
  or rename-and-rebuild (default for caches and indexes)

The default policy is "rename and rebuild." Migrations are real
engineering work and we should only pay that cost when rebuild is
expensive. For Tier 2 (cache), rebuild is free — just empty it. For Tier
3 (index), rebuild requires a catch-up scan, which costs time and broker
bandwidth but is mechanically the same as the initial indexing path.

Rename-instead-of-delete preserves user data for post-mortem
investigation. The renamed dir gets garbage-collected after 7 days.

### 2. Single-writer locking

Each state directory gets an advisory `flock` on a `lock` file at its
root. The acquire pattern:

```go
lock, err := state.AcquireWriter("~/.k4a/index/<cluster>/<topic>")
if err != nil {
    // Another k4a instance is the writer. We become a reader.
    return state.OpenReadOnly(...)
}
defer lock.Release()
```

Reader mode means: query the existing state, but don't write to it. The
UI surfaces this clearly ("read-only — another k4a instance is the
active writer for this state"). No corruption is possible because no
unsynchronized writes happen.

If the writer crashes without releasing the lock, the kernel reclaims it
on process exit. No stale-lock problem.

### 3. User-visible state surface

The user can always see and reset what k4a is keeping. A small CLI subcommand:

```
$ k4a state status
~/.k4a/
├── config.yaml          (Tier 0, 2.4 KB, 5 contexts)
├── state.json           (Tier 1, 12 KB, updated 3 min ago)
├── cache/               (Tier 2, 47 MB, 12 entries)
│   └── ...
└── index/               (Tier 3, 8.2 GB, 4 topics)
    ├── prod-msk/
    │   ├── example.ingest.prices.v1   (4.1 GB, 6h old, healthy)
    │   ├── orders.events.v3             (2.8 GB, 2d old, healthy)
    │   └── _consumer_offsets             (disabled)
    └── ...

$ k4a state clear cache
Cleared 47 MB in ~/.k4a/cache.

$ k4a state clear index --topic example.ingest.prices.v1
Cleared 4.1 GB in ~/.k4a/index/prod-msk/example.ingest.prices.v1.

$ k4a state clear --all
Cleared 8.2 GB total. Config preserved.

$ k4a state dir
~/.k4a
```

Plus inline visibility in the TUI: a small indicator on each topic in
the Topics view showing whether it's indexed and how fresh.

### 4. Per-cluster keying

Two k4a contexts that point at the same physical cluster (e.g., a
read-only IAM context and an admin SCRAM context, or a staging context
and an integration-test context that share the staging cluster) should
share state. The natural key is **cluster ID** (the `ClusterID` field
from the broker's MetadataResponse), not the context name.

This avoids duplicating multi-GB indexes for the same data and keeps
search results consistent regardless of which auth context the user
happens to be in. The lock model in §2 handles the case of two contexts
trying to write at once.

Cluster ID is fetched on first connection and cached in Tier 1
(session). Contexts whose cluster ID hasn't been resolved yet get a
provisional key based on the bootstrap broker host until the metadata
fetch completes.

### 5. No background daemon (initially)

State only updates while k4a is running. Closing k4a stops indexing,
caching, all of it. On reopen, catch-up runs against the broker for
whatever gap accumulated.

We don't ship a `k4a indexd` service initially because:

- It introduces platform-specific service management (launchd plist,
  systemd unit, Windows service)
- It introduces version skew (what if `k4a` and `k4a indexd` are
  different versions?)
- It introduces "is it running?" UX
- It eliminates the "single binary, drop it on PATH" promise

The catch-up scan model handles the "user closed k4a overnight" case
adequately: next morning, opening a topic kicks off a backfill that's
bounded by the gap size.

If a daemon ever becomes necessary, it's an opt-in power-user feature,
not the default. The user explicitly installs it; the CLI behaves
identically with or without it.

### 6. Local-only, never network-shared as default

State directories live on the user's machine. Period. A future "share
my index of the incident topic with a colleague" feature is fine, but
as an explicit operation:

```
$ k4a state export --topic example.ingest.prices.v1 --output /tmp/incident.k4aindex
$ # send the tarball

$ k4a state import /tmp/incident.k4aindex
Imported 2.8 GB for topic example.ingest.prices.v1 (cluster prod-msk).
```

The tarball is a self-describing artifact (schema_version, cluster ID,
topic name, time range, byte size) that the importer validates before
unpacking. Importing into a context whose cluster ID doesn't match the
tarball's gets rejected.

A shared backend (Redis, S3, Postgres) is explicitly out of scope.
That's where k4a stops being one binary and we don't want to cross that
line for this feature.

### 7. Schema-on-write, not schema-on-read

When the indexer or cache writes a document, it commits to that
document's shape. Search reads what's there. Migrations are explicit
events (the schema_version bump in §1), not implicit "guess the shape"
operations.

Example: if v1 indexes `key` and `value` as full-text and v2 adds
`headers` as a separate field, v1 indexes simply don't have the
`headers` field and v2 search of `headers` returns nothing for those
records — *unless* the user explicitly triggers a re-index. We don't
try to be clever and partially backfill.

This rule keeps the index small, predictable, and debuggable.

## Layout

```
~/.k4a/
├── config.yaml                              # Tier 0
├── state.json                               # Tier 1
├── cache/                                   # Tier 2
│   ├── manifest.json                        # version + entries index
│   └── <hash>.lz4                           # serialized entries
└── index/                                   # Tier 3
    ├── <cluster_id>/
    │   ├── <topic_name>/
    │   │   ├── state.json                   # version + bookkeeping
    │   │   ├── bleve/                       # Bleve index files
    │   │   └── lock                         # flock advisory
    │   └── <another_topic>/
    └── <another_cluster>/
```

Notes:

- All Tier 0/1 files are JSON for human inspectability. The cache and
  index serialize via library-native formats (lz4 for cache entries,
  Bleve's mixed format for the index).
- The lock file is empty; only its presence and flock status matter.
- Tier 4 doesn't appear in the layout because it has no on-disk
  presence beyond references in `config.yaml`.

## Failure modes

| Failure | k4a's behavior |
|---|---|
| `~/.k4a/` doesn't exist | Create on first write; behave as on fresh install for reads |
| `config.yaml` corrupt | Show error on startup, refuse to launch until the user fixes or removes it |
| `state.json` corrupt | Rename to `.state.json.bak.<ts>`, start fresh |
| `cache/manifest.json` corrupt | Wipe the cache dir entirely, start fresh |
| Index `state.json` corrupt | Rebuild from the Bleve index directly (Bleve is the data, state.json is the cache of bookkeeping) |
| Bleve index corrupt | Rename the topic's index dir to `.bak.<ts>`, fall back to scan, schedule a re-index |
| Disk full | Refuse new writes; existing reads continue working; UI surfaces a clear error and offers `state clear` |
| Two k4a instances racing | Second instance becomes read-only for any directory the first has locked |
| Cluster ID changes (cluster rebuild) | Old index dir orphaned, never garbage-collected automatically (user runs `state clear`) — alternative is auto-prune but that's risky |
| Schema version mismatch | Per §1: rename-and-rebuild for caches/indexes; refuse-and-error for config |

The pattern: state subsystems above Tier 0 fail-soft and rebuild. Tier
0 fails-hard because it's user-owned and we can't guess what the user
wanted.

## What's intentionally NOT here (v1)

- **Encrypted state.** Bleve indexes can contain message values, which
  may include sensitive data. v1 relies on filesystem permissions
  (`~/.k4a` is owned by the user, mode `0700`). Encryption at rest is a
  reasonable v2 addition but not foundational.
- **State sync across machines.** Same reasoning as the "no shared
  backend" rule. Export/import (§6) is the manual workaround.
- **Background trimmer process.** Trimming runs inline during indexing,
  not as a separate process. If the indexer crashes mid-trim, next
  startup detects and resumes. No daemon needed.
- **State quotas across subsystems.** Each subsystem manages its own
  size limits independently. A global "max k4a state size" is plausible
  but adds cross-subsystem coordination we don't need yet.
- **Per-topic auth gating of indexed reads.** The single-user-local-tool
  model means whoever can read `~/.k4a/index/` can read everything in
  it. Multi-tenancy concerns are out of scope; if k4a ever runs in a
  shared environment, the index needs ACL gating.

## Migration / shipping plan

Each tier is shippable independently and reversible (delete the
relevant directory to undo).

### Phase A — Foundation (≈ 1 week)
- `internal/state/` package with the locking, schema-versioning,
  rename-and-rebuild primitives
- `~/.k4a/state.json` (Tier 1) for last-context and recent-searches
- `k4a state status` / `k4a state clear` / `k4a state dir` CLI subcommands
- No behavior change yet — just the plumbing — but every subsequent
  feature uses it

### Phase B — Query cache (≈ half a week, builds on A)
- Tier 2 cache for completed deep-search results
- 32-entry in-memory LRU; flushes to disk on quit
- Search header annotates "from cache" when hit
- First chance to validate the lock/version/wipe machinery in practice

### Phase C — Continuous indexer (the local-index.md plan)
- Tier 3 (Bleve indexes), populated by tail fan-out
- Search dispatcher routes to index when the time range is covered
- Per-cluster keying, single-writer lock per topic
- See `local-index.md` for the full design

### Phase D — Catch-up indexer (continues from local-index.md)
- Backfills gaps when topics are opened after a quit

### Phase E — Retention UI
- Topics view shows "indexed?" indicator
- `state` subcommand grows status/clear at finer granularity

### Phase F (optional) — Export/import
- `k4a state export` / `k4a state import` for sharing indexes
- Self-describing tarball format

## Pickup notes

- **Always put new state behind a tier.** If you find yourself wanting
  to add `~/.k4a/foo.json`, ask: which tier? If none fit, the new state
  probably shouldn't exist.
- **Start with Phase A even if the first feature using it is small.**
  The discipline of going through the lock/version machinery for the
  smallest meaningful state (recent searches) catches the API mistakes
  cheaply. Don't be tempted to skip A and build the index directly.
- **Never let state contradict the broker.** If `kadm.ListEndOffsets`
  says a partition's high watermark is N and our index has documents
  with offsets > N, the broker wins. The index gets the conflicting
  records purged. The rule from §1 is paramount.
- **Tests:** state primitives are pure Go and unit-testable with
  `t.TempDir()`. Each subsystem should have a "corrupt state.json"
  test that confirms graceful recovery.
- **Don't index secrets.** When a topic config explicitly marks the
  value field as sensitive (rare but real), the indexer skips that
  topic. Config knob: `index.topics.<name>.enabled: false`.
