# Design: Local rolling index for fast topic search

**Status:** shipped — `internal/index` exists and the index is opt-in via
`index.enabled` in `~/.k4a/config.yaml`. The tier plan at the bottom is
kept as the record of what shipped in which order; Tiers 5-6 remain
unbuilt. The shared deployment of the same machinery is
[`shared-index-service.md`](shared-index-service.md).
**Depends on:** [`state-management.md`](state-management.md) — this design
adds Tier 3 state per that doc's tiering model.
**Code:** `internal/index/` (`index.go`, `query.go`, `pattern.go`,
`async.go`, `catchup.go`, `inventory.go`); dispatch from
`internal/tui/actions.go` and `internal/searchcache/wrap.go`; the scan
floor in `internal/kafka/deep_search.go`.

## Purpose

The current deep search is bounded by broker outbound bandwidth. We've
already cranked the parallelism: 96 concurrent fetchers, a byte-level
literal-prefix prefilter, batched fetches, sub-worker fan-out per partition.
There's no more juice to squeeze from the scan path itself — Kafka isn't a
queryable store, and we can't read faster than the broker can serve.

The 90% case is "find something from the last few hours." For that, the
broker round-trip is the wrong tool. Instead, k4a maintains its own local
rolling index of recent records — populated continuously as the user tails
topics — and answers searches against the indexed window far faster than a
broker round-trip, with zero broker load (on the order of ~100–500ms on a
20-minute warm index, rising with index size). The existing scan path stays as the
correctness floor: searches against time ranges older than the indexed
window (or topics that have never been indexed) work exactly as today.

This document specifies the indexed fast path and the dispatcher that
chooses between index and scan.

## User model

The user sees no new mode. `s` still opens the search bar, query still
runs against the topic, results stream in. What changes is *latency*: for
queries inside the indexed window, results appear instantly. The search
header gains one new field telling the user where the result came from:

```
search: /foo · example.ingest.prices.v1
indexed · 47 matches · 12ms
```

vs.

```
search: /foo · example.ingest.prices.v1
scanned · 47 matches · 9.4s · 167 MB · 4/6 partitions
```

Power users can force a fresh scan via a flag (`/foo @fresh` inline modifier
or a setting), but the default is "use the index if it covers the window."

## Architecture

```
                    ┌────────────────────────────────────┐
                    │            k4a runtime             │
                    │                                    │
       ┌────────────┼─ live tail (Consume) ─┐            │
       ▼            │                       ▼            │
  MessagesView      │              ┌─────────────────┐   │
  (display)         │              │  Indexer        │   │
                    │              │  (per-topic)    │   │
                    │              └────────┬────────┘   │
                    │                       │            │
                    │                       ▼            │
                    │           ┌────────────────────┐   │
                    │           │  Bleve index files │   │
                    │           │  ~/.k4a/index/...  │   │
                    │           └─────────┬──────────┘   │
                    │                     │              │
       's' →        │   ┌─────────────────┴───────────┐  │
       SearchView   │   │   SearchDispatcher          │  │
                    │   │   (decides index vs scan)   │  │
                    │   └─────┬───────────────────┬───┘  │
                    │         │                   │      │
                    │         ▼                   ▼      │
                    │   ┌──────────┐       ┌──────────┐  │
                    │   │ Index    │       │ Scan     │  │
                    │   │ query    │       │ (kgo)    │  │
                    │   └──────────┘       └──────────┘  │
                    └────────────────────────────────────┘
```

Three new components:

### 1. The Indexer (`internal/index/index.go`, `async.go`, `catchup.go`)

`index.go` owns one topic's Bleve index and the `Bookkeeper` sidecar
(`Open`/`Close`, `Index`/`IndexBatch`, `Trim`, the Bleve mapping, the flock).
`async.go` wraps it in the `AsyncIndexer` — the bounded-buffer background
goroutine that batches records and flushes them. `catchup.go` is the
backfill scan. Two intake paths:

- **Live tail tap.** The existing `client.Consume(ctx, topic)` returns a
  channel that today feeds MessagesView. We add a fan-out so each record
  also lands in the indexer's input channel. Zero new broker fetches —
  the indexer rides along on the existing consume.
- **Catch-up scan.** On topic open, the indexer compares its newest
  indexed offset against the broker's high watermark. If there's a gap
  (the topic was tailed yesterday, k4a was closed, new records arrived),
  the indexer kicks off a one-shot backfill scan using the same scan
  machinery as deep search. While catching up, the search dispatcher
  knows the indexed window has a gap and falls back to scan for queries
  that cross it.

The indexer writes Bleve documents shaped like:

```go
type indexDoc struct {
    Offset    int64     `json:"offset"`
    Partition int32     `json:"partition"`
    Timestamp time.Time `json:"timestamp"`
    Key       string    `json:"key"`
    Value     string    `json:"value"`
    Headers   string    `json:"headers"`   // serialized "k=v\nk=v"
}
```

`Key`, `Value`, and `Headers` are indexed as full-text fields with a
standard tokenizer. Numeric fields (`Offset`, `Partition`, `Timestamp`)
are stored for retrieval and use as filters (time range, partition
selection). Regex support is NOT "out of the box" — see "Regex semantics"
below for why the user's pattern is never run against the index verbatim.

**Backpressure.** The `AsyncIndexer`'s input channel is bounded
(`asyncBufferSize`, 4096). If the indexer falls behind, `Submit` drops
rather than blocking — the caller is the live-tail consume loop, which must
never be slowed — and the drop is counted (`Dropped()`) and recorded as a
`Gap`. The dispatcher then knows the index can't be trusted for the gap
window and falls back to scan. We never block the live tail to keep the
index consistent.

**Persistence.** Bleve writes to `~/.k4a/index/<cluster>/<topic>/`
(directory layout below). Index segments are rolled by Bleve's internal
mechanics; we don't need to manage them by hand.

### 2. The Dispatcher (`internal/index/query.go` + `internal/tui/actions.go`)

There is no standalone dispatcher file. The *decision* lives in
`query.go` as `Bookkeeper.CanServe(QueryParams)` (plus `(*Indexer).Query`
to run it), deliberately split so the same decision can be made on a
coverage snapshot that arrived over the wire — that is exactly what
`internal/remoteindex` does for the shared daemon. The *routing* lives at
the call sites: `internal/tui/actions.go` for the TUI, and
`internal/searchcache/wrap.go` + `internal/remoteindex` for the cache and
shared-index layers around the scan. Given a
`SearchRequest{topic, query, scope, since, until, partitions, cap}`:

1. Check the indexer's bookkeeping: does the index cover `[since, until]`
   for this topic? Three cases:
   - **Fully covered, no gaps.** Translate the request to a Bleve query
     and run it. Return matches via the existing `<-chan ConsumedMessage`
     channel.
   - **Partially covered.** Split the range. Query the index for the
     covered portion, scan the gap, merge results (newest-first, like
     today). Both sources emit into the same channel.
   - **Not covered at all.** Pure live scan, identical to today.
2. Return the same three channels SearchView already consumes
   (`matchCh`, `progCh`, `errCh`) so the view doesn't care which source
   answered.

The progress events get a new `Source` field (`SourceIndex`, `SourceScan`,
`SourceHybrid`) that the search header uses to render "indexed · …" vs.
"scanned · …".

### 3. The Bookkeeper (`internal/index/index.go`, read side in `query.go` / `inventory.go`)

The `Bookkeeper` struct and its atomic read/write live in `index.go`
alongside the indexer that maintains it; `query.go` holds `CanServe`, and
`inventory.go` summarizes every topic's sidecar for `k4a state status`
without taking the per-topic writer lock.

Per-topic state on disk: `~/.k4a/index/<cluster>/<topic>/state.json`.
Tracks:

```json
{
  "schema_version": 2,
  "newest_offset_per_partition": {"0": 12345678, "1": 12345012},
  "oldest_offset_per_partition": {"0": 12000000},
  "newest_time_per_partition": {"0": 1746093000000},
  "oldest_time_per_partition": {"0": 1746006600000},
  "gaps": [
    {"partition": 2, "start": 12340000, "end": 12340500, "reason": "indexer_overflow"}
  ],
  "first_indexed_at": "2026-05-01T10:30:00Z",
  "last_updated_at": "2026-05-02T10:30:00Z",
  "byte_size": 4823456789
}
```

The dispatcher reads this on every search to decide coverage. The indexer
writes it on every flush. Recovery: if state.json is corrupt, the indexer
rebuilds from the Bleve index directly (it has the data; the bookkeeper
is just a cache).

## Regex semantics — term anchoring and the candidate rewrite

The scan path's matching is grep-style: the pattern can hit anywhere
inside the raw key/value/headers. Bleve regexp queries are nothing like
that — they are **term-anchored**: the pattern must match an *entire
analyzed token*. k4a's analyzer (Bleve's standard tokenizer and stop list, with a byte-safe lowercase filter) tokenizes `"orderId":4100000042`
into terms like `orderid`, so the query `order` matches no term and
the index confidently answers zero, while a scan of the same records
matches every one — a silent wrong answer. The two search paths the
dispatcher unifies must never fork semantics.

The fix (`internal/index/pattern.go`) treats the Bleve query as a
**candidate filter that must never miss**; precision is restored by
re-applying the exact original regex to the stored text in
`hitToConsumed` (which also enforces scope and case). Two candidate
strategies, tried in order:

1. **Token-local patterns** — every string the regex can match consists
   solely of token runes (letters/digits/underscore), so any occurrence
   in raw text lies inside a single analyzed token. Candidate: the
   pattern itself, case-folded and unanchored — `(?i).*(?:pat).*`.
2. **Mandatory-literal patterns** — the regex can match across token
   separators, but every match provably contains some contiguous run of
   token runes (`"orderId":16` always contains `orderid`). That run
   always lands inside a single token, so `(?i).*run.*` is
   recall-complete. Requires a run of ≥ 3 runes for selectivity.

Patterns fitting neither strategy — or that could be satisfied inside an
English **stop word** (which the analyzer drops, so the token
was never indexed), or that the vellum term automaton rejects — are
**unservable**: `CanServe` returns false with the reason and the
dispatcher falls back to the scan, on both the local and shared-index
routes (they share the one `Bookkeeper.CanServe`). The daemon's `Query`
itself stays best-effort (an older client that skips the gate still
gets the widened candidate, a strict superset of the old anchored
matches — never worse than before).

**Cost.** The wrapped candidate has no literal prefix, so Bleve walks the
full term dictionary per field instead of seeking (measured ~5× on a
small index: 670 ms vs 130 ms). That is the price of correct substring
semantics on an analyzed field; it still beats a broker scan by orders of
magnitude. If it ever becomes prohibitive on multi-GiB indexes, the
follow-ups are a trigram sub-field (codesearch-style) or an unanalyzed
keyword sub-field — both index-size trades deferred until measured need.

## Storage layout

```
~/.k4a/index/
├── <cluster_id>/                        # one dir per Kafka cluster ID
│   ├── <topic_name>/
│   │   ├── state.json                   # bookkeeping
│   │   ├── bleve/                       # Bleve index files
│   │   │   ├── index_meta.json
│   │   │   ├── store/
│   │   │   └── ...
│   │   └── lock                         # advisory single-writer lock
│   └── <another_topic>/
└── <another_cluster>/
```

**Per-cluster, not per-context.** Multiple contexts (e.g., a staging
context and a production context) that point at the same physical cluster
share the index. We key on cluster ID (which kgo reports in Metadata),
not on context name. Contexts pointing at different clusters get
different index directories naturally.

**Single-writer lock.** Two k4a instances on the same machine trying to
index the same topic would corrupt the index. The lock is a
flock-style file under each topic directory; second-comer reads from the
index but doesn't write. Bookkeeper records "shared mode" so the user
sees that their k4a isn't the active indexer.

## Retention

> **Not shipped locally.** The only config key that exists today is
> `index.enabled` (`internal/config.Index`) — there are no
> `max_per_topic_gb` / `max_age_days` / per-topic override keys, and no
> `k4a index status` / `k4a index clear` subcommands. The **local** index
> is uncapped: `index.NewAsync` deliberately wires no trim cap, so it grows
> until the user clears it with `k4a state clear index` (`k4a state status`
> reports its size). The byte-cap trimmer described below **is**
> implemented (`(*Indexer).Trim`, driven by `AsyncOptions.TrimBytes`) but
> only the shared daemon sets it — see
> [`shared-index-service.md`](shared-index-service.md) §Per-topic
> retention. The rest of this section is the target design.

Default per-topic cap: **10 GB or 7 days of indexed data, whichever
smaller**. Both knobs configurable in `~/.k4a/config.yaml`:

```yaml
index:
  enabled: true              # master switch
  max_per_topic_gb: 10
  max_age_days: 7
  topics:                    # per-topic overrides
    example.ingest.prices.v1:
      max_per_topic_gb: 50   # keep more of this critical topic
    _consumer_offsets:
      enabled: false         # never index
```

Indexer trims oldest indexed records when either limit trips. Trim is a
Bleve delete-by-query for `timestamp < cutoff` followed by an optimize
pass. The bookkeeper updates `oldest_offset_per_partition` so the
dispatcher knows the new floor.

## What's intentionally NOT here (v1)

- **Schema-aware field extraction.** `key:1234567` as a structured query
  rather than a substring is a v2 feature. v1 indexes raw bytes only;
  Bleve full-text catches "1234567" as a token within JSON values so the
  common case still works.
- **Cross-topic search.** Indexes are per-topic; cross-topic search would
  need a meta-index. Defer.
- **Shared/networked indexes.** Local only. The whole point is to keep
  k4a a single binary.
- **Compressed index export/import.** Useful for "send me your index of
  the incident topic" but adds tool surface. Defer.
- **Auto-start indexing on every cluster.** Indexing is opt-in via
  config; new users get scan-only behavior until they enable it. Avoids
  surprise disk usage.
- **Background daemon.** Indexer runs only while k4a is running. If you
  close k4a, indexing stops; on reopen the catch-up pass fills the gap.
  A long-running daemon (`k4a indexd`) is plausible but out of scope.

## Why Bleve (vs alternatives)

| Option | Pros | Cons |
|---|---|---|
| **Bleve** | Pure Go, no CGO; full-text + regex + numeric range; battle-tested at Couchbase scale; embedded; fast | Disk format somewhat opaque; tuning has learning curve |
| sqlite-FTS5 | Familiar SQL; small footprint; good docs | Needs CGO (breaks k4a's pure-Go build); FTS5 is slower than Bleve on large corpora |
| RocksDB + custom inverted index | Maximum flexibility; very fast | Massive engineering; CGO; reinventing what Bleve already does |
| File-per-time-bucket + grep | Trivial to build | Slow; no real query language; no field filtering |

Bleve wins on "best Go fit for an embedded full-text index." The CGO
constraint is real — k4a's release pipeline depends on cross-compiling
without a C toolchain.

## Performance estimates

Rough back-of-envelope, to be replaced by real numbers:

| Scenario | Today (scan) | With index (covered) |
|---|---|---|
| Search last 1h, 1M records/h topic, common substring | 8–15 sec | 50–150 ms |
| Search last 24h, 24M records, common substring | 2–4 min | 300–800 ms |
| Search last 7d, 168M records, common substring | 15–25 min | 1–3 sec |
| Search last 1h, regex with `.*` everywhere | 8–15 sec (prefilter helps) | 200–500 ms (Bleve regex slower than substring) |
| First-ever search on a topic (no index yet) | 8–15 sec | 8–15 sec (no change — falls back to scan) |

So: 100–1000× faster for indexed queries, no slower than today for
unindexed ones.

## Migration / shipping plan

Each tier is independently shippable behind a feature flag, with the
existing scan path as the default fallback. We don't break any current
behavior at any step.

### Tier 1 — Query result cache (≈ half a day)
- In-memory cache keyed by `(cluster, topic, query, scope, since, until)`
- Stores last 32 completed scans
- Invalidates when the topic's high watermark changes
- Search header annotates "from cache" when a hit
- **Ships:** the "I just ran this query 30 seconds ago" win, no
  persistent state, no schema changes. Tells us how often users repeat
  queries (instrumentation).

### Tier 2 — Continuous indexer (≈ 2 weeks)
- `internal/index/` package with Bleve
- Tail fan-out: every record consumed by MessagesView also enters the
  indexer's intake
- Bookkeeper tracks indexed offset ranges per partition
- `index.enabled: true` in config; off by default
- SearchDispatcher routes to index when the request's time range falls
  inside the indexed window; otherwise unchanged
- **Ships:** instant search for any topic the user has been tailing. No
  catch-up yet — searches against time ranges before the user started
  tailing fall back to scan.

### Tier 3 — Catch-up indexer (≈ 1 week)
- On topic-view open, detect gap between newest indexed offset and high
  watermark
- If small (last hour), backfill silently in the background
- If large (first time on topic), prompt the user: "Index last 24h?" /
  "Index last 7d?" / "Scan only"
- Catch-up uses the existing deep-search scan machinery, writing into
  the index instead of (or in addition to) streaming to the UI
- **Ships:** the first interactive search on any topic is now instant
  too, after a one-time investment.

### Tier 4 — Retention & disk management (≈ 1 week)
- Background trimmer enforces per-topic caps
- `k4a index status` CLI command lists topic indexes, sizes, ages
- `k4a index clear <topic>` and `k4a index clear --all` for explicit cleanup
- Bookkeeper exposes age + size via UI (Topics view gains an "indexed?"
  column maybe)
- **Ships:** users have control of disk usage; index isn't a runaway
  silent resource hog.

### Tier 5 (optional) — Schema-aware field indexing (≈ 2–3 weeks)
- Detect JSON values, parse, emit individual fields into Bleve as typed
  fields
- Search bar accepts `field:value` syntax (`orderId:1234567`,
  `customerId:42`)
- Topic-config UI for "treat this topic's values as JSON"
- **Ships:** structured search on top of the substring search. Huge UX
  win for users who know their schema.

### Tier 6 (optional) — External-sink integration (≈ 1–2 weeks)
- Detect Confluent S3 sink connectors via Kafka Connect REST API
- For topics with active sinks, offer "search archive (Athena)" as a
  third option alongside indexed/scan
- Athena query generation, results streamed back through the same
  channel SearchView consumes
- **Ships:** orgs with existing archives get an even faster path for
  cold data than the local index can offer.

## Open questions

- **Index format upgrades.** Bleve's on-disk format is stable but not
  forever-compatible. Plan for `schema_version` in `state.json` and a
  v→v+1 migrator command. Probably fine, but call out.
- **What about compacted topics?** Compaction means old records vanish
  from the broker. The indexer might have indexed records that no longer
  exist in Kafka. For point lookups by offset that's a problem; for
  search ("did this ever happen?") it's a feature — k4a remembers what
  the broker forgot. We'd need to mark compacted-source records as such
  in the UI so users don't trust them as "current state."
- **Headers indexing default.** Yes? Cheap, and "did anyone publish with
  trace-id X" is a common search. I'd say yes; flag for review.
- **Partitions in search results.** Today the dispatcher receives a
  `Partitions []int32` filter. The Bleve index has a `partition` field,
  filter is trivial. No change.
- **Multi-context with same cluster ID but different auth.** Two contexts
  both point at prod-msk but one is read-only IAM and one is admin. They
  share the index. The read-only user might see records they shouldn't
  via cached index hits. Probably fine for a single-user
  local tool, but call out — if k4a ever becomes multi-tenant, the index
  has to gate by auth context.

## Pickup notes

- **Start with Tier 1.** The query cache is the cheapest, gives us
  instrumentation on repeat-query frequency to calibrate whether Tiers
  2–3 are worth the investment.
- **Indexer goroutine lifecycle:** App-owned, one AsyncIndexer per
  visited topic, each with its own dedicated background tail
  consumer. Opens on first visit to a topic's Messages view; persists
  across search overlays, tab switches, message-detail drill-ins,
  pops between views, AND across leaving the topic entirely. Closed
  only at app shutdown (ctrl-c, :quit, upgrade), which cancels every
  tail, drains pending batches, releases the per-topic flock, and
  flushes the bookkeeper.

  The dedicated tail is the indexer's only source of truth —
  MessagesView does not feed it. This keeps the index advancing for
  every topic that's been visited in the session, even ones the user
  has since navigated away from. The cost is one extra kgo consumer
  per visited topic; in practice users open a handful of topics per
  session, and the indexer's consumer is no busier than
  MessagesView's.

  *Why not view-scoped (history):* tying lifecycle to MessagesView
  destruction (the original v1 sketch) broke the indexer the moment
  any overlay appeared — popping out of Search tore down the consumer
  feeding the observer, leaving the bookkeeper frozen. Promoting the
  consumer to app scope and decoupling it from UI navigation made
  this go away.

  Bleve docs are keyed by `(partition, offset)`, so the small overlap
  between Catchup (forward from the bookkeeper's newest offset) and
  the dedicated tail (starting at `end - perPartition`) is idempotent
  and harmless. Bookkeeper offset updates take the max.
- **Don't index `_*` internal topics by default.** Adds noise, doubles
  disk for offsets/transactions topics most users won't search. Make it
  opt-in via per-topic config.
- **Tests:** `kfake` already gives us an in-process Kafka for the kafka
  package tests. For index tests, use a `tmp` Bleve directory in
  `t.TempDir()` and exercise the indexer + dispatcher end-to-end without
  needing a real broker.
