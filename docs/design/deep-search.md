# Design: Filter and search across a topic

**Status:** shipped
**Code:** `internal/tui/views/messages.go`, `internal/tui/views/search.go`, `internal/tui/views/search_topics.go`, `internal/tui/search_view.go`, `internal/kafka/deep_search.go`, `internal/kafka/decoder.go`, `internal/kafka/multi_search.go`, `internal/command/search.go`, `internal/command/search_topics.go`, `internal/searchcli/`, `internal/jsonpath/`

## Purpose

The Messages view has two distinct discovery needs:

1. **Filter** — narrow the messages I'm currently looking at. Cheap, local,
   instant.
2. **Search** — find messages I don't have yet, anywhere in the topic.
   Expensive, scans the broker, surfaces results from outside the buffer.

Conflating them produces friction in both directions: a typo'd `/` regex
silently triggers a multi-second cluster scan; a real search has nowhere to
express scope (time range, key vs value, headers) because it has to fit
into a single regex prompt.

This design splits them into two features with distinct ergonomics that
share the same fast underlying scanner.

## Filter (`/`)

In-memory only. Operates on the current message buffer (live tail +
fetched recent). Matches today's behavior up to the point it would
trigger a deep scan.

- `/pattern` — case-insensitive regex over visible columns
- `!/pattern` or `/!pattern` — negated
- Invalid regex falls back to literal
- Instant; no broker calls
- Header chip: `</pattern>` shown next to the resource title (today's UI)
- Empty state when 0 matches: dim message "No matches in buffer for /foo —
  press `s` to search the whole topic."

The filter is **never** authoritative. It only tells you what's in the
buffer. The empty-state hint is the bridge to search.

## Search (`s`)

A dedicated, modal dialog opened with `s` from the Messages view. The
dialog collects search parameters; pressing Enter runs the scan; the
results stream into a Search Results view (distinct from the live
Messages view).

### Search dialog

A small bubble-tea form (huh) with these fields:

| Field           | Type     | Default                  |
|-----------------|----------|--------------------------|
| Pattern         | text     | (current `/filter` value)|
| Match in        | select   | key + value              |
|                 |          | key only                 |
|                 |          | value only               |
|                 |          | + headers                |
| Time range      | select   | all time                 |
|                 |          | last 1h                  |
|                 |          | last 24h                 |
|                 |          | last 7d                  |
|                 |          | custom (date pickers)    |
| Partitions      | multi    | all                      |
| Case sensitive  | toggle   | off (case-insensitive)   |

The dialog defers to sensible defaults; power users get every knob. The
"all time / all partitions / key+value / case-insensitive" combination
reproduces today's deep-search behavior.

### Search Results view

Once the user submits, the dialog closes and a new view replaces the
Messages table:

- Same columns as Messages (offset, partition, time, key, value)
- Sticky two-line header (see below)
- Same key actions as Messages: `enter` for detail, `s` again to refine,
  `esc` returns to Messages
- Matches stream in newest-first within each partition (current behavior)

### Two-line header

Line 1 — search params (editable inline with `e`):

```
search: /foo · value only · last 24h · partitions [0,1,2] · case-insensitive
```

Line 2 — progress / completion:

```
running · 47% · 152K msgs · 167 MB · 9s · 4/6 partitions · 3 matches
```

On terminal widths < 100 cols, line 1 condenses; line 2 drops the rarest
fields first (bytes, then per-partition counts).

## Where the speed comes from

(Largely unchanged from the prior design — the underlying scanner is
shared between filter's transparent-scan path and the new explicit-search
path. Both reach `c.DeepSearch` in `internal/kafka/deep_search.go`.)

### 1. Newest-first reverse-chunked scan with adaptive chunk size

Per partition, the first chunk is small (~5K offsets) so the first match
surfaces fast. Subsequent chunks double up to ~1M. The chunk plan is a
pure function of partition size — no user input.

### 2. Per-partition parallelism with sub-worker fan-out

One goroutine per partition, all writing into the same match channel.
Within a partition, chunks are dispatched to a small pool of sub-workers
(1–8 each, sized by partition byte estimate) so a single fat partition
doesn't bottleneck on one fetch connection. A 12-partition topic with
large partitions can have up to 96 concurrent fetchers.

### 3. Match on `[]byte`, with literal-prefix prefilter

The regex's literal prefix (`regexp.LiteralPrefix`) is extracted up front
and used as a `bytes.Contains` necessary-condition gate before the full
regex engine runs. Common substring searches (`foo`, `^foo.*bar`) skip
the regex engine entirely on the negative case.

With a schema registry configured (`SearchParams.Decoder`), a record whose
key or value could be registry framed (a leading zero byte, or a schema-ID
header) is decoded first and the decoded key or value is matched as text,
the text Messages shows and the indexes store; the prefilter applies to it
unchanged, since a match contains its literal prefix whatever it is matched
against. Every other record, and every field that did not decode, is
matched on its bytes, so a topic without registry framing pays nothing.
The JSON-path predicate reads the same decoded text. See
[`schema-registry.md`](schema-registry.md) § *Search and the indexes*.

### 4. Big fetch batches

Scan readers hard-code MinBytes=1 MiB, MaxBytes=10 MiB, MaxWait=100 ms —
tuned for throughput, not latency.

### 5. Early termination on cap

Match cap (10,000 by default) cancels all partition workers as soon as
it's reached.

### 6. Single kgo.Client per sub-worker; SetOffsets *between* chunks

Each sub-worker holds one kgo.Client; chunk transitions happen via
`SetOffsets` rather than client recreation. Cuts dial + metadata cost
per chunk to zero.

**The first chunk is different, and this is load-bearing.** `SetOffsets`
only repositions partitions that have already been returned from a
`PollFetches`; before the first poll the partition's offset load has not
completed, `assignSetMatching` finds no cursor to set, and **the call is a
silent no-op** (franz-go `kgo/consumer.go`). So a sub-worker positions its
*first* chunk through the client constructor — `newScanClient` is given the
start of the first span that worker pulls off the queue — and only uses
`SetOffsets` for subsequent chunks.

Getting this wrong is not a performance bug, it is a correctness bug that
does not announce itself. Constructing every client at the partition's
oldest offset and relying on `SetOffsets` for the first chunk (the shape
this code originally shipped with) makes the newest chunk get scanned
with the *oldest* records: the newest region is never read and the oldest
region is emitted twice. It is invisible below 5,000 offsets, because a
one-span plan has `span.start == r.first` and the mismatch cannot occur.
`internal/kafka/deep_search_scan_test.go` pins both symptoms against kfake.

As defense in depth, `scanRange` discards records outside `[start, end)`
rather than counting them, so any future positioning mistake costs time
instead of silently corrupting results.

## Architecture

```
MessagesView
  ├─ '/' → SetFilter(regex), in-memory rebuildRows() — instant
  ├─ empty state hints at 's' when filter has zero matches
  └─ 's' → push SearchView (dialog → results)

SearchView (new, internal/tui/views/search.go)
  ├─ stateForm    — huh form gathers params
  ├─ stateRunning — Search Results view with two-line header
  ├─ stateDone    — same view, header shows completion
  └─ stateErrored — header shows error
        ↓
client.DeepSearch(ctx, topic, params) (<-chan ConsumedMessage, <-chan Progress, <-chan error)
  (existing scanner — accepts an extended SearchParams struct in place of just regex)
```

The TUI changes are localized to two files: `views/messages.go` (drop the
transparent-scan trigger, point empty state at `s`) and a new
`views/search.go`. The scanner itself (`internal/kafka/deep_search.go`)
gets a new `SearchParams` input but the scan logic is the same.

## SearchParams

```go
type SearchParams struct {
    Pattern     *regexp.Regexp
    MatchKey    bool
    MatchValue  bool
    MatchHeader bool
    Since       time.Time     // zero = no lower bound
    Until       time.Time     // zero = no upper bound
    Partitions  []int32       // nil = all
    Cap         int           // 0 = default 10,000
    Decoder     RecordDecoder // nil = match the bytes as consumed
}
```

`Since`/`Until` are mapped to offset bounds via
`kadm.ListOffsetsAfterMilli` before the per-partition scan starts —
**both** ends, not just `Since`. Resolving only `Since` and leaving the
upper bound at the live high watermark means a bounded historical query
still reads to the end of the topic and throws the tail away per-record,
so consecutive windows each re-read everything newer than their own start.

The per-record `Until` filter in `scanRange` stays regardless: offsets are
ordered within a partition but timestamps are not strictly so, so the
offset bound is a coarse trim and the record check is the precise one.
Partition filtering trims `deepSearchRanges` output.

## Match cap

Hard default of **10,000 matches**, raisable in the dialog (up to
100,000). When reached, all partition workers cancel and the header
shows `stopped at N matches — narrow your search`.

## Streaming results, no global sort

Matches stream in arrival order across partitions, newest-first within
each partition (because of reverse-chunked scan). We do not buffer-and-sort.

## Failure modes

| Failure | Behavior |
|---|---|
| Chunk cannot be fully read | `DeepSearchProgress.Truncated` is set; header/CLI say `INCOMPLETE`, and the result is **not** written to the query cache. A scan that ends short must never look like a clean completion |
| Schema registry cannot answer for a framed record (search with a decoder) | The record is matched on its bytes and `Truncated` is set, as for a short read: once the registry answers, the same search may match it decoded, so this result is neither cached nor presented as complete |
| Auth error mid-search | abort scan, show formatted error in header, keep already-found matches visible |
| One partition unreachable | that partition's worker exits; others continue. Header: `scanned 11/12 partitions` |
| Invalid regex | caught at form submit; the form rejects with inline error |
| Topic deleted mid-search | `ListOffsets` or read returns error → partition treated as unreachable |
| User presses `esc` | SearchView pops; the scan keeps running in the background (see below) |
| User presses `x` / `:search-stop` | scan canceled; the matches so far stay, header says `stopped · … · results are partial` |
| A new search starts, the context switches, or k4a quits | the old scan is canceled — never left running with nothing reading it |
| Topic glob selects nothing | error, never a clean empty result (`kafka.ErrNoTopicMatch`) |
| One topic of a glob fails | that topic's coverage shows the error; the others continue; the whole answer is INCOMPLETE |

## What's intentionally NOT here

- **Server-side filter.** Kafka has none.
- **More than one search at a time.** One background search; starting
  another cancels it. A queue of scans would multiply broker load behind the
  user's back and needs a list view to manage; nothing has asked for it.
- **Background scans that outlive k4a.** Quitting cancels the scan. A result
  worth keeping is in the query cache already (when it completed) or can be
  exported with `e`.
- **One merged answer across topics.** A cross-topic search is N searches
  side by side; there is no cross-topic index, no global cap and no merged
  ordering (see below).
- **Cross-topic search in MCP.** `k4a mcp` and `k4a-mcp` still take one
  topic. The per-topic summary (`searchcli.TopicsSummary`) is shaped so the
  tools can grow a glob without a new wire format.
- **Match count only mode.** Counting requires the same read.

## Background scans

A scan does not belong to the Search view's lifetime. Every scan message is
routed to the Search view wherever the user is (`App.updateSearch`), so `esc`
back to Messages, a tab switch or a trip into Message Detail leaves it
streaming. While it runs behind another view the status bar shows the same
progress line as the header (`backgroundSearchStatus`), and when it ends a
flash says how it ended (`complete`, `INCOMPLETE`, capped, error) and points
at `:results`, which pushes the Search view back on top so `esc` returns to
where the user was.

Stopping is explicit: `x` in the results or `:search-stop` anywhere calls
`SearchView.Cancel`, which cancels the scan's context and moves the view to
`searchStateCanceled` — the matches so far stay, and the header says they are
partial. The context error the cancellation produces is not shown as a
failure. A canceled scan is never cached (`searchcache` skips a canceled
relay), so the partial set cannot come back later as a complete answer.

**One search at a time.** A new search, a context switch (its client is
gone) and quitting all call `App.stopSearch` first. Before this, `esc` left
the scan running invisibly and a second search replaced the view without
canceling the first, which kept scanning with nothing reading it; its
`searchcache` relay then blocked forever on its match send. Two fixes make
abandoning a scan safe: the relay's send watches the context, and scan IDs
come from one process-wide counter (`nextScanID`), because per-view IDs
restarted at 1 and let a replaced view's late messages pass as the new
view's. A stale message is now dropped without starting another reader.

## Cross-topic search

`k4a search -t 'orders.*'`, and `:search orders.*` in the TUI, search every
topic a glob selects. A topic argument is a glob when it contains `*`, `?` or
`[` (`kafka.IsTopicGlob`): Kafka topic names are limited to `[a-zA-Z0-9._-]`,
so the two readings cannot collide. `path.Match` semantics; the match runs
against `FetchTopics`, and internal topics (`__consumer_offsets`,
`_schemas`) are only selected by a glob that itself starts with `_` — `*`
means the user's topics. A glob that selects nothing is an error, never an
empty, clean-looking result.

**Each topic is its own search.** `kafka.SearchTopics` runs the same
`SearchFunc` once per topic — cache, local index, shared index or scan,
decided per topic against that topic's coverage — and merges the three
channels. Every progress frame is tagged with its topic
(`DeepSearchProgress.Topic`) and every error is a `*kafka.TopicError`, so one
topic failing does not stop the others. At most `MaxParallelTopics` (4)
topics run at once: each scan already fans out to a worker per partition and
up to eight fetchers each, and a wide glob times that would swamp the
brokers and the client. The match cap applies per topic, so a busy topic
cannot starve a quiet one of its matches.

**Coverage stays per topic.** A merged total would read as one clean answer
while hiding the topic that was truncated, capped or failed, so no surface
reports only a total:

- **TUI:** a TOPIC column, and a third header line that stays visible after
  the scan — `orders.a: scanned 2 · orders.b: INCOMPLETE 1 · orders.c: error:
  … · orders.d: indexed (shared) 0`, or `queued` / a percentage while
  running. The whole search is `complete` only when every topic reported
  `Done` and was neither truncated nor capped; a topic that failed, was
  truncated or ended without a final frame makes it `INCOMPLETE · k of N
  topics not fully searched`; all topics failing is an error.
- **CLI streaming:** text output names the topic on each match; stderr gets
  one line per topic (the single-topic summary line, or `error:` / `not
  finished`) and a total with `(INCOMPLETE — k of N topics not fully
  searched)`. Exit status is 1 if any topic failed.
- **CLI `--format summary`:** a `searchcli.TopicsSummary` — `topic_glob`,
  `topics` (one complete `Summary` per topic, with its own `stats.source`,
  `capped` and `truncated`), a `matches` total and `incomplete`. The
  single-topic document is unchanged.

Progress frames in the merged stream are sent blocking (with the context),
never offered and dropped as a single scan's are: a topic's last frame is the
only place it says it was truncated, and dropping it would make an incomplete
topic look complete.

## JSON path search

`k4a search --path EXPR`, `path:EXPR` in the TUI search bar, and the `path`
field of both MCP `search` tools match a predicate on the record's JSON
structure rather than a regex over its bytes:

```
.order.status == "failed"
.items[0].qty >= 10
.["content-type"] != "text/plain"
.trace.id exists          (a bare path means the same)
```

**Syntax: a jq subset, not JSONPath.** A path is `.` followed by `.name`,
`["any name"]` (a JSON string) or `[N]` steps; a predicate is `exists` or one
of `== != < <= > >=` against one JSON scalar. jq won because its path
spelling is what people already type at `--format json | jq`, and because the
subset is closed: no wildcards, recursive descent, filters or functions, so a
predicate selects at most one value and its meaning fits in a paragraph.
JSONPath (RFC 9535) brings `$`, `..`, `*` and filter expressions, i.e.
multi-valued results and a second query language inside the first — none of
which the "find the failed orders" case needs. Anything wider can grow from
this grammar later without breaking it.

**Semantics** (`internal/jsonpath`): evaluated on the raw key and/or value
bytes per `--scope` (headers are never evaluated); a key or value that is not
exactly one JSON document never matches. Numbers compare numerically at
256-bit precision, so `12.50 == 12.5` and IDs above 2^53 compare exactly;
strings compare exactly (case-sensitive) and order bytewise. A comparison
between different JSON types is never `<`/`>`/`==`, and is `!=`. **A missing
path matches nothing, `!=` included** — `.status != "ok"` does not select
records that have no status, unlike jq's `null != "ok"`. With a regex as
well, both must match. With a schema registry, a framed key or value is
evaluated as its decoded JSON (schema-registry.md § *Search and the
indexes*).

**`field:value` shorthand.** A predicate that does not start with `.` may be
`name(.name)*:value`, short for an equality: `orderId:42` is
`.orderId == 42`, `order.status:failed` is `.order.status == "failed"`. The
value is a JSON scalar when it reads as one (`42`, `true`, `null`,
`"quoted"`), else the rest as a string, so `id:007` is the string `"007"`
and `flag:"true"` the string. It canonicalizes to the long form, so the two
share a cache entry.

**Equalities are index-served (Tier 5); everything else scans.** The index
stores, beside the analyzed text, one keyword term per JSON scalar leaf of
each key and value (`jsonpath.Terms`: canonical path, then a typed canonical
value, numbers in exact binary at the comparison precision so `42`, `42.0`
and `4.2e1` agree). An equality has exactly one term it could equal
(`Expr.Term`), so `Bookkeeper.CanServe` serves it when coverage holds, and
`Query` looks it up, then re-checks every candidate with `kafka.PathMatches`
on the stored text, the scan's own rule. A key or value with more than 1024
leaves gets only `jsonpath.PartialTerm`, which makes it a candidate for
every path; a string leaf over 256 bytes gets no term, and an equality with
a longer literal is declined. `!=`, the orderings and `exists` still decline
(`ReasonJSONPathScanOnly`), as does any path against an index older than
`index.FieldsSchemaVersion`, which a daemon reports as
`Coverage.schema_version`. `FuzzTerms` is the recall proof: whenever `Match`
holds for an indexable predicate, its term is among the document's. The
result cache keys on the canonical path (`Expr.String`), so equivalent
spellings share an entry. `k4a-mcp` still filters the daemon's records by
the path itself and reports `path_recall` (see shared-index-service.md).

Like the regex, the field terms come from the text the index stores. A key
or value the display renders as `hex:` (invalid UTF-8 or a control byte)
holds no terms, while a scan parses its bytes; Go's decoder accepts such
JSON, so an index can miss a record a scan matches there. It is the same
gap the regex has on those payloads.

`FuzzParsePath` checks that any predicate that parses has a canonical form
that re-parses to itself and matches exactly the same records.

An **on-disk result cache** does ship, despite the "sessions are short,
search params change" argument against one — `internal/searchcache`, at
`~/.k4a/cache/searches/`, keyed on the query and validated against each
partition's high watermark. See
[`state-management.md`](state-management.md) for the tiering rules and the
failure-modes table above for why an incomplete scan is never cached.

## Migration from the unified flow

`/` never triggers a deep scan; on an empty buffer the empty-state hint
points users at `s`.

Pre-existing scan infrastructure in `internal/kafka/deep_search.go`
stays as-is (already factored cleanly behind `DeepSearch`). The
`MessagesView` scan state machine (`scanPending`, `scanRunning`, etc.)
moves into `SearchView`. The scan-header rendering moves too.

## Pickup notes

- **Filter (`/`) implementation:** unchanged from today minus the
  `scheduleDeepSearch` / `startDeepSearch` calls and the `scanState`
  field. Remove `DeepSearchTriggerMsg` plumbing from Messages.
- **SearchView (`s`) implementation:** new file modeled on
  `views/produce.go` (also a form-then-progress view). Reuse
  `formatBytes` / `formatCount` / `formatElapsed`.
- **Scanner changes:** add `SearchParams` struct; `DeepSearch(ctx, topic, params)`
  replaces `DeepSearch(ctx, topic, re)`. `deepSearchRanges` accepts a
  partition filter and a `since` cutoff (translated via
  `ListOffsetsAfterMilli`).
- **Adjusting the initial-chunk size, fetch batches, sub-worker cap,
  match cap:** the speed-related constants are still constants, not
  config. If you find yourself wanting a flag, fix the heuristic.
