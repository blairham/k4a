# Design: Filter and search across a topic

**Status:** shipped
**Code:** `internal/tui/views/messages.go`, `internal/tui/views/search.go`, `internal/kafka/deep_search.go`, `internal/command/search.go`

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
| Auth error mid-search | abort scan, show formatted error in header, keep already-found matches visible |
| One partition unreachable | that partition's worker exits; others continue. Header: `scanned 11/12 partitions` |
| Invalid regex | caught at form submit; the form rejects with inline error |
| Topic deleted mid-search | `ListOffsets` or read returns error → partition treated as unreachable |
| User presses `esc` | scan canceled; SearchView pops, returns to Messages |

## What's intentionally NOT here

- **Server-side filter.** Kafka has none.
- **Background scans after view exit.** When the user leaves SearchView,
  the scan cancels. Background scans add state and surface area we don't
  want until the foreground shape is proven.
- **Cross-topic search.** Switch topic, re-run.
- **Match count only mode.** Counting requires the same read.
- **JSON path search (v1).** A follow-up; the regex form covers most needs.

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
