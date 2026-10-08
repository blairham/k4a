# Design: Async enrichment

**Status:** living document — describes current behavior
**Code:** `internal/tui/views/topics.go`, `internal/tui/views/groups.go`,
`internal/kafka/client.go` (`FetchTopicMessageCounts`, `FetchGroupsEnrichment`)

## Purpose

Show the most useful columns of the Topics and Groups tables (message
count, total lag, coordinator) without making the initial render wait
on slow operations. The observation is that listing topics or groups is
fast, but the *interesting* numbers per row require additional Kafka
roundtrips that get expensive on large clusters.

The pattern: render the cheap data immediately, show `"..."` in the
expensive columns, and merge enrichment in as it arrives. Hold prior
values across refresh cycles so columns don't flicker.

## The two-phase fetch

Both Topics and Groups follow the same shape:

1. **Phase 1 — metadata.** `FetchTopics` / `FetchGroups` returns rows
   with name, partition count, replication factor, member count, etc.
   The view's `Refresh()` returns a single `tea.Cmd` that emits a
   `*RefreshMsg` carrying these rows.
2. **Phase 2 — enrichment.** On receiving the `RefreshMsg`, the view
   immediately returns a *second* command that calls
   `FetchTopicMessageCounts` / `FetchGroupsEnrichment` and emits a
   `*EnrichmentMsg` (`TopicCountsMsg`, `GroupEnrichmentMsg`) when done.

So one user-visible refresh produces two messages, asynchronously, and
the table redraws between them.

## Preserving values across refresh

The 2-second refresh tick re-runs phase 1 every two seconds. Naively,
every tick would blank out the message count column for a beat (until
phase 2 returns). To avoid that, the views snapshot the current
enrichment values *before* applying the new metadata:

```go
oldCounts := make(map[string]int64)
for _, t := range v.allTopics {
    if t.Messages >= 0 {
        oldCounts[t.Name] = t.Messages
    }
}
v.allTopics = msg.Topics
for i := range v.allTopics {
    if count, ok := oldCounts[v.allTopics[i].Name]; ok {
        v.allTopics[i].Messages = count
    }
}
```

A negative sentinel (`-1`) means "never enriched"; only those render as
`"..."`. Topics that disappeared between refreshes drop out cleanly;
new topics show `"..."` until the next phase 2 completes.

## Batching expensive ops

The phase 2 fetchers are written to do as few network roundtrips as
possible. Two notable cases:

- **Topic message counts** use a single batched `ListOffsets` call
  spanning every topic/partition the user can see. We don't dial
  per-partition.
- **Group enrichment** does a single phase-1 OffsetFetch per group, then
  a single batched `ListOffsets` covering every (topic, partition)
  appearing in any group's committed offsets, then merges to compute
  total lag per group. Per-group code paths existed once; they were
  consolidated when a 200-group cluster started taking 30+ seconds per
  refresh.

The cost is that one slow broker can stall the whole batch. In practice
that's still better than serializing — we'd rather hit the 2-second
tick boundary on a slow cluster (and skip a refresh) than have the UI
feel responsive but display stale numbers indefinitely.

## Why not just fetch enrichment in phase 1?

We tried this. Two problems:

1. **First render takes too long.** On a cluster with hundreds of topics,
   the initial blank screen looks like the tool is broken. Splitting
   the fetch lets the table appear in <1s with names + partitions even
   if message counts take 5s.
2. **Refresh becomes a global stall.** A 2s tick that takes 4s to
   complete will skip ticks; visible rows stop updating *at all*. With
   the split, name/partition/replication updates land on schedule even
   when message counts are slow.

## Why not push enrichment to a separate goroutine pool?

Each view manages exactly one in-flight enrichment at a time, gated by
the `tickMsg` cycle. We don't need a pool. If a phase 2 takes longer
than 2s, the next tick still fires phase 1 (cheap), and the late phase
2 still merges correctly because it's keyed by name. No coordination
required.

The only failure mode worth naming: if a phase 2 takes longer than the
*next* refresh's phase 2, they can land out of order. Since they merge
by name, the result is one stale tick — acceptable.

## What's intentionally NOT here

- **No partial-progress messages.** Phase 2 returns a complete map or
  fails. Streaming partial counts would let the user watch numbers fill
  in, but every variant we tried felt twitchy on a busy cluster.
- **No caching across context switches.** Switching contexts (different
  cluster) blows away `allTopics` entirely. Caching by cluster ID would
  save a fetch on switch-back, but contexts switch rarely and the
  staleness risk isn't worth the code.
- **No back-pressure on slow clusters.** If phase 2 consistently takes
  >2s, we just skip overlapping ticks. We don't dynamically widen the
  refresh interval. Users on slow clusters can `r` manually; the tick
  is a convenience, not a contract.

## Pickup notes

- **Adding a new enrichment column:** mirror the message-counts pattern.
  Add the field with a negative sentinel, snapshot it on refresh, fetch
  it in phase 2, merge by name in the `*EnrichmentMsg` handler.
- **Adding a third phase:** don't. If you have *more* expensive data,
  fold it into the existing phase 2 fetch (one batched call) or move
  it to a drill-down view that fetches on demand.
- **Sentinel choice:** `-1` for "not yet enriched". Don't use 0 — 0 is a
  valid count for empty topics and would show as "0" instead of "...".
