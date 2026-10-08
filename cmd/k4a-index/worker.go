// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/log"

	"github.com/blairham/k4a/internal/index"
	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/state"
)

// statsInterval is how often the worker logs a throughput/coverage snapshot.
const statsInterval = 15 * time.Second

// tailSource is the slice of *kafka.Client a topicWorker needs: a live tail
// (Consume) plus the catch-up primitives (EndOffsets/ConsumeRange). Splitting
// it out mirrors internal/index/catchup.go's kafkaClient interface — it keeps
// the worker testable against an in-memory source and against kfake, without
// dragging the whole client into a unit test.
type tailSource interface {
	Consume(ctx context.Context, topic string) (<-chan kafka.ConsumedMessage, <-chan error, <-chan struct{})
}

// topicWorker owns one followed topic's index. It wraps the SAME
// index.AsyncIndexer the local index uses (docs/design/shared-index-service.md
// §3 — one indexer implementation, never forked) and feeds it from a live
// tail. The follow-set manager (manager.go) spawns one worker per followed
// topic on demand and evicts it (LRU / cap) via cancel + stop.
type topicWorker struct {
	src       tailSource
	async     *index.AsyncIndexer
	readyCh   chan struct{}
	topic     string
	opts      workerOptions
	submitted atomic.Int64
	readyOnce sync.Once
}

// workerOptions carries the per-topic tunables the manager plumbs down from
// the daemon flags.
type workerOptions struct {
	// TrimBytes caps the topic's index on disk (0 = unbounded); high-volume
	// high-volume topics need it or they'd fill the emptyDir and
	// evict the pod.
	TrimBytes int64
	// StallAfter arms the drain-stall watchdog (0 disables — see watchdog).
	StallAfter time.Duration
	// MergeGrace is how long a drain stall is tolerated while a scorch file
	// merge is in flight (0 = no grace; every stall trips at StallAfter).
	MergeGrace time.Duration
	// WipeStrikes is the consecutive-stall count at which the watchdog
	// quarantines the topic's index directory before exiting (0 disables).
	WipeStrikes int
}

// newTopicWorker opens (or re-opens) the topic's on-disk index and wraps it in
// an async indexer. cluster keys the index directory so two clusters' copies of
// the same topic name never collide — the same isolation the local index uses.
func newTopicWorker(
	root *state.Root, cluster, topic string, src tailSource, opts workerOptions,
) (*topicWorker, error) {
	ix, err := index.Open(root, cluster, topic)
	if err != nil {
		return nil, err
	}
	return &topicWorker{
		async: index.NewAsyncWithOptions(ix, index.AsyncOptions{
			TrimBytes: opts.TrimBytes,
			// Never swallow a flush failure: whatever
			// bleve reports on the way down is the RCA breadcrumb.
			OnFlushError: func(stage string, records int, err error) {
				log.Error("index flush failed", "topic", topic, "stage", stage, "records", records, "err", err)
			},
		}),
		src:     src,
		topic:   topic,
		opts:    opts,
		readyCh: make(chan struct{}),
	}, nil
}

// run drives the live tail until ctx is canceled. Each consumed record is
// Submit()ted to the async indexer, which is non-blocking and drops on
// overflow (the tail is the hot path and must never stall — staleness is
// surfaced later as a coverage gap, never as backpressure on the broker).
//
// Warming is live-tail only: the recent window comes from Consume's initial
// fetch and grows forward from registration. Bounded HISTORICAL backfill on
// registration (indexing the window BEFORE the topic was first searched) is a
// deliberate follow-up — it needs a timestamp→offset lookup internal/kafka
// doesn't yet expose, and coverage stays honest without it (the covered window
// starts at registration; clients scan anything older). See the design doc.
func (w *topicWorker) run(ctx context.Context) {
	log.Info("tail starting", "topic", w.topic)
	ch, errCh, ready := w.src.Consume(ctx, w.topic)
	go w.logStats(ctx)
	go w.watchdog(ctx)

	for {
		select {
		case <-ctx.Done():
			log.Info("tail stopping", "topic", w.topic, "submitted", w.submitted.Load())
			return
		case <-ready:
			// Initial fetch has started flowing; unblock waitReady once.
			log.Info("tail warm (first fetch flowing)", "topic", w.topic)
			w.readyOnce.Do(func() { close(w.readyCh) })
			ready = nil
		case m, ok := <-ch:
			if !ok {
				log.Info("tail channel closed", "topic", w.topic, "submitted", w.submitted.Load())
				return
			}
			if w.async.Submit(m) {
				w.submitted.Add(1)
			} else {
				log.Warn("indexer buffer full — dropped record (surfaces as a coverage gap)",
					"topic", w.topic, "partition", m.Partition, "offset", m.Offset)
			}
		case err, ok := <-errCh:
			if ok && err != nil {
				log.Error("tail consume error", "topic", w.topic, "err", err)
			}
		}
	}
}

// logStats emits a periodic throughput + coverage snapshot so we can watch the
// index warm, see the covered window move, and catch drops. Noisy on purpose.
func (w *topicWorker) logStats(ctx context.Context) {
	t := time.NewTicker(statsInterval)
	defer t.Stop()
	var lastSubmitted, lastDropped int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			book := w.async.Bookkeeper()
			submitted := w.submitted.Load()
			dropped := w.async.Dropped()
			docs, _ := w.async.Inner().DocCount() //nolint:errcheck // best-effort stat
			since, until := coveredWindow(book)
			log.Info(
				"index stats",
				"topic", w.topic,
				"docs", docs,
				"partitions", len(book.NewestOffsetPerPartition),
				"indexed_per_s", (submitted-lastSubmitted)/int64(statsInterval/time.Second),
				"submitted_total", submitted,
				"dropped_total", dropped,
				"dropped_delta", dropped-lastDropped,
				"covered_from", since,
				"covered_to", until,
			)
			lastSubmitted, lastDropped = submitted, dropped
		}
	}
}

// drainStallCheckEvery is how often the watchdog samples drain progress.
const drainStallCheckEvery = 30 * time.Second

// drainStallExit is os.Exit, indirected so tests can observe the trip without
// killing the test binary.
var drainStallExit = os.Exit

// watchdog fail-fasts the daemon if this topic's drain goroutine stops making
// progress while records are queued — the drain wedge: the drain blocked
// inside a bleve call holding the indexer mutex, so Search/Coverage/stats all
// hung and the daemon served nothing, silently, for 13 hours. It samples ONLY
// atomics (LastFlush/Buffered/Dropped — never the mutex; the stats logger
// blocks on it when wedged, which is exactly why the heartbeat vanished).
//
// On trip it dumps every goroutine stack to stderr — the distroless image
// has no exec/pprof, so this dump is the only way an occurrence can complete
// the root-cause analysis — then exits.
//
// Two escapes keep the exit from becoming a livelock (an observed
// failure: /data is an emptyDir, which is POD-scoped, so exit 2 restarted
// the container onto the same wedged index, each exit
// discarding the in-flight merge the drain was actually waiting on):
//
//   - A stall behind an in-flight scorch file merge is deferred up to
//     MergeGrace: the merger is working (runnable, burning CPU), and killing
//     it restarts the merge from scratch on the same volume, forever.
//   - Stalls are counted in a strike file INSIDE the topic's index directory
//     (so the count survives container restarts and dies with the index). On
//     the WipeStrikes-th consecutive stall the index dir is quarantined
//     before exiting — the next container boots onto a fresh directory and
//     re-warms from Kafka, which is exactly what the manual rollout-restart
//     remedy did. The index is disposable; a re-warm costs ~1 minute.
func (w *topicWorker) watchdog(ctx context.Context) {
	if w.opts.StallAfter <= 0 {
		return
	}
	start := time.Now()
	strikesCleared := false
	prevMerge := w.async.Inner().MergeProgress()
	t := time.NewTicker(drainStallCheckEvery)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			buffered := w.async.Buffered()
			sinceFlush := time.Since(w.async.LastFlush())
			stalled := drainStalled(buffered, sinceFlush, w.opts.StallAfter)

			// A merge only earns grace while it is demonstrably WORKING. A
			// merger that panicked leaves Started > Finished forever with no
			// goroutine behind it, so requiring movement is what
			// stops the watchdog waiting out the full grace on a corpse.
			merge := w.async.Inner().MergeProgress()
			merging := merge.InFlight() && merge.Advanced(prevMerge)
			prevMerge = merge

			switch watchdogVerdict(stalled, stalled && merging, sinceFlush, w.opts.MergeGrace) {
			case watchdogHealthy:
				// A previous container's stall streak is over once this one
				// has outlived the stall window with a live drain (each wedged
				// container died at exactly StallAfter, never later).
				if !strikesCleared && time.Since(start) > w.opts.StallAfter {
					resetStallStrikes(w.async.Inner().Dir())
					strikesCleared = true
				}
			case watchdogDefer:
				log.Warn("drain stalled behind an in-flight segment merge — deferring the watchdog",
					"topic", w.topic,
					"buffered", buffered,
					"stalled_for", sinceFlush.Round(time.Second),
					"merge_bytes_written", merge.BytesWritten,
					"merge_grace", w.opts.MergeGrace)
			case watchdogExit:
				dir := w.async.Inner().Dir()
				strikes := bumpStallStrikes(dir)
				log.Error("drain goroutine stalled — dumping all goroutine stacks and exiting (drain watchdog)",
					"topic", w.topic,
					"buffered", buffered,
					"stalled_for", sinceFlush.Round(time.Second),
					"dropped_total", w.async.Dropped(),
					"stall_strikes", strikes,
					"wipe_at", w.opts.WipeStrikes,
					// merge_stalled=true means a merge was begun and never
					// finished while making no progress: the dead-merger
					// signature. Pair it with any "scorch async error" line.
					"merge_stalled", merge.InFlight())
				dumpGoroutines(os.Stderr)
				if w.opts.WipeStrikes > 0 && strikes >= w.opts.WipeStrikes {
					quarantineIndexDir(dir, w.topic)
				}
				drainStallExit(2)
				return
			}
		}
	}
}

// watchdogAction is the per-tick watchdog decision, split out pure so the
// escalation ladder is table-testable without a wedged bleve index.
type watchdogAction int

const (
	watchdogHealthy watchdogAction = iota // drain is live — no action
	watchdogDefer                         // stalled behind a working merge — hold fire
	watchdogExit                          // genuine stall (or merge overran its grace) — dump and exit
)

// watchdogVerdict maps one watchdog sample to an action. A stall defers while
// a file merge is in flight and the stall is still inside mergeGrace; grace 0
// disables deferral. Everything else stalled exits.
func watchdogVerdict(stalled, mergeInFlight bool, sinceFlush, mergeGrace time.Duration) watchdogAction {
	switch {
	case !stalled:
		return watchdogHealthy
	case mergeInFlight && mergeGrace > 0 && sinceFlush < mergeGrace:
		return watchdogDefer
	default:
		return watchdogExit
	}
}

// drainStalled is the watchdog trip condition: records are waiting AND the
// drain goroutine hasn't completed a flush cycle within the stall window. A
// healthy drain flushes at least every asyncFlushInterval whenever input
// arrives, so a live loop can never trip a multi-minute window.
func drainStalled(buffered int, sinceFlush, stallAfter time.Duration) bool {
	return stallAfter > 0 && buffered > 0 && sinceFlush >= stallAfter
}

// stallStrikesFile counts consecutive watchdog trips for one topic index. It
// lives inside the index directory on purpose: the emptyDir survives container
// restarts (the drain-stall livelock's scope), and anything that replaces the
// index — quarantine here, backupTopicDir, a fresh pod — resets the count with
// it.
const stallStrikesFile = "stall-strikes"

// stallStrikes reads the current strike count; absent or unreadable is 0.
func stallStrikes(dir string) int {
	b, err := os.ReadFile(filepath.Join(dir, stallStrikesFile)) //nolint:gosec // the daemon's own index dir
	if err != nil {
		return 0
	}
	n, err := strconv.Atoi(strings.TrimSpace(string(b)))
	if err != nil || n < 0 {
		return 0
	}
	return n
}

// bumpStallStrikes increments the strike count and returns the new value.
// Best-effort: if the write fails the count stays where it was, which only
// delays escalation.
func bumpStallStrikes(dir string) int {
	n := stallStrikes(dir) + 1
	_ = os.WriteFile(filepath.Join(dir, stallStrikesFile), //nolint:errcheck // best-effort
		[]byte(strconv.Itoa(n)), 0o600)
	return n
}

// resetStallStrikes clears the strike count once the drain has proven live.
func resetStallStrikes(dir string) {
	_ = os.Remove(filepath.Join(dir, stallStrikesFile)) //nolint:errcheck // best-effort
}

// quarantineIndexDir renames the wedged topic index aside and deletes it, so
// the NEXT container boots onto a fresh directory and re-warms from Kafka.
// Rename-then-remove rather than remove-in-place: the wedged goroutines are
// still live until the exit that follows, and a rename atomically stops them
// recreating files inside a tree mid-RemoveAll. The copy is deleted, not kept:
// repeated 1GiB quarantines would fill the 10Gi emptyDir, and the goroutine
// dump is the post-mortem artifact.
func quarantineIndexDir(dir, topic string) {
	aside := dir + ".wedged." + time.Now().UTC().Format("20060102T150405")
	if err := os.Rename(dir, aside); err != nil {
		log.Error("stall strikes exhausted but quarantine rename failed — next boot reuses the wedged index",
			"topic", topic, "dir", dir, "err", err)
		return
	}
	log.Error(
		"stall strikes exhausted — quarantined the wedged index; next boot re-warms from Kafka (drain-stall escalation)",
		"topic",
		topic,
		"dir",
		dir,
	)
	if err := os.RemoveAll(aside); err != nil {
		log.Error("removing quarantined index", "topic", topic, "dir", aside, "err", err)
	}
}

// dumpGoroutines writes every goroutine's stack to out, growing the buffer
// until the full dump fits.
func dumpGoroutines(out io.Writer) {
	buf := make([]byte, 1<<20)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			buf = buf[:n]
			break
		}
		buf = make([]byte, 2*len(buf))
	}
	_, _ = out.Write(buf) //nolint:errcheck // best-effort post-mortem output
}

// coveredWindow is the all-partition covered range: [max(oldest), min(newest)]
// across partitions — the window a client query must fall inside to be served
// from the index. "-" when nothing is indexed yet.
func coveredWindow(b index.Bookkeeper) (from, to string) {
	var maxOld, minNew int64
	first := true
	for p, oldest := range b.OldestTimePerPartition {
		newest := b.NewestTimePerPartition[p]
		if first {
			maxOld, minNew, first = oldest, newest, false
			continue
		}
		if oldest > maxOld {
			maxOld = oldest
		}
		if newest < minNew {
			minNew = newest
		}
	}
	if first {
		return "-", "-"
	}
	return time.UnixMilli(maxOld).UTC().Format(time.RFC3339), time.UnixMilli(minNew).UTC().Format(time.RFC3339)
}

// waitReady blocks until the live tail's first fetch has started, or ctx is
// done. It signals the consumer connected — not that any record is indexed
// yet (indexing is asynchronous and batched); callers that need coverage poll
// coverage() instead.
func (w *topicWorker) waitReady(ctx context.Context) {
	select {
	case <-w.readyCh:
	case <-ctx.Done():
	}
}

// coverage snapshots the underlying Bookkeeper — the per-partition indexed
// offset/time ranges the wire Coverage frame is projected from.
func (w *topicWorker) coverage() index.Bookkeeper {
	return w.async.Bookkeeper()
}

// canServe runs the shared coverage decision on the current snapshot. It is
// the SAME (*Indexer).CanServe the local dispatcher calls — the daemon and a
// client fed its wire coverage reach identical index-vs-scan verdicts.
func (w *topicWorker) canServe(params index.QueryParams) (bool, string) {
	return w.async.Inner().CanServe(params)
}

// query runs a single search against the index, newest-first. The result
// carries whether the match limit truncated it, which the stream must relay.
func (w *topicWorker) query(ctx context.Context, params index.QueryParams) (index.QueryResult, error) {
	return w.async.Inner().Query(ctx, params)
}

// stop drains the in-flight batch, flushes bookkeeper state, and closes the
// underlying indexer (releasing its writer lock).
func (w *topicWorker) stop() error {
	return w.async.Stop()
}
