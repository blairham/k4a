// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"context"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blairham/k4a/internal/kafka"
)

// asyncBatchSize is how many records the async indexer collects before
// committing a Bleve batch. Larger batches amortize Bleve overhead but
// delay durability; this is a reasonable middle ground.
const asyncBatchSize = 64

// asyncFlushInterval bounds how long a partial batch can sit before
// being flushed. Keeps "indexed up through ~5s ago" honest even at
// low record rates.
const asyncFlushInterval = 5 * time.Second

// asyncBufferSize is the bounded channel capacity. When full, Submit
// drops with a gap-marker rather than blocking the caller — the
// caller is the live-tail consume loop, which we must never slow down.
const asyncBufferSize = 4096

// AsyncOptions configures NewAsyncWithOptions beyond the bare NewAsync
// defaults.
type AsyncOptions struct {
	// OnFlushError, when non-nil, is called from the drain goroutine whenever a
	// flush stage fails (stage is "index", "trim", or "bookkeeper"; records is
	// the size of the batch being flushed). The default (nil) keeps failures
	// silent — the TUI's local indexer treats indexing as best-effort — but the
	// shared daemon MUST wire it to a log line: swallowing these errors is how
	// the drain wedge stayed invisible for 13 hours.
	OnFlushError func(stage string, records int, err error)

	// TrimBytes is a per-topic byte cap: after each flush the drain goroutine
	// calls ix.Trim(TrimBytes), deleting the oldest records once the index
	// exceeds the cap. <=0 disables trimming (the historical unbounded behavior
	// the local index and catch-up path rely on). Only the shared daemon caps.
	TrimBytes int64
}

// AsyncIndexer wraps an Indexer with a background goroutine that
// consumes records from a bounded channel and writes them to Bleve in
// batches. Submit() is non-blocking and drops overflow with a gap
// marker, so it's safe to call from the live-tail consume hot path.
//
// Lifecycle: NewAsync → repeated Submit() calls → Stop() (waits for
// the in-flight batch, FlushBookkeeper, Close underlying Indexer).
type AsyncIndexer struct {
	ix           *Indexer
	onFlushError func(stage string, records int, err error)
	input        chan kafka.ConsumedMessage
	done         chan struct{}
	dropped      atomic.Int64
	lastFlush    atomic.Int64
	trimBytes    int64
	wg           sync.WaitGroup
	stopped      atomic.Bool
}

// NewAsync wraps ix in an async indexer with no retention cap (the index grows
// unbounded — the local index and catch-up path rely on this). ix must already
// be Open()ed; the returned AsyncIndexer takes ownership — Stop() closes it.
func NewAsync(ix *Indexer) *AsyncIndexer {
	return NewAsyncWithOptions(ix, AsyncOptions{})
}

// NewAsyncWithOptions is NewAsync with the shared daemon's extras: a per-topic
// byte cap and a flush-error hook. See AsyncOptions.
func NewAsyncWithOptions(ix *Indexer, opts AsyncOptions) *AsyncIndexer {
	a := &AsyncIndexer{
		ix:           ix,
		input:        make(chan kafka.ConsumedMessage, asyncBufferSize),
		done:         make(chan struct{}),
		trimBytes:    opts.TrimBytes,
		onFlushError: opts.OnFlushError,
	}
	a.lastFlush.Store(time.Now().UnixNano())
	a.wg.Add(1)
	go a.run()
	return a
}

// Submit pushes a record into the indexer's input channel. Non-blocking;
// if the channel is full, the record is dropped and Dropped() counts it.
// Returns true if accepted, false if dropped.
func (a *AsyncIndexer) Submit(m kafka.ConsumedMessage) bool {
	if a.stopped.Load() {
		return false
	}
	select {
	case a.input <- m:
		return true
	default:
		a.dropped.Add(1)
		return false
	}
}

// Dropped returns the number of records that were dropped due to a
// full buffer since the indexer started. Lifetime counter.
func (a *AsyncIndexer) Dropped() int64 {
	return a.dropped.Load()
}

// Stop closes the input channel, waits for the background goroutine
// to drain pending records and flush bookkeeper state, then closes the
// underlying Indexer. Safe to call multiple times.
func (a *AsyncIndexer) Stop() error {
	if !a.stopped.CompareAndSwap(false, true) {
		return nil
	}
	close(a.input)
	a.wg.Wait()
	return a.ix.Close()
}

// Bookkeeper returns a snapshot of the underlying indexer's bookkeeper.
func (a *AsyncIndexer) Bookkeeper() Bookkeeper {
	return a.ix.BookkeeperSnapshot()
}

// Inner returns the wrapped Indexer for read-side access (CanServe,
// Query, BookkeeperSnapshot). Writes should still go through Submit so
// they're batched on the background worker.
func (a *AsyncIndexer) Inner() *Indexer { return a.ix }

// LastFlush returns when the drain goroutine last completed a flush cycle
// (including empty ticker no-ops). With a live drain it advances at least
// every asyncFlushInterval; a stale value alongside a non-empty Buffered() is
// the drain wedge signature — the drain goroutine blocked inside a flush
// while holding the indexer mutex. It deliberately reads only an atomic,
// never ix.mu: a watchdog polling it must stay responsive precisely when the
// mutex holder is stuck.
func (a *AsyncIndexer) LastFlush() time.Time {
	return time.Unix(0, a.lastFlush.Load())
}

// Buffered returns how many submitted records are waiting in the input
// channel. Lock-free.
func (a *AsyncIndexer) Buffered() int { return len(a.input) }

// run is the background worker. Reads from input, accumulates batches,
// flushes on size or time threshold, and on input close exits cleanly.
func (a *AsyncIndexer) run() {
	defer a.wg.Done()
	ctx := context.Background()

	batch := make([]kafka.ConsumedMessage, 0, asyncBatchSize)
	ticker := time.NewTicker(asyncFlushInterval)
	defer ticker.Stop()

	// Indexing stays best-effort (the data is in the broker; a failed batch is
	// discarded and surfaces as a coverage gap) — but failures are REPORTED,
	// never swallowed: a silent flush path is how the drain wedge went
	// unnoticed until the drop counter was the only signal left.
	flush := func() {
		defer func() { a.lastFlush.Store(time.Now().UnixNano()) }()
		if len(batch) == 0 {
			return
		}
		n := len(batch)
		if err := a.ix.IndexBatch(ctx, batch); err != nil {
			a.reportFlushError("index", n, err)
		}
		if err := a.ix.Trim(ctx, a.trimBytes); err != nil {
			a.reportFlushError("trim", n, err)
		}
		if err := a.ix.FlushBookkeeper(); err != nil {
			a.reportFlushError("bookkeeper", n, err)
		}
		batch = batch[:0]
	}

	for {
		select {
		case msg, ok := <-a.input:
			if !ok {
				flush()
				return
			}
			batch = append(batch, msg)
			if len(batch) >= asyncBatchSize {
				flush()
			}
		case <-ticker.C:
			flush()
		}
	}
}

// reportFlushError forwards a flush-stage failure to the configured hook, if
// any. Runs on the drain goroutine.
func (a *AsyncIndexer) reportFlushError(stage string, records int, err error) {
	if a.onFlushError != nil {
		a.onFlushError(stage, records, err)
	}
}
