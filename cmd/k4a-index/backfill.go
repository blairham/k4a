// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"time"

	"github.com/blairham/k4a/internal/index"
	"github.com/blairham/k4a/internal/kafka"
)

// backfillOptions bounds the history a newly followed topic indexes before
// its live tail. Window 0 keeps the tail's old start, the most recent
// recentRecords across the topic, with no time window.
type backfillOptions struct {
	// Window is how far back from registration to start (by record time).
	Window time.Duration
	// MaxRecords caps the history read across all partitions, split evenly;
	// a partition with more than its share in the window starts later.
	// 0 means no record cap.
	MaxRecords int64
}

// backfillSource is what planning and an ordered history-then-tail read need.
// *kafka.Client has both; a tail source without them (the test fakes) keeps
// the plain live tail.
type backfillSource interface {
	TopicOffsetBounds(ctx context.Context, topic string, since time.Time) (kafka.OffsetBounds, error)
	ConsumeFrom(ctx context.Context, topic string, starts map[int32]int64) (
		<-chan kafka.ConsumedMessage, <-chan error, <-chan struct{},
	)
}

var _ backfillSource = (*kafka.Client)(nil)

// recentRecords is where a tail with no backfill window starts: the same
// recent slice kafka.Client.Consume reads, split across partitions.
const recentRecords = 100

// backfillPlanTimeout bounds the three ListOffsets calls planning makes.
const backfillPlanTimeout = 10 * time.Second

// backfillDeadline ends the "backfilling" report even if a partition never
// shows an offset at its target — a quiet partition whose last offset is a
// transaction marker never delivers one. Coverage stays honest either way;
// this only bounds how long the flag can stay up.
const backfillDeadline = 15 * time.Minute

// backfillPlan is where each partition's read starts, and the registration-time
// high watermark each partition must reach before its history is complete.
type backfillPlan struct {
	starts  map[int32]int64
	targets map[int32]int64
}

// planBackfill decides where each partition's single ordered read begins.
//
// A partition the index already holds resumes right after its newest record —
// the catch-up the daemon used to skip on restart, which left a silent hole
// between what an index held from before and what the new tail read. Resuming
// is only possible when every offset in between is still on the broker and
// within the record cap; otherwise the index is stale, and the caller starts
// it over rather than claim a contiguous range it cannot have.
//
// A partition the index does not hold starts at the first record of the
// window, pulled forward to its share of maxRecords.
//
// Reading forward from these offsets in one stream per partition keeps the
// covered range contiguous at every moment: the coverage frame can lag the
// broker while the history is read, but it never spans a hole.
func planBackfill(book index.Bookkeeper, b kafka.OffsetBounds, opts backfillOptions) (plan backfillPlan, stale bool) {
	plan = backfillPlan{starts: map[int32]int64{}, targets: map[int32]int64{}}
	var share int64
	if opts.MaxRecords > 0 && len(b.End) > 0 {
		share = max(1, opts.MaxRecords/int64(len(b.End)))
	}
	for p, end := range b.End {
		newest, held := book.NewestOffsetPerPartition[p]
		from, ok := partitionStart(b.Start[p], end, b.After, p, newest, held, share, opts.Window)
		if !ok {
			return backfillPlan{}, true
		}
		plan.starts[p] = from
		if from < end {
			plan.targets[p] = end
		}
	}
	// A partition the index holds that the broker no longer lists means the
	// topic was recreated under the same name.
	for p := range book.NewestOffsetPerPartition {
		if _, ok := b.End[p]; !ok {
			return backfillPlan{}, true
		}
	}
	return plan, false
}

// partitionStart is one partition's start offset; ok is false when a held
// partition cannot resume without a hole.
func partitionStart(
	floor, end int64, after map[int32]int64, p int32, newest int64, held bool, share int64, window time.Duration,
) (int64, bool) {
	if held {
		resume := newest + 1
		if resume > end || resume < floor || (share > 0 && end-resume > share) {
			return 0, false
		}
		return resume, true
	}
	from := end - max(1, recentRecords/int64(max(1, len(after))))
	if window > 0 {
		from = end
		if a, ok := after[p]; ok && a >= 0 && a < end {
			from = a
		}
	}
	from = max(from, floor)
	if share > 0 && end-from > share {
		from = end - share
	}
	return from, true
}

// backfillTracker reports whether a worker is still reading history. Only the
// tail goroutine calls observe; any goroutine may call active.
type backfillTracker struct {
	remaining map[int32]int64
	deadline  time.Time
	done      chan struct{}
}

func newBackfillTracker(targets map[int32]int64) *backfillTracker {
	t := &backfillTracker{
		remaining: make(map[int32]int64, len(targets)),
		deadline:  time.Now().Add(backfillDeadline),
		done:      make(chan struct{}),
	}
	for p, end := range targets {
		t.remaining[p] = end
	}
	if len(t.remaining) == 0 {
		close(t.done)
	}
	return t
}

// inHistory reports whether m is history for its partition (before the
// registration-time high watermark). Tail goroutine only.
func (t *backfillTracker) inHistory(m kafka.ConsumedMessage) bool {
	end, ok := t.remaining[int32(m.Partition)] //nolint:gosec // partition IDs fit in int32
	return ok && m.Offset < end
}

// observe retires a partition once a record at or past its last history
// offset arrives. Tail goroutine only.
func (t *backfillTracker) observe(m kafka.ConsumedMessage) {
	if t.finished() {
		return
	}
	p := int32(m.Partition) //nolint:gosec // partition IDs fit in int32
	if end, ok := t.remaining[p]; ok && m.Offset+1 >= end {
		delete(t.remaining, p)
	}
	if len(t.remaining) == 0 || time.Now().After(t.deadline) {
		close(t.done)
	}
}

func (t *backfillTracker) finished() bool {
	select {
	case <-t.done:
		return true
	default:
		return false
	}
}

// active is the coverage frame's backfilling flag.
func (t *backfillTracker) active() bool {
	return t != nil && !t.finished() && time.Now().Before(t.deadline)
}
