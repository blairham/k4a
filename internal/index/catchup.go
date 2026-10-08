// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/blairham/k4a/internal/kafka"
)

// CatchupOptions tunes the backfill scan. Defaults are chosen to bound
// wall time so opening a topic never feels stuck on catch-up; if a gap
// can't be filled in the budget, we record a Gap marker in the
// bookkeeper and let the live tail proceed.
type CatchupOptions struct {
	// MaxRecords caps the total records ingested across all partitions.
	// 0 → DefaultMaxRecords.
	MaxRecords int64
	// MaxDuration caps wall time. 0 → DefaultMaxDuration.
	MaxDuration time.Duration
}

const (
	// DefaultMaxRecords bounds catch-up to ~100K records, which is a
	// few seconds even on slow connections. Bigger gaps record Gap
	// markers and proceed.
	DefaultMaxRecords = 100_000
	// DefaultMaxDuration bounds catch-up wall time so a topic open
	// never feels stuck.
	DefaultMaxDuration = 30 * time.Second
)

// kafkaClient is the subset of *kafka.Client the catch-up needs. Lets
// tests substitute without dragging the whole client in.
type kafkaClient interface {
	EndOffsets(ctx context.Context, topic string) (map[int32]int64, error)
	ConsumeRange(
		ctx context.Context,
		topic string,
		starts, ends map[int32]int64,
		onRecord func(kafka.ConsumedMessage),
	) error
}

// CatchupResult summarizes one catch-up pass.
type CatchupResult struct {
	StartedAt      time.Time
	FinishedAt     time.Time
	RecordsIndexed int64
	RecordsDropped int64 // submitted-but-dropped (channel overflow)
	GapsRecorded   int   // new gap markers added to the bookkeeper
	TimedOut       bool
	HitRecordCap   bool
}

// Catchup backfills the index by reading every record between the
// bookkeeper's newest-per-partition offset and the broker's current
// high watermark, feeding records into the AsyncIndexer.
//
// Behavior summary (see design doc local-index.md Phase D / C-2):
//   - No prior indexed state for a partition → skip (live tail takes
//     over from "now" forward; users wanting historical data trigger
//     manual backfills in a later feature).
//   - Newest_indexed_offset+1 < HWM → backfill the gap.
//   - Limits exceeded → record Gap markers in the bookkeeper, flush,
//     return.
//
// Returns the result summary. Errors are reserved for fatal broker
// failures; partial successes (timeouts, caps) come back via the
// result flags so the caller can log + proceed.
func Catchup(
	ctx context.Context,
	client kafkaClient,
	async *AsyncIndexer,
	topic string,
	opts CatchupOptions,
) (CatchupResult, error) {
	opts = opts.withDefaults()

	res := CatchupResult{StartedAt: time.Now().UTC()}
	defer func() { res.FinishedAt = time.Now().UTC() }()

	if async == nil || async.ix == nil {
		return res, nil
	}
	book := async.Bookkeeper()
	if len(book.NewestOffsetPerPartition) == 0 {
		// No prior state → nothing to backfill. Live tail takes over.
		return res, nil
	}

	endOffsets, err := client.EndOffsets(ctx, topic)
	if err != nil {
		return res, err
	}

	starts, ends := catchupRanges(book.NewestOffsetPerPartition, endOffsets)
	if len(starts) == 0 {
		return res, nil
	}

	cctx, cancel := context.WithTimeout(ctx, opts.MaxDuration)
	defer cancel()

	dropPrev := async.Dropped()
	var indexed atomic.Int64

	// Sentinel error used to short-circuit ConsumeRange when we want
	// to stop because of the record cap. We swallow it; the result
	// flags tell the caller what happened.
	stop := false

	err = client.ConsumeRange(cctx, topic, starts, ends, func(m kafka.ConsumedMessage) {
		if stop {
			return
		}
		if async.Submit(m) {
			indexed.Add(1)
		}
		if indexed.Load() >= opts.MaxRecords {
			stop = true
			cancel()
		}
	})

	res.RecordsIndexed = indexed.Load()
	res.RecordsDropped = async.Dropped() - dropPrev
	res.HitRecordCap = stop
	res.TimedOut = cctx.Err() == context.DeadlineExceeded && !stop

	// If we couldn't finish the requested ranges, mark the unscanned
	// remainder as gaps so the dispatcher's CanServe honors the missing
	// coverage. Record regardless of how ConsumeRange returned — what
	// matters is whether *we* decided to bail.
	if res.TimedOut || res.HitRecordCap {
		gaps := computeRemainingGaps(starts, ends, &indexed, opts.MaxRecords)
		if len(gaps) > 0 {
			recordGaps(async.ix, gaps)
			res.GapsRecorded = len(gaps)
		}
	}

	// Flush bookkeeper so the dispatcher sees fresh ranges/gaps even
	// if the user immediately searches.
	_ = async.ix.FlushBookkeeper() //nolint:errcheck // best-effort

	// We deliberately swallow ConsumeRange errors caused by our own
	// context cancellation. Anything else is a real failure.
	if err != nil && cctx.Err() == nil {
		return res, err
	}
	return res, nil
}

// withDefaults fills the zero-valued limits.
func (o CatchupOptions) withDefaults() CatchupOptions {
	if o.MaxRecords <= 0 {
		o.MaxRecords = DefaultMaxRecords
	}
	if o.MaxDuration <= 0 {
		o.MaxDuration = DefaultMaxDuration
	}
	return o
}

// catchupRanges pairs each partition's next unindexed offset with its high
// watermark, leaving out partitions the broker did not report and those
// already caught up.
func catchupRanges(newest, endOffsets map[int32]int64) (starts, ends map[int32]int64) {
	starts = make(map[int32]int64, len(newest))
	ends = make(map[int32]int64, len(endOffsets))
	for partition, newestIndexed := range newest {
		hwm, ok := endOffsets[partition]
		if !ok {
			continue
		}
		nextWanted := newestIndexed + 1
		if hwm <= nextWanted {
			continue
		}
		starts[partition] = nextWanted
		ends[partition] = hwm
	}
	return starts, ends
}

// computeRemainingGaps figures out which offset ranges were not
// covered before catch-up bailed. We attribute the entire un-consumed
// remainder to one Gap per partition — at the kafka level we don't
// know precisely which offsets were processed since records arrive
// out-of-order across partitions; we conservatively assume nothing
// past the original start was indexed.
func computeRemainingGaps(starts, ends map[int32]int64, _ *atomic.Int64, _ int64) []Gap {
	gaps := make([]Gap, 0, len(starts))
	for partition, start := range starts {
		end := ends[partition]
		if end > start {
			gaps = append(gaps, Gap{
				Partition: partition,
				Start:     start,
				End:       end,
				Reason:    "catchup_bailed",
			})
		}
	}
	return gaps
}

func recordGaps(ix *Indexer, gaps []Gap) {
	ix.mu.Lock()
	ix.book.Gaps = append(ix.book.Gaps, gaps...)
	ix.book.LastUpdatedAt = time.Now().UTC()
	ix.publishLocked()
	ix.mu.Unlock()
}
