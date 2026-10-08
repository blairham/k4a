// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package searchcache

import (
	"context"
	"log/slog"
	"time"

	"github.com/blairham/k4a/internal/kafka"
)

// Searcher is the subset of *kafka.Client the cache needs to delegate a
// miss to. Concretely *kafka.Client satisfies this; tests can substitute.
type Searcher interface {
	DeepSearch(ctx context.Context, topic string, params kafka.SearchParams) (
		<-chan kafka.ConsumedMessage, <-chan kafka.DeepSearchProgress, <-chan error,
	)
	EndOffsets(ctx context.Context, topic string) (map[int32]int64, error)
	ClusterID(ctx context.Context) (string, error)
}

// Search wraps a deep search with cache lookup/storage. The return shape
// is identical to kafka.Client.DeepSearch so callers can swap one for
// the other without other changes.
//
// On a cache hit, matches are replayed from disk and the final
// DeepSearchProgress carries Source=SourceCache. On a miss, the
// underlying scan runs, results are forwarded to the caller as they
// arrive, and a successful (uncapped, error-free) completion seeds a
// new cache entry.
//
// Cache misses for any reason — first-ever query, partition HWM
// advanced, schema version bumped, ctx canceled, scan errored, scan
// capped — fall through to the underlying scan cleanly. The cardinal
// "cluster is source of truth" rule from state-management.md is
// preserved: we never serve stale data from the cache.
func (c *Cache) Search(
	ctx context.Context,
	client Searcher,
	topic string,
	params kafka.SearchParams,
) (<-chan kafka.ConsumedMessage, <-chan kafka.DeepSearchProgress, <-chan error) {
	matchCh := make(chan kafka.ConsumedMessage, 256)
	progCh := make(chan kafka.DeepSearchProgress, 1)
	errCh := make(chan error, 1)

	go c.runSearch(ctx, client, topic, params, matchCh, progCh, errCh)
	return matchCh, progCh, errCh
}

func (c *Cache) runSearch(
	ctx context.Context,
	client Searcher,
	topic string,
	params kafka.SearchParams,
	matchCh chan<- kafka.ConsumedMessage,
	progCh chan<- kafka.DeepSearchProgress,
	errCh chan<- error,
) {
	defer close(matchCh)
	defer close(progCh)
	defer close(errCh)

	clusterID, err := client.ClusterID(ctx)
	if err != nil {
		// Without a cluster ID we can't safely cache (different
		// clusters could collide). Fall through to a passthrough scan
		// with no caching.
		slog.Debug("searchcache: cluster ID lookup failed, bypassing cache", "err", err)
		c.passthrough(ctx, client, topic, params, matchCh, progCh, errCh)
		return
	}

	key := BuildKey(clusterID, topic, params)
	entry, hit := c.Get(key)
	if hit {
		current, hwmErr := client.EndOffsets(ctx, topic)
		if hwmErr == nil && entry.IsValidAgainst(current, params.Until) {
			c.replay(ctx, entry, matchCh, progCh)
			return
		}
		// HWM check failed or fetch errored — drop the stale entry and scan.
		c.Forget(key)
	}

	c.passthroughAndCache(ctx, client, topic, params, key, matchCh, progCh, errCh)
}

// replay streams the cached matches and emits a terminal progress with
// Source=SourceCache. Context cancellation aborts early.
func (c *Cache) replay(
	ctx context.Context,
	entry *Entry,
	matchCh chan<- kafka.ConsumedMessage,
	progCh chan<- kafka.DeepSearchProgress,
) {
	start := time.Now()
	for i, m := range entry.Matches {
		select {
		case <-ctx.Done():
			return
		case matchCh <- m:
		}
		// Emit periodic progress so views can keep their headers fresh
		// during long replays.
		if i > 0 && i%matchProgressChunk == 0 {
			select {
			case progCh <- kafka.DeepSearchProgress{
				Elapsed:        time.Since(start),
				Source:         kafka.SourceCache,
				Matches:        i + 1,
				Partitions:     entry.Summary.Partitions,
				DonePartitions: entry.Summary.DonePartitions,
			}:
			default:
			}
		}
	}

	// Terminal progress carries the original scan's elapsed/scanned
	// stats so the user can see what the original work was, plus a
	// replay-elapsed annotation isn't shown — the wall time is just
	// time.Since(start), but that's misleadingly small. We emit the
	// original elapsed so "complete · 1.2s · 167 MB" still makes sense
	// after a hit. The view can decide whether to render an additional
	// "from cache" tag.
	select {
	case progCh <- kafka.DeepSearchProgress{
		Elapsed:        entry.Summary.Elapsed,
		Scanned:        entry.Summary.Scanned,
		Bytes:          entry.Summary.Bytes,
		Matches:        len(entry.Matches),
		Partitions:     entry.Summary.Partitions,
		DonePartitions: entry.Summary.DonePartitions,
		Source:         kafka.SourceCache,
		Done:           true,
	}:
	default:
	}
}

// passthrough forwards an underlying scan to the caller without caching.
// Used when we can't determine a cluster ID — we'd rather degrade
// gracefully than refuse to search.
func (c *Cache) passthrough(
	ctx context.Context,
	client Searcher,
	topic string,
	params kafka.SearchParams,
	matchCh chan<- kafka.ConsumedMessage,
	progCh chan<- kafka.DeepSearchProgress,
	errCh chan<- error,
) {
	srcMatch, srcProg, srcErr := client.DeepSearch(ctx, topic, params)
	forwardScan(ctx, srcMatch, srcProg, srcErr, matchCh, progCh, errCh, nil)
}

// forwarded is what forwardScan saw of an underlying scan.
type forwarded struct {
	lastProg kafka.DeepSearchProgress
	scanErr  bool // the scan reported an error
	canceled bool // ctx ended before the scan's channels closed
}

// forwardScan relays an underlying scan's three channels to the caller's until
// all of them close or ctx ends. Matches are sent blocking; progress and errors
// are offered without blocking. onMatch, when set, also sees every match.
func forwardScan(
	ctx context.Context,
	srcMatch <-chan kafka.ConsumedMessage,
	srcProg <-chan kafka.DeepSearchProgress,
	srcErr <-chan error,
	matchCh chan<- kafka.ConsumedMessage,
	progCh chan<- kafka.DeepSearchProgress,
	errCh chan<- error,
	onMatch func(kafka.ConsumedMessage),
) forwarded {
	var res forwarded
	for srcMatch != nil || srcProg != nil || srcErr != nil {
		select {
		case <-ctx.Done():
			res.canceled = true
			return res
		case m, ok := <-srcMatch:
			if !ok {
				srcMatch = nil
				continue
			}
			if onMatch != nil {
				onMatch(m)
			}
			matchCh <- m
		case p, ok := <-srcProg:
			if !ok {
				srcProg = nil
				continue
			}
			res.lastProg = p
			offer(progCh, p)
		case e, ok := <-srcErr:
			if !ok {
				srcErr = nil
				continue
			}
			if e != nil {
				res.scanErr = true
				offer(errCh, e)
			}
		}
	}
	return res
}

// offer sends v on ch only if ch can take it without blocking.
func offer[T any](ch chan<- T, v T) {
	select {
	case ch <- v:
	default:
	}
}

// passthroughAndCache forwards the underlying scan and, on a clean
// completion, snapshots the result into the cache.
func (c *Cache) passthroughAndCache(
	ctx context.Context,
	client Searcher,
	topic string,
	params kafka.SearchParams,
	key Key,
	matchCh chan<- kafka.ConsumedMessage,
	progCh chan<- kafka.DeepSearchProgress,
	errCh chan<- error,
) {
	hwmStart, hwmErr := client.EndOffsets(ctx, topic)
	canCache := hwmErr == nil && len(hwmStart) > 0

	srcMatch, srcProg, srcErr := client.DeepSearch(ctx, topic, params)
	var matches []kafka.ConsumedMessage
	var onMatch func(kafka.ConsumedMessage)
	if canCache {
		onMatch = func(m kafka.ConsumedMessage) { matches = append(matches, m) }
	}
	res := forwardScan(ctx, srcMatch, srcProg, srcErr, matchCh, progCh, errCh, onMatch)
	if res.canceled {
		return
	}
	lastProg := res.lastProg

	// Never persist a result the scanner could not complete. A truncated scan
	// still reports Done, so gating on Done alone would cache a known-partial
	// result and re-serve it as authoritative on every later hit.
	if !canCache || res.scanErr || lastProg.Incomplete() || !lastProg.Done {
		return
	}

	entry := &Entry{
		Key:     key,
		Matches: matches,
		HWM:     hwmStart,
		Summary: Summary{
			Elapsed:        lastProg.Elapsed,
			Scanned:        lastProg.Scanned,
			Bytes:          lastProg.Bytes,
			Matches:        len(matches),
			Partitions:     lastProg.Partitions,
			DonePartitions: lastProg.DonePartitions,
		},
		CreatedAt: time.Now().UTC(),
	}
	c.Put(entry)
}
