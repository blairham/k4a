// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"bytes"
	"context"
	"regexp"
	"sync"
	"sync/atomic"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// Deep search tuning constants. These are deliberately not exposed as
// config — see docs/design/deep-search.md ("zero knobs"). If you find
// yourself wanting to add a flag for one, fix the heuristic instead.
const (
	deepSearchMatchCap = 10_000
	// Initial chunk is small so the first batch of recent matches lands fast.
	// Subsequent chunks double up to the cap so we can walk back through
	// large topics in a reasonable number of round-trips. 1M offsets per
	// chunk is roughly 1 GiB at 1 KiB/msg — large enough that broker fetch
	// throughput, not chunk overhead, dominates.
	deepSearchInitialChunk = int64(5_000)
	deepSearchMaxChunk     = int64(1_000_000)
	deepSearchFetchMin     = int32(1 << 20)  // 1 MiB
	deepSearchFetchMax     = int32(10 << 20) // 10 MiB
	deepSearchFetchWait    = 100 * time.Millisecond
	deepSearchProgressHz   = 10

	// PollFetches deadline. The primary chunk-drain signal is offset
	// counting (see scanRange); the timeout only legitimately fires on
	// the very newest chunk (where there is no message past the high
	// watermark) and as a safety net for slow brokers.
	deepSearchPollTimeout = 5 * time.Second

	// Sub-worker fan-out. A single kgo.Client holds one TCP connection per
	// leader broker; on a large partition the broker can push more bytes than
	// one fetcher consumes. Multiple sub-workers each open their own client
	// and pull independent chunks from a shared queue.
	//
	// deepSearchSubWorkerCap bounds the sub-workers a SINGLE partition may
	// open; deepSearchScanClientBudget bounds them across the WHOLE scan.
	// The budget is the load-bearing one: partitions are scanned
	// concurrently, so a per-partition cap alone multiplies by the partition
	// count — an unbounded search of a 6-partition topic opened 48 clients,
	// each with a 1-10 MiB fetch, and made no progress at all while the same
	// search restricted to one partition finished in 1.6s.
	deepSearchSubWorkerCap        = 8
	deepSearchScanClientBudget    = 8
	deepSearchOffsetsPerSubWorker = 200_000

	// Literal-prefix prefilter threshold. If the regex has a literal prefix
	// shorter than this, the prefilter is skipped — single-byte prefixes
	// match too much to be selective and add overhead.
	deepSearchPrefilterMinLen = 2
)

// SearchScope is a bitmask describing where in a record to apply the regex.
type SearchScope uint8

const (
	// ScopeKey matches against the record key.
	ScopeKey SearchScope = 1 << iota
	// ScopeValue matches against the record value.
	ScopeValue
	// ScopeHeaders matches against each header's `key=value` serialization.
	ScopeHeaders
)

// SearchParams configures a deep search. Zero values mean "no constraint":
// an empty Partitions slice means all partitions; a zero Since/Until means
// no time bound on that side; a zero Cap means deepSearchMatchCap.
type SearchParams struct {
	Pattern    *regexp.Regexp
	Since      time.Time
	Until      time.Time
	Partitions []int32
	Scope      SearchScope // 0 means ScopeKey|ScopeValue
	Cap        int
}

func (p SearchParams) effectiveScope() SearchScope {
	if p.Scope == 0 {
		return ScopeKey | ScopeValue
	}
	return p.Scope
}

func (p SearchParams) effectiveCap() int {
	if p.Cap <= 0 {
		return deepSearchMatchCap
	}
	return p.Cap
}

// EffectiveCap exposes the resolved match cap so alternate search backends
// send the same explicit limit the live scan would enforce, instead of a zero
// that each backend defaults on its own.
func (p SearchParams) EffectiveCap() int { return p.effectiveCap() }

// SearchSource identifies where matches in a deep-search result were
// served from. The default zero value is SourceScan (live broker scan);
// wrappers like searchcache and the future index dispatcher set this to
// other values so the UI/CLI can render an appropriate label.
type SearchSource uint8

const (
	// SourceScan is a live broker scan — the default and authoritative path.
	SourceScan SearchSource = iota
	// SourceCache is a hit from the Tier 2 query-result cache.
	SourceCache
	// SourceIndex is a hit from the Tier 3 local index (reserved; not yet implemented).
	SourceIndex
	// SourceHybrid is a mix of cache/index and live scan (reserved).
	SourceHybrid
	// SourceSharedIndex is a hit from the shared k4a-index daemon over gRPC —
	// the same coverage-gated index as SourceIndex, but served by a remote,
	// always-warm daemon (docs/design/shared-index-service.md).
	SourceSharedIndex
)

// String returns the lowercase label used by progress headers.
func (s SearchSource) String() string {
	switch s {
	case SourceCache:
		return "cached"
	case SourceIndex:
		return "indexed"
	case SourceHybrid:
		return "hybrid"
	case SourceSharedIndex:
		return "indexed (shared)"
	default:
		return "scanned"
	}
}

// DeepSearchProgress is emitted periodically while a scan is running and
// once more on terminal state (Done=true).
type DeepSearchProgress struct {
	Elapsed        time.Duration // wall time since scan start
	Scanned        int64         // messages read across all partitions
	Total          int64         // sum of (lastOffset-firstOffset) across partitions
	Bytes          int64         // key+value bytes read across all partitions
	Matches        int
	Partitions     int
	DonePartitions int
	Source         SearchSource
	Capped         bool
	// Truncated reports that at least one chunk ended before covering its
	// offset range, so the result is missing records the search window
	// asked for. A scan that cannot reach the end of its range must never
	// look like a clean completion.
	Truncated bool
	Done      bool
}

// Incomplete reports whether the result is missing matches the search window
// asked for — either because the match cap bound or because a chunk short-read.
// Callers that persist or present a result as authoritative (the result cache,
// the CLI summary, the TUI header) must consult this, not Done alone.
func (p DeepSearchProgress) Incomplete() bool { return p.Capped || p.Truncated }

// DeepSearch scans a topic newest-first across the partitions selected by
// params, emitting records whose configured fields match params.Pattern.
// The scan terminates when every selected partition is exhausted, the match
// cap is reached, or ctx is canceled.
//
// All three returned channels are closed by the scanner when it exits.
func (c *Client) DeepSearch(
	ctx context.Context,
	topic string,
	params SearchParams,
) (<-chan ConsumedMessage, <-chan DeepSearchProgress, <-chan error) {
	matchCh := make(chan ConsumedMessage, consumerChannelBuffer)
	progCh := make(chan DeepSearchProgress, 1)
	errCh := make(chan error, 1)

	go c.runDeepSearch(ctx, topic, params, matchCh, progCh, errCh)

	return matchCh, progCh, errCh
}

func (c *Client) runDeepSearch(
	ctx context.Context,
	topic string,
	params SearchParams,
	matchCh chan<- ConsumedMessage,
	progCh chan<- DeepSearchProgress,
	errCh chan<- error,
) {
	defer close(matchCh)
	defer close(progCh)
	defer close(errCh)

	ranges, err := c.deepSearchRanges(ctx, topic, params)
	if err != nil {
		sendErr(errCh, err)
		return
	}
	if len(ranges) == 0 {
		emitProgress(progCh, DeepSearchProgress{Done: true})
		return
	}

	var totalScan int64
	for _, r := range ranges {
		totalScan += r.last - r.first
	}

	scanCtx, scanCancel := context.WithCancel(ctx)
	defer scanCancel()

	ds := &deepScan{
		topic:      topic,
		re:         params.Pattern,
		prefilter:  extractPrefilter(params.Pattern),
		scope:      params.effectiveScope(),
		until:      params.Until,
		matchCap:   params.effectiveCap(),
		matchCh:    matchCh,
		cancelAll:  scanCancel,
		partitions: len(ranges),
		total:      totalScan,
		start:      time.Now(),
	}

	progDone := make(chan struct{})
	go func() {
		defer close(progDone)
		ds.tickProgress(scanCtx, progCh)
	}()

	var wg sync.WaitGroup
	for _, r := range ranges {
		wg.Go(func() {
			defer ds.done.Add(1)
			c.scanPartitionReverse(scanCtx, ds, r)
		})
	}
	wg.Wait()
	scanCancel()
	<-progDone

	// Terminal progress must not be dropped — the cache layer keys cache
	// writes off lastProg.Done, and the periodic ticker may have left a
	// non-terminal snapshot in the size-1 progCh buffer. Block until the
	// consumer reads (or ctx ends), then let the deferred close fire.
	select {
	case progCh <- ds.snapshot(true):
	case <-ctx.Done():
	}
}

// deepScan is the state one deep search shares across every partition and
// chunk it scans: the match criteria, the output channel, and the counters
// progress is reported from.
type deepScan struct {
	start      time.Time
	until      time.Time
	re         *regexp.Regexp
	matchCh    chan<- ConsumedMessage
	cancelAll  context.CancelFunc
	topic      string
	prefilter  []byte
	scanned    atomic.Int64
	bytesRd    atomic.Int64
	matches    atomic.Int64
	done       atomic.Int64
	total      int64
	matchCap   int
	partitions int
	capped     atomic.Bool
	truncated  atomic.Bool
	scope      SearchScope
}

// snapshot is the scan's progress so far; terminal marks the final report.
func (ds *deepScan) snapshot(terminal bool) DeepSearchProgress {
	return DeepSearchProgress{
		Elapsed:        time.Since(ds.start),
		Scanned:        ds.scanned.Load(),
		Total:          ds.total,
		Bytes:          ds.bytesRd.Load(),
		Matches:        int(ds.matches.Load()),
		Partitions:     ds.partitions,
		DonePartitions: int(ds.done.Load()),
		Capped:         ds.capped.Load(),
		Truncated:      ds.truncated.Load(),
		Done:           terminal,
	}
}

// tickProgress emits a non-terminal snapshot deepSearchProgressHz times a
// second until ctx ends.
func (ds *deepScan) tickProgress(ctx context.Context, progCh chan<- DeepSearchProgress) {
	tick := time.NewTicker(time.Second / deepSearchProgressHz)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			emitProgress(progCh, ds.snapshot(false))
		}
	}
}

type partitionRange struct {
	id    int32
	first int64
	last  int64
}

func (c *Client) deepSearchRanges(
	ctx context.Context, topic string, params SearchParams,
) ([]partitionRange, error) {
	startsCtx, startsCancel := context.WithTimeout(ctx, fetchRecentTimeout)
	defer startsCancel()
	starts, err := c.admin.ListStartOffsets(startsCtx, topic)
	if err != nil {
		return nil, err
	}

	// If Since is set, replace the per-partition start with the offset of the
	// first record at or after that timestamp — anything older is outside the
	// search window and can be skipped wholesale.
	if sinceOffsets, ok := c.offsetsAfter(ctx, topic, params.Since); ok {
		starts = sinceOffsets
	}

	endsCtx, endsCancel := context.WithTimeout(ctx, fetchRecentTimeout)
	defer endsCancel()
	ends, err := c.admin.ListEndOffsets(endsCtx, topic)
	if err != nil {
		return nil, err
	}

	// If Until is set, pull the scan's upper bound back to the first record at
	// or after that timestamp. Without this the scan runs to the live high
	// watermark and discards the tail per-record, so a bounded historical
	// query costs the whole topic and consecutive windows re-read each
	// other's newer records.
	//
	// This only trims whole offsets that are certainly outside the window; the
	// per-record Until filter in scanRange still runs, because records are not
	// strictly time-ordered within a partition.
	untilOffsets, _ := c.offsetsAfter(ctx, topic, params.Until)

	allow := partitionAllowSet(params.Partitions)

	var ranges []partitionRange
	for partition, eo := range ends[topic] {
		if allow != nil {
			if _, ok := allow[partition]; !ok {
				continue
			}
		}
		if r, ok := scanRangeFor(topic, partition, eo, starts, untilOffsets); ok {
			ranges = append(ranges, r)
		}
	}
	return ranges, nil
}

// offsetsAfter lists, per partition, the first offset at or after t. ok is
// false when t is zero or the lookup failed; callers then keep their default
// bound.
func (c *Client) offsetsAfter(ctx context.Context, topic string, t time.Time) (kadm.ListedOffsets, bool) {
	if t.IsZero() {
		return nil, false
	}
	ctx, cancel := context.WithTimeout(ctx, fetchRecentTimeout)
	defer cancel()
	o, err := c.admin.ListOffsetsAfterMilli(ctx, t.UnixMilli(), topic)
	if err != nil {
		return nil, false
	}
	return o, true
}

// scanRangeFor is the offset range to scan in one partition: from its start
// (or first record since the window opened) to its high watermark, pulled
// back to untilOffsets when that bound is known. ok is false when there is
// nothing to scan.
func scanRangeFor(
	topic string,
	partition int32,
	eo kadm.ListedOffset,
	starts, untilOffsets kadm.ListedOffsets,
) (partitionRange, bool) {
	if eo.Err != nil {
		return partitionRange{}, false
	}
	so, ok := starts.Lookup(topic, partition)
	if !ok || so.Err != nil {
		return partitionRange{}, false
	}
	// ListOffsetsAfterMilli returns -1 for partitions with no record past
	// the timestamp; skip those (nothing to scan in window).
	if so.Offset < 0 {
		return partitionRange{}, false
	}
	last := eo.Offset
	if untilOffsets != nil {
		// A -1 here means no record exists at or after Until, so
		// everything through the high watermark is in window.
		if uo, uok := untilOffsets.Lookup(topic, partition); uok && uo.Err == nil && uo.Offset >= 0 {
			last = min(last, uo.Offset)
		}
	}
	if last <= so.Offset {
		return partitionRange{}, false
	}
	return partitionRange{id: partition, first: so.Offset, last: last}, true
}

// partitionAllowSet returns a lookup set for the configured partition filter,
// or nil meaning "all partitions allowed".
func partitionAllowSet(partitions []int32) map[int32]struct{} {
	if len(partitions) == 0 {
		return nil
	}
	out := make(map[int32]struct{}, len(partitions))
	for _, p := range partitions {
		out[p] = struct{}{}
	}
	return out
}

// chunkSpan is a [start, end) offset range to scan.
type chunkSpan struct{ start, end int64 }

// planReverseChunks returns the chunks to scan within [first, last), in
// newest-first order. Each chunk starts at deepSearchInitialChunk offsets
// and doubles up to deepSearchMaxChunk. Pure function; testable.
func planReverseChunks(first, last int64) []chunkSpan {
	if last <= first {
		return nil
	}
	var spans []chunkSpan
	chunk := deepSearchInitialChunk
	end := last
	for end > first {
		start := max(end-chunk, first)
		spans = append(spans, chunkSpan{start: start, end: end})
		end = start
		chunk = min(chunk*2, deepSearchMaxChunk)
	}
	return spans
}

// scanPartitionReverse walks a partition newest-chunk-first, with each chunk
// doubling in size up to deepSearchMaxChunk. Within a chunk, messages flow
// oldest→newest (Kafka consumers are forward-only), but the overall partition
// order is newest-chunk-first.
//
// For large partitions, the chunks are dispatched to a small pool of
// sub-workers (one kgo.Client, i.e. one fetch connection, each) so the
// broker's outbound bandwidth isn't bottlenecked on a single connection.
// Workers pull chunks newest-first from a shared queue, preserving the
// "recent matches surface first" property regardless of pool size.
//
// Each worker builds its client lazily, positioned at the first span it
// actually pulls off the queue. This is load-bearing, not a micro-optimization:
// kgo.SetOffsets only repositions partitions that have already
// been returned from a PollFetches — before the first poll the partition's
// offset load has not completed, assignSetMatching finds no cursor to set, and
// the call is a silent no-op. A client constructed at some other offset would
// therefore scan its first span from the wrong position, emitting that region
// twice and never reading the span it was asked for.
func (c *Client) scanPartitionReverse(ctx context.Context, ds *deepScan, r partitionRange) {
	spans := planReverseChunks(r.first, r.last)
	if len(spans) == 0 {
		return
	}

	workers := min(subWorkerCount(r.last-r.first, ds.partitions), len(spans))

	spanCh := make(chan chunkSpan, len(spans))
	for _, s := range spans {
		spanCh <- s
	}
	close(spanCh)

	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			c.scanSpans(ctx, ds, r.id, spanCh)
		})
	}
	wg.Wait()
}

// scanSpans is one sub-worker: it scans spans off spanCh until the queue is
// empty or the scan aborts, on a client it builds at its first span.
func (c *Client) scanSpans(ctx context.Context, ds *deepScan, partition int32, spanCh <-chan chunkSpan) {
	var cl *kgo.Client
	defer func() {
		if cl != nil {
			cl.Close()
		}
	}()

	for span := range spanCh {
		if ctx.Err() != nil {
			return
		}

		if cl == nil {
			// First span for this worker: position via the
			// constructor, the only positioning that is honored
			// before the partition has ever been polled.
			var err error
			if cl, err = c.newScanClient(ds.topic, partition, span.start); err != nil {
				ds.truncated.Store(true)
				return
			}
		} else {
			cl.SetOffsets(map[string]map[int32]kgo.EpochOffset{
				ds.topic: {partition: {Offset: span.start, Epoch: -1}},
			})
		}

		switch ds.scanRange(ctx, cl, span.start, span.end) {
		case scanComplete:
		case scanShort:
			ds.truncated.Store(true)
		case scanAbort:
			return
		}
	}
}

// newScanClient builds a fresh kgo.Client tuned for bulk scanning a single
// partition, positioned at initialOffset. Callers must pass the start of the
// first chunk this client will scan — the constructor offset is authoritative
// until the first PollFetches, after which SetOffsets takes over between
// chunks (see scanPartitionReverse).
func (c *Client) newScanClient(topic string, partition int32, initialOffset int64) (*kgo.Client, error) {
	opts, err := NewClientOpts(c.authCfg, c.brokers)
	if err != nil {
		return nil, err
	}
	opts = append(
		opts,
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {partition: kgo.NewOffset().At(initialOffset)},
		}),
		kgo.FetchMinBytes(deepSearchFetchMin),
		kgo.FetchMaxBytes(deepSearchFetchMax),
		kgo.FetchMaxWait(deepSearchFetchWait),
	)
	return kgo.NewClient(opts...)
}

// subWorkerCount picks a per-partition sub-worker count from observed range
// size and the number of partitions sharing the scan. Tiny partitions get 1
// (coordination overhead would exceed any gain); large partitions get up to
// deepSearchSubWorkerCap, but never more than their share of
// deepSearchScanClientBudget.
//
// Every partition always gets at least one sub-worker — starving a partition
// would silently drop its matches, which is far worse than being slow.
func subWorkerCount(rangeSize int64, partitions int) int {
	if rangeSize <= 0 {
		return 1
	}
	bySize := max(int(rangeSize/deepSearchOffsetsPerSubWorker), 1)
	share := deepSearchScanClientBudget
	if partitions > 1 {
		share = max(deepSearchScanClientBudget/partitions, 1)
	}
	return min(bySize, deepSearchSubWorkerCap, share)
}

// extractPrefilter returns a byte slice suitable for a bytes.Contains
// negative-check before running the full regex match. We use the regex's
// literal prefix: for any unanchored regex `foo.*bar`, no match is possible
// unless the value contains "foo", so bytes.Contains is a sound necessary
// condition. Returns nil for regexes with no useful literal prefix.
func extractPrefilter(re *regexp.Regexp) []byte {
	p, _ := re.LiteralPrefix()
	if len(p) < deepSearchPrefilterMinLen {
		return nil
	}
	return []byte(p)
}

// scanOutcome is how a single chunk scan ended.
type scanOutcome uint8

const (
	// scanComplete means the chunk's whole offset range was covered.
	scanComplete scanOutcome = iota
	// scanShort means the chunk ended before covering its range, so matches
	// in the uncovered part were never seen. Never silent — it propagates to
	// DeepSearchProgress.Truncated.
	scanShort
	// scanAbort means the whole scan is over: ctx done or the match cap hit.
	scanAbort
	// scanContinue is scanRecord's "keep reading"; a chunk scan never ends
	// with it.
	scanContinue
)

// scanRange reads messages in [start, end) from cl, emitting matches.
//
// The chunk is "drained" when we've either (a) read a record with
// r.Offset >= end, or (b) consumed `end - start` records (kafka offsets are
// dense for non-compacted topics — we know exactly how many to expect).
// Counting is what makes the *newest* chunk terminate cleanly: there is no
// record at or past `end == lastOffset`, so without the counter we'd block
// until the poll timeout fires. The timeout is still a backstop for errors,
// but the counter is the fast path.
//
// Records outside [start, end) are discarded rather than counted or emitted.
// A correctly positioned client never produces any, but that check is what
// keeps a positioning bug from silently corrupting results: a client reading
// from too far back re-walks records it already emitted and, if those counted
// toward `consumed`, would burn the chunk's budget on the wrong offsets. That
// is precisely how the chunked-scan bug produced duplicates and a short read at once.
func (ds *deepScan) scanRange(ctx context.Context, cl *kgo.Client, start, end int64) scanOutcome {
	expected := end - start
	var consumed int64

	for consumed < expected {
		if ctx.Err() != nil {
			return scanAbort
		}

		pollCtx, cancel := context.WithTimeout(ctx, deepSearchPollTimeout)
		fetches := cl.PollFetches(pollCtx)
		cancel()

		if ctx.Err() != nil {
			return scanAbort
		}
		if fetches.NumRecords() == 0 {
			// Poll timed out or fetched nothing while records we expected
			// are still unread. On a dense (non-compacted) topic that means
			// we could not read part of the range — report it rather than
			// passing a short read off as a clean finish.
			return scanShort
		}

		outcome := scanContinue
		fetches.EachRecord(func(r *kgo.Record) {
			if outcome == scanContinue {
				outcome = ds.scanRecord(ctx, r, start, end, &consumed)
			}
		})
		if outcome != scanContinue {
			return outcome
		}
	}
	return scanComplete
}

// scanRecord handles one polled record of the chunk [start, end), counting it
// in consumed when it is in range. It returns scanContinue to keep reading,
// scanComplete when the record is past the chunk, or scanAbort when the match
// cap is hit or ctx ends while emitting.
func (ds *deepScan) scanRecord(ctx context.Context, r *kgo.Record, start, end int64, consumed *int64) scanOutcome {
	if r.Offset >= end {
		return scanComplete
	}
	if r.Offset < start {
		// Outside this chunk — belongs to an older span. Do not
		// count it against `expected` and do not emit it.
		return scanContinue
	}
	*consumed++
	ds.scanned.Add(1)
	ds.bytesRd.Add(int64(len(r.Key) + len(r.Value)))

	// Time upper bound: skip records past Until without ending the
	// chunk (records aren't strictly time-ordered within a partition
	// the way offsets are, so we keep scanning).
	if !ds.until.IsZero() && r.Timestamp.After(ds.until) {
		return scanContinue
	}

	if !recordMatches(r, ds.re, ds.prefilter, ds.scope) {
		return scanContinue
	}

	n := ds.matches.Add(1)
	if int(n) > ds.matchCap {
		ds.capped.Store(true)
		ds.cancelAll()
		return scanAbort
	}

	select {
	case ds.matchCh <- recordToConsumed(r):
		return scanContinue
	case <-ctx.Done():
		return scanAbort
	}
}

// recordMatches reports whether r matches re in the configured scope.
// The literal-prefix prefilter is applied first as a cheap negative gate.
func recordMatches(r *kgo.Record, re *regexp.Regexp, prefilter []byte, scope SearchScope) bool {
	if prefilter != nil && !recordContains(r, prefilter, scope) {
		return false
	}
	if scope&ScopeKey != 0 && re.Match(r.Key) {
		return true
	}
	if scope&ScopeValue != 0 && re.Match(r.Value) {
		return true
	}
	if scope&ScopeHeaders != 0 {
		for _, h := range r.Headers {
			if re.MatchString(h.Key + "=" + string(h.Value)) {
				return true
			}
		}
	}
	return false
}

func recordContains(r *kgo.Record, needle []byte, scope SearchScope) bool {
	if scope&ScopeKey != 0 && bytes.Contains(r.Key, needle) {
		return true
	}
	if scope&ScopeValue != 0 && bytes.Contains(r.Value, needle) {
		return true
	}
	if scope&ScopeHeaders != 0 {
		for _, h := range r.Headers {
			if bytes.Contains([]byte(h.Key), needle) || bytes.Contains(h.Value, needle) {
				return true
			}
		}
	}
	return false
}

func emitProgress(ch chan<- DeepSearchProgress, p DeepSearchProgress) {
	select {
	case ch <- p:
	default:
		// Drop on backpressure — the next tick will carry fresher numbers.
	}
}
