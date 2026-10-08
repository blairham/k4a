// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Package index implements the Tier 3 local index per
// docs/design/local-index.md.
//
// An Indexer owns a Bleve index on disk for a single (cluster, topic)
// pair, plus a Bookkeeper tracking the per-partition offset ranges
// that have been indexed. Records are pushed in via Index() — typically
// from a live-tail fan-out or a catch-up backfill scan — and queried by
// a future dispatcher (Phase D) that decides index-vs-scan based on
// time-range coverage.
//
// State-management rules (state-management.md):
//   - The cluster is the source of truth. The index is purely an
//     optimization; any inconsistency between index and broker is
//     resolved by the broker.
//   - Each topic's index directory is independently deletable. On
//     corruption, the directory is renamed aside and rebuilt from a
//     fresh catch-up scan.
//   - Single-writer per topic via flock; a second k4a process reading
//     the same topic enters read-only mode.
package index

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/index/scorch"
	"github.com/blevesearch/bleve/v2/mapping"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/state"
)

const (
	// SchemaVersion is the on-disk schema version. Bumping it causes
	// existing indexes to be renamed aside and rebuilt from scratch.
	// 2: text fields use textAnalyzer; version 1 terms could be corrupted.
	SchemaVersion = 2

	// bookkeeperFile is the per-topic state file inside the index dir.
	bookkeeperFile = "state.json"

	// indexSubdir holds the Bleve index data under the topic dir.
	indexSubdir = "bleve"
)

// Doc is the shape of an indexed record. Keys/values/headers are stored
// as text for full-text search; offset/partition/timestamp are stored
// (and queryable) for filtering by time range or partition.
//
// We deliberately keep this shape simple in v1 — no schema-aware JSON
// field extraction. That's a v2 follow-up per the design doc's "Phase F
// schema-aware indexing" section.
type Doc struct {
	Timestamp time.Time `json:"timestamp"`
	Topic     string    `json:"topic"`
	Key       string    `json:"key,omitempty"`
	Value     string    `json:"value,omitempty"`
	Headers   string    `json:"headers,omitempty"`
	Offset    int64     `json:"offset"`
	Partition int32     `json:"partition"`
}

// Bleve field names. They are Doc's JSON names, which is how a Doc's fields
// reach the index, so each must match its tag above.
const (
	fieldTimestamp = "timestamp"
	fieldTopic     = "topic"
	fieldKey       = "key"
	fieldValue     = "value"
	fieldHeaders   = "headers"
	fieldOffset    = "offset"
	fieldPartition = "partition"
)

// DocID returns the Bleve document ID we use for a (partition, offset)
// pair. The ID is short, sortable, and gives us cheap exists-by-offset
// lookups for incremental indexing.
func DocID(partition int32, offset int64) string {
	return strconv.Itoa(int(partition)) + "/" + strconv.FormatInt(offset, 10)
}

// Bookkeeper is the per-topic JSON metadata sidecar. It mirrors the
// shape described in docs/design/local-index.md. Tracks indexed offset
// ranges per partition so the dispatcher can quickly decide whether a
// search's time range is covered.
type Bookkeeper struct {
	FirstIndexedAt           time.Time       `json:"first_indexed_at"`
	LastUpdatedAt            time.Time       `json:"last_updated_at"`
	NewestOffsetPerPartition map[int32]int64 `json:"newest_offset_per_partition"`
	OldestOffsetPerPartition map[int32]int64 `json:"oldest_offset_per_partition"`
	NewestTimePerPartition   map[int32]int64 `json:"newest_time_per_partition"`
	OldestTimePerPartition   map[int32]int64 `json:"oldest_time_per_partition"`
	Gaps                     []Gap           `json:"gaps,omitempty"`
	ByteSize                 int64           `json:"byte_size"`
	SchemaVersion            int             `json:"schema_version"`
}

// Gap describes a contiguous range of offsets within a partition that
// the indexer knows it skipped (because of overflow, an aborted
// catch-up, etc.). The dispatcher must not assume coverage inside a gap.
type Gap struct {
	Reason    string `json:"reason"`
	Partition int32  `json:"partition"`
	Start     int64  `json:"start"`
	End       int64  `json:"end"`
}

// Indexer owns one topic's index. Open()/Close() manage the lifecycle;
// Index() and IndexBatch() push records in.
type Indexer struct {
	idx  bleve.Index
	lock *state.Lock
	// snap is the last COMMITTED bookkeeper, published under mu and read
	// without it. It is what makes coverage answerable while the drain holds
	// mu for a whole IndexBatch — see BookkeeperSnapshot.
	snap    atomic.Pointer[Bookkeeper]
	dir     string
	cluster string
	topic   string
	book    Bookkeeper
	mu      sync.Mutex
	closed  bool
}

// Open creates or opens the topic's index. The directory layout is:
//
//	<stateRoot>/index/<cluster>/<topic>/
//	  state.json          // bookkeeper
//	  bleve/              // Bleve index files
//	  lock                // flock advisory
//
// On a schema-version mismatch the directory is renamed aside and
// rebuilt from scratch.
func Open(root *state.Root, cluster, topic string) (*Indexer, error) {
	if cluster == "" {
		return nil, fmt.Errorf("cluster ID is required for index isolation")
	}
	if topic == "" {
		return nil, fmt.Errorf("topic is required")
	}
	dir := topicDir(root, cluster, topic)

	lock, err := state.AcquireWriter(dir)
	if err != nil {
		return nil, fmt.Errorf("acquiring writer lock: %w", err)
	}

	ix := &Indexer{
		dir:     dir,
		cluster: cluster,
		topic:   topic,
		lock:    lock,
	}
	if err := ix.loadOrInit(); err != nil {
		_ = lock.Release() //nolint:errcheck // best-effort on failure path
		return nil, err
	}
	return ix, nil
}

// topicDir returns the absolute path for one topic's index directory.
func topicDir(root *state.Root, cluster, topic string) string {
	return filepath.Join(root.File(state.SubsystemIndex), cluster, topic)
}

func (ix *Indexer) loadOrInit() error {
	book, err := readBookkeeper(ix.dir)
	switch {
	case errors.Is(err, os.ErrNotExist):
		// Fresh install — no state.json yet. Defer until first commit.
		ix.book = Bookkeeper{
			SchemaVersion:            SchemaVersion,
			NewestOffsetPerPartition: make(map[int32]int64),
			OldestOffsetPerPartition: make(map[int32]int64),
			NewestTimePerPartition:   make(map[int32]int64),
			OldestTimePerPartition:   make(map[int32]int64),
			FirstIndexedAt:           time.Now().UTC(),
		}
	case err != nil:
		// Corrupt bookkeeper → wipe and start over.
		backupTopicDir(ix.dir, "bookkeeper")
		return ix.loadOrInit()
	case book.SchemaVersion != SchemaVersion:
		// Schema bump → wipe and start over.
		backupTopicDir(ix.dir, "schema")
		return ix.loadOrInit()
	default:
		ix.book = book
		// Empty maps from older payloads should still be non-nil so the
		// rest of the code can assume map-safe behavior.
		ensureMaps(&ix.book)
	}

	idx, err := openBleve(filepath.Join(ix.dir, indexSubdir))
	if err != nil {
		backupTopicDir(ix.dir, "bleve-open")
		return fmt.Errorf("opening bleve: %w", err)
	}
	ix.idx = idx
	ix.publishLocked()
	return nil
}

func ensureMaps(b *Bookkeeper) {
	if b.NewestOffsetPerPartition == nil {
		b.NewestOffsetPerPartition = make(map[int32]int64)
	}
	if b.OldestOffsetPerPartition == nil {
		b.OldestOffsetPerPartition = make(map[int32]int64)
	}
	if b.NewestTimePerPartition == nil {
		b.NewestTimePerPartition = make(map[int32]int64)
	}
	if b.OldestTimePerPartition == nil {
		b.OldestTimePerPartition = make(map[int32]int64)
	}
}

// Close flushes the index and releases the writer lock. Safe to call
// multiple times.
func (ix *Indexer) Close() error {
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if ix.closed {
		return nil
	}
	ix.closed = true

	var firstErr error
	if ix.idx != nil {
		if err := ix.idx.Close(); err != nil {
			firstErr = err
		}
		ix.idx = nil
	}
	if ix.lock != nil {
		if err := ix.lock.Release(); err != nil && firstErr == nil {
			firstErr = err
		}
		ix.lock = nil
	}
	return firstErr
}

// BookkeeperSnapshot returns the most recently published bookkeeper state
// WITHOUT taking ix.mu.
//
// The read side used to take the same mutex the drain holds for the whole of
// IndexBatch, so on a high-ingest topic coverage, CanServe and the stats
// heartbeat were starved by the writer: measured on a staging deployment at ~3,300
// docs/sec, coverage RPCs for that topic exceeded a 10s deadline while a quiet
// topic on the same daemon answered in ~150ms, and the topic's own 15s stats
// heartbeat managed 31 lines in 40 minutes. Worse, that presents
// exactly like the drain wedge, so a healthy-but-busy index looks broken.
//
// Reading a published copy also means coverage stays answerable when the drain
// is genuinely wedged — which is precisely when an operator most wants to ask.
//
// The published value is never NEWER than reality, and on the OLDEST edge never
// CLAIMS MORE than reality (see publishTrimmingLocked): under-claiming makes a
// client scan, which is safe, while over-claiming would silently serve an
// incomplete window.
func (ix *Indexer) BookkeeperSnapshot() Bookkeeper {
	if b := ix.snap.Load(); b != nil {
		return b.clone()
	}
	// Pre-publish (only between Open and the first publish).
	ix.mu.Lock()
	defer ix.mu.Unlock()
	return ix.book.clone()
}

// publishLocked republishes the bookkeeper for lock-free readers. Call it at
// the end of every mutation, while still holding ix.mu, so readers only ever
// observe committed states — never a half-applied batch or a partial trim.
func (ix *Indexer) publishLocked() {
	b := ix.book.clone()
	ix.snap.Store(&b)
}

// publishTrimmingLocked publishes a deliberately PESSIMISTIC view of the
// partitions a trim is about to delete from, collapsing their covered window to
// nothing so CanServe declines and the client scans.
//
// It exists because Trim deletes in chunks and only refreshes the oldest
// markers afterwards, via refreshOldestLocked. Under the old locked read that
// intermediate state was invisible; with lock-free reads it would be
// observable, and it is wrong in the DANGEROUS direction — the records are
// already gone from bleve while the bookkeeper still advertises them, so a
// query landing in that window would be served from the index and silently miss
// them. Under-claiming for the length of a chunk costs a scan; over-claiming
// costs correctness, which is the one thing coverage exists to guarantee.
func (ix *Indexer) publishTrimmingLocked(touched map[int32]struct{}) {
	b := ix.book.clone()
	for p := range touched {
		// Oldest := newest ⇒ an empty covered window for that partition.
		if newest, ok := b.NewestTimePerPartition[p]; ok {
			b.OldestTimePerPartition[p] = newest
		}
		if newest, ok := b.NewestOffsetPerPartition[p]; ok {
			b.OldestOffsetPerPartition[p] = newest
		}
	}
	ix.snap.Store(&b)
}

// MergeProgress is a point-in-time sample of scorch's merge counters. Compare
// two samples to tell a merge that is WORKING from a merger that is DEAD — the
// distinction the drain-stall watchdog turns on, and one no single sample can make.
type MergeProgress struct {
	// Started/Finished are TotFileMergeZapBeg/End. Started > Finished means a
	// merge began and has not ended — which is true both of a running merge and
	// of one whose goroutine panicked mid-merge and will never return.
	Started  uint64
	Finished uint64
	// BytesWritten is TotFileMergeWrittenBytes, incremented as a merge streams
	// out. It is the only counter that moves DURING a merge, so it is what
	// separates the two cases above.
	BytesWritten uint64
}

// InFlight reports that a merge has begun and not finished.
func (m MergeProgress) InFlight() bool { return m.Started > m.Finished }

// Advanced reports whether merge work actually happened between prev and m:
// either a merge completed, or bytes were written by one in progress.
func (m MergeProgress) Advanced(prev MergeProgress) bool {
	return m.Finished > prev.Finished || m.BytesWritten > prev.BytesWritten
}

// MergeProgress samples the underlying scorch index's merge counters.
//
// The drain-stall watchdog uses this to decide whether a stalled drain is waiting
// on real work. A single sample cannot tell: scorch's mergerLoop recovers a
// panic and RETURNS, leaving TotFileMergeZapBeg permanently ahead of ...ZapEnd
// with no goroutine behind it, so "a merge is in flight" stays true forever on
// an index that can never drain again. Only movement in BytesWritten/Finished
// proves a merger is alive — see OnAsyncError for the panic itself.
//
// Deliberately does NOT take ix.mu — the caller polls this precisely when the
// mutex holder is blocked. Bleve's StatsMap reads its own atomics and briefly
// RLocks scorch's root, neither of which the wedge holds.
func (ix *Indexer) MergeProgress() MergeProgress {
	inner, ok := ix.idx.StatsMap()["index"].(map[string]any)
	if !ok {
		return MergeProgress{}
	}
	u := func(key string) uint64 {
		v, isUint := inner[key].(uint64)
		if !isUint {
			return 0
		}
		return v
	}
	return MergeProgress{
		Started:      u("TotFileMergeZapBeg"),
		Finished:     u("TotFileMergeZapEnd"),
		BytesWritten: u("TotFileMergeWrittenBytes"),
	}
}

func (b Bookkeeper) clone() Bookkeeper {
	out := b
	out.NewestOffsetPerPartition = mapClone(b.NewestOffsetPerPartition)
	out.OldestOffsetPerPartition = mapClone(b.OldestOffsetPerPartition)
	out.NewestTimePerPartition = mapClone(b.NewestTimePerPartition)
	out.OldestTimePerPartition = mapClone(b.OldestTimePerPartition)
	if len(b.Gaps) > 0 {
		out.Gaps = append([]Gap(nil), b.Gaps...)
	}
	return out
}

func mapClone(m map[int32]int64) map[int32]int64 {
	if m == nil {
		return nil
	}
	out := make(map[int32]int64, len(m))
	maps.Copy(out, m)
	return out
}

// Index writes a single record. The doc ID is derived from
// (partition, offset). Re-indexing the same offset overwrites the
// existing doc (Bleve upsert semantics) — useful for catch-up scans
// that overlap the live tail.
func (ix *Indexer) Index(ctx context.Context, msg kafka.ConsumedMessage) error {
	return ix.IndexBatch(ctx, []kafka.ConsumedMessage{msg})
}

// IndexBatch writes a batch atomically. Bookkeeper updates happen in
// memory; persistence to state.json is deferred to FlushBookkeeper().
func (ix *Indexer) IndexBatch(ctx context.Context, msgs []kafka.ConsumedMessage) error {
	if len(msgs) == 0 {
		return nil
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if ix.closed {
		return fmt.Errorf("indexer closed")
	}
	if err := ctx.Err(); err != nil {
		return err
	}

	batch := ix.idx.NewBatch()
	var addedBytes int64
	seen := make(map[string]struct{}, len(msgs))
	for _, m := range msgs {
		doc := Doc{
			Topic:     m.Topic,
			Key:       m.Key,
			Value:     m.Value,
			Headers:   serializeHeaders(m.Headers),
			Timestamp: m.Time,
			Partition: int32(m.Partition), //nolint:gosec // partition IDs fit in int32
			Offset:    m.Offset,
		}
		id := DocID(doc.Partition, doc.Offset)
		if err := batch.Index(id, doc); err != nil {
			return fmt.Errorf("queueing doc %d/%d: %w", doc.Partition, doc.Offset, err)
		}
		if ix.countsTowardByteSizeLocked(id, doc.Partition, doc.Offset, seen) {
			addedBytes += payloadBytes(doc.Key, doc.Value, doc.Headers)
		}
	}
	if err := ix.idx.Batch(batch); err != nil {
		return fmt.Errorf("committing batch: %w", err)
	}

	for _, m := range msgs {
		ix.updateBookkeeper(m)
	}
	// ByteSize is the running payload-size estimate the byte-cap trimmer caps
	// against (Trim). Upserts (catch-up overlap, redelivered tail records) are
	// counted once — see countsTowardByteSizeLocked.
	ix.book.ByteSize += addedBytes
	ix.book.LastUpdatedAt = time.Now().UTC()
	ix.publishLocked()
	return nil
}

// countsTowardByteSizeLocked reports whether a record adds its payload to the
// running ByteSize estimate. A record whose doc ID is already present in the
// index — or earlier in this same batch — is an upsert: bleve stores one copy,
// so counting it again inflates the estimate and makes the byte-cap trimmer
// over-trim the coverage window (the drain wedge's secondary defect). The existence
// probe only runs for suspects (offset at or below the partition's newest
// marker); the monotonic live-tail hot path never pays it. Caller holds ix.mu.
func (ix *Indexer) countsTowardByteSizeLocked(id string, part int32, offset int64, seen map[string]struct{}) bool {
	if _, dup := seen[id]; dup {
		return false
	}
	seen[id] = struct{}{}
	cur, ok := ix.book.NewestOffsetPerPartition[part]
	if !ok || offset > cur {
		return true
	}
	// At or below the newest marker → possibly already indexed (and counted).
	// Count it only if it is genuinely absent (e.g. previously trimmed or
	// dropped on overflow).
	d, err := ix.idx.Document(id)
	return err != nil || d == nil
}

// payloadBytes estimates the stored payload size of one indexed record — the
// figure Bookkeeper.ByteSize tracks and Trim caps against. It counts the stored
// text (key+value+headers). Bleve's on-disk footprint is a roughly-constant
// multiple on top (see buildMapping), so a logical-byte cap bounds disk
// predictably without chasing scorch's deferred segment reclamation.
func payloadBytes(key, value, headers string) int64 {
	return int64(len(key) + len(value) + len(headers))
}

func (ix *Indexer) updateBookkeeper(m kafka.ConsumedMessage) {
	part := int32(m.Partition) //nolint:gosec // partition IDs fit in int32

	if cur, ok := ix.book.NewestOffsetPerPartition[part]; !ok || m.Offset > cur {
		ix.book.NewestOffsetPerPartition[part] = m.Offset
	}
	if cur, ok := ix.book.OldestOffsetPerPartition[part]; !ok || m.Offset < cur {
		ix.book.OldestOffsetPerPartition[part] = m.Offset
	}

	ts := m.Time.UnixMilli()
	if ts != 0 {
		if cur, ok := ix.book.NewestTimePerPartition[part]; !ok || ts > cur {
			ix.book.NewestTimePerPartition[part] = ts
		}
		if cur, ok := ix.book.OldestTimePerPartition[part]; !ok || ts < cur {
			ix.book.OldestTimePerPartition[part] = ts
		}
	}
}

// FlushBookkeeper writes the in-memory bookkeeper to state.json
// atomically. Call this periodically (or on graceful shutdown) so a
// crash mid-session loses at most the bookkeeper updates since the
// last flush — the Bleve index itself is durable across crashes.
func (ix *Indexer) FlushBookkeeper() error {
	ix.mu.Lock()
	snap := ix.book.clone()
	dir := ix.dir
	ix.mu.Unlock()

	if err := writeBookkeeperAtomic(dir, &snap); err != nil {
		return fmt.Errorf("flushing bookkeeper: %w", err)
	}
	return nil
}

// serializeHeaders turns []MessageHeader into the "k=v\nk=v" text form
// stored in the index. Stable ordering keeps queries deterministic.
func serializeHeaders(headers []kafka.MessageHeader) string {
	if len(headers) == 0 {
		return ""
	}
	out := make([]byte, 0, 64)
	for i, h := range headers {
		if i > 0 {
			out = append(out, '\n')
		}
		out = append(out, h.Key...)
		out = append(out, '=')
		out = append(out, h.Value...)
	}
	return string(out)
}

// openBleve opens or creates a Bleve index at dir using the schema in
// buildMapping().
// asyncErrorCallbackName names the scorch async-error callback k4a registers in
// init(). Scorch resolves it from the index config by name (there is no way to
// pass a closure), so the name must match on both sides.
const asyncErrorCallbackName = "k4a-index-async-error"

// OnAsyncError is called, when non-nil, for every scorch background failure —
// most importantly a PANIC in the merger, persister, or introducer goroutine.
// The daemon wires it to a log line; the TUI leaves it nil.
//
// This exists because those goroutines die silently by default.
// scorch's mergerLoop recovers a panic, calls fireAsyncError, and RETURNS — so
// the Scorch instance keeps accepting batches with no merger, forever. Segments
// then pile up until the persister parks in pausePersisterForMergerCatchUp
// waiting for a merger that no longer exists, Batch blocks in prepareSegment
// holding ix.mu, and every reader hangs behind it. fireAsyncError is a no-op
// unless a callback is registered, so before this the panic that started it all
// left no trace at all.
var OnAsyncError func(err error, path string)

func init() {
	scorch.RegistryAsyncErrorCallbacks[asyncErrorCallbackName] = func(err error, path string) {
		if OnAsyncError != nil {
			OnAsyncError(err, path)
		}
	}
}

// bleveConfig is the runtime config every k4a index is opened with. It carries
// only the async-error callback name — everything else stays on scorch defaults.
func bleveConfig() map[string]interface{} {
	return map[string]interface{}{"asyncErrorCallbackName": asyncErrorCallbackName}
}

func openBleve(dir string) (bleve.Index, error) {
	idx, err := bleve.OpenUsing(dir, bleveConfig())
	if err == nil {
		return idx, nil
	}
	if !errors.Is(err, bleve.ErrorIndexPathDoesNotExist) {
		return nil, err
	}
	if err := os.MkdirAll(filepath.Dir(dir), 0o700); err != nil {
		return nil, err
	}
	return bleve.NewUsing(dir, buildMapping(), bleve.Config.DefaultIndexType, scorch.Name, bleveConfig())
}

// buildMapping returns the Bleve document mapping. We tokenize text
// fields with textAnalyzer, the standard analyzer with a lowercase filter
// that does not corrupt terms (good for free-text JSON values).
// We also *store* text fields: when the dispatcher serves a search from
// the index, materializing matches without a second broker round-trip
// per record is the difference between sub-second and "still cheaper
// than scan but not magical." Index size grows roughly 1.5x raw data.
func buildMapping() mapping.IndexMapping {
	textField := bleve.NewTextFieldMapping()
	textField.Analyzer = textAnalyzer
	textField.Store = true
	textField.IncludeInAll = false

	numField := bleve.NewNumericFieldMapping()
	numField.Store = true
	numField.Index = true

	dateField := bleve.NewDateTimeFieldMapping()
	dateField.Store = true
	dateField.Index = true

	keywordField := bleve.NewKeywordFieldMapping()
	keywordField.Store = true
	keywordField.IncludeInAll = false

	doc := bleve.NewDocumentMapping()
	doc.AddFieldMappingsAt(fieldTopic, keywordField)
	doc.AddFieldMappingsAt(fieldKey, textField)
	doc.AddFieldMappingsAt(fieldValue, textField)
	doc.AddFieldMappingsAt(fieldHeaders, textField)
	doc.AddFieldMappingsAt(fieldTimestamp, dateField)
	doc.AddFieldMappingsAt(fieldPartition, numField)
	doc.AddFieldMappingsAt(fieldOffset, numField)

	im := bleve.NewIndexMapping()
	// Only an unregistered tokenizer or filter name can fail this.
	if err := im.AddCustomAnalyzer(textAnalyzer, textAnalyzerConfig()); err != nil {
		panic(fmt.Sprintf("index: defining %s: %v", textAnalyzer, err))
	}
	im.DefaultMapping = doc
	im.DefaultAnalyzer = textAnalyzer
	return im
}

// readBookkeeper reads state.json from dir. Returns os.ErrNotExist if
// the file isn't there yet (fresh index) or a parse error otherwise.
func readBookkeeper(dir string) (Bookkeeper, error) {
	path := filepath.Join(dir, bookkeeperFile)
	raw, err := os.ReadFile(path) //nolint:gosec // path is under our state dir
	if err != nil {
		return Bookkeeper{}, err
	}
	var b Bookkeeper
	if err := json.Unmarshal(raw, &b); err != nil {
		return Bookkeeper{}, fmt.Errorf("parse bookkeeper: %w", err)
	}
	return b, nil
}

// writeBookkeeperAtomic writes state.json via temp-file + rename so
// readers never see a partially-written document.
func writeBookkeeperAtomic(dir string, b *Bookkeeper) error {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	body, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, bookkeeperFile+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) //nolint:errcheck // cleanup after Rename
	if _, err := tmp.Write(body); err != nil {
		tmp.Close() //nolint:errcheck // already failing
		return err
	}
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close() //nolint:errcheck // already failing
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, filepath.Join(dir, bookkeeperFile))
}

// backupTopicDir renames a misbehaving topic dir aside so the user can
// inspect it post-mortem. The replacement dir is created fresh by the
// caller's next loadOrInit pass. Failures are swallowed — recovery
// doesn't depend on the backup succeeding.
func backupTopicDir(dir, reason string) {
	stamp := time.Now().UTC().Format("20060102T150405")
	_ = os.Rename(dir, dir+".bak."+reason+"."+stamp) //nolint:errcheck // best-effort
}

// Dir returns the on-disk directory backing this topic's index
// (<stateRoot>/index/<cluster>/<topic>/). The shared daemon's follow-set
// manager needs it to delete the index after LRU eviction — index dirs
// are independently deletable (state-management.md), so removing an evicted
// topic's dir reclaims its disk without touching any other topic.
func (ix *Indexer) Dir() string { return ix.dir }

// DocCount returns the number of indexed documents. Used by tests and
// by the dispatcher when deciding whether the index has enough data to
// be worth querying.
func (ix *Indexer) DocCount() (uint64, error) {
	ix.mu.Lock()
	idx := ix.idx
	ix.mu.Unlock()
	if idx == nil {
		return 0, fmt.Errorf("indexer closed")
	}
	return idx.DocCount()
}

const (
	// trimChunkSize is how many oldest docs the trimmer deletes per Bleve batch
	// while working the index back under its byte cap.
	trimChunkSize = 512

	// trimLowWaterPct is the fraction of the cap the trimmer drives down to once
	// triggered, so it doesn't re-trim on every flush (hysteresis).
	trimLowWaterPct = 90
)

// trimHit is one deletion candidate: its DocID, the partition it lives in (to
// know which oldest-marker to advance afterward), and its stored payload size
// (to decrement the running ByteSize estimate as we delete).
type trimHit struct {
	id        string
	bytes     int64
	partition int32
}

// Trim enforces a per-topic byte cap by deleting the OLDEST indexed records
// until the running payload-byte estimate (Bookkeeper.ByteSize) drops back under
// capBytes, then advances each affected partition's oldest markers so coverage
// stays honest — CanServe and coveredWindow gate on those markers, so leaving a
// stale-old marker after a delete would wrongly claim coverage.
//
// capBytes <= 0 disables trimming (the historical unbounded behavior the local
// index and catch-up path rely on); only the shared daemon sets a cap. It is
// meant to run inline on the async flush goroutine, right after a batch commit
// and before FlushBookkeeper, so the advanced markers persist in the same cycle.
//
// It drives the LOGICAL byte estimate (decremented by each deleted record's
// stored size), never the on-disk figure: scorch defers segment removal, so a
// loop watching on-disk bytes would over-delete while waiting for a merge. The
// logical estimate converges in a single pass.
func (ix *Indexer) Trim(ctx context.Context, capBytes int64) error {
	if capBytes <= 0 {
		return nil
	}
	ix.mu.Lock()
	defer ix.mu.Unlock()
	if ix.closed || ix.idx == nil {
		return nil
	}
	if ix.book.ByteSize <= capBytes {
		return nil // fast path: under cap, no query
	}

	lowWater := capBytes * trimLowWaterPct / 100
	touched := make(map[int32]struct{})
	for ix.book.ByteSize > lowWater {
		if err := ctx.Err(); err != nil {
			return err
		}
		hits, err := ix.oldestHitsLocked(ctx, trimChunkSize)
		if err != nil {
			return err
		}
		if len(hits) == 0 {
			// Index drained faster than the estimate tracked (e.g. a stale
			// ByteSize) — reconcile the estimate to reality and stop.
			ix.book.ByteSize = 0
			break
		}
		// Publish BEFORE the delete lands: from here until refreshOldestLocked
		// runs, bleve has fewer records than the bookkeeper advertises, and a
		// lock-free reader must not be told we still cover them.
		for _, h := range hits {
			touched[h.partition] = struct{}{}
		}
		ix.publishTrimmingLocked(touched)

		if err := ix.deleteHitsLocked(hits, lowWater); err != nil {
			return err
		}
	}

	for p := range touched {
		if err := ix.refreshOldestLocked(ctx, p); err != nil {
			return err
		}
	}
	ix.publishLocked()
	return nil
}

// deleteHitsLocked deletes hits oldest-first in one batch and lowers the
// logical byte estimate by what they freed. Caller holds ix.mu.
func (ix *Indexer) deleteHitsLocked(hits []trimHit, lowWater int64) error {
	batch := ix.idx.NewBatch()
	var freed int64
	for _, h := range hits {
		batch.Delete(h.id)
		freed += h.bytes
		// Stop mid-chunk the moment we've freed enough to reach the low-water
		// mark, so a large chunk against a small cap doesn't over-delete.
		if ix.book.ByteSize-freed <= lowWater {
			break
		}
	}
	if err := ix.idx.Batch(batch); err != nil {
		return fmt.Errorf("committing trim batch: %w", err)
	}
	ix.book.ByteSize -= freed
	if ix.book.ByteSize < 0 {
		ix.book.ByteSize = 0
	}
	return nil
}

// oldestHitsLocked returns up to n indexed records sorted oldest-first, with
// their DocIDs and stored payload sizes, for the trimmer to delete. Caller holds
// ix.mu; it talks to ix.idx directly, never the locking Query wrapper (which
// would deadlock).
func (ix *Indexer) oldestHitsLocked(ctx context.Context, n int) ([]trimHit, error) {
	req := bleve.NewSearchRequestOptions(bleve.NewMatchAllQuery(), n, 0, false)
	req.SortBy([]string{fieldTimestamp}) // ascending → oldest first
	req.Fields = []string{fieldKey, fieldValue, fieldHeaders, fieldPartition}
	res, err := ix.idx.SearchInContext(ctx, req)
	if err != nil {
		return nil, fmt.Errorf("trim search: %w", err)
	}
	hits := make([]trimHit, 0, len(res.Hits))
	for _, h := range res.Hits {
		th := trimHit{id: h.ID}
		if v, ok := h.Fields[fieldPartition].(float64); ok {
			th.partition = int32(v)
		}
		var key, val, hdr string
		if v, ok := h.Fields[fieldKey].(string); ok {
			key = v
		}
		if v, ok := h.Fields[fieldValue].(string); ok {
			val = v
		}
		if v, ok := h.Fields[fieldHeaders].(string); ok {
			hdr = v
		}
		th.bytes = payloadBytes(key, val, hdr)
		hits = append(hits, th)
	}
	return hits, nil
}

// refreshOldestLocked recomputes partition p's oldest indexed offset/time after
// a trim by reading the single oldest surviving record. If the partition has no
// surviving records it is dropped from the coverage maps entirely — it can no
// longer be served. Caller holds ix.mu.
func (ix *Indexer) refreshOldestLocked(ctx context.Context, p int32) error {
	lo, hi := float64(p), float64(p)
	incl := true
	// Inclusive on BOTH ends: NewNumericRangeQuery's max is exclusive, so a
	// lo==hi range would match nothing and drop the partition's markers.
	pq := bleve.NewNumericRangeInclusiveQuery(&lo, &hi, &incl, &incl)
	pq.SetField(fieldPartition)
	req := bleve.NewSearchRequestOptions(pq, 1, 0, false)
	req.SortBy([]string{fieldOffset}) // ascending → oldest surviving offset
	req.Fields = []string{fieldOffset, fieldTimestamp}
	res, err := ix.idx.SearchInContext(ctx, req)
	if err != nil {
		return fmt.Errorf("trim refresh partition %d: %w", p, err)
	}
	if len(res.Hits) == 0 {
		delete(ix.book.OldestOffsetPerPartition, p)
		delete(ix.book.OldestTimePerPartition, p)
		delete(ix.book.NewestOffsetPerPartition, p)
		delete(ix.book.NewestTimePerPartition, p)
		return nil
	}
	h := res.Hits[0]
	if v, ok := h.Fields[fieldOffset].(float64); ok {
		ix.book.OldestOffsetPerPartition[p] = int64(v)
	}
	if v, ok := h.Fields[fieldTimestamp].(string); ok {
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			ix.book.OldestTimePerPartition[p] = t.UnixMilli()
		}
	}
	return nil
}
