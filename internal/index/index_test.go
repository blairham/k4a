// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blevesearch/bleve/v2/index/scorch"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/state"
)

const testCluster = "cluster-A"

func newTestIndexer(t *testing.T) *Indexer {
	t.Helper()
	root := state.NewRoot(t.TempDir())
	ix, err := Open(root, testCluster, "orders")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = ix.Close() })
	return ix
}

func sampleMsg(partition int32, offset int64, key, value string, ts time.Time) kafka.ConsumedMessage {
	return kafka.ConsumedMessage{
		Topic:     "orders",
		Key:       key,
		Value:     value,
		Partition: int(partition),
		Offset:    offset,
		Time:      ts,
	}
}

func TestOpen_CreatesDirAndAcquiresLock(t *testing.T) {
	t.Parallel()

	root := state.NewRoot(t.TempDir())
	ix, err := Open(root, testCluster, "orders")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { _ = ix.Close() })

	dir := topicDir(root, testCluster, "orders")
	if _, err := os.Stat(filepath.Join(dir, "lock")); err != nil {
		t.Errorf("lock file not created: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, indexSubdir)); err != nil {
		t.Errorf("bleve dir not created: %v", err)
	}
}

func TestOpen_RejectsBlankIdentifiers(t *testing.T) {
	t.Parallel()

	root := state.NewRoot(t.TempDir())
	if _, err := Open(root, "", "t"); err == nil {
		t.Error("expected error for empty cluster")
	}
	if _, err := Open(root, "c", ""); err == nil {
		t.Error("expected error for empty topic")
	}
}

func TestOpen_SecondInstanceIsBlocked(t *testing.T) {
	t.Parallel()

	root := state.NewRoot(t.TempDir())
	first, err := Open(root, testCluster, "orders")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Close() })

	second, err := Open(root, testCluster, "orders")
	if !errors.Is(err, state.ErrLocked) {
		t.Fatalf("second Open err = %v, want ErrLocked", err)
	}
	if second != nil {
		t.Errorf("second Open should have returned nil indexer")
	}
}

func TestIndex_SingleRecord_UpdatesBookkeeper(t *testing.T) {
	t.Parallel()

	ix := newTestIndexer(t)
	now := time.Date(2026, 5, 14, 10, 0, 0, 0, time.UTC)
	if err := ix.Index(context.Background(), sampleMsg(0, 42, "key", `{"id":"foo"}`, now)); err != nil {
		t.Fatalf("Index: %v", err)
	}

	n, err := ix.DocCount()
	if err != nil {
		t.Fatal(err)
	}
	if n != 1 {
		t.Errorf("DocCount = %d, want 1", n)
	}

	book := ix.BookkeeperSnapshot()
	if book.NewestOffsetPerPartition[0] != 42 || book.OldestOffsetPerPartition[0] != 42 {
		t.Errorf("offset bookkeeping = %+v, want both 42 for partition 0", book)
	}
	if book.NewestTimePerPartition[0] != now.UnixMilli() {
		t.Errorf("time bookkeeping = %d, want %d", book.NewestTimePerPartition[0], now.UnixMilli())
	}
}

func TestIndexBatch_TracksOffsetRanges(t *testing.T) {
	t.Parallel()

	ix := newTestIndexer(t)
	now := time.Now().UTC()
	msgs := []kafka.ConsumedMessage{
		sampleMsg(0, 100, "a", "alpha", now.Add(-3*time.Minute)),
		sampleMsg(0, 105, "b", "beta", now.Add(-2*time.Minute)),
		sampleMsg(1, 200, "c", "gamma", now.Add(-1*time.Minute)),
	}
	if err := ix.IndexBatch(context.Background(), msgs); err != nil {
		t.Fatalf("IndexBatch: %v", err)
	}

	n, err := ix.DocCount()
	if err != nil {
		t.Fatal(err)
	}
	if n != 3 {
		t.Errorf("DocCount = %d, want 3", n)
	}

	book := ix.BookkeeperSnapshot()
	if book.OldestOffsetPerPartition[0] != 100 || book.NewestOffsetPerPartition[0] != 105 {
		t.Errorf("partition 0 range = [%d, %d], want [100, 105]",
			book.OldestOffsetPerPartition[0], book.NewestOffsetPerPartition[0])
	}
	if book.OldestOffsetPerPartition[1] != 200 || book.NewestOffsetPerPartition[1] != 200 {
		t.Errorf("partition 1 range = [%d, %d], want [200, 200]",
			book.OldestOffsetPerPartition[1], book.NewestOffsetPerPartition[1])
	}
}

func TestFlushBookkeeper_PersistsAcrossReopen(t *testing.T) {
	t.Parallel()

	root := state.NewRoot(t.TempDir())
	first, openErr := Open(root, testCluster, "orders")
	if openErr != nil {
		t.Fatal(openErr)
	}
	now := time.Now().UTC()
	if ierr := first.Index(context.Background(), sampleMsg(0, 7, "k", "v", now)); ierr != nil {
		t.Fatal(ierr)
	}
	if ferr := first.FlushBookkeeper(); ferr != nil {
		t.Fatalf("FlushBookkeeper: %v", ferr)
	}
	if cerr := first.Close(); cerr != nil {
		t.Fatal(cerr)
	}

	// Re-open after a clean close.
	second, reopenErr := Open(root, testCluster, "orders")
	if reopenErr != nil {
		t.Fatal(reopenErr)
	}
	t.Cleanup(func() { _ = second.Close() })

	book := second.BookkeeperSnapshot()
	if book.NewestOffsetPerPartition[0] != 7 {
		t.Errorf("reopened newest offset = %d, want 7", book.NewestOffsetPerPartition[0])
	}
	n, _ := second.DocCount()
	if n != 1 {
		t.Errorf("reopened DocCount = %d, want 1", n)
	}
}

func TestOpen_SchemaMismatch_RebuildsFromScratch(t *testing.T) {
	t.Parallel()

	root := state.NewRoot(t.TempDir())
	first, openErr := Open(root, testCluster, "orders")
	if openErr != nil {
		t.Fatal(openErr)
	}
	if ierr := first.Index(context.Background(), sampleMsg(0, 1, "k", "v", time.Now())); ierr != nil {
		t.Fatal(ierr)
	}
	if ferr := first.FlushBookkeeper(); ferr != nil {
		t.Fatal(ferr)
	}

	// Tamper with the bookkeeper to fake a newer-on-disk schema_version.
	bookPath := filepath.Join(topicDir(root, testCluster, "orders"), bookkeeperFile)
	if cerr := first.Close(); cerr != nil {
		t.Fatal(cerr)
	}
	bad := `{"schema_version": 9999, "newest_offset_per_partition": {"0": 1}}`
	if werr := os.WriteFile(bookPath, []byte(bad), 0o600); werr != nil {
		t.Fatal(werr)
	}

	second, err := Open(root, testCluster, "orders")
	if err != nil {
		t.Fatalf("Open after schema mismatch: %v", err)
	}
	t.Cleanup(func() { _ = second.Close() })

	book := second.BookkeeperSnapshot()
	if book.SchemaVersion != SchemaVersion {
		t.Errorf("schema_version after rebuild = %d, want %d", book.SchemaVersion, SchemaVersion)
	}
	if len(book.NewestOffsetPerPartition) != 0 {
		t.Errorf("expected empty bookkeeper after rebuild, got %+v", book.NewestOffsetPerPartition)
	}
}

func TestIndex_AfterClose_ReturnsError(t *testing.T) {
	t.Parallel()

	ix := newTestIndexer(t)
	_ = ix.Close()
	err := ix.Index(context.Background(), sampleMsg(0, 1, "k", "v", time.Now()))
	if err == nil {
		t.Error("Index after Close should fail")
	}
}

func TestSerializeHeaders_DeterministicOrder(t *testing.T) {
	t.Parallel()

	headers := []kafka.MessageHeader{
		{Key: "trace-id", Value: "abc"},
		{Key: "source", Value: "api"},
	}
	got := serializeHeaders(headers)
	want := "trace-id=abc\nsource=api"
	if got != want {
		t.Errorf("serializeHeaders = %q, want %q", got, want)
	}
}

func TestDocID_ShapeIsPredictable(t *testing.T) {
	t.Parallel()
	if got := DocID(3, 42); got != "3/42" {
		t.Errorf("DocID = %q, want 3/42", got)
	}
}

func TestBookkeeperSnapshot_IsDeepCopy(t *testing.T) {
	t.Parallel()

	ix := newTestIndexer(t)
	_ = ix.Index(context.Background(), sampleMsg(0, 1, "k", "v", time.Now()))

	snap := ix.BookkeeperSnapshot()
	snap.NewestOffsetPerPartition[0] = 9999

	current := ix.BookkeeperSnapshot()
	if current.NewestOffsetPerPartition[0] == 9999 {
		t.Error("internal bookkeeper mutated by snapshot caller")
	}
}

func TestIndexBatch_ByteSizeCountsUpsertsOnce(t *testing.T) {
	t.Parallel()

	ix := newTestIndexer(t)
	ctx := context.Background()
	base := time.Now().UTC().Truncate(time.Millisecond)

	msgs := make([]kafka.ConsumedMessage, 0, 10)
	for i := range 10 {
		msgs = append(msgs, sampleMsg(0, int64(i), "k", "v-payload", base.Add(time.Duration(i)*time.Second)))
	}
	if err := ix.IndexBatch(ctx, msgs); err != nil {
		t.Fatalf("IndexBatch: %v", err)
	}
	first := ix.BookkeeperSnapshot().ByteSize
	if first == 0 {
		t.Fatal("expected a non-zero ByteSize after the first batch")
	}

	// Redelivered records (catch-up overlap, consumer re-fetch) are upserts —
	// bleve stores one copy, so the estimate must not grow.
	if err := ix.IndexBatch(ctx, msgs); err != nil {
		t.Fatalf("IndexBatch (re-index): %v", err)
	}
	if got := ix.BookkeeperSnapshot().ByteSize; got != first {
		t.Errorf("ByteSize after re-index = %d, want %d (upserts must count once)", got, first)
	}

	// A duplicate within a single batch also stores one copy — count it once.
	dup := sampleMsg(0, 10, "k", "v-payload", base.Add(10*time.Second))
	if err := ix.IndexBatch(ctx, []kafka.ConsumedMessage{dup, dup}); err != nil {
		t.Fatalf("IndexBatch (in-batch dup): %v", err)
	}
	want := first + payloadBytes(dup.Key, dup.Value, "")
	if got := ix.BookkeeperSnapshot().ByteSize; got != want {
		t.Errorf("ByteSize after in-batch duplicate = %d, want %d", got, want)
	}
}

func TestMergeProgress_IdleIndex(t *testing.T) {
	t.Parallel()

	ix := newTestIndexer(t)
	// An idle index has no zap merge running; after a small batch the merge
	// counters must still balance out quickly. Either way the drain-stall watchdog
	// must see "no merge in flight" — a false positive here would defer every
	// genuine stall by the whole merge grace.
	if ix.MergeProgress().InFlight() {
		t.Fatal("fresh index reports a merge in flight")
	}
	msgs := []kafka.ConsumedMessage{
		sampleMsg(0, 1, "k1", `{"a":1}`, time.Now()),
		sampleMsg(0, 2, "k2", `{"a":2}`, time.Now()),
	}
	if err := ix.IndexBatch(context.Background(), msgs); err != nil {
		t.Fatalf("IndexBatch: %v", err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for ix.MergeProgress().InFlight() {
		if time.Now().After(deadline) {
			t.Fatal("merge still reported in flight 5s after a tiny batch")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func TestMergeProgress_Advanced(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		prev MergeProgress
		cur  MergeProgress
		want bool
	}{
		{
			"dead merger: started, never finished, no bytes moving",
			MergeProgress{Started: 3, Finished: 2, BytesWritten: 900},
			MergeProgress{Started: 3, Finished: 2, BytesWritten: 900},
			false,
		},
		{
			"live merge: bytes streaming out",
			MergeProgress{Started: 3, Finished: 2, BytesWritten: 900},
			MergeProgress{Started: 3, Finished: 2, BytesWritten: 4096},
			true,
		},
		{
			"merge completed between samples",
			MergeProgress{Started: 3, Finished: 2, BytesWritten: 900},
			MergeProgress{Started: 3, Finished: 3, BytesWritten: 900},
			true,
		},
		{"idle index, nothing to report", MergeProgress{}, MergeProgress{}, false},
	}
	for _, tc := range cases {
		if got := tc.cur.Advanced(tc.prev); got != tc.want {
			t.Errorf("%s: Advanced = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestAsyncErrorCallbackIsRegistered(t *testing.T) {
	// Not parallel: it swaps the package-level OnAsyncError hook.
	cb, ok := scorch.RegistryAsyncErrorCallbacks[asyncErrorCallbackName]
	if !ok {
		t.Fatalf("init() did not register %q — scorch panics stay silent", asyncErrorCallbackName)
	}
	var gotErr error
	var gotPath string
	prev := OnAsyncError
	t.Cleanup(func() { OnAsyncError = prev })
	OnAsyncError = func(err error, path string) { gotErr, gotPath = err, path }

	cb(errors.New("merger panic"), "/data/index/topic")
	if gotErr == nil || gotErr.Error() != "merger panic" || gotPath != "/data/index/topic" {
		t.Errorf("callback did not forward to OnAsyncError: err=%v path=%q", gotErr, gotPath)
	}

	// A nil hook (the TUI's default) must not panic.
	OnAsyncError = nil
	cb(errors.New("ignored"), "/data/index/topic")
}

// TestSnapshotIsLockFreeDuringWrite proves the lock-free snapshot: coverage must be
// answerable while the drain holds ix.mu for a long IndexBatch. Before this,
// BookkeeperSnapshot took that same mutex, so on a high-ingest topic coverage
// RPCs blew their deadline while a quiet topic on the same daemon answered in
// milliseconds — and that looks exactly like the drain wedge.
func TestSnapshotIsLockFreeDuringWrite(t *testing.T) {
	t.Parallel()

	ix := newTestIndexer(t)
	if err := ix.IndexBatch(context.Background(), []kafka.ConsumedMessage{
		sampleMsg(0, 1, "k", `{"a":1}`, time.Now()),
	}); err != nil {
		t.Fatalf("IndexBatch: %v", err)
	}

	// Hold the writer lock, standing in for an IndexBatch blocked inside bleve.
	ix.mu.Lock()
	done := make(chan Bookkeeper, 1)
	go func() { done <- ix.BookkeeperSnapshot() }()

	select {
	case b := <-done:
		if len(b.NewestOffsetPerPartition) == 0 {
			t.Error("snapshot returned but carries no coverage")
		}
	case <-time.After(3 * time.Second):
		ix.mu.Unlock()
		t.Fatal("BookkeeperSnapshot blocked on the writer lock — readers still starve")
	}
	ix.mu.Unlock()
}

// TestTrimNeverOverClaimsCoverage is the correctness guard on the lock-free
// read. Trim deletes in chunks and only refreshes the oldest markers at the
// end, so between the delete and the refresh the bookkeeper still advertises
// records bleve no longer has. Under the old locked read that window was
// invisible; a lock-free reader can land in it.
//
// Being stale in the NEWEST direction is safe (we under-claim, the client
// scans). Being stale in the OLDEST direction is not: we would serve a window
// whose earliest records were already deleted and silently return less than
// asked for. So the published snapshot must never claim an older floor than
// the index actually holds.
func TestTrimNeverOverClaimsCoverage(t *testing.T) {
	t.Parallel()

	ix := newTestIndexer(t)
	ctx := context.Background()
	base := time.Now().Add(-time.Hour)
	msgs := make([]kafka.ConsumedMessage, 0, 200)
	for i := range 200 {
		msgs = append(msgs, sampleMsg(0, int64(i+1), "k",
			`{"padding":"`+string(make([]byte, 512))+`"}`, base.Add(time.Duration(i)*time.Second)))
	}
	if err := ix.IndexBatch(ctx, msgs); err != nil {
		t.Fatalf("IndexBatch: %v", err)
	}

	before := ix.BookkeeperSnapshot()
	oldestBefore, ok := before.OldestTimePerPartition[0]
	if !ok {
		t.Fatal("no oldest marker after indexing")
	}

	// Trim hard enough to delete a large share of the records.
	if err := ix.Trim(ctx, 4096); err != nil {
		t.Fatalf("Trim: %v", err)
	}

	// Guard against a vacuous pass: if Trim took its under-cap fast path,
	// nothing was deleted and the assertions below prove nothing.
	docsAfter, err := ix.DocCount()
	if err != nil {
		t.Fatalf("DocCount: %v", err)
	}
	if docsAfter >= uint64(len(msgs)) {
		t.Fatalf("trim did not delete anything (%d docs of %d) — test proves nothing", docsAfter, len(msgs))
	}

	after := ix.BookkeeperSnapshot()
	oldestAfter, ok := after.OldestTimePerPartition[0]
	if !ok {
		return // everything trimmed; nothing is claimed, which is safe
	}
	if oldestAfter == oldestBefore {
		t.Error("records were trimmed but the published floor did not advance")
	}
	// The floor must have moved FORWARD (or held). Moving backward would mean
	// claiming data we deleted.
	if oldestAfter < oldestBefore {
		t.Errorf("published floor moved backwards after trim: %d -> %d", oldestBefore, oldestAfter)
	}
	// And it must match what the index actually holds.
	ix.mu.Lock()
	truth := ix.book.OldestTimePerPartition[0]
	ix.mu.Unlock()
	if oldestAfter < truth {
		t.Errorf("published floor %d is OLDER than the real floor %d — over-claiming coverage", oldestAfter, truth)
	}
}

// TestPublishedSnapshotIsIsolated proves callers cannot mutate shared state
// through a returned snapshot (the maps are cloned, not aliased).
func TestPublishedSnapshotIsIsolated(t *testing.T) {
	t.Parallel()

	ix := newTestIndexer(t)
	if err := ix.IndexBatch(context.Background(), []kafka.ConsumedMessage{
		sampleMsg(3, 7, "k", `{"a":1}`, time.Now()),
	}); err != nil {
		t.Fatalf("IndexBatch: %v", err)
	}
	a := ix.BookkeeperSnapshot()
	a.NewestOffsetPerPartition[3] = 99999
	b := ix.BookkeeperSnapshot()
	if b.NewestOffsetPerPartition[3] == 99999 {
		t.Error("mutating a snapshot leaked into the published bookkeeper")
	}
}
