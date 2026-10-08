// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/blairham/k4a/internal/state"
)

// trimValBytes is the value size each test record carries, so a record's payload
// is ~trimValBytes+1 bytes (key "k") and a handful of records comfortably exceed
// the small byte caps these tests use — letting them reason about bytes directly.
const trimValBytes = 1000

// indexN writes n records to partition p with monotonically increasing offsets
// and timestamps (1s apart), each carrying a ~trimValBytes payload.
func indexN(t *testing.T, ix *Indexer, p int32, n int, base time.Time) {
	t.Helper()
	ctx := context.Background()
	val := strings.Repeat("x", trimValBytes)
	for i := range n {
		off := int64(i)
		ts := base.Add(time.Duration(i) * time.Second)
		if err := ix.Index(ctx, sampleMsg(p, off, "k", val, ts)); err != nil {
			t.Fatalf("Index p%d/%d: %v", p, off, err)
		}
	}
}

func TestTrim_DisabledIsNoOp(t *testing.T) {
	t.Parallel()
	ix := newTestIndexer(t)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	indexN(t, ix, 0, 10, base)

	before, _ := ix.DocCount()
	if err := ix.Trim(context.Background(), 0); err != nil {
		t.Fatalf("Trim(0): %v", err)
	}
	after, _ := ix.DocCount()
	if before != after {
		t.Fatalf("Trim(0) changed doc count: %d -> %d", before, after)
	}
}

func TestTrim_UnderCapIsNoOp(t *testing.T) {
	t.Parallel()
	ix := newTestIndexer(t)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	indexN(t, ix, 0, 5, base) // ~5 KB total

	before, _ := ix.DocCount()
	if err := ix.Trim(context.Background(), 1<<20); err != nil { // 1 MiB cap
		t.Fatalf("Trim: %v", err)
	}
	after, _ := ix.DocCount()
	if before != after {
		t.Fatalf("under-cap Trim changed doc count: %d -> %d", before, after)
	}
}

func TestTrim_EvictsOldestAndAdvancesMarkers(t *testing.T) {
	t.Parallel()
	ix := newTestIndexer(t)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	const (
		n    = 20
		capB = int64(5000) // lowWater 4500 → keeps ~4 newest records (~1001 B each)
	)
	indexN(t, ix, 0, n, base)

	if err := ix.Trim(context.Background(), capB); err != nil {
		t.Fatalf("Trim: %v", err)
	}

	book := ix.BookkeeperSnapshot()
	if book.ByteSize > capB {
		t.Fatalf("ByteSize %d still over cap %d", book.ByteSize, capB)
	}
	docs, _ := ix.DocCount()
	if docs == 0 || docs >= n {
		t.Fatalf("expected a bounded doc count after trim, got %d (started %d)", docs, n)
	}
	// Newest end untouched; oldest advanced forward off offset 0.
	if got := book.NewestOffsetPerPartition[0]; got != n-1 {
		t.Fatalf("newest offset moved: got %d want %d", got, n-1)
	}
	if got := book.OldestOffsetPerPartition[0]; got == 0 {
		t.Fatalf("oldest offset not advanced past 0")
	}
	// Coverage window shrinks from the old side: oldest time must match the
	// oldest surviving record, not the original base.
	wantOldestMS := base.Add(time.Duration(book.OldestOffsetPerPartition[0]) * time.Second).UnixMilli()
	if got := book.OldestTimePerPartition[0]; got != wantOldestMS {
		t.Fatalf("oldest time marker stale: got %d want %d", got, wantOldestMS)
	}
}

func TestTrim_AdvancesMarkersPerPartition(t *testing.T) {
	t.Parallel()
	ix := newTestIndexer(t)
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	// Two partitions interleaved in time so a global oldest-first trim touches
	// both; each must get its own advanced oldest marker.
	indexN(t, ix, 0, 15, base)
	indexN(t, ix, 1, 15, base.Add(500*time.Millisecond))

	if err := ix.Trim(context.Background(), 8000); err != nil {
		t.Fatalf("Trim: %v", err)
	}

	book := ix.BookkeeperSnapshot()
	for _, p := range []int32{0, 1} {
		if _, ok := book.NewestOffsetPerPartition[p]; !ok {
			continue // partition fully drained is acceptable; nothing to assert
		}
		oldest, ok := book.OldestOffsetPerPartition[p]
		if !ok {
			t.Fatalf("partition %d has newest but no oldest marker", p)
		}
		if oldest > book.NewestOffsetPerPartition[p] {
			t.Fatalf("partition %d oldest %d past newest %d", p, oldest, book.NewestOffsetPerPartition[p])
		}
	}
	if book.ByteSize > 8000 {
		t.Fatalf("ByteSize %d over cap after multi-partition trim", book.ByteSize)
	}
}

func TestTrim_PersistsAcrossReopen(t *testing.T) {
	t.Parallel()
	root := state.NewRoot(t.TempDir())
	base := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)

	ix, err := Open(root, testCluster, "orders")
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	indexN(t, ix, 0, 20, base)
	if err = ix.Trim(context.Background(), 5000); err != nil {
		t.Fatalf("Trim: %v", err)
	}
	if err = ix.FlushBookkeeper(); err != nil {
		t.Fatalf("FlushBookkeeper: %v", err)
	}
	trimmedOldest := ix.BookkeeperSnapshot().OldestOffsetPerPartition[0]
	trimmedDocs, _ := ix.DocCount()
	if err = ix.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	reopened, err := Open(root, testCluster, "orders")
	if err != nil {
		t.Fatalf("re-Open: %v", err)
	}
	t.Cleanup(func() { _ = reopened.Close() })

	if got := reopened.BookkeeperSnapshot().OldestOffsetPerPartition[0]; got != trimmedOldest {
		t.Fatalf("advanced oldest marker not persisted: got %d want %d", got, trimmedOldest)
	}
	if got, _ := reopened.DocCount(); got != trimmedDocs {
		t.Fatalf("trimmed docs came back after reopen: got %d want %d", got, trimmedDocs)
	}
}
