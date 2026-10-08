// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"context"
	"maps"
	"slices"
	"testing"
	"time"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/state"
)

// fakeKafka implements the small surface Catchup needs. We supply
// per-partition record lists keyed by offset; ConsumeRange replays
// them.
type fakeKafka struct {
	hwms    map[int32]int64
	records map[int32]map[int64]kafka.ConsumedMessage
}

func (f *fakeKafka) EndOffsets(_ context.Context, _ string) (map[int32]int64, error) {
	out := make(map[int32]int64, len(f.hwms))
	maps.Copy(out, f.hwms)
	return out, nil
}

func (f *fakeKafka) ConsumeRange(
	ctx context.Context,
	_ string,
	starts, ends map[int32]int64,
	onRecord func(kafka.ConsumedMessage),
) error {
	for partition, start := range starts {
		end := ends[partition]
		offsets := sortedOffsets(f.records[partition])
		for _, off := range offsets {
			if off < start || off >= end {
				continue
			}
			if cerr := ctx.Err(); cerr != nil {
				return cerr
			}
			onRecord(f.records[partition][off])
		}
	}
	return nil
}

func sortedOffsets(m map[int64]kafka.ConsumedMessage) []int64 {
	out := make([]int64, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	slices.Sort(out)
	return out
}

// msgAt returns a sample message at the given (partition, offset).
func msgAt(partition int32, offset int64) kafka.ConsumedMessage {
	return kafka.ConsumedMessage{
		Topic:     "orders",
		Partition: int(partition),
		Offset:    offset,
		Key:       "k",
		Value:     "v",
		Time:      time.Now().UTC(),
	}
}

func TestCatchup_NoPriorState_IsNoOp(t *testing.T) {
	t.Parallel()

	root := state.NewRoot(t.TempDir())
	ix, err := Open(root, testCluster, "orders")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ix.Close() })
	async := NewAsync(ix)
	t.Cleanup(func() { _ = async.Stop() })

	fake := &fakeKafka{
		hwms:    map[int32]int64{0: 100},
		records: map[int32]map[int64]kafka.ConsumedMessage{0: {50: msgAt(0, 50)}},
	}

	res, err := Catchup(context.Background(), fake, async, "orders", CatchupOptions{})
	if err != nil {
		t.Fatalf("Catchup: %v", err)
	}
	if res.RecordsIndexed != 0 {
		t.Errorf("RecordsIndexed = %d, want 0 (no prior state)", res.RecordsIndexed)
	}
}

func TestCatchup_FillsGap(t *testing.T) {
	t.Parallel()

	root := state.NewRoot(t.TempDir())
	ix, err := Open(root, testCluster, "orders")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ix.Close() })

	// Prime bookkeeper with newest_indexed_offset = 4 for partition 0.
	if ierr := ix.Index(context.Background(), msgAt(0, 4)); ierr != nil {
		t.Fatal(ierr)
	}

	async := NewAsync(ix)
	t.Cleanup(func() { _ = async.Stop() })

	// Broker HWM is 10; offsets 5..9 should be backfilled.
	records := map[int64]kafka.ConsumedMessage{}
	for off := int64(5); off < 10; off++ {
		records[off] = msgAt(0, off)
	}
	fake := &fakeKafka{
		hwms:    map[int32]int64{0: 10},
		records: map[int32]map[int64]kafka.ConsumedMessage{0: records},
	}

	res, err := Catchup(context.Background(), fake, async, "orders", CatchupOptions{})
	if err != nil {
		t.Fatalf("Catchup: %v", err)
	}
	if res.RecordsIndexed != 5 {
		t.Errorf("RecordsIndexed = %d, want 5", res.RecordsIndexed)
	}
	if res.TimedOut || res.HitRecordCap {
		t.Errorf("unexpected limit hit: %+v", res)
	}
	// Stop drains pending work; check the bookkeeper afterwards.
	_ = async.Stop()
	book := ix.BookkeeperSnapshot()
	if book.NewestOffsetPerPartition[0] != 9 {
		t.Errorf("post-catchup newest offset = %d, want 9",
			book.NewestOffsetPerPartition[0])
	}
}

func TestCatchup_NoGap_NoOp(t *testing.T) {
	t.Parallel()

	root := state.NewRoot(t.TempDir())
	ix, err := Open(root, testCluster, "orders")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ix.Close() })

	if ierr := ix.Index(context.Background(), msgAt(0, 9)); ierr != nil {
		t.Fatal(ierr)
	}
	async := NewAsync(ix)
	t.Cleanup(func() { _ = async.Stop() })

	fake := &fakeKafka{hwms: map[int32]int64{0: 10}}
	res, err := Catchup(context.Background(), fake, async, "orders", CatchupOptions{})
	if err != nil {
		t.Fatalf("Catchup: %v", err)
	}
	if res.RecordsIndexed != 0 {
		t.Errorf("RecordsIndexed = %d, want 0 (already at HWM)", res.RecordsIndexed)
	}
}

func TestCatchup_MultiPartition_FillsAllGaps(t *testing.T) {
	t.Parallel()

	root := state.NewRoot(t.TempDir())
	ix, err := Open(root, testCluster, "orders")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ix.Close() })

	if ierr := ix.IndexBatch(context.Background(), []kafka.ConsumedMessage{
		msgAt(0, 4),
		msgAt(1, 100),
	}); ierr != nil {
		t.Fatal(ierr)
	}

	async := NewAsync(ix)
	t.Cleanup(func() { _ = async.Stop() })

	p0 := map[int64]kafka.ConsumedMessage{5: msgAt(0, 5), 6: msgAt(0, 6)}
	p1 := map[int64]kafka.ConsumedMessage{101: msgAt(1, 101)}
	fake := &fakeKafka{
		hwms: map[int32]int64{0: 7, 1: 102},
		records: map[int32]map[int64]kafka.ConsumedMessage{
			0: p0,
			1: p1,
		},
	}

	res, err := Catchup(context.Background(), fake, async, "orders", CatchupOptions{})
	if err != nil {
		t.Fatalf("Catchup: %v", err)
	}
	if res.RecordsIndexed != 3 {
		t.Errorf("RecordsIndexed = %d, want 3", res.RecordsIndexed)
	}
	_ = async.Stop()
	book := ix.BookkeeperSnapshot()
	if book.NewestOffsetPerPartition[0] != 6 || book.NewestOffsetPerPartition[1] != 101 {
		t.Errorf("post-catchup offsets = %v, want {0:6, 1:101}",
			book.NewestOffsetPerPartition)
	}
}

func TestCatchup_RecordCap_LeavesGap(t *testing.T) {
	t.Parallel()

	root := state.NewRoot(t.TempDir())
	ix, err := Open(root, testCluster, "orders")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = ix.Close() })
	if ierr := ix.Index(context.Background(), msgAt(0, 4)); ierr != nil {
		t.Fatal(ierr)
	}
	async := NewAsync(ix)
	t.Cleanup(func() { _ = async.Stop() })

	// Far more records available than the cap allows.
	records := map[int64]kafka.ConsumedMessage{}
	for off := int64(5); off < 1000; off++ {
		records[off] = msgAt(0, off)
	}
	fake := &fakeKafka{
		hwms:    map[int32]int64{0: 1000},
		records: map[int32]map[int64]kafka.ConsumedMessage{0: records},
	}

	res, err := Catchup(context.Background(), fake, async, "orders",
		CatchupOptions{MaxRecords: 10})
	if err != nil {
		t.Fatalf("Catchup: %v", err)
	}
	if !res.HitRecordCap {
		t.Errorf("expected HitRecordCap=true, got %+v", res)
	}
	if res.GapsRecorded == 0 {
		t.Errorf("expected at least one gap recorded, got %+v", res)
	}
}
