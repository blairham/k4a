// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"testing"
	"time"

	"github.com/blairham/k4a/internal/index"
	"github.com/blairham/k4a/internal/indexwire/indexv1"
	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/state"
)

func TestPlanBackfill(t *testing.T) {
	bounds := kafka.OffsetBounds{
		Start: map[int32]int64{0: 100, 1: 0},
		End:   map[int32]int64{0: 1000, 1: 50},
		After: map[int32]int64{0: 400, 1: 50}, // p1 has nothing in the window
	}
	held := func(newest map[int32]int64) index.Bookkeeper {
		return index.Bookkeeper{NewestOffsetPerPartition: newest}
	}
	win := backfillOptions{Window: 24 * time.Hour}

	type planCase struct {
		wantStarts map[int32]int64
		name       string
		book       index.Bookkeeper
		opts       backfillOptions
		wantStale  bool
	}
	pc := func(name string, book index.Bookkeeper, opts backfillOptions, starts map[int32]int64, stale bool) planCase {
		return planCase{name: name, book: book, opts: opts, wantStarts: starts, wantStale: stale}
	}
	cases := []planCase{
		pc(
			"fresh index starts at the window", held(nil), win,
			map[int32]int64{0: 400, 1: 50},
			false,
		),
		pc(
			"record cap pulls the start forward", held(nil),
			backfillOptions{Window: 24 * time.Hour, MaxRecords: 200},
			map[int32]int64{0: 900, 1: 50},
			false,
		),
		pc(
			"no window keeps the recent slice", held(nil),
			backfillOptions{},
			map[int32]int64{0: 950, 1: 0},
			false,
		),
		pc(
			"held partitions resume right after their newest", held(map[int32]int64{0: 199, 1: 49}), win,
			map[int32]int64{0: 200, 1: 50},
			false,
		),
		pc("retention deleted the records after the newest", held(map[int32]int64{0: 50}), win, nil, true),
		pc("newest past the high watermark (topic recreated)", held(map[int32]int64{1: 80}), win, nil, true),
		pc(
			"too far behind for the cap", held(map[int32]int64{0: 200}),
			backfillOptions{Window: 24 * time.Hour, MaxRecords: 200},
			nil, true,
		),
		pc("a held partition the broker no longer has", held(map[int32]int64{7: 3}), win, nil, true),
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			plan, stale := planBackfill(tc.book, bounds, tc.opts)
			if stale != tc.wantStale {
				t.Fatalf("stale = %v, want %v", stale, tc.wantStale)
			}
			if stale {
				return
			}
			for p, want := range tc.wantStarts {
				if got := plan.starts[p]; got != want {
					t.Errorf("partition %d starts at %d, want %d", p, got, want)
				}
				if _, ok := plan.targets[p]; ok != (want < bounds.End[p]) {
					t.Errorf("partition %d target set = %v, want %v", p, ok, want < bounds.End[p])
				}
			}
		})
	}
}

// oldestOffset is partition 0's oldest indexed offset.
func oldestOffset(w *topicWorker) int64 {
	return w.async.Bookkeeper().OldestOffsetPerPartition[0]
}

func waitBackfilled(t *testing.T, w *topicWorker) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for w.backfilling() {
		if time.Now().After(deadline) {
			t.Fatal("backfilling never cleared")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestBackfillIndexesTheWindow: a topic followed with a window indexes every
// record already on the broker, not just the tail's recent slice, and stops
// reporting backfilling once it has.
func TestBackfillIndexesTheWindow(t *testing.T) {
	const records = 300 // well past the 100-record recent slice
	fake := newFakeCluster(t, testTopic)
	seeds := fake.ListenAddrs()
	produceRecords(t, seeds, testTopic, records)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	mgr := newManager(ctx, state.NewRoot(t.TempDir()), "test-cluster", newKafkaClient(t, seeds),
		mustPolicy(t, testTopic), managerConfig{Worker: workerOptions{
			Backfill: backfillOptions{Window: 24 * time.Hour, MaxRecords: 1000},
		}})
	defer mgr.stopAll()
	mgr.ensureFollowed(testTopic)
	w := waitFollowed(t, mgr, testTopic)
	waitIndexed(t, w, records)
	waitBackfilled(t, w)
	if got := oldestOffset(w); got != 0 {
		t.Errorf("oldest indexed offset = %d, want 0 (the whole window)", got)
	}

	cov, err := dialServer(t, mgr).Coverage(ctx, coverageReq(testTopic))
	if err != nil {
		t.Fatal(err)
	}
	if cov.GetCoverage().GetBackfilling() {
		t.Error("coverage still reports backfilling after the history was indexed")
	}
}

// TestBackfillRecordCap: the cap bounds how much history is read, from the
// newest end.
func TestBackfillRecordCap(t *testing.T) {
	fake := newFakeCluster(t, testTopic)
	seeds := fake.ListenAddrs()
	produceRecords(t, seeds, testTopic, 300)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	mgr := newManager(ctx, state.NewRoot(t.TempDir()), "test-cluster", newKafkaClient(t, seeds),
		mustPolicy(t, testTopic), managerConfig{Worker: workerOptions{
			Backfill: backfillOptions{Window: 24 * time.Hour, MaxRecords: 120},
		}})
	defer mgr.stopAll()
	mgr.ensureFollowed(testTopic)
	w := waitFollowed(t, mgr, testTopic)
	waitIndexed(t, w, 120)
	waitBackfilled(t, w)
	if got := oldestOffset(w); got != 180 {
		t.Errorf("oldest indexed offset = %d, want 180 (the newest 120 of 300)", got)
	}
}

// TestRestartResumesPersistedFollowSet: a container restart (a new manager on
// the same data dir) re-follows the persisted set and resumes each index
// right after its newest record, so records produced while it was down are
// indexed and the covered range stays contiguous from the original start.
func TestRestartResumesPersistedFollowSet(t *testing.T) {
	fake := newFakeCluster(t, testTopic)
	seeds := fake.ListenAddrs()
	client := newKafkaClient(t, seeds)
	root := state.NewRoot(t.TempDir())
	cfg := managerConfig{Worker: workerOptions{Backfill: backfillOptions{Window: 24 * time.Hour, MaxRecords: 1000}}}
	produceRecords(t, seeds, testTopic, 200)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	first := newManager(ctx, root, "test-cluster", client, mustPolicy(t, testTopic), cfg)
	first.ensureFollowed(testTopic)
	waitIndexed(t, waitFollowed(t, first, testTopic), 200)
	first.stopAll()

	// Written while "down": the old tail never saw these.
	produceRecords(t, seeds, testTopic, 150)

	second := newManager(ctx, root, "test-cluster", client, mustPolicy(t, testTopic), cfg)
	defer second.stopAll()
	if n := second.restoreFollowSet(); n != 1 {
		t.Fatalf("restored %d topics, want 1", n)
	}
	w := second.lookup(testTopic)
	if w == nil {
		t.Fatal("the persisted topic was not re-followed")
	}
	waitIndexed(t, w, 350)
	book := w.async.Bookkeeper()
	if book.OldestOffsetPerPartition[0] != 0 || book.NewestOffsetPerPartition[0] != 349 {
		t.Errorf("covered offsets %d-%d, want 0-349 with nothing re-read or skipped",
			book.OldestOffsetPerPartition[0], book.NewestOffsetPerPartition[0])
	}
}

// TestRestoreHonorsTodaysAllowlist: a narrowed --allow keeps a persisted topic
// from coming back.
func TestRestoreHonorsTodaysAllowlist(t *testing.T) {
	fake := newFakeCluster(t, topicA, topicB)
	client := newKafkaClient(t, fake.ListenAddrs())
	root := state.NewRoot(t.TempDir())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	first := newManager(ctx, root, "test-cluster", client, mustPolicy(t, topicA, topicB), managerConfig{})
	first.ensureFollowed(topicA)
	first.ensureFollowed(topicB)
	waitFollowed(t, first, topicA)
	waitFollowed(t, first, topicB)
	first.stopAll()

	second := newManager(ctx, root, "test-cluster", client, mustPolicy(t, topicA), managerConfig{})
	defer second.stopAll()
	if n := second.restoreFollowSet(); n != 1 || !second.followed(topicA) || second.followed(topicB) {
		t.Errorf("restored %d; followed A=%v B=%v, want only A", n, second.followed(topicA), second.followed(topicB))
	}

	other := newManager(ctx, state.NewRoot(root.Path()), "another-cluster", client, mustPolicy(t, topicA), managerConfig{})
	defer other.stopAll()
	if n := other.restoreFollowSet(); n != 0 {
		t.Errorf("another cluster restored %d topics from this one's set", n)
	}
}

// TestStaleIndexStartsOver: an index too far behind to resume within the
// cap is deleted and rebuilt from the window, rather than kept with a hole
// between its newest record and the new tail.
func TestStaleIndexStartsOver(t *testing.T) {
	fake := newFakeCluster(t, testTopic)
	seeds := fake.ListenAddrs()
	client := newKafkaClient(t, seeds)
	root := state.NewRoot(t.TempDir())
	produceRecords(t, seeds, testTopic, 50)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	cfg := managerConfig{Worker: workerOptions{Backfill: backfillOptions{Window: 24 * time.Hour, MaxRecords: 100}}}
	first := newManager(ctx, root, "test-cluster", client, mustPolicy(t, testTopic), cfg)
	first.ensureFollowed(testTopic)
	waitIndexed(t, waitFollowed(t, first, testTopic), 50)
	first.stopAll()

	produceRecords(t, seeds, testTopic, 250) // 250 behind, cap 100

	second := newManager(ctx, root, "test-cluster", client, mustPolicy(t, testTopic), cfg)
	defer second.stopAll()
	second.restoreFollowSet()
	w := second.lookup(testTopic)
	if w == nil {
		t.Fatal("the persisted topic was not re-followed")
	}
	waitIndexed(t, w, 100)
	waitBackfilled(t, w)
	if got := oldestOffset(w); got != 200 {
		t.Errorf("oldest indexed offset = %d, want 200: the old index should be gone, not joined across a hole", got)
	}
}

func coverageReq(topic string) *indexv1.CoverageRequest {
	return &indexv1.CoverageRequest{Topic: topic}
}
