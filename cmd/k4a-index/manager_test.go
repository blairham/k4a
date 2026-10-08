// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/blairham/k4a/internal/state"
)

const (
	topicA = "demo.a.v1"
	topicB = "demo.b.v1"
)

// TestManagerCapEvictsColdest proves the hard cap bounds the follow-set: at
// maxFollowed=1, following a second topic evicts the first (the coldest).
func TestManagerCapEvictsColdest(t *testing.T) {
	fake := newFakeCluster(t, topicA, topicB)
	client := newKafkaClient(t, fake.ListenAddrs())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mgr := newManager(ctx, state.NewRoot(t.TempDir()), "test-cluster", client,
		mustPolicy(t, topicA, topicB), managerConfig{MaxFollowed: 1})
	defer mgr.stopAll()

	mgr.ensureFollowed(topicA)
	waitFollowed(t, mgr, topicA)

	mgr.ensureFollowed(topicB)
	waitFollowed(t, mgr, topicB)

	// B's registration evicts A to stay at the cap; eviction (stop + rmdir) is
	// async, so poll for the steady state.
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if mgr.size() == 1 && !mgr.followed(topicA) && mgr.followed(topicB) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("cap not enforced: size=%d followedA=%v followedB=%v",
		mgr.size(), mgr.followed(topicA), mgr.followed(topicB))
}

// TestManagerLRUEvictsIdleButNotPinned proves the LRU sweeper drops a topic idle
// past evictAfter, while a pinned (pre-follow) topic survives regardless of age.
func TestManagerLRUEvictsIdleButNotPinned(t *testing.T) {
	fake := newFakeCluster(t, topicA, topicB)
	client := newKafkaClient(t, fake.ListenAddrs())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mgr := newManager(ctx, state.NewRoot(t.TempDir()), "test-cluster", client,
		mustPolicy(t, topicA, topicB), managerConfig{EvictAfter: 40 * time.Millisecond})
	defer mgr.stopAll()

	mgr.preFollow([]string{topicB}) // pinned
	mgr.ensureFollowed(topicA)      // on-demand, evictable
	waitFollowed(t, mgr, topicA)

	// Let both age past evictAfter, then run one sweep pass. Do NOT lookup(A)
	// in between — that would touch it and reset its LRU clock.
	time.Sleep(80 * time.Millisecond)
	mgr.evictIdle()

	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if !mgr.followed(topicA) && mgr.followed(topicB) {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("LRU eviction wrong: followedA=%v (want false) followedB=%v (want true)",
		mgr.followed(topicA), mgr.followed(topicB))
}

// TestDiskBudgetReapsOrphansNotLiveTopics proves the budget sweep's reclaim
// order and its safety guards: an abandoned index dir (the orphaned-index leak, left
// behind when a watchdog restart re-follows only the pinned set) is deleted,
// while a directory belonging to a live worker — and a young one that may be
// mid-registration — are both left alone.
func TestDiskBudgetReapsOrphansNotLiveTopics(t *testing.T) {
	fake := newFakeCluster(t, topicA, topicB)
	client := newKafkaClient(t, fake.ListenAddrs())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	root := state.NewRoot(t.TempDir())
	// A 1-byte budget keeps the sweep permanently hungry, so what survives is
	// decided by the guards rather than by the arithmetic.
	mgr := newManager(ctx, root, "test-cluster", client,
		mustPolicy(t, topicA, topicB), managerConfig{MaxTotalBytes: 1})
	defer mgr.stopAll()

	mgr.ensureFollowed(topicA)
	waitFollowed(t, mgr, topicA)

	base := filepath.Join(root.File(state.SubsystemIndex), "test-cluster")
	// An orphan, aged past orphanMinAge.
	orphan := filepath.Join(base, "orphaned.topic.v1")
	writeIndexDir(t, orphan, time.Now().Add(-2*orphanMinAge))
	// An unowned but freshly-touched dir: may be a registration in progress.
	young := filepath.Join(base, "young.topic.v1")
	writeIndexDir(t, young, time.Now())

	mgr.enforceDiskBudget()

	if _, err := os.Stat(orphan); !os.IsNotExist(err) {
		t.Errorf("orphaned index dir survived the budget sweep: %v", err)
	}
	if _, err := os.Stat(young); err != nil {
		t.Errorf("freshly-touched dir was reaped — races a registration: %v", err)
	}
	// topicA is followed, so it must be evicted through teardown (which stops
	// the worker first), never reaped out from under a live writer.
	if mgr.followed(topicA) {
		t.Error("over-budget sweep left the only evictable followed topic in place")
	}
}

// TestDiskBudgetDisabledByDefault proves a zero budget is inert: without it the
// daemon must behave exactly as it did before the disk budget existed.
func TestDiskBudgetDisabledByDefault(t *testing.T) {
	fake := newFakeCluster(t, topicA)
	client := newKafkaClient(t, fake.ListenAddrs())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	root := state.NewRoot(t.TempDir())
	mgr := newManager(ctx, root, "test-cluster", client, mustPolicy(t, topicA), managerConfig{})
	defer mgr.stopAll()

	mgr.ensureFollowed(topicA)
	waitFollowed(t, mgr, topicA)

	orphan := filepath.Join(root.File(state.SubsystemIndex), "test-cluster", "orphaned.topic.v1")
	writeIndexDir(t, orphan, time.Now().Add(-2*orphanMinAge))

	mgr.enforceDiskBudget()

	if _, err := os.Stat(orphan); err != nil {
		t.Errorf("budget disabled but the sweep still deleted a dir: %v", err)
	}
	if !mgr.followed(topicA) {
		t.Error("budget disabled but the sweep still evicted a topic")
	}
}

// writeIndexDir creates a plausible on-disk index dir and backdates it, so the
// orphan-age guard sees the intended age.
func writeIndexDir(t *testing.T, dir string, mod time.Time) {
	t.Helper()
	if err := os.MkdirAll(filepath.Join(dir, "bleve"), 0o755); err != nil {
		t.Fatal(err)
	}
	f := filepath.Join(dir, "bleve", "segment.zap")
	if err := os.WriteFile(f, make([]byte, 4096), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{f, filepath.Join(dir, "bleve"), dir} {
		if err := os.Chtimes(p, mod, mod); err != nil {
			t.Fatal(err)
		}
	}
}

// TestIdleSweepAgesOutOrphans proves orphan cleanup: an orphaned index
// dir is deleted once it passes evictAfter, on the idle sweep, with the disk
// budget DISABLED. Before this the budget's sweep was the only reaper, so an
// orphan under budget held disk for the life of the pod — the behavior that
// issue reported.
func TestIdleSweepAgesOutOrphans(t *testing.T) {
	fake := newFakeCluster(t, topicA)
	client := newKafkaClient(t, fake.ListenAddrs())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	root := state.NewRoot(t.TempDir())
	// No MaxTotalBytes: the budget is off, so only the idle path can reap.
	mgr := newManager(ctx, root, "test-cluster", client, mustPolicy(t, topicA),
		managerConfig{EvictAfter: time.Hour})
	defer mgr.stopAll()

	base := filepath.Join(root.File(state.SubsystemIndex), "test-cluster")
	stale := filepath.Join(base, "stale.topic.v1")
	writeIndexDir(t, stale, time.Now().Add(-2*time.Hour)) // past evictAfter
	fresh := filepath.Join(base, "fresh.topic.v1")
	writeIndexDir(t, fresh, time.Now().Add(-time.Minute)) // well inside it

	mgr.evictIdle()

	if _, err := os.Stat(stale); !os.IsNotExist(err) {
		t.Errorf("orphan past evictAfter survived the idle sweep: %v", err)
	}
	// The warm-cache trade-off: while there is disk headroom, a recent orphan
	// is kept so a re-follow reuses it instead of re-warming from Kafka.
	if _, err := os.Stat(fresh); err != nil {
		t.Errorf("recent orphan was reaped despite headroom: %v", err)
	}
}

// TestIdleSweepSpareLiveTopics proves the idle orphan reap never touches a
// directory a live worker owns, even when that worker is not idle enough to
// evict.
func TestIdleSweepSpareLiveTopics(t *testing.T) {
	fake := newFakeCluster(t, topicA)
	client := newKafkaClient(t, fake.ListenAddrs())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	root := state.NewRoot(t.TempDir())
	mgr := newManager(ctx, root, "test-cluster", client, mustPolicy(t, topicA),
		managerConfig{EvictAfter: time.Hour})
	defer mgr.stopAll()

	mgr.ensureFollowed(topicA)
	waitFollowed(t, mgr, topicA)

	// Backdate the live topic's dir past evictAfter. Ownership, not age, is
	// what must protect it — reaping here would delete files under a running
	// bleve writer.
	dir := filepath.Join(root.File(state.SubsystemIndex), "test-cluster", topicA)
	old := time.Now().Add(-2 * time.Hour)
	_ = filepath.Walk(dir, func(p string, _ os.FileInfo, err error) error {
		if err == nil {
			_ = os.Chtimes(p, old, old)
		}
		return nil
	})

	mgr.evictIdle()

	if _, err := os.Stat(dir); err != nil {
		t.Errorf("live followed topic's index dir was reaped: %v", err)
	}
	if !mgr.followed(topicA) {
		t.Error("live topic was evicted by the orphan sweep")
	}
}
