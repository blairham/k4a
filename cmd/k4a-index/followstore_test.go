// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/blairham/k4a/internal/state"
)

const storeTopic = "k4a-index.followset"

// TestNewPodRestoresFollowSetFromStore is #123's case: a pod on an empty
// volume re-follows what an earlier pod followed, with its LRU clocks, through
// the compacted topic — and a replica with a narrower allowlist leaves the
// entries it does not follow in the store for the replicas that do.
func TestNewPodRestoresFollowSetFromStore(t *testing.T) {
	fake := newFakeCluster(t, topicA, topicB)
	client := newKafkaClient(t, fake.ListenAddrs())
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	openStore := func() followStore {
		t.Helper()
		s, err := openKafkaFollowStore(ctx, client, storeTopic)
		if err != nil {
			t.Fatalf("open store: %v", err)
		}
		return s
	}

	first := newManager(ctx, state.NewRoot(t.TempDir()), "test-cluster", client,
		mustPolicy(t, topicA, topicB), managerConfig{Store: openStore()})
	first.restoreFollowSet(ctx)
	first.startStore(ctx)
	first.ensureFollowed(topicA)
	first.ensureFollowed(topicB)
	waitFollowed(t, first, topicA)
	waitFollowed(t, first, topicB)
	clockA := first.lastUse()[topicA]
	first.stopAll()

	// A new pod: a fresh volume, so nothing local to restore.
	narrow := newManager(ctx, state.NewRoot(t.TempDir()), "test-cluster", client,
		mustPolicy(t, topicA), managerConfig{Store: openStore()})
	if n := narrow.restoreFollowSet(ctx); n != 1 || !narrow.followed(topicA) || narrow.followed(topicB) {
		t.Fatalf("restored %d; A=%v B=%v, want only A", n, narrow.followed(topicA), narrow.followed(topicB))
	}
	if got := narrow.lastUse()[topicA]; got != clockA {
		t.Errorf("restored clock %d, want the earlier pod's %d", got, clockA)
	}
	narrow.startStore(ctx)
	narrow.stopAll()

	wide := newManager(ctx, state.NewRoot(t.TempDir()), "test-cluster", client,
		mustPolicy(t, topicA, topicB), managerConfig{Store: openStore()})
	defer wide.stopAll()
	if n := wide.restoreFollowSet(ctx); n != 2 {
		t.Errorf("restored %d topics, want 2: the narrow replica must not delete B from the store", n)
	}
}

// TestReplicasConvergeThroughStore: a topic one replica starts following is
// followed by the other, and a later use on one moves the other's LRU clock.
func TestReplicasConvergeThroughStore(t *testing.T) {
	fake := newFakeCluster(t, topicA)
	client := newKafkaClient(t, fake.ListenAddrs())
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	replica := func() *manager {
		t.Helper()
		s, err := openKafkaFollowStore(ctx, client, storeTopic)
		if err != nil {
			t.Fatalf("open store: %v", err)
		}
		m := newManager(ctx, state.NewRoot(t.TempDir()), "test-cluster", client,
			mustPolicy(t, topicA), managerConfig{Store: s})
		m.restoreFollowSet(ctx)
		m.startStore(ctx)
		t.Cleanup(m.stopAll)
		return m
	}
	one, two := replica(), replica()

	one.ensureFollowed(topicA)
	waitFollowed(t, one, topicA)
	waitFollowed(t, two, topicA)

	time.Sleep(10 * time.Millisecond)
	later := time.Now().UnixMilli() // a use on one after two registered
	one.mu.Lock()
	one.set[topicA].lastQuery.Store(later)
	one.mu.Unlock()
	one.kickStore()
	deadline := time.Now().Add(10 * time.Second)
	for two.lastUse()[topicA] < later {
		if time.Now().After(deadline) {
			t.Fatalf("replica two's clock = %d, want it moved to %d", two.lastUse()[topicA], later)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// TestStoreEntryThisReplicaDroppedStaysDropped: after a replica evicts a
// topic, an entry carrying the clock it was evicted at (its own write read
// back, or another replica's equally old one) does not re-follow it, a
// deletion does not unfollow anything, and a genuinely newer use does.
func TestStoreEntryThisReplicaDroppedStaysDropped(t *testing.T) {
	fake := newFakeCluster(t, topicA, topicB)
	client := newKafkaClient(t, fake.ListenAddrs())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	mgr := newManager(ctx, state.NewRoot(t.TempDir()), "test-cluster", client,
		mustPolicy(t, topicA, topicB), managerConfig{Store: newMemStore(nil)})
	defer mgr.stopAll()

	mgr.ensureFollowed(topicA)
	mgr.ensureFollowed(topicB)
	waitFollowed(t, mgr, topicA)
	waitFollowed(t, mgr, topicB)
	clock := mgr.lastUse()[topicA]

	mgr.mu.Lock()
	e := mgr.set[topicA]
	delete(mgr.set, topicA)
	mgr.registering[topicA] = struct{}{}
	mgr.mu.Unlock()
	mgr.teardown(topicA, e)

	mgr.applyStoreEntry(topicA, clock, false)
	mgr.applyStoreEntry(topicB, 0, true)
	time.Sleep(200 * time.Millisecond) // a registration, had one started, lands well within this
	if mgr.followed(topicA) {
		t.Error("a store entry no newer than the eviction re-followed the topic")
	}
	if !mgr.followed(topicB) {
		t.Error("another replica's deletion unfollowed a topic this replica still follows")
	}

	mgr.applyStoreEntry(topicA, clock+1, false)
	deadline := time.Now().Add(10 * time.Second)
	for !mgr.followed(topicA) { // not waitFollowed: its lookup would touch the clock
		if time.Now().After(deadline) {
			t.Fatal("a newer use did not re-follow the topic")
		}
		time.Sleep(20 * time.Millisecond)
	}
	if got := mgr.lastUse()[topicA]; got != clock+1 {
		t.Errorf("re-followed with clock %d, want the entry's %d", got, clock+1)
	}
}

// TestRestoreSyncDeletesOnlyStaleEntries: after a restore the first sync
// deletes the entries past evictAfter and writes nothing back for what the
// store already holds, and it never deletes a fresh entry this replica does
// not follow.
func TestRestoreSyncDeletesOnlyStaleEntries(t *testing.T) {
	fake := newFakeCluster(t, topicA, topicB)
	client := newKafkaClient(t, fake.ListenAddrs())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	now := time.Now().UnixMilli()
	const stale = "demo.stale.v1"
	store := newMemStore(map[string]int64{
		topicA: now,
		topicB: now,                                       // fresh, but outside this replica's allowlist
		stale:  now - (8 * 24 * time.Hour).Milliseconds(), // past the default evictAfter
	})
	mgr := newManager(ctx, state.NewRoot(t.TempDir()), "test-cluster", client,
		mustPolicy(t, topicA, stale), managerConfig{Store: store})
	defer mgr.stopAll()

	if n := mgr.restoreFollowSet(ctx); n != 1 {
		t.Fatalf("restored %d, want 1", n)
	}
	mgr.syncStore(ctx)
	puts, removes := store.ops()
	if len(puts) != 0 || !slices.Equal(removes, []string{stale}) {
		t.Errorf("sync wrote %v and deleted %v, want nothing written and only %q deleted", puts, removes, stale)
	}
}

// memStore is an in-memory followStore whose tail delivers nothing.
type memStore struct {
	data    map[string]int64
	puts    []string
	removes []string
	mu      sync.Mutex
}

func newMemStore(data map[string]int64) *memStore {
	if data == nil {
		data = map[string]int64{}
	}
	return &memStore{data: data}
}

func (s *memStore) snapshot(context.Context) (map[string]int64, map[int32]int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]int64, len(s.data))
	for k, v := range s.data {
		out[k] = v
	}
	return out, map[int32]int64{0: int64(len(s.data))}, nil
}

func (s *memStore) put(_ context.Context, topic string, lastUse int64) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[topic] = lastUse
	s.puts = append(s.puts, topic)
	return nil
}

func (s *memStore) remove(_ context.Context, topic string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, topic)
	s.removes = append(s.removes, topic)
	return nil
}

func (s *memStore) tail(ctx context.Context, _ map[int32]int64, _ func(string, int64, bool)) {
	<-ctx.Done()
}

func (s *memStore) close() {}

func (s *memStore) ops() (puts, removes []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return slices.Clone(s.puts), slices.Clone(s.removes)
}
