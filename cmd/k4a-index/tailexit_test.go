// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"testing"
	"time"

	"github.com/blairham/k4a/internal/index"
	"github.com/blairham/k4a/internal/indexwire/indexv1"
	"github.com/blairham/k4a/internal/state"
)

// TestDeadTailLeavesTheFollowSet is the regression for a tail whose consume
// channel closes on its own — here a topic the broker does not have, one of the
// paths where Consume gives up. The worker's run() returned, but its entry
// stayed in the follow-set, so Coverage kept answering following=true for an
// index that would never grow, and nothing ever restarted it.
func TestDeadTailLeavesTheFollowSet(t *testing.T) {
	const missing = "demo.missing.v1"
	fake := newFakeCluster(t, testTopic)
	client := newKafkaClient(t, fake.ListenAddrs())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	root := state.NewRoot(t.TempDir())
	const cluster = "test-cluster"
	mgr := newManager(ctx, root, cluster, client, mustPolicy(t, "demo.*"), managerConfig{})
	defer mgr.stopAll()
	rpc := dialServer(t, mgr)

	if !mgr.ensureFollowed(missing) {
		t.Fatal("an allowlisted topic was refused")
	}

	// Registration is asynchronous and a dead tail leaves on its own, so wait
	// for both the in-flight marker and the follow-set entry to clear.
	waitNotFollowed(t, mgr, missing, time.Now().Add(10*time.Second))

	resp, err := rpc.Coverage(ctx, &indexv1.CoverageRequest{Topic: missing})
	if err != nil {
		t.Fatalf("Coverage: %v", err)
	}
	if resp.GetCoverage().GetFollowing() {
		t.Error("Coverage reports following=true for a topic whose tail is dead")
	}

	// The dead worker was stopped, so its writer lock is free again.
	ix, err := index.Open(root, cluster, missing)
	if err != nil {
		t.Fatalf("the dead worker still holds the index: %v", err)
	}
	_ = ix.Close() //nolint:errcheck // test cleanup

	// A live topic on the same manager is unaffected.
	mgr.ensureFollowed(testTopic)
	waitFollowed(t, mgr, testTopic)
}

// TestDeadPinnedTailRetriesPinned proves a pinned topic whose tail dies keeps
// its pin: it leaves the follow-set like any dead tail, and the search that
// re-registers it registers it pinned again, so LRU can never drop it.
func TestDeadPinnedTailRetriesPinned(t *testing.T) {
	const missing = "demo.missing.v1"
	fake := newFakeCluster(t, testTopic)
	client := newKafkaClient(t, fake.ListenAddrs())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mgr := newManager(ctx, state.NewRoot(t.TempDir()), "test-cluster", client,
		mustPolicy(t, "demo.*"), managerConfig{})
	defer mgr.stopAll()

	mgr.preFollow([]string{missing})
	waitNotFollowed(t, mgr, missing, time.Now().Add(10*time.Second))

	mgr.mu.Lock()
	_, pinned := mgr.pins[missing]
	mgr.mu.Unlock()
	if !pinned {
		t.Fatal("a dead pinned tail lost its pin")
	}
}

// waitNotFollowed waits until topic is neither followed nor registering.
func waitNotFollowed(t *testing.T, mgr *manager, topic string, deadline time.Time) {
	t.Helper()
	for {
		mgr.mu.Lock()
		_, followed := mgr.set[topic]
		_, inflight := mgr.registering[topic]
		mgr.mu.Unlock()
		if !followed && !inflight {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("topic %q whose tail exited is still in the follow-set", topic)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
