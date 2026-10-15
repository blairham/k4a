// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"testing"
	"time"

	"github.com/blairham/k4a/internal/indexwire/indexv1"
	"github.com/blairham/k4a/internal/state"
)

// TestCoverageProbeDoesNotResetLRU is the regression for Coverage touching
// the LRU clock: a topic that was only ever probed for coverage never looked
// idle, so evictAfter could not reclaim it. A client search still keeps a
// topic warm — through Search when the daemon covers it, through Follow when
// it does not — and both of those are checked here too.
func TestCoverageProbeDoesNotResetLRU(t *testing.T) {
	fake := newFakeCluster(t, topicA, topicB, testTopic)
	client := newKafkaClient(t, fake.ListenAddrs())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const idle = 300 * time.Millisecond
	mgr := newManager(ctx, state.NewRoot(t.TempDir()), "test-cluster", client,
		mustPolicy(t, topicA, topicB, testTopic), managerConfig{EvictAfter: idle})
	defer mgr.stopAll()
	rpc := dialServer(t, mgr)

	for _, topic := range []string{topicA, topicB, testTopic} {
		mgr.ensureFollowed(topic)
		waitFollowed(t, mgr, topic)
	}

	// Keep each topic busy in a different way for longer than evictAfter.
	end := time.Now().Add(2 * idle)
	for time.Now().Before(end) {
		if _, err := rpc.Coverage(ctx, &indexv1.CoverageRequest{Topic: topicA}); err != nil {
			t.Fatalf("Coverage: %v", err)
		}
		if _, err := rpc.Follow(ctx, &indexv1.FollowRequest{Topic: topicB}); err != nil {
			t.Fatalf("Follow: %v", err)
		}
		stream, err := rpc.Search(ctx, &indexv1.SearchRequest{Topic: testTopic, Pattern: "x"})
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
		drainSearch(t, stream)
		time.Sleep(idle / 10)
	}
	mgr.evictIdle()

	if mgr.followed(topicA) {
		t.Error("a topic only probed by Coverage was never evicted: Coverage reset its LRU clock")
	}
	if !mgr.followed(topicB) {
		t.Error("a topic a client keeps Following (the uncovered-search path) was evicted")
	}
	if !mgr.followed(testTopic) {
		t.Error("a topic a client keeps Searching was evicted")
	}
}
