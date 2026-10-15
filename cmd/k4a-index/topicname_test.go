// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/blairham/k4a/internal/indexwire/indexv1"
	"github.com/blairham/k4a/internal/state"
)

// TestIllegalTopicNameNeverReachesTheIndex is the regression for a client
// asking a broad-allowlist daemon to follow "." or "..". path.Match("*", "..")
// is true, so without a name check the allowlist accepted them, register() ran
// index.Open on <root>/index/<cluster>/.. — the index root itself — and a later
// eviction would RemoveAll that directory and every other topic's index with it.
func TestIllegalTopicNameNeverReachesTheIndex(t *testing.T) {
	fake := newFakeCluster(t, testTopic)
	client := newKafkaClient(t, fake.ListenAddrs())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	rootDir := t.TempDir()
	root := state.NewRoot(rootDir)
	const cluster = "test-cluster"
	mgr := newManager(ctx, root, cluster, client, mustPolicy(t, "*"), managerConfig{})
	defer mgr.stopAll()
	rpc := dialServer(t, mgr)

	bad := []string{".", "..", "a/b", "../x", strings.Repeat("a", 250), "bad name", "tab\tname"}
	for _, topic := range bad {
		resp, err := rpc.Follow(ctx, &indexv1.FollowRequest{Topic: topic})
		if err != nil {
			t.Fatalf("Follow(%q): %v", topic, err)
		}
		if resp.GetAccepted() {
			t.Errorf("Follow(%q) accepted; an illegal Kafka topic name must be refused", topic)
		}
		stream, err := rpc.Search(ctx, &indexv1.SearchRequest{Topic: topic, Pattern: "x"})
		if err != nil {
			t.Fatalf("Search(%q): %v", topic, err)
		}
		drainSearch(t, stream)
	}

	// Registration is asynchronous; give a wrongly-accepted name time to reach
	// index.Open, which creates a "lock" file in the directory it resolves to.
	indexRoot := root.File(state.SubsystemIndex)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		for _, dir := range []string{indexRoot, filepath.Join(indexRoot, cluster), filepath.Dir(indexRoot)} {
			if _, err := os.Stat(filepath.Join(dir, "lock")); err == nil {
				t.Fatalf("an illegal topic name opened an index at %s", dir)
			}
		}
		for _, topic := range bad {
			if mgr.followed(topic) {
				t.Fatalf("illegal topic %q entered the follow-set", topic)
			}
		}
		time.Sleep(20 * time.Millisecond)
	}

	// A legal name under the same broad allowlist still follows.
	if resp, err := rpc.Follow(ctx, &indexv1.FollowRequest{Topic: testTopic}); err != nil || !resp.GetAccepted() {
		t.Fatalf("Follow(%q) = %+v, %v; want accepted", testTopic, resp, err)
	}
	waitFollowed(t, mgr, testTopic)
}
