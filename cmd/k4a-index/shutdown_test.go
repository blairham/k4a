// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/blairham/k4a/internal/index"
	"github.com/blairham/k4a/internal/state"
)

// TestRegisterAfterStopAllReleasesTheIndex is the regression for the untracked
// register goroutine: a follow that was still opening its index when stopAll
// ran used to land in the follow-set afterwards, so its worker was never
// stopped — no final flush, and its writer lock held for good.
func TestRegisterAfterStopAllReleasesTheIndex(t *testing.T) {
	fake := newFakeCluster(t, topicA)
	client := newKafkaClient(t, fake.ListenAddrs())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	root := state.NewRoot(t.TempDir())
	const cluster = "test-cluster"
	mgr := newManager(ctx, root, cluster, client, mustPolicy(t, topicA), managerConfig{})

	mgr.ensureFollowed(topicA) // registration starts in the background...
	mgr.stopAll()              // ...and shutdown begins before it lands

	if mgr.ensureFollowed(topicA) {
		t.Error("ensureFollowed accepted a topic after shutdown began")
	}
	if mgr.size() != 0 {
		t.Errorf("follow-set has %d entries after stopAll", mgr.size())
	}
	// stopAll must not return while a registration is still holding the index.
	ix, err := index.Open(root, cluster, topicA)
	if err != nil {
		t.Fatalf("a registration that finished after stopAll still holds the index: %v", err)
	}
	_ = ix.Close() //nolint:errcheck // test cleanup
}

// TestReRegisterDuringTeardownKeepsTheIndex is the regression for teardown's
// window between stopping a worker and deleting its directory: a follow of the
// same topic arriving there used to open the directory, which RemoveAll then
// deleted from under the new worker.
func TestReRegisterDuringTeardownKeepsTheIndex(t *testing.T) {
	fake := newFakeCluster(t, topicA)
	client := newKafkaClient(t, fake.ListenAddrs())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mgr := newManager(ctx, state.NewRoot(t.TempDir()), "test-cluster", client,
		mustPolicy(t, topicA), managerConfig{EvictAfter: 20 * time.Millisecond})
	defer mgr.stopAll()

	mgr.ensureFollowed(topicA)
	waitFollowed(t, mgr, topicA)
	time.Sleep(40 * time.Millisecond) // age past evictAfter

	// Inside the window: ask for the topic again and give the registration
	// every chance to land before teardown deletes the directory.
	var hooked atomic.Bool
	beforeTeardownRemove = func(topic string) {
		if topic != topicA || !hooked.CompareAndSwap(false, true) {
			return
		}
		mgr.ensureFollowed(topicA)
		deadline := time.Now().Add(time.Second)
		for !mgr.followed(topicA) && time.Now().Before(deadline) {
			time.Sleep(10 * time.Millisecond)
		}
	}
	t.Cleanup(func() { beforeTeardownRemove = nil })

	mgr.evictIdle()
	if !hooked.Load() {
		t.Fatal("teardown never reached the hook; the test exercised nothing")
	}

	w := mgr.peek(topicA)
	if w == nil {
		return // the re-follow was deferred past teardown: nothing to delete under it
	}
	if _, err := os.Stat(w.async.Inner().Dir()); err != nil {
		t.Fatalf("teardown deleted the directory of the worker that replaced it: %v", err)
	}
}

// stallingServer models a grpc.Server with one stream that never finishes:
// GracefulStop blocks until Stop is called.
type stallingServer struct {
	stopped chan struct{}
	stops   atomic.Int32
}

func (s *stallingServer) GracefulStop() { <-s.stopped }

func (s *stallingServer) Stop() {
	if s.stops.Add(1) == 1 {
		close(s.stopped)
	}
}

// TestStopServerBoundsAStalledStream is the regression for an unbounded
// GracefulStop: one stalled Search held shutdown open indefinitely.
func TestStopServerBoundsAStalledStream(t *testing.T) {
	srv := &stallingServer{stopped: make(chan struct{})}
	start := time.Now()
	stopServer(srv, 50*time.Millisecond)
	if srv.stops.Load() != 1 {
		t.Fatalf("Stop called %d times, want 1", srv.stops.Load())
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Fatalf("stopServer took %s with a 50ms grace", elapsed)
	}
}

// drainingServer finishes its graceful drain on its own.
type drainingServer struct{ stops atomic.Int32 }

func (s *drainingServer) GracefulStop() {}
func (s *drainingServer) Stop()         { s.stops.Add(1) }

func TestStopServerLetsAGracefulDrainFinish(t *testing.T) {
	srv := &drainingServer{}
	stopServer(srv, time.Minute)
	if srv.stops.Load() != 0 {
		t.Error("Stop was called although GracefulStop finished within the grace")
	}
}
