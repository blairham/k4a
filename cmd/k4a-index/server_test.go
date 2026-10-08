// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/blairham/k4a/internal/index"
	"github.com/blairham/k4a/internal/indexwire"
	"github.com/blairham/k4a/internal/indexwire/indexv1"
	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/state"
)

const (
	testTopic     = "demo.topic.v1"
	onDemandTopic = "demo.ondemand.v1"
	deniedTopic   = "secret.orders.v1"
	recordCount   = 80 // < the consumer's 100-record initial fetch, so all are indexed
)

// TestDaemonServesNetworkedQuery is the original networked-query proof
// (docs/design/shared-index-service.md): stand the daemon up against an
// in-memory Kafka (kfake), produce a known set of records, then a real gRPC
// client asks it for Search + Coverage and gets back exactly what the local
// index would — including the load-bearing coverage-decision parity. Here the
// topic is warmed via the follow-set manager's pre-follow path.
func TestDaemonServesNetworkedQuery(t *testing.T) {
	fake := newFakeCluster(t, testTopic)
	seeds := fake.ListenAddrs()

	wantNeedles := produceRecords(t, seeds, testTopic, recordCount)

	client := newKafkaClient(t, seeds)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mgr := newManager(ctx, state.NewRoot(t.TempDir()), "test-cluster", client,
		mustPolicy(t, testTopic), managerConfig{})
	defer mgr.stopAll()
	mgr.preFollow([]string{testTopic})

	worker := mgr.lookup(testTopic)
	if worker == nil {
		t.Fatal("pre-follow did not register the topic")
	}
	worker.waitReady(ctx)
	waitIndexed(t, worker, recordCount)

	client2 := dialServer(t, mgr)

	t.Run("coverage reports the followed topic", func(t *testing.T) {
		resp, err := client2.Coverage(ctx, &indexv1.CoverageRequest{Topic: testTopic})
		if err != nil {
			t.Fatalf("Coverage: %v", err)
		}
		if !resp.GetCoverage().GetFollowing() {
			t.Error("expected following=true for the followed topic")
		}
		if len(resp.GetCoverage().GetPartitions()) == 0 {
			t.Fatal("expected non-empty per-partition coverage")
		}

		// Coverage-decision parity: the verdict a client reaches on a
		// Bookkeeper rebuilt from this wire coverage must match the daemon's
		// own — for the identical params, whatever the verdict.
		book := worker.coverage()
		params := index.QueryParams{
			Since: time.UnixMilli(book.OldestTimePerPartition[0]).UTC(),
			Until: time.UnixMilli(book.NewestTimePerPartition[0]).UTC(),
		}
		wantOK, _ := worker.canServe(params)
		gotOK, _ := indexwire.BookkeeperFromCoverage(resp.GetCoverage()).CanServe(params)
		if wantOK != gotOK {
			t.Fatalf("coverage decision diverged over the wire: daemon=%v client=%v", wantOK, gotOK)
		}
	})

	t.Run("search streams coverage, matches, then done", func(t *testing.T) {
		stream, err := client2.Search(ctx, &indexv1.SearchRequest{
			Topic:   testTopic,
			Pattern: "needle",
			Scopes:  []string{"value"},
		})
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
		cov, matches, done := drainSearch(t, stream)
		if !cov {
			t.Error("expected a leading coverage frame")
		}
		if !done {
			t.Error("expected a terminal progress frame with done=true")
		}
		if matches != wantNeedles {
			t.Errorf("match count = %d, want %d", matches, wantNeedles)
		}
	})

	t.Run("follow accepts an allowlisted topic, refuses a denied one", func(t *testing.T) {
		if resp, err := client2.Follow(ctx, &indexv1.FollowRequest{Topic: testTopic}); err != nil || !resp.GetAccepted() {
			t.Errorf("Follow(%s) = %+v, %v; want accepted", testTopic, resp, err)
		}
		resp, err := client2.Follow(ctx, &indexv1.FollowRequest{Topic: deniedTopic})
		if err != nil {
			t.Fatalf("Follow(denied): %v", err)
		}
		if resp.GetAccepted() {
			t.Error("expected a non-allowlisted topic to be refused")
		}
		if resp.GetReason() == "" {
			t.Error("expected a refusal reason")
		}
	})

	t.Run("denied topic reports no coverage and is never followed", func(t *testing.T) {
		resp, err := client2.Coverage(ctx, &indexv1.CoverageRequest{Topic: deniedTopic})
		if err != nil {
			t.Fatalf("Coverage(denied): %v", err)
		}
		if resp.GetCoverage().GetFollowing() {
			t.Error("expected following=false for a denied topic")
		}
		if mgr.followed(deniedTopic) {
			t.Error("a denied topic must never enter the follow-set")
		}
	})
}

// TestDemandDrivenFollow is the Phase 2 proof: a Search for an allowlisted topic
// nobody has followed yet reports no coverage for THAT call (the client scans)
// but registers the topic as a side effect, so a subsequent search is served
// from the warm index — warm-on-first-search. A denied topic is never followed.
func TestDemandDrivenFollow(t *testing.T) {
	fake := newFakeCluster(t, onDemandTopic)
	seeds := fake.ListenAddrs()
	wantNeedles := produceRecords(t, seeds, onDemandTopic, recordCount)

	client := newKafkaClient(t, seeds)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Allowlist admits the on-demand topic but NOT the denied one.
	mgr := newManager(ctx, state.NewRoot(t.TempDir()), "test-cluster", client,
		mustPolicy(t, onDemandTopic), managerConfig{})
	defer mgr.stopAll()
	client2 := dialServer(t, mgr)

	// First search: cold topic → no coverage, client scans, follow kicks off.
	stream, err := client2.Search(ctx, &indexv1.SearchRequest{Topic: onDemandTopic, Pattern: "needle"})
	if err != nil {
		t.Fatalf("first Search: %v", err)
	}
	cov, matches, done := drainSearch(t, stream)
	if !cov || !done {
		t.Fatalf("first search: cov=%v done=%v, want both true", cov, done)
	}
	if matches != 0 {
		t.Errorf("first search matched %d, want 0 (topic not yet warm)", matches)
	}

	// The side-effect registration warms the topic; wait for it.
	worker := waitFollowed(t, mgr, onDemandTopic)
	worker.waitReady(ctx)
	waitIndexed(t, worker, recordCount)

	// Second search: now served from the warm index.
	stream2, err := client2.Search(ctx, &indexv1.SearchRequest{
		Topic:   onDemandTopic,
		Pattern: "needle",
		Scopes:  []string{"value"},
	})
	if err != nil {
		t.Fatalf("second Search: %v", err)
	}
	cov2, matches2, done2 := drainSearch(t, stream2)
	if !cov2 || !done2 {
		t.Fatalf("second search: cov=%v done=%v, want both true", cov2, done2)
	}
	if matches2 != wantNeedles {
		t.Errorf("second search matched %d, want %d", matches2, wantNeedles)
	}

	// A denied topic must not register even when searched.
	if _, err := client2.Search(ctx, &indexv1.SearchRequest{Topic: deniedTopic}); err != nil {
		t.Fatalf("Search(denied): %v", err)
	}
	if mgr.followed(deniedTopic) {
		t.Error("a denied topic must never be followed on search")
	}
}

// newFakeCluster spins up a single-broker kfake seeding the given topics.
func newFakeCluster(t *testing.T, topics ...string) *kfake.Cluster {
	t.Helper()
	fake, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topics...))
	if err != nil {
		t.Fatalf("kfake cluster: %v", err)
	}
	t.Cleanup(fake.Close)
	return fake
}

// newKafkaClient dials the fake with plaintext auth.
func newKafkaClient(t *testing.T, seeds []string) *kafka.Client {
	t.Helper()
	client, err := kafka.NewClient(kafka.AuthConfig{Method: kafka.AuthPlaintext}, join(seeds))
	if err != nil {
		t.Fatalf("kafka client: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

// mustPolicy builds an allowlist from the given patterns or fails the test.
func mustPolicy(t *testing.T, patterns ...string) allowPolicy {
	t.Helper()
	p, err := newAllowPolicy(patterns)
	if err != nil {
		t.Fatalf("newAllowPolicy: %v", err)
	}
	return p
}

// waitFollowed blocks until the manager has registered the topic (the async
// side-effect of a cold search), returning its worker.
func waitFollowed(t *testing.T, mgr *manager, topic string) *topicWorker {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if w := mgr.lookup(topic); w != nil {
			return w
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("topic %q was never followed", topic)
	return nil
}

// produceRecords writes recordCount records to the topic, half tagged with the
// searchable token "needle". Returns how many carry it.
func produceRecords(t *testing.T, seeds []string, topic string, n int) int {
	t.Helper()
	cl, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.DefaultProduceTopic(topic))
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer cl.Close()

	needles := 0
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for i := range n {
		value := `{"tag":"straw"}`
		if i%2 == 0 {
			value = `{"tag":"needle"}`
			needles++
		}
		rec := &kgo.Record{Key: []byte(fmt.Sprintf("order-%03d", i)), Value: []byte(value)}
		if err := cl.ProduceSync(ctx, rec).FirstErr(); err != nil {
			t.Fatalf("produce %d: %v", i, err)
		}
	}
	return needles
}

// waitIndexed blocks until the async indexer has committed at least want
// documents, so the query assertions see a warm index.
func waitIndexed(t *testing.T, w *topicWorker, want int) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if n, err := w.async.Inner().DocCount(); err == nil && int(n) >= want {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	n, _ := w.async.Inner().DocCount()
	t.Fatalf("index did not warm: DocCount=%d want>=%d", n, want)
}

// dialServer serves the manager over a real localhost gRPC listener and returns
// a connected client. Cleanup is registered on t.
func dialServer(t *testing.T, mgr *manager) indexv1.IndexClient {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	indexv1.RegisterIndexServer(srv, newIndexServer(mgr))
	go srv.Serve(lis) //nolint:errcheck // stopped in cleanup
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() }) //nolint:errcheck // test cleanup
	return indexv1.NewIndexClient(conn)
}

// drainSearch consumes a Search stream, reporting whether it saw a leading
// coverage frame, the match count, and whether it ended with done=true.
func drainSearch(
	t *testing.T,
	stream grpc.ServerStreamingClient[indexv1.SearchEvent],
) (coverage bool, matches int, done bool) {
	t.Helper()
	first := true
	for {
		ev, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return coverage, matches, done
		}
		if err != nil {
			t.Fatalf("stream recv: %v", err)
		}
		switch ev.GetEvent().(type) {
		case *indexv1.SearchEvent_Coverage:
			if first {
				coverage = true
			}
		case *indexv1.SearchEvent_Match:
			matches++
		case *indexv1.SearchEvent_Progress:
			if ev.GetProgress().GetDone() {
				done = true
			}
		}
		first = false
	}
}

func join(addrs []string) string {
	out := ""
	for i, a := range addrs {
		if i > 0 {
			out += ","
		}
		out += a
	}
	return out
}

// TestSearchReportsCappedAtLimit is the wire-level regression proof for issue
// the daemon used to truncate at the query limit and still close the
// stream with a bare done=true, so a partial answer was indistinguishable
// from a complete one on the client side.
func TestSearchReportsCappedAtLimit(t *testing.T) {
	fake := newFakeCluster(t, testTopic)
	seeds := fake.ListenAddrs()
	wantNeedles := produceRecords(t, seeds, testTopic, recordCount)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	mgr := newManager(ctx, state.NewRoot(t.TempDir()), "test-cluster", newKafkaClient(t, seeds),
		mustPolicy(t, testTopic), managerConfig{})
	defer mgr.stopAll()
	mgr.preFollow([]string{testTopic})

	worker := mgr.lookup(testTopic)
	if worker == nil {
		t.Fatal("pre-follow did not register the topic")
	}
	worker.waitReady(ctx)
	waitIndexed(t, worker, recordCount)

	client := dialServer(t, mgr)

	t.Run("limit binds → capped", func(t *testing.T) {
		const limit = 5
		stream, err := client.Search(ctx, &indexv1.SearchRequest{
			Topic:   testTopic,
			Pattern: "needle",
			Scopes:  []string{"value"},
			Limit:   limit,
		})
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
		matches, capped := drainSearchCapped(t, stream)
		if matches != limit {
			t.Errorf("streamed %d matches, want the %d the limit allows", matches, limit)
		}
		if !capped {
			t.Errorf("terminal progress reports capped=false after truncating %d matches to %d",
				wantNeedles, limit)
		}
	})

	t.Run("limit does not bind → not capped", func(t *testing.T) {
		stream, err := client.Search(ctx, &indexv1.SearchRequest{
			Topic:   testTopic,
			Pattern: "needle",
			Scopes:  []string{"value"},
			Limit:   int32(wantNeedles * 10),
		})
		if err != nil {
			t.Fatalf("Search: %v", err)
		}
		matches, capped := drainSearchCapped(t, stream)
		if matches != wantNeedles {
			t.Errorf("streamed %d matches, want all %d", matches, wantNeedles)
		}
		if capped {
			t.Error("terminal progress reports capped=true even though every match fit")
		}
	})
}

// drainSearchCapped drains a search stream, returning the match count and the
// capped flag from the terminal progress frame.
func drainSearchCapped(
	t *testing.T,
	stream grpc.ServerStreamingClient[indexv1.SearchEvent],
) (matches int, capped bool) {
	t.Helper()
	for {
		ev, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return matches, capped
		}
		if err != nil {
			t.Fatalf("stream recv: %v", err)
		}
		switch ev.GetEvent().(type) {
		case *indexv1.SearchEvent_Match:
			matches++
		case *indexv1.SearchEvent_Progress:
			if ev.GetProgress().GetDone() {
				capped = ev.GetProgress().GetCapped()
			}
		}
	}
}
