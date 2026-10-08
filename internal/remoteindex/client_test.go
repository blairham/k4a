// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package remoteindex

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/blairham/k4a/internal/indexwire/indexv1"
	"github.com/blairham/k4a/internal/kafka"
)

// t0/t1 bound the indexed window the fake daemon reports.
var (
	t0 = time.UnixMilli(1_000_000).UTC()
	t1 = time.UnixMilli(2_000_000).UTC()
)

// fakeIndex is a canned indexv1.IndexServer. following + covered control the
// coverage it reports; followed records Follow calls so the fallback path can
// be asserted.
type fakeIndex struct {
	indexv1.UnimplementedIndexServer
	followed  atomic.Int32
	following bool
	matches   int
}

func (f *fakeIndex) cov() *indexv1.Coverage {
	return &indexv1.Coverage{
		Following: f.following,
		Partitions: []*indexv1.PartitionCoverage{
			{Partition: 0, OffsetLo: 100, OffsetHi: 500, TimestampLoMs: t0.UnixMilli(), TimestampHiMs: t1.UnixMilli()},
		},
	}
}

func (f *fakeIndex) Coverage(context.Context, *indexv1.CoverageRequest) (*indexv1.CoverageResponse, error) {
	return &indexv1.CoverageResponse{Coverage: f.cov()}, nil
}

func (f *fakeIndex) Search(_ *indexv1.SearchRequest, stream grpc.ServerStreamingServer[indexv1.SearchEvent]) error {
	if err := stream.Send(&indexv1.SearchEvent{Event: &indexv1.SearchEvent_Coverage{Coverage: f.cov()}}); err != nil {
		return err
	}
	for i := 0; i < f.matches; i++ {
		if err := stream.Send(&indexv1.SearchEvent{Event: &indexv1.SearchEvent_Match{Match: &indexv1.Match{
			Partition: 0, Offset: int64(200 + i), TimestampMs: 1_500_000, Key: "k", Value: "needle",
		}}}); err != nil {
			return err
		}
	}
	return stream.Send(
		&indexv1.SearchEvent{
			Event: &indexv1.SearchEvent_Progress{Progress: &indexv1.Progress{Matched: int64(f.matches), Done: true}},
		},
	)
}

func (f *fakeIndex) Follow(context.Context, *indexv1.FollowRequest) (*indexv1.FollowResponse, error) {
	f.followed.Add(1)
	return &indexv1.FollowResponse{Accepted: true}, nil
}

func dialFake(t *testing.T, fake indexv1.IndexServer) *Client {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	indexv1.RegisterIndexServer(srv, fake)
	go srv.Serve(lis) //nolint:errcheck // stopped in cleanup
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() }) //nolint:errcheck // test cleanup
	return &Client{conn: conn, idx: indexv1.NewIndexClient(conn)}
}

// covering params fall inside [t0, t1] so CanServe returns true.
func coveringParams() kafka.SearchParams {
	return kafka.SearchParams{Since: t0.Add(time.Millisecond), Until: t1.Add(-time.Millisecond)}
}

// recordingNext is a fallback SearchFunc that records whether it ran and
// returns closed channels.
func recordingNext(called *atomic.Bool) SearchFunc {
	return func(context.Context, string, kafka.SearchParams) (
		<-chan kafka.ConsumedMessage, <-chan kafka.DeepSearchProgress, <-chan error,
	) {
		called.Store(true)
		m := make(chan kafka.ConsumedMessage)
		p := make(chan kafka.DeepSearchProgress)
		e := make(chan error)
		close(m)
		close(p)
		close(e)
		return m, p, e
	}
}

func drain(
	t *testing.T,
	m <-chan kafka.ConsumedMessage,
	p <-chan kafka.DeepSearchProgress,
	e <-chan error,
) (int, kafka.DeepSearchProgress) {
	t.Helper()
	var (
		n    int
		last kafka.DeepSearchProgress
	)
	for m != nil || p != nil || e != nil {
		select {
		case _, ok := <-m:
			if !ok {
				m = nil
				continue
			}
			n++
		case pr, ok := <-p:
			if !ok {
				p = nil
				continue
			}
			last = pr
		case err, ok := <-e:
			if !ok {
				e = nil
				continue
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("timed out draining search channels")
		}
	}
	return n, last
}

func TestWrapCoveredServesFromIndex(t *testing.T) {
	t.Parallel()
	client := dialFake(t, &fakeIndex{following: true, matches: 3})
	var nextCalled atomic.Bool

	m, p, e := client.Wrap(recordingNext(&nextCalled))(context.Background(), "demo", coveringParams())
	n, last := drain(t, m, p, e)

	if nextCalled.Load() {
		t.Error("covered query must NOT fall through to the local scan")
	}
	if n != 3 {
		t.Errorf("matches = %d, want 3 (from the shared index)", n)
	}
	if last.Source != kafka.SourceSharedIndex {
		t.Errorf("Source = %v, want SourceSharedIndex", last.Source)
	}
}

func TestWrapUnfollowedFallsBackAndFollows(t *testing.T) {
	t.Parallel()
	fake := &fakeIndex{following: false}
	client := dialFake(t, fake)
	var nextCalled atomic.Bool

	m, p, e := client.Wrap(recordingNext(&nextCalled))(context.Background(), "cold", coveringParams())
	drain(t, m, p, e)

	if !nextCalled.Load() {
		t.Error("an unfollowed topic must fall through to the local scan")
	}
	// Follow is fire-and-forget; give the goroutine a moment.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && fake.followed.Load() == 0 {
		time.Sleep(20 * time.Millisecond)
	}
	if fake.followed.Load() == 0 {
		t.Error("a cold search should fire a fire-and-forget Follow to warm the topic")
	}
}

func TestWrapUnreachableFallsBack(t *testing.T) {
	t.Parallel()
	// Dial a closed port: RPCs fail fast, so covers() returns false → scan.
	conn, err := grpc.NewClient("127.0.0.1:1", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	t.Cleanup(func() { conn.Close() }) //nolint:errcheck // test cleanup
	client := &Client{conn: conn, idx: indexv1.NewIndexClient(conn)}

	var nextCalled atomic.Bool
	m, p, e := client.Wrap(recordingNext(&nextCalled))(context.Background(), "any", coveringParams())
	drain(t, m, p, e)
	if !nextCalled.Load() {
		t.Error("an unreachable daemon must fall through to the local scan")
	}
}
