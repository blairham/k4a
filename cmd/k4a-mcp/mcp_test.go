// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"net"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/blairham/k4a/internal/indexwire/indexv1"
)

// fakeIndex is a canned indexv1.IndexServer so the MCP tool handlers can be
// exercised over REAL gRPC without standing up the daemon + Kafka. following
// controls whether it reports the topic as followed (served_from_index).
type fakeIndex struct {
	indexv1.UnimplementedIndexServer
	following bool
	matches   int
}

func (f *fakeIndex) coverage() *indexv1.Coverage {
	return &indexv1.Coverage{
		Following: f.following,
		Partitions: []*indexv1.PartitionCoverage{
			{Partition: 0, OffsetLo: 100, OffsetHi: 500, TimestampLoMs: 1_000_000, TimestampHiMs: 2_000_000},
		},
	}
}

func (f *fakeIndex) Search(_ *indexv1.SearchRequest, stream grpc.ServerStreamingServer[indexv1.SearchEvent]) error {
	if err := stream.Send(&indexv1.SearchEvent{
		Event: &indexv1.SearchEvent_Coverage{Coverage: f.coverage()},
	}); err != nil {
		return err
	}
	for i := 0; i < f.matches; i++ {
		if err := stream.Send(&indexv1.SearchEvent{Event: &indexv1.SearchEvent_Match{Match: &indexv1.Match{
			Partition: 0, Offset: int64(200 + i), TimestampMs: 1_500_000, Key: "k", Value: `{"tag":"needle"}`,
		}}}); err != nil {
			return err
		}
	}
	return stream.Send(&indexv1.SearchEvent{
		Event: &indexv1.SearchEvent_Progress{Progress: &indexv1.Progress{Matched: int64(f.matches), Done: true}},
	})
}

func (f *fakeIndex) Coverage(_ context.Context, _ *indexv1.CoverageRequest) (*indexv1.CoverageResponse, error) {
	return &indexv1.CoverageResponse{Coverage: f.coverage()}, nil
}

func dialFake(t *testing.T, fake indexv1.IndexServer) indexv1.IndexClient {
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
	return indexv1.NewIndexClient(conn)
}

func TestSearchHandlerFollowedTopic(t *testing.T) {
	t.Parallel()
	client := dialFake(t, &fakeIndex{following: true, matches: 3})
	ctx := context.Background()

	_, out, err := searchHandler(client)(ctx, nil, mcpSearchInput{Topic: "demo", Query: "needle", Scope: "value"})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if !out.ServedFromIndex {
		t.Error("expected served_from_index=true for a followed topic")
	}
	if out.Coverage == nil || len(out.Coverage.Partitions) != 1 {
		t.Fatalf("expected coverage with one partition, got %+v", out.Coverage)
	}
	if out.Matched != 3 || len(out.Matches) != 3 {
		t.Errorf("matched=%d matches=%d, want 3/3", out.Matched, len(out.Matches))
	}
	if out.Matches[0].Value != `{"tag":"needle"}` {
		t.Errorf("unexpected match value %q", out.Matches[0].Value)
	}
	if out.Matches[0].Timestamp == "" {
		t.Error("expected an RFC3339 timestamp on the match")
	}
}

func TestSearchHandlerUnfollowedTopic(t *testing.T) {
	t.Parallel()
	client := dialFake(t, &fakeIndex{following: false, matches: 0})

	_, out, err := searchHandler(client)(context.Background(), nil, mcpSearchInput{Topic: "cold", Query: "x"})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if out.ServedFromIndex {
		t.Error("expected served_from_index=false for an unfollowed topic (client should scan)")
	}
}

func TestCoverageHandler(t *testing.T) {
	t.Parallel()
	client := dialFake(t, &fakeIndex{following: true})

	_, out, err := coverageHandler(client)(context.Background(), nil, mcpCoverageInput{Topic: "demo"})
	if err != nil {
		t.Fatalf("coverage: %v", err)
	}
	if !out.ServedFromIndex || out.Coverage == nil {
		t.Fatalf("expected following coverage, got served=%v cov=%+v", out.ServedFromIndex, out.Coverage)
	}
	if out.Coverage.Partitions[0].OffsetHi != 500 {
		t.Errorf("offset_hi = %d, want 500", out.Coverage.Partitions[0].OffsetHi)
	}
}

func TestMissingTopicIsAnError(t *testing.T) {
	t.Parallel()
	client := dialFake(t, &fakeIndex{})
	if _, _, err := searchHandler(client)(context.Background(), nil, mcpSearchInput{Query: "x"}); err == nil {
		t.Error("expected an error when topic is empty")
	}
}

func TestBuildSearchRequest(t *testing.T) {
	t.Parallel()

	req, err := buildSearchRequest(mcpSearchInput{
		Topic: "t", Query: "Foo", Scope: "key+value+headers", Since: "1h", Limit: 50,
	})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if req.GetPattern() != "(?i)Foo" {
		t.Errorf("pattern = %q, want (?i)Foo (case-insensitive by default)", req.GetPattern())
	}
	if len(req.GetScopes()) != 3 {
		t.Errorf("scopes = %v, want key+value+headers", req.GetScopes())
	}
	if req.GetSinceMs() == 0 {
		t.Error("since=1h should resolve to a non-zero Unix-ms bound")
	}
	if req.GetLimit() != 50 {
		t.Errorf("limit = %d, want 50", req.GetLimit())
	}
	// A malformed regex falls back to a quoted literal rather than erroring.
	req2, err := buildSearchRequest(mcpSearchInput{Topic: "t", Query: "a(b"})
	if err != nil {
		t.Fatalf("build (bad regex): %v", err)
	}
	if req2.GetPattern() == "" {
		t.Error("malformed regex should fall back to a literal pattern, not empty")
	}
}

// ensure time isn't flagged unused if the file's assertions change.
var _ = time.Now

// wedgedIndex simulates the drain wedge: the daemon accepts the call and
// then never answers — exactly what a drain goroutine blocked on the indexer
// mutex looks like from the wire.
type wedgedIndex struct {
	indexv1.UnimplementedIndexServer
}

func (w *wedgedIndex) Search(_ *indexv1.SearchRequest, stream grpc.ServerStreamingServer[indexv1.SearchEvent]) error {
	<-stream.Context().Done()
	return stream.Context().Err()
}

func (w *wedgedIndex) Coverage(ctx context.Context, _ *indexv1.CoverageRequest) (*indexv1.CoverageResponse, error) {
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestHandlersTimeOutAgainstAWedgedDaemon(t *testing.T) {
	t.Parallel()
	client := dialFake(t, &wedgedIndex{})

	start := time.Now()
	_, _, err := searchHandlerWithTimeout(client, 200*time.Millisecond)(
		context.Background(), nil, mcpSearchInput{Topic: "demo"},
	)
	if err == nil {
		t.Fatal("expected a timeout error from a wedged daemon")
	}
	if !strings.Contains(err.Error(), "did not respond") {
		t.Errorf("error should attribute the timeout to the daemon, got: %v", err)
	}
	if elapsed := time.Since(start); elapsed > 5*time.Second {
		t.Errorf("search error took %s — the deadline was not applied", elapsed)
	}

	_, _, err = coverageHandlerWithTimeout(client, 200*time.Millisecond)(
		context.Background(), nil, mcpCoverageInput{Topic: "demo"},
	)
	if err == nil || !strings.Contains(err.Error(), "did not respond") {
		t.Errorf("coverage: expected an attributed timeout, got %v", err)
	}
}

// TestSearchHandlerPatternRecall pins the recall honesty field:
// a pattern the tokenized index answers with full recall reports "full"; one
// it can only best-effort (a stop word — its token is never indexed) reports
// "best_effort" with a reason, telling the agent a zero-match answer is not
// proof of absence. This frontend has no scan to fall back to.
func TestSearchHandlerPatternRecall(t *testing.T) {
	t.Parallel()
	client := dialFake(t, &fakeIndex{following: true, matches: 3})

	_, out, err := searchHandler(client)(context.Background(), nil, mcpSearchInput{Topic: "demo", Query: "order"})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if out.PatternRecall != "full" || out.PatternRecallNote != "" {
		t.Errorf("recall=%q note=%q, want full recall with no note", out.PatternRecall, out.PatternRecallNote)
	}

	_, out, err = searchHandler(client)(context.Background(), nil, mcpSearchInput{Topic: "demo", Query: "the"})
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if out.PatternRecall != "best_effort" || out.PatternRecallNote == "" {
		t.Errorf("recall=%q note=%q, want best_effort with a reason", out.PatternRecall, out.PatternRecallNote)
	}
}
