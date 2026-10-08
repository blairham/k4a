// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"time"

	"github.com/charmbracelet/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blairham/k4a/internal/indexwire"
	"github.com/blairham/k4a/internal/indexwire/indexv1"
)

// indexServer implements the indexv1.IndexServer contract over the demand-driven
// follow-set manager. A followed topic is served from its warm index; an
// unfollowed-but-allowed topic is registered as a side effect of the first
// Search/Follow (warm-on-first-search) and reports no coverage for that call so
// the client scans; a denied topic simply comes back following=false forever and
// the client always scans it.
type indexServer struct {
	indexv1.UnimplementedIndexServer
	mgr *manager
}

func newIndexServer(mgr *manager) *indexServer {
	return &indexServer{mgr: mgr}
}

// Search streams a leading Coverage event, then the indexed matches
// (newest-first), then a terminal Progress with done=true. The daemon never
// decides covered-vs-scan itself — it reports coverage and returns what it has
// indexed; the client applies the identical CanServe logic and tops up any
// uncovered tail with its own scan (docs/design/shared-index-service.md §4).
func (s *indexServer) Search(req *indexv1.SearchRequest, stream indexv1.Index_SearchServer) error {
	start := time.Now()
	topic := req.GetTopic()
	log.Info(
		"search",
		"topic", topic,
		"pattern", req.GetPattern(),
		"scopes", req.GetScopes(),
		"since_ms", req.GetSinceMs(),
		"until_ms", req.GetUntilMs(),
		"partitions", req.GetPartitions(),
		"limit", req.GetLimit(),
	)

	w := s.mgr.lookup(topic)
	if w == nil {
		// Not followed yet. If the allowlist permits it, begin following as a
		// side effect so the NEXT search is warm; this call still reports no
		// coverage and the client scans (warm-on-first-search). A denied topic
		// just reports no coverage and is never followed.
		// contextcheck: registration deliberately runs on the daemon-lifetime ctx,
		// not this request's stream ctx — the follow must OUTLIVE the search that
		// triggered it (warm-on-first-search), so the request ctx must not thread in.
		if s.mgr.ensureFollowed(topic) { //nolint:contextcheck // see comment above
			log.Info("search: topic not followed — now warming, client scans this time", "topic", topic)
		} else {
			log.Warn("search: topic not allowlisted → client will scan (never followed)", "topic", topic)
		}
		if err := sendCoverage(stream, &indexv1.Coverage{Following: false}); err != nil {
			return err
		}
		return sendDone(stream, 0, false)
	}

	params, err := indexwire.RequestToQueryParams(req)
	if err != nil {
		log.Error("search: bad request", "topic", topic, "err", err)
		return status.Errorf(codes.InvalidArgument, "invalid search request: %v", err)
	}

	// Report the coverage decision so we can see WHY a client did/didn't
	// end up scanning (the client runs the same CanServe on this snapshot).
	book := w.coverage()
	canServe, reason := w.canServe(params)
	log.Info(
		"search coverage",
		"topic", topic,
		"can_serve", canServe,
		"reason", reason,
		"cov_partitions", len(book.NewestOffsetPerPartition),
	)

	cov := indexwire.CoverageFromBookkeeper(book)
	cov.Following = true
	// A plain assignment rather than an if-init: an `err :=` there would
	// shadow the err the query below reuses.
	err = sendCoverage(stream, cov)
	if err != nil {
		return err
	}

	res, err := w.query(stream.Context(), params)
	if err != nil {
		log.Error("search: query failed", "topic", topic, "err", err)
		return status.Errorf(codes.Internal, "query: %v", err)
	}
	for _, m := range res.Matches {
		ev := &indexv1.SearchEvent{Event: &indexv1.SearchEvent_Match{Match: indexwire.MatchFromConsumed(m)}}
		if err := stream.Send(ev); err != nil {
			return err
		}
	}
	log.Info(
		"search done",
		"topic", topic,
		"matches", len(res.Matches),
		"capped", res.Capped,
		"can_serve", canServe,
		"dur", time.Since(start).Round(time.Millisecond),
	)
	return sendDone(stream, int64(len(res.Matches)), res.Capped)
}

// Coverage reports a topic's indexed ranges without running a query — used by
// the client's Topics view to render an "indexed?" column.
func (s *indexServer) Coverage(_ context.Context, req *indexv1.CoverageRequest) (*indexv1.CoverageResponse, error) {
	topic := req.GetTopic()
	w := s.mgr.lookup(topic)
	if w == nil {
		// Coverage is a read-only probe (the Topics view's "indexed?" column) —
		// it reports the current state but does NOT trigger a follow. Only a
		// Search (or an explicit Follow) warms a topic, so probing coverage for
		// every listed topic can't stampede the follow-set.
		log.Info("coverage", "topic", topic, "following", false)
		return &indexv1.CoverageResponse{Coverage: &indexv1.Coverage{Following: false}}, nil
	}
	book := w.coverage()
	cov := indexwire.CoverageFromBookkeeper(book)
	cov.Following = true
	log.Info("coverage", "topic", topic, "following", true, "partitions", len(book.NewestOffsetPerPartition))
	return &indexv1.CoverageResponse{Coverage: cov}, nil
}

// Follow registers a topic without querying it — the client's fire-and-forget
// warm-up when it scans a cold topic itself. It runs the same allowlist gate as
// Search: an allowed topic is accepted (and begins following if it wasn't
// already); a denied topic is refused with a reason, and the daemon never
// persists its payloads.
func (s *indexServer) Follow(_ context.Context, req *indexv1.FollowRequest) (*indexv1.FollowResponse, error) {
	topic := req.GetTopic()
	// Registration runs on the daemon-lifetime ctx, not this RPC's ctx — the
	// whole point of Follow is to warm a topic that outlives this call.
	if s.mgr.ensureFollowed(topic) { //nolint:contextcheck // intentional: see comment
		log.Info("follow", "topic", topic, "accepted", true)
		return &indexv1.FollowResponse{Accepted: true}, nil
	}
	log.Warn("follow refused (not allowlisted)", "topic", topic)
	return &indexv1.FollowResponse{
		Accepted: false,
		Reason:   "topic is not in the daemon's follow allowlist (--allow)",
	}, nil
}

func sendCoverage(stream indexv1.Index_SearchServer, cov *indexv1.Coverage) error {
	return stream.Send(&indexv1.SearchEvent{Event: &indexv1.SearchEvent_Coverage{Coverage: cov}})
}

// sendDone closes the stream. capped must be reported honestly: a truncated
// answer that arrives as a clean completion is the whole of the truncation fix.
func sendDone(stream indexv1.Index_SearchServer, matched int64, capped bool) error {
	return stream.Send(&indexv1.SearchEvent{
		Event: &indexv1.SearchEvent_Progress{
			Progress: &indexv1.Progress{Matched: matched, Done: true, Capped: capped},
		},
	})
}
