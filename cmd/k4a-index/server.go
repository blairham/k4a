// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"time"

	"github.com/charmbracelet/log"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/blairham/k4a/internal/endpointauth"
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
	// authz is per-caller authorization (--authz-policy). Its zero value
	// admits every caller the Guard let in, the behavior before Phase 5.
	authz endpointauth.Authorizer
	// maxLimit bounds the matches one Search returns, whatever the request
	// asks for: a single request must not be able to make the daemon
	// materialize and stream an arbitrarily large result.
	maxLimit int
}

// defaultMaxSearchLimit matches the client's own default cap, so an
// unconfigured daemon lowers nothing a stock k4a sends.
const defaultMaxSearchLimit = 10_000

func newIndexServer(mgr *manager) *indexServer {
	return &indexServer{mgr: mgr, maxLimit: defaultMaxSearchLimit}
}

// clampLimit lowers a missing or oversized request limit to maxLimit. The
// query then reports capped honestly when the bound binds, exactly as for a
// limit the client chose.
func (s *indexServer) clampLimit(req *indexv1.SearchRequest) {
	if s.maxLimit <= 0 {
		return
	}
	if l := req.GetLimit(); l <= 0 || int(l) > s.maxLimit {
		req.Limit = int32(s.maxLimit) //nolint:gosec // validated positive; a flag-sized int
	}
}

// Search streams a leading Coverage event, then the indexed matches
// (newest-first), then a terminal Progress with done=true. The daemon never
// decides covered-vs-scan itself — it reports coverage and returns what it has
// indexed; the client applies the identical CanServe logic and tops up any
// uncovered tail with its own scan (docs/design/shared-index-service.md §4).
func (s *indexServer) Search(req *indexv1.SearchRequest, stream indexv1.Index_SearchServer) error {
	start := time.Now()
	topic := req.GetTopic()
	if err := s.authorize(stream.Context(), endpointauth.ActionSearch, topic); err != nil {
		searchesTotal.WithLabelValues(searchForbidden).Inc()
		return err
	}
	s.clampLimit(req)
	// The pattern is what the caller is looking for — an account number, an
	// email address — so it is logged only at debug.
	log.Debug("search pattern", "topic", topic, "pattern", req.GetPattern())
	log.Info(
		"search",
		"topic", topic,
		"scopes", req.GetScopes(),
		"since_ms", req.GetSinceMs(),
		"until_ms", req.GetUntilMs(),
		"partitions", req.GetPartitions(),
		"limit", req.GetLimit(),
	)

	w := s.mgr.lookup(topic)
	if w == nil {
		return s.searchUnfollowed(stream, topic)
	}

	params, err := indexwire.RequestToQueryParams(req)
	if err != nil {
		searchesTotal.WithLabelValues(searchError).Inc()
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
	cov.Backfilling = w.backfilling()
	// A plain assignment rather than an if-init: an `err :=` there would
	// shadow the err the query below reuses.
	err = sendCoverage(stream, cov)
	if err != nil {
		return err
	}

	res, err := w.query(stream.Context(), params)
	if err != nil {
		searchesTotal.WithLabelValues(searchError).Inc()
		log.Error("search: query failed", "topic", topic, "err", err)
		return status.Errorf(codes.Internal, "query: %v", err)
	}
	for _, m := range res.Matches {
		ev := &indexv1.SearchEvent{Event: &indexv1.SearchEvent_Match{Match: indexwire.MatchFromConsumed(m)}}
		if err := stream.Send(ev); err != nil {
			return err
		}
	}
	searchesTotal.WithLabelValues(searchServed).Inc()
	searchDuration.Observe(time.Since(start).Seconds())
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

// searchUnfollowed answers a Search for a topic that is not followed yet. If
// the allowlist and the caller's grant permit it, the daemon begins following
// as a side effect so the NEXT search is warm; this call still reports no
// coverage and the client scans (warm-on-first-search). A topic outside the
// allowlist, or one this caller may search but not follow, just reports no
// coverage and is not followed.
func (s *indexServer) searchUnfollowed(stream indexv1.Index_SearchServer, topic string) error {
	ctx := stream.Context()
	switch {
	case !s.authz.Allows(ctx, endpointauth.ActionFollow, topic):
		searchesTotal.WithLabelValues(searchForbidden).Inc()
		log.Info("search: topic not followed and the caller may not follow it → client scans",
			"topic", topic, "caller", endpointauth.GrantFromContext(ctx).Caller())
	// contextcheck: registration deliberately runs on the daemon-lifetime ctx,
	// not this request's stream ctx — the follow must OUTLIVE the search that
	// triggered it (warm-on-first-search), so the request ctx must not thread in.
	case s.mgr.ensureFollowed(topic): //nolint:contextcheck // see comment above
		searchesTotal.WithLabelValues(searchWarming).Inc()
		log.Info("search: topic not followed — now warming, client scans this time", "topic", topic)
	default:
		searchesTotal.WithLabelValues(searchDenied).Inc()
		log.Warn("search: topic not allowlisted → client will scan (never followed)", "topic", topic)
	}
	if err := sendCoverage(stream, &indexv1.Coverage{Following: false}); err != nil {
		return err
	}
	return sendDone(stream, 0, false)
}

// Coverage reports a topic's indexed ranges without running a query. Clients
// call it before every search to decide index-vs-scan (internal/remoteindex),
// and k4a-mcp exposes it as a tool.
//
// It is read-only in both senses: it never starts a follow, and it never
// resets a followed topic's LRU clock. The client's next call is what counts
// as use — a Search when the coverage serves the query, a Follow when it does
// not — so a topic that is only ever probed still ages out at evictAfter.
func (s *indexServer) Coverage(ctx context.Context, req *indexv1.CoverageRequest) (*indexv1.CoverageResponse, error) {
	topic := req.GetTopic()
	// Coverage says whether, and how far, a topic is indexed, so it needs the
	// same grant as a Search.
	if err := s.authorize(ctx, endpointauth.ActionSearch, topic); err != nil {
		return nil, err
	}
	w := s.mgr.peek(topic)
	if w == nil {
		// Only a Search (or an explicit Follow) warms a topic, so probing
		// coverage for many topics can't stampede the follow-set.
		log.Info("coverage", "topic", topic, "following", false)
		return &indexv1.CoverageResponse{Coverage: &indexv1.Coverage{Following: false}}, nil
	}
	book := w.coverage()
	cov := indexwire.CoverageFromBookkeeper(book)
	cov.Following = true
	cov.Backfilling = w.backfilling()
	log.Info("coverage", "topic", topic, "following", true, "partitions", len(book.NewestOffsetPerPartition))
	return &indexv1.CoverageResponse{Coverage: cov}, nil
}

// Follow registers a topic without querying it — the client's fire-and-forget
// warm-up when it scans a cold topic itself. It runs the same allowlist gate as
// Search: an allowed topic is accepted (and begins following if it wasn't
// already); a denied topic is refused with a reason, and the daemon never
// persists its payloads.
func (s *indexServer) Follow(ctx context.Context, req *indexv1.FollowRequest) (*indexv1.FollowResponse, error) {
	topic := req.GetTopic()
	if err := s.authorize(ctx, endpointauth.ActionFollow, topic); err != nil {
		return nil, err
	}
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

// authorize applies --authz-policy: PermissionDenied when the caller's grant
// does not cover action on topic. The refusal names the caller's policy entry
// and the topic, never a credential. A client treats any error from the
// daemon as "not covered" and scans with its own Kafka credentials, so a
// refusal costs the caller the index, not the answer.
func (s *indexServer) authorize(ctx context.Context, action endpointauth.Action, topic string) error {
	err := s.authz.Check(ctx, action, topic)
	if err == nil {
		return nil
	}
	log.Warn("refused by the authorization policy", "action", action.String(), "topic", topic,
		"caller", endpointauth.GrantFromContext(ctx).Caller())
	return status.Error(codes.PermissionDenied, err.Error())
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
