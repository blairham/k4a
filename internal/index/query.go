// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"context"
	"fmt"
	"regexp"
	"time"

	"github.com/blevesearch/bleve/v2"
	"github.com/blevesearch/bleve/v2/search/query"

	"github.com/blairham/k4a/internal/jsonpath"
	"github.com/blairham/k4a/internal/kafka"
)

// QueryParams describes a single search of the local index. Mirrors the
// shape of kafka.SearchParams but explicitly typed for the index
// layer — no regex compilation, no scope bitmask, just the strings the
// dispatcher already computed.
type QueryParams struct {
	Since   time.Time
	Until   time.Time
	Pattern *regexp.Regexp
	// JSONPath is the canonical text of a JSON-path predicate, or "". An
	// equality (`.a.b == 42`, `a.b:42`) is answered from the field terms;
	// CanServe declines every other predicate.
	JSONPath string
	// Decoder is the identity of the decoder the search's own scan would
	// match with (kafka.SearchParams.DecoderIdentity), or "" for none.
	Decoder    string
	Scopes     []string // any of: "key", "value", "headers"
	Partitions []int32  // empty = all
	Limit      int      // 0 = unlimited
}

// ReasonJSONPathScanOnly is CanServe's reason for declining a JSON-path query
// the field terms cannot answer.
const ReasonJSONPathScanOnly = "only == JSON paths (field:value) are indexed; this one is scan-only"

// ReasonNoFieldTerms is CanServe's reason for declining a JSON path against
// an index (a daemon, in practice) built before field terms existed.
const ReasonNoFieldTerms = "index predates field terms; JSON paths scan"

// path parses JSONPath, nil when there is none or it does not parse (a
// request from a client the index cannot vouch for; CanServe declines it).
func (p QueryParams) path() *jsonpath.Expr {
	if p.JSONPath == "" {
		return nil
	}
	e, err := jsonpath.Parse(p.JSONPath)
	if err != nil {
		return nil
	}
	return e
}

// queryDefaultLimit caps how many hits we return from a single Query()
// call. Matches the deep-search default match cap.
const queryDefaultLimit = 10_000

// QueryResult is a single search's answer plus whether it is the whole
// answer. Query returns this rather than a bare slice so a caller cannot
// forget to ask: a truncated result that reports success is the failure
// mode of returning a limited page as if it were complete (and, on the scan path, of a short deep-search read).
type QueryResult struct {
	// Matches are the hits, newest-first.
	Matches []kafka.ConsumedMessage
	// Capped reports that more records matched than Limit allowed, so
	// Matches holds only the newest page and older matches are missing.
	Capped bool
}

// CanServe reports whether the index can answer params without falling
// back to a scan. The cardinal rule (state-management.md): the cluster
// is the source of truth — we only return "yes" if every selected
// partition's indexed time range fully covers [since, until], including
// no gaps.
//
// Returns (true, "") when the index is authoritative for params.
// Returns (false, reason) with a short human-readable reason on miss
// so the dispatcher can surface why it fell back.
func (ix *Indexer) CanServe(params QueryParams) (bool, string) {
	// Lock-free: the same starvation that made coverage time out under heavy
	// ingest applies here, and a stale-but-conservative answer is always safe
	// (it sends the client to a scan). See BookkeeperSnapshot, which keeps coverage reads lock-free.
	return ix.BookkeeperSnapshot().CanServe(params)
}

// CanServe is the coverage decision itself, operating on a Bookkeeper
// snapshot alone. It is split off (*Indexer).CanServe so the shared-index
// client can run the IDENTICAL decision on a Bookkeeper reconstructed from a
// daemon's wire coverage (docs/design/shared-index-service.md) — "indexed
// (local)" and "indexed (shared)" must never fork the coverage logic.
func (b Bookkeeper) CanServe(params QueryParams) (bool, string) {
	// A JSON-path predicate is about document structure, and the index holds
	// analyzed terms of flattened text: it can neither find every match nor
	// prove a non-match, so the dispatcher must scan.
	if params.JSONPath != "" {
		if e := params.path(); e == nil || !e.Indexable() {
			return false, ReasonJSONPathScanOnly
		}
		if b.SchemaVersion < FieldsSchemaVersion {
			return false, ReasonNoFieldTerms
		}
	}
	// The index holds text as its decoder rendered it. A search that would
	// match other text (raw bytes, or another registry's decoding) must scan,
	// or the index and the scan would answer the same query differently.
	if params.Decoder != b.Decoder {
		return false, decoderMismatch(b.Decoder, params.Decoder)
	}
	// Pattern gate: Bleve regexp queries are term-anchored, so a
	// pattern the tokenized terms cannot answer with full recall is
	// unservable no matter how much of the range is indexed —
	// the dispatcher must scan instead of confidently reporting zero.
	if params.Pattern != nil {
		if _, reason := indexablePattern(params.Pattern); reason != "" {
			return false, reason
		}
	}
	if len(b.NewestOffsetPerPartition) == 0 {
		return false, "index empty"
	}
	if params.Since.IsZero() && params.Until.IsZero() {
		// No time bound means "search all time"; the index has finite
		// data, so it can't claim authoritative coverage.
		return false, "no time bound (would scan all time)"
	}

	sinceMS := int64(0)
	untilMS := int64(0)
	if !params.Since.IsZero() {
		sinceMS = params.Since.UnixMilli()
	}
	if !params.Until.IsZero() {
		untilMS = params.Until.UnixMilli()
	}

	if reason := b.uncoveredPartition(params, sinceMS, untilMS); reason != "" {
		return false, reason
	}

	// Any partition-spanning gap means we can't claim coverage even if
	// the time bounds look OK.
	for _, g := range b.Gaps {
		if gapOverlapsRange(g, sinceMS, untilMS) {
			return false, fmt.Sprintf("gap in partition %d at offsets %d-%d",
				g.Partition, g.Start, g.End)
		}
	}

	return true, ""
}

// decoderMismatch says why an index cannot serve a search that decodes
// differently from it.
func decoderMismatch(indexed, search string) string {
	name := func(id string) string {
		if id == "" {
			return "no schema registry"
		}
		return "schema registry " + id
	}
	return fmt.Sprintf("index decoded with %s, search with %s", name(indexed), name(search))
}

// uncoveredPartition checks each requested partition (all indexed ones when
// none were named) against the time bounds, returning why the first one that
// falls short cannot be served, or "" when all are covered.
func (b Bookkeeper) uncoveredPartition(params QueryParams, sinceMS, untilMS int64) string {
	partitions := params.Partitions
	if len(partitions) == 0 {
		partitions = make([]int32, 0, len(b.NewestOffsetPerPartition))
		for p := range b.NewestOffsetPerPartition {
			partitions = append(partitions, p)
		}
	}

	for _, p := range partitions {
		oldest, ok := b.OldestTimePerPartition[p]
		if !ok {
			return fmt.Sprintf("partition %d not indexed", p)
		}
		newest := b.NewestTimePerPartition[p]
		if sinceMS != 0 && oldest > sinceMS {
			return fmt.Sprintf("partition %d only indexed back to %s",
				p, time.UnixMilli(oldest).UTC().Format(time.RFC3339))
		}
		if untilMS != 0 && newest < untilMS {
			return fmt.Sprintf("partition %d not indexed up to %s",
				p, params.Until.Format(time.RFC3339))
		}
	}
	return ""
}

func gapOverlapsRange(g Gap, _, _ int64) bool {
	// We don't currently track per-gap timestamps, only offsets. v1
	// answer: any non-empty gap invalidates coverage. We can tighten
	// this in v2 when gaps carry time metadata.
	return g.End > g.Start
}

// Query runs a single search against the index and returns matches
// newest-first (by timestamp). The match shape mirrors what the live
// scan emits so the dispatcher can return either source uniformly.
//
// CanServe should be consulted first; calling Query for a range the
// index doesn't cover returns whatever it has, not an error.
//
// Note that CanServe and QueryResult.Capped answer different questions and
// neither substitutes for the other: CanServe is about offset/time *range*
// coverage (and gaps), while Capped is about the *match count* exceeding
// Limit. A topic can be fully covered with no gaps and still come back
// capped.
func (ix *Indexer) Query(ctx context.Context, params QueryParams) (QueryResult, error) {
	ix.mu.Lock()
	idx := ix.idx
	topic := ix.topic
	ix.mu.Unlock()
	if idx == nil {
		return QueryResult{}, fmt.Errorf("indexer closed")
	}

	path := params.path()
	q := buildBleveQuery(params, path)
	limit := params.Limit
	if limit <= 0 {
		limit = queryDefaultLimit
	}

	req := bleve.NewSearchRequestOptions(q, limit, 0, false)
	req.SortBy([]string{"-timestamp"})
	req.Fields = []string{fieldKey, fieldValue, fieldHeaders, fieldTimestamp, fieldPartition, fieldOffset}

	res, err := idx.SearchInContext(ctx, req)
	if err != nil {
		return QueryResult{}, fmt.Errorf("bleve search: %w", err)
	}

	// res.Total is every matching document; res.Hits is only the page the
	// limit allowed. Comparing them is the one reliable way to know we
	// truncated -- len(hits) is not, because hitToConsumed drops
	// unparseable hits and would understate the page. Total counts
	// CANDIDATES (the widened Bleve query, see pattern.go), so this can
	// over-report capping when the post-filter drops false positives —
	// erring toward "maybe incomplete" over a silent claim of completeness.
	capped := res.Total > uint64(limit) //nolint:gosec // limit is > 0 here

	hits := make([]kafka.ConsumedMessage, 0, len(res.Hits))
	for _, h := range res.Hits {
		msg, ok := hitToConsumed(&bleveSearchHit{Fields: h.Fields}, topic, params.Pattern, path, params.Scopes)
		if !ok {
			continue
		}
		hits = append(hits, msg)
	}
	return QueryResult{Matches: hits, Capped: capped}, nil
}

// buildBleveQuery composes the Bleve query from the search params:
// partition range filter ∩ time range filter ∩ regex on selected
// text fields.
func buildBleveQuery(params QueryParams, path *jsonpath.Expr) query.Query {
	must := make([]query.Query, 0, 4)

	if len(params.Partitions) > 0 {
		parts := make([]query.Query, 0, len(params.Partitions))
		for _, p := range params.Partitions {
			lo, hi := float64(p), float64(p)
			rng := bleve.NewNumericRangeQuery(&lo, &hi)
			rng.SetField(fieldPartition)
			parts = append(parts, rng)
		}
		must = append(must, bleve.NewDisjunctionQuery(parts...))
	}

	if !params.Since.IsZero() || !params.Until.IsZero() {
		incl := true
		dr := bleve.NewDateRangeInclusiveQuery(params.Since, params.Until, &incl, &incl)
		dr.SetField(fieldTimestamp)
		must = append(must, dr)
	}

	if params.Pattern != nil {
		scopes := params.Scopes
		if len(scopes) == 0 {
			scopes = []string{fieldKey, fieldValue}
		}
		shoulds := make([]query.Query, 0, len(scopes))
		// Not the pattern verbatim: Bleve regexes are term-anchored, so the
		// raw pattern silently under-matches. The candidate
		// pattern is recall-complete against the analyzed terms; precision
		// is restored by hitToConsumed's exact re-check on the stored text.
		pat := bestEffortBlevePattern(params.Pattern)
		for _, field := range scopes {
			rq := bleve.NewRegexpQuery(pat)
			rq.SetField(field)
			shoulds = append(shoulds, rq)
		}
		must = append(must, bleve.NewDisjunctionQuery(shoulds...))
	}

	if q := pathQuery(path, params.Scopes); q != nil {
		must = append(must, q)
	}

	if len(must) == 0 {
		return bleve.NewMatchAllQuery()
	}
	if len(must) == 1 {
		return must[0]
	}
	return bleve.NewConjunctionQuery(must...)
}

// hitToConsumed reconstructs a kafka.ConsumedMessage from a Bleve hit's
// stored fields. Returns false if the doc is missing required fields
// (unlikely but defensive). The exact-regex re-check against the stored
// text is LOAD-BEARING, not belt-and-suspenders: the Bleve query runs a
// widened candidate pattern (case-folded, unanchored — see pattern.go), so
// this is where precision — exact case, cross-token position, and scope —
// is restored to match the scan path.
func hitToConsumed(
	h *bleveSearchHit, topic string, re *regexp.Regexp, path *jsonpath.Expr, scopes []string,
) (kafka.ConsumedMessage, bool) {
	msg := kafka.ConsumedMessage{Topic: topic}

	if v, ok := h.Fields[fieldPartition].(float64); ok {
		msg.Partition = int(v)
	}
	if v, ok := h.Fields[fieldOffset].(float64); ok {
		msg.Offset = int64(v)
	}
	if v, ok := h.Fields[fieldTimestamp].(string); ok {
		if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
			msg.Time = t
		}
	}
	if v, ok := h.Fields[fieldKey].(string); ok {
		msg.Key = v
	}
	if v, ok := h.Fields[fieldValue].(string); ok {
		msg.Value = v
	}
	if v, ok := h.Fields[fieldHeaders].(string); ok {
		msg.Headers = parseHeaders(v)
	}
	if re != nil && !matchesScopes(re, &msg, scopes) {
		// A candidate the exact regex doesn't actually match — drop.
		return kafka.ConsumedMessage{}, false
	}
	// The scan's own rule on the stored text, so a partial document, or a
	// term that only looked equal, is held to exactly what a scan returns.
	if path != nil && !kafka.PathMatches(path, []byte(msg.Key), []byte(msg.Value), scopeMask(scopes)) {
		return kafka.ConsumedMessage{}, false
	}
	return msg, true
}

// matchesScopes applies the exact regex to the requested scopes only,
// mirroring the scan path's recordMatches so a key-scoped search can't
// return a record that only matches in its value. Empty scopes default to
// key+value, same as buildBleveQuery.
func matchesScopes(re *regexp.Regexp, msg *kafka.ConsumedMessage, scopes []string) bool {
	if len(scopes) == 0 {
		scopes = []string{fieldKey, fieldValue}
	}
	for _, s := range scopes {
		switch s {
		case fieldKey:
			if re.MatchString(msg.Key) {
				return true
			}
		case fieldValue:
			if re.MatchString(msg.Value) {
				return true
			}
		case fieldHeaders:
			if regexMatchesHeaders(re, msg.Headers) {
				return true
			}
		}
	}
	return false
}

func regexMatchesHeaders(re *regexp.Regexp, headers []kafka.MessageHeader) bool {
	for _, h := range headers {
		if re.MatchString(h.Key + "=" + h.Value) {
			return true
		}
	}
	return false
}

// parseHeaders is the inverse of serializeHeaders.
func parseHeaders(s string) []kafka.MessageHeader {
	if s == "" {
		return nil
	}
	out := make([]kafka.MessageHeader, 0, 4)
	for _, line := range splitLines(s) {
		idx := indexByte(line, '=')
		if idx < 0 {
			continue
		}
		out = append(out, kafka.MessageHeader{Key: line[:idx], Value: line[idx+1:]})
	}
	return out
}

// splitLines splits on "\n" without importing strings.
func splitLines(s string) []string {
	if s == "" {
		return nil
	}
	out := make([]string, 0, 4)
	start := 0
	for i := 0; i < len(s); i++ {
		if s[i] == '\n' {
			out = append(out, s[start:i])
			start = i + 1
		}
	}
	out = append(out, s[start:])
	return out
}

func indexByte(s string, b byte) int {
	for i := 0; i < len(s); i++ {
		if s[i] == b {
			return i
		}
	}
	return -1
}

// bleveSearchHit is a tiny indirection so unit tests can build hits
// without a full Bleve index. It mirrors the fields we read from a real
// bleve/search.DocumentMatch.
type bleveSearchHit struct {
	Fields map[string]any
}

// pathQuery is the candidate filter for an equality JSON path: documents
// whose key or value (as scoped; headers hold no JSON documents here) has the
// predicate's term, or that were too large to enumerate. Nil when there is no
// path or it is not an equality, leaving the exact check to filter alone.
func pathQuery(path *jsonpath.Expr, scopes []string) query.Query {
	if path == nil {
		return nil
	}
	term, ok := path.Term()
	if !ok {
		return nil
	}
	mask := scopeMask(scopes)
	var fields []string
	if mask&kafka.ScopeKey != 0 {
		fields = append(fields, fieldKeyTerms)
	}
	if mask&kafka.ScopeValue != 0 {
		fields = append(fields, fieldValTerms)
	}
	shoulds := make([]query.Query, 0, 2*len(fields))
	for _, f := range fields {
		for _, t := range []string{term, jsonpath.PartialTerm} {
			tq := bleve.NewTermQuery(t)
			tq.SetField(f)
			shoulds = append(shoulds, tq)
		}
	}
	return bleve.NewDisjunctionQuery(shoulds...)
}

// scopeMask turns the index's scope names back into the scan's bitmask; none
// means key and value, as everywhere else.
func scopeMask(scopes []string) kafka.SearchScope {
	if len(scopes) == 0 {
		return kafka.ScopeKey | kafka.ScopeValue
	}
	var m kafka.SearchScope
	for _, s := range scopes {
		switch s {
		case fieldKey:
			m |= kafka.ScopeKey
		case fieldValue:
			m |= kafka.ScopeValue
		case fieldHeaders:
			m |= kafka.ScopeHeaders
		}
	}
	return m
}
