// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"context"
	"regexp"
	"testing"
	"time"
)

// queryBase is the timestamp origin for these tests; indexN (trim_test.go)
// lays records out one second apart from it, so newest-first is unambiguous.
var queryBase = time.Date(2026, 8, 14, 0, 0, 0, 0, time.UTC)

// TestQuery_ReportsCappedWhenLimitBinds is the regression proof for issue
// the index used to return the limited page and drop bleve's total, so
// a truncated answer was indistinguishable from a complete one.
func TestQuery_ReportsCappedWhenLimitBinds(t *testing.T) {
	t.Parallel()

	ix := newTestIndexer(t)
	indexN(t, ix, 0, 25, queryBase)

	res, err := ix.Query(context.Background(), QueryParams{Limit: 10})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Matches) != 10 {
		t.Errorf("returned %d matches, want the 10 the limit allows", len(res.Matches))
	}
	if !res.Capped {
		t.Error("Capped is false after the limit truncated 25 matches down to 10")
	}
}

// TestQuery_NotCappedWhenLimitDoesNotBind guards the other direction: a
// warning on every complete result is as useless as no warning at all.
func TestQuery_NotCappedWhenLimitDoesNotBind(t *testing.T) {
	t.Parallel()

	ix := newTestIndexer(t)
	indexN(t, ix, 0, 5, queryBase)

	res, err := ix.Query(context.Background(), QueryParams{Limit: 10})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Matches) != 5 {
		t.Errorf("returned %d matches, want all 5", len(res.Matches))
	}
	if res.Capped {
		t.Error("Capped is true even though every match fit within the limit")
	}
}

// TestQuery_ExactlyAtLimitIsNotCapped pins the boundary: matches == limit
// means everything fit, and must not be reported as truncated.
func TestQuery_ExactlyAtLimitIsNotCapped(t *testing.T) {
	t.Parallel()

	ix := newTestIndexer(t)
	indexN(t, ix, 0, 10, queryBase)

	res, err := ix.Query(context.Background(), QueryParams{Limit: 10})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if len(res.Matches) != 10 || res.Capped {
		t.Errorf("matches=%d capped=%v, want 10 and false", len(res.Matches), res.Capped)
	}
}

// TestQuery_CappedKeepsNewest documents which end gets dropped. The index
// sorts -timestamp, so truncation loses the OLDEST matches -- the opposite of
// the scan path once, and worth pinning so a sort change can't silently
// invert it.
func TestQuery_CappedKeepsNewest(t *testing.T) {
	t.Parallel()

	ix := newTestIndexer(t)
	indexN(t, ix, 0, 20, queryBase)

	res, err := ix.Query(context.Background(), QueryParams{Limit: 5})
	if err != nil {
		t.Fatalf("Query: %v", err)
	}
	if !res.Capped {
		t.Fatal("expected a capped result")
	}
	for _, m := range res.Matches {
		if m.Offset < 15 {
			t.Errorf("offset %d is among the oldest records; a capped query must keep the newest", m.Offset)
		}
	}
}

// TestQuery_PlainWordMatchesInsideToken is the regression proof for issue
// Bleve regexp queries are term-anchored, so the raw pattern `order`
// used to match zero records in a topic whose every value contains
// "orderId" — while the scan path matched all of them. The candidate
// rewrite (pattern.go) must give grep-style substring semantics.
func TestQuery_PlainWordMatchesInsideToken(t *testing.T) {
	t.Parallel()

	ix := newTestIndexer(t)
	msg := sampleMsg(0, 1, "k-1", `{"orderId":4100000042,"status":"SHIPPED"}`, queryBase)
	if err := ix.Index(context.Background(), msg); err != nil {
		t.Fatalf("Index: %v", err)
	}

	for _, q := range []string{
		"(?i)order",            // the headline: plain word inside a token
		"(?i)410000",           // digits inside a numeric token
		"(?i)orderid",          // whole token, still works
		"(?i)order.*",          // trailing wildcard, via the literal branch
		`(?i)"orderId":41`,     // punctuation spanning tokens, via the literal branch
		"(?i)shipped|returned", // token-local alternation
	} {
		res, err := ix.Query(context.Background(), QueryParams{
			Pattern: regexp.MustCompile(q),
			Limit:   10,
		})
		if err != nil {
			t.Fatalf("Query(%q): %v", q, err)
		}
		if len(res.Matches) != 1 {
			t.Errorf("Query(%q) matched %d records, want 1 (scan-path parity)", q, len(res.Matches))
		}
	}
}

// TestQuery_ExactCaseRestoredByPostFilter: the Bleve candidate is case-folded
// (the term dictionary is lowercased), so the exact-case decision belongs to
// the post-filter. A case-sensitive pattern must match stored text by its own
// rules, not the widened candidate's.
func TestQuery_ExactCaseRestoredByPostFilter(t *testing.T) {
	t.Parallel()

	ix := newTestIndexer(t)
	msg := sampleMsg(0, 1, "k-1", `{"orderId":4100000042}`, queryBase)
	if err := ix.Index(context.Background(), msg); err != nil {
		t.Fatalf("Index: %v", err)
	}

	cases := []struct {
		pattern string
		want    int
	}{
		{"orderId", 1}, // exact case of the stored text
		{"ORDER", 0},   // candidate hits the term, post-filter drops it
		{"orderid", 0}, // ditto — stored text has the capital I
	}
	for _, tc := range cases {
		res, err := ix.Query(context.Background(), QueryParams{
			Pattern: regexp.MustCompile(tc.pattern),
			Limit:   10,
		})
		if err != nil {
			t.Fatalf("Query(%q): %v", tc.pattern, err)
		}
		if len(res.Matches) != tc.want {
			t.Errorf("Query(%q) matched %d records, want %d", tc.pattern, len(res.Matches), tc.want)
		}
	}
}

// TestQuery_PostFilterRespectsScope mirrors the scan path's recordMatches: a
// search scoped to one field must not return a record that only matches in
// another, even when the widened Bleve candidate surfaced it.
func TestQuery_PostFilterRespectsScope(t *testing.T) {
	t.Parallel()

	ix := newTestIndexer(t)
	msg := sampleMsg(0, 1, "order-key", `{"nothing":"here"}`, queryBase)
	if err := ix.Index(context.Background(), msg); err != nil {
		t.Fatalf("Index: %v", err)
	}

	re := regexp.MustCompile("(?i)order")
	res, err := ix.Query(context.Background(), QueryParams{Pattern: re, Scopes: []string{"key"}, Limit: 10})
	if err != nil {
		t.Fatalf("Query key-scope: %v", err)
	}
	if len(res.Matches) != 1 {
		t.Errorf("key-scoped query matched %d, want 1 (key is %q)", len(res.Matches), msg.Key)
	}

	res, err = ix.Query(context.Background(), QueryParams{Pattern: re, Scopes: []string{"value"}, Limit: 10})
	if err != nil {
		t.Fatalf("Query value-scope: %v", err)
	}
	if len(res.Matches) != 0 {
		t.Errorf("value-scoped query matched %d, want 0 (pattern only occurs in the key)", len(res.Matches))
	}
}

// TestCanServe_RejectsUnservablePatterns: coverage alone no longer decides —
// a pattern the tokenized terms cannot answer with full recall must push the
// dispatcher to the scan path, on both the local and shared-index routes
// (they share this exact method).
func TestCanServe_RejectsUnservablePatterns(t *testing.T) {
	t.Parallel()

	since := queryBase
	until := queryBase.Add(time.Minute)
	book := Bookkeeper{
		NewestOffsetPerPartition: map[int32]int64{0: 100},
		OldestOffsetPerPartition: map[int32]int64{0: 0},
		NewestTimePerPartition:   map[int32]int64{0: until.UnixMilli()},
		OldestTimePerPartition:   map[int32]int64{0: since.UnixMilli()},
	}
	params := QueryParams{Since: since, Until: until}

	params.Pattern = regexp.MustCompile("(?i)order")
	if ok, reason := book.CanServe(params); !ok {
		t.Errorf("CanServe with a servable pattern = false (%s), want true", reason)
	}

	for _, q := range []string{"(?i)the", "(?i)a.b"} {
		params.Pattern = regexp.MustCompile(q)
		ok, reason := book.CanServe(params)
		if ok {
			t.Errorf("CanServe(%q) = true, want false — the index would silently under-match", q)
		}
		if reason == "" {
			t.Errorf("CanServe(%q) returned no reason for the fallback", q)
		}
	}
}
