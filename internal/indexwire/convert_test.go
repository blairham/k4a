// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package indexwire_test

import (
	"regexp"
	"testing"
	"time"

	"github.com/blairham/k4a/internal/index"
	"github.com/blairham/k4a/internal/indexwire"
	"github.com/blairham/k4a/internal/kafka"
)

// t0/t1 bound the indexed window used across the coverage-parity cases.
var (
	t0ms = time.UnixMilli(1_000_000).UTC().UnixMilli()
	t1ms = time.UnixMilli(2_000_000).UTC().UnixMilli()
)

func indexedBook() index.Bookkeeper {
	return index.Bookkeeper{
		SchemaVersion:            index.SchemaVersion,
		NewestOffsetPerPartition: map[int32]int64{0: 500, 1: 480},
		OldestOffsetPerPartition: map[int32]int64{0: 100, 1: 90},
		NewestTimePerPartition:   map[int32]int64{0: t1ms, 1: t1ms},
		OldestTimePerPartition:   map[int32]int64{0: t0ms, 1: t0ms},
	}
}

// TestCoverageDecisionParity is the load-bearing Phase 0 guarantee: the
// coverage decision a client reaches on a Bookkeeper reconstructed from wire
// coverage is identical to the decision on the original book. If this ever
// diverges, "indexed (shared)" and "indexed (local)" disagree and the whole
// design collapses to "just scan it".
func TestCoverageDecisionParity(t *testing.T) {
	t.Parallel()

	inWindow := index.QueryParams{
		Since: time.UnixMilli(t0ms + 100).UTC(),
		Until: time.UnixMilli(t1ms - 100).UTC(),
	}
	cases := []struct {
		name   string
		params index.QueryParams
		book   index.Bookkeeper
	}{
		{name: "fully covered", book: indexedBook(), params: inWindow},
		{
			name:   "since before indexed floor",
			book:   indexedBook(),
			params: index.QueryParams{Since: time.UnixMilli(t0ms - 100).UTC(), Until: time.UnixMilli(t1ms - 100).UTC()},
		},
		{
			name:   "until after indexed ceiling",
			book:   indexedBook(),
			params: index.QueryParams{Since: time.UnixMilli(t0ms + 100).UTC(), Until: time.UnixMilli(t1ms + 100).UTC()},
		},
		{name: "no time bound", book: indexedBook(), params: index.QueryParams{}},
		{name: "empty index", book: index.Bookkeeper{NewestOffsetPerPartition: map[int32]int64{}}, params: inWindow},
		{
			name:   "gap invalidates coverage",
			book:   withGap(indexedBook(), index.Gap{Partition: 0, Start: 200, End: 250, Reason: "overflow"}),
			params: inWindow,
		},
		{
			name:   "single-partition filter covered",
			book:   indexedBook(),
			params: index.QueryParams{Since: inWindow.Since, Until: inWindow.Until, Partitions: []int32{1}},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			wantOK, wantReason := tc.book.CanServe(tc.params)

			roundTripped := indexwire.BookkeeperFromCoverage(indexwire.CoverageFromBookkeeper(tc.book))
			gotOK, gotReason := roundTripped.CanServe(tc.params)

			if gotOK != wantOK {
				t.Fatalf("decision diverged after round-trip: local=%v (%q) remote=%v (%q)",
					wantOK, wantReason, gotOK, gotReason)
			}
			// The reason string is cosmetic and, for a multi-partition miss,
			// depends on map iteration order — so assert only that both sides
			// agree on whether there IS a reason.
			if (wantReason == "") != (gotReason == "") {
				t.Fatalf("reason presence diverged: local=%q remote=%q", wantReason, gotReason)
			}
		})
	}
}

func TestQueryParamsRoundTrip(t *testing.T) {
	t.Parallel()

	want := index.QueryParams{
		Since:      time.UnixMilli(t0ms).UTC(),
		Until:      time.UnixMilli(t1ms).UTC(),
		Pattern:    regexp.MustCompile("foo.*bar"),
		Scopes:     []string{"key", "value"},
		Partitions: []int32{0, 2},
		Limit:      500,
	}

	got, err := indexwire.RequestToQueryParams(indexwire.QueryParamsToRequest("t", want))
	if err != nil {
		t.Fatalf("round-trip: %v", err)
	}
	if got.Pattern.String() != want.Pattern.String() {
		t.Errorf("pattern: got %q want %q", got.Pattern, want.Pattern)
	}
	if !got.Since.Equal(want.Since) || !got.Until.Equal(want.Until) {
		t.Errorf("time bounds: got [%s,%s] want [%s,%s]", got.Since, got.Until, want.Since, want.Until)
	}
	if got.Limit != want.Limit {
		t.Errorf("limit: got %d want %d", got.Limit, want.Limit)
	}
	if !equalInt32(got.Partitions, want.Partitions) {
		t.Errorf("partitions: got %v want %v", got.Partitions, want.Partitions)
	}
	if !equalStr(got.Scopes, want.Scopes) {
		t.Errorf("scopes: got %v want %v", got.Scopes, want.Scopes)
	}
}

func TestEmptyPatternIsMatchAll(t *testing.T) {
	t.Parallel()
	// A nil Pattern must survive as nil (match-all), not become a regex that
	// matches the literal empty string in some other way.
	req := indexwire.QueryParamsToRequest("t", index.QueryParams{})
	if req.GetPattern() != "" {
		t.Fatalf("nil pattern should serialize to empty string, got %q", req.GetPattern())
	}
	got, err := indexwire.RequestToQueryParams(req)
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if got.Pattern != nil {
		t.Fatalf("empty pattern should decode to nil, got %v", got.Pattern)
	}
}

func TestMatchRoundTrip(t *testing.T) {
	t.Parallel()

	want := kafka.ConsumedMessage{
		Topic:     "orders.v1",
		Partition: 3,
		Offset:    9981,
		Time:      time.UnixMilli(t1ms).UTC(),
		Key:       "key-1",
		Value:     `{"a":1}`,
		Headers:   []kafka.MessageHeader{{Key: "trace", Value: "abc"}, {Key: "src", Value: "x"}},
	}

	got := indexwire.ConsumedFromMatch(want.Topic, indexwire.MatchFromConsumed(want))
	if got.Topic != want.Topic || got.Partition != want.Partition || got.Offset != want.Offset {
		t.Errorf("identity fields: got %+v want %+v", got, want)
	}
	if !got.Time.Equal(want.Time) {
		t.Errorf("time: got %s want %s", got.Time, want.Time)
	}
	if got.Key != want.Key || got.Value != want.Value {
		t.Errorf("key/value: got %q/%q want %q/%q", got.Key, got.Value, want.Key, want.Value)
	}
	if len(got.Headers) != len(want.Headers) {
		t.Fatalf("headers len: got %d want %d", len(got.Headers), len(want.Headers))
	}
	for i := range want.Headers {
		if got.Headers[i] != want.Headers[i] {
			t.Errorf("header %d: got %+v want %+v", i, got.Headers[i], want.Headers[i])
		}
	}
}

func withGap(b index.Bookkeeper, g index.Gap) index.Bookkeeper {
	b.Gaps = append(b.Gaps, g)
	return b
}

func equalInt32(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func equalStr(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
