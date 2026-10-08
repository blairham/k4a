// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package views

import (
	"context"
	"testing"

	tea "charm.land/bubbletea/v2"

	"github.com/blairham/k4a/internal/kafka"
)

// drainView pumps the view's Cmd loop the way bubbletea does, so a Cmd that
// returns nil (dispatching no message) ends the loop exactly as it would
// in the real event loop.
func drainView(t *testing.T, v *SearchView, cmd tea.Cmd) {
	t.Helper()
	for i := 0; cmd != nil && i < 10_000; i++ {
		msg := cmd()
		if msg == nil {
			return
		}
		cmd = v.Update(msg)
	}
}

// closedFirstSearch returns the three channels already loaded with matches and
// a terminal progress, closed in the order the real producer closes them:
// errors, then progress, then matches (searchcache.runSearch stacks
// `defer close(matchCh); defer close(progCh); defer close(errCh)`, and defers
// run LIFO). Nothing may be dropped just because errCh closed first.
func closedFirstSearch(matches int) SearchFunc {
	return func(_ context.Context, topic string, _ kafka.SearchParams) (
		<-chan kafka.ConsumedMessage, <-chan kafka.DeepSearchProgress, <-chan error,
	) {
		matchCh := make(chan kafka.ConsumedMessage, matches)
		progCh := make(chan kafka.DeepSearchProgress, 1)
		errCh := make(chan error, 1)

		for i := 0; i < matches; i++ {
			matchCh <- kafka.ConsumedMessage{Topic: topic, Key: "4100000056", Offset: int64(i)}
		}
		progCh <- kafka.DeepSearchProgress{Done: true, Matches: matches, Source: kafka.SourceScan}

		close(errCh)
		close(progCh)
		close(matchCh)
		return matchCh, progCh, errCh
	}
}

// A closed errCh must not strand the scan: the matches buffered in matchCh and
// the terminal progress in progCh still have to reach the view.
func TestSearchView_DrainsMatchesWhenErrChClosesFirst(t *testing.T) {
	v := NewSearchView(closedFirstSearch(5), "example.ingest.prices.v1", "4100000056")
	drainView(t, v, v.Init())

	if got := v.Count(); got != 5 {
		t.Fatalf("Count() = %d, want 5 — matches were dropped when errCh closed first", got)
	}
	if v.state == searchStateRunning {
		t.Fatal("view stranded in searchStateRunning — terminal progress never drained")
	}
	if v.progress.Matches != 5 {
		t.Errorf("progress.Matches = %d, want 5", v.progress.Matches)
	}
}

// The scan is only over once every channel has closed.
func TestSearchView_OneClosedChannelIsNotTheEnd(t *testing.T) {
	v := NewSearchView(closedFirstSearch(3), "t", "q")
	drainView(t, v, v.Init())

	if v.matchCh != nil || v.progCh != nil || v.errCh != nil {
		t.Error("all three channels should be nil once fully drained")
	}
	if v.Count() != 3 {
		t.Errorf("Count() = %d, want 3", v.Count())
	}
}

// An incomplete result set must keep its header — that is where "capped" and
// "INCOMPLETE" are reported. Hiding it leaves a short result looking whole.
func TestSearchView_HeaderStaysVisibleWhenResultsAreIncomplete(t *testing.T) {
	cases := []struct {
		name  string
		state searchState
		want  bool
	}{
		{"running", searchStateRunning, true},
		{"errored", searchStateErrored, true},
		{"capped", searchStateCapped, true},
		{"truncated", searchStateTruncated, true},
		{"done", searchStateDone, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			v := &SearchView{state: tc.state}
			if got := v.headerVisible(); got != tc.want {
				t.Errorf("headerVisible() = %v, want %v", got, tc.want)
			}
		})
	}
}
