// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package views

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/blairham/k4a/internal/kafka"
)

// topicScript is one topic's canned answer.
type topicScript struct {
	err     error
	prog    kafka.DeepSearchProgress
	matches int
}

// scriptedSearch answers each topic from script with pre-loaded, closed
// channels, as a finished source would.
func scriptedSearch(script map[string]topicScript) SearchFunc {
	return func(_ context.Context, topic string, _ kafka.SearchParams) (
		<-chan kafka.ConsumedMessage, <-chan kafka.DeepSearchProgress, <-chan error,
	) {
		s := script[topic]
		m := make(chan kafka.ConsumedMessage, s.matches)
		p := make(chan kafka.DeepSearchProgress, 1)
		e := make(chan error, 1)
		for i := range s.matches {
			m <- kafka.ConsumedMessage{Topic: topic, Offset: int64(i), Value: "hit"}
		}
		if s.err != nil {
			e <- s.err
		} else {
			p <- s.prog
		}
		close(e)
		close(p)
		close(m)
		return m, p, e
	}
}

func resolveTo(topics ...string) TopicResolver {
	return func(context.Context, string) ([]string, error) { return topics, nil }
}

func done(source kafka.SearchSource, matches int) kafka.DeepSearchProgress {
	return kafka.DeepSearchProgress{Done: true, Source: source, Matches: matches}
}

// TestSearchView_TopicGlobReportsCoveragePerTopic: a glob search names each
// topic's source and completeness, and one bad topic makes the whole answer
// INCOMPLETE instead of hiding behind the clean ones.
func TestSearchView_TopicGlobReportsCoveragePerTopic(t *testing.T) {
	t.Parallel()
	run := scriptedSearch(map[string]topicScript{
		"orders.a": {matches: 2, prog: done(kafka.SourceScan, 2)},
		"orders.b": {matches: 1, prog: kafka.DeepSearchProgress{Done: true, Truncated: true, Matches: 1}},
		"orders.c": {err: errors.New("not authorized")},
		"orders.d": {prog: done(kafka.SourceSharedIndex, 0)},
	})
	v := NewSearchView(run, "orders.*", "hit").
		WithTopicResolver(resolveTo("orders.a", "orders.b", "orders.c", "orders.d"))
	v.Resize(200, 30)
	drainView(t, v, v.Init())

	if v.state != searchStateTruncated {
		t.Errorf("state = %v, want truncated: two of four topics are not whole", v.state)
	}
	if v.Count() != 3 {
		t.Errorf("Count() = %d, want 3", v.Count())
	}
	cov := v.coverageLine()
	for _, want := range []string{
		"orders.a: scanned 2", "orders.b: INCOMPLETE 1",
		"orders.c: error: not authorized", "orders.d: indexed (shared) 0",
	} {
		if !strings.Contains(cov, want) {
			t.Errorf("coverage %q lacks %q", cov, want)
		}
	}
	if got := v.progressFragments(); !strings.Contains(got, "INCOMPLETE · 2 of 4 topics") {
		t.Errorf("header = %q, want it to count the topics not fully searched", got)
	}
	if !strings.Contains(v.View(), "orders.c: error") {
		t.Error("the rendered view does not show per-topic coverage")
	}
	if row := v.table.Rows()[0]; row[0] != "orders.a" && row[0] != "orders.b" {
		t.Errorf("first row starts with %q, want the topic", row[0])
	}
}

func TestSearchView_TopicGlobEndStates(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")
	tests := []struct {
		script map[string]topicScript
		name   string
		header string
		want   searchState
	}{
		{
			name: "all clean",
			script: map[string]topicScript{
				"x.1": {matches: 1, prog: done(kafka.SourceScan, 1)},
				"x.2": {prog: done(kafka.SourceCache, 0)},
			},
			want: searchStateDone, header: "complete · 2 topics · 1 matches",
		},
		{
			name: "one capped",
			script: map[string]topicScript{
				"x.1": {prog: kafka.DeepSearchProgress{Done: true, Capped: true}},
				"x.2": {prog: done(kafka.SourceScan, 0)},
			},
			want: searchStateCapped, header: "1 of 2 topics stopped at their match cap",
		},
		{
			name:   "every topic failed",
			script: map[string]topicScript{"x.1": {err: boom}, "x.2": {err: boom}},
			want:   searchStateErrored, header: "error: x.1: boom",
		},
		{
			// A topic that ended without saying it finished is not whole.
			name: "no final frame",
			script: map[string]topicScript{
				"x.1": {prog: kafka.DeepSearchProgress{Scanned: 5}},
				"x.2": {prog: done(kafka.SourceScan, 0)},
			},
			want: searchStateTruncated, header: "INCOMPLETE · 1 of 2 topics",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			v := NewSearchView(scriptedSearch(tt.script), "x.*", "q").WithTopicResolver(resolveTo("x.1", "x.2"))
			drainView(t, v, v.Init())
			if v.state != tt.want {
				t.Errorf("state = %v, want %v", v.state, tt.want)
			}
			if got := v.progressFragments(); !strings.Contains(got, tt.header) {
				t.Errorf("header = %q, want %q", got, tt.header)
			}
			if !v.headerVisible() {
				t.Error("a cross-topic search hid its per-topic coverage")
			}
		})
	}
}

func TestSearchView_TopicGlobResolution(t *testing.T) {
	t.Parallel()
	run := scriptedSearch(nil)

	v := NewSearchView(run, "x.*", "q")
	drainView(t, v, v.Init())
	if v.state != searchStateErrored || !errors.Is(v.scanErr, errNoResolver) {
		t.Errorf("without a resolver: state %v, err %v", v.state, v.scanErr)
	}

	v = NewSearchView(run, "x.*", "q").WithTopicResolver(func(context.Context, string) ([]string, error) {
		return nil, kafka.ErrNoTopicMatch
	})
	drainView(t, v, v.Init())
	if v.state != searchStateErrored || !errors.Is(v.scanErr, kafka.ErrNoTopicMatch) {
		t.Errorf("a glob matching nothing: state %v, err %v; it must not read as a clean empty result", v.state, v.scanErr)
	}
}

// blockingSearch never finishes on its own; it reports when its context ends.
func blockingSearch(canceled chan<- struct{}) SearchFunc {
	return func(ctx context.Context, _ string, _ kafka.SearchParams) (
		<-chan kafka.ConsumedMessage, <-chan kafka.DeepSearchProgress, <-chan error,
	) {
		m, p, e := make(chan kafka.ConsumedMessage), make(chan kafka.DeepSearchProgress), make(chan error)
		go func() {
			<-ctx.Done()
			close(canceled)
			close(e)
			close(p)
			close(m)
		}()
		return m, p, e
	}
}

// TestSearchView_CancelKeepsPartialResults: x stops the scan, keeps what was
// found and says it is partial; with nothing running, x does nothing.
func TestSearchView_CancelKeepsPartialResults(t *testing.T) {
	t.Parallel()
	canceled := make(chan struct{})
	v := NewSearchView(blockingSearch(canceled), "orders", "q")
	cmd := v.Init()
	v.Update(SearchMatchMsg{ScanID: v.scanID, Msg: kafka.ConsumedMessage{Topic: "orders", Value: "q"}}) //nolint:errcheck

	if action, _ := v.HandleKey(KeyStopSearch); action != ActionStopSearch {
		t.Fatalf("x on a running search = %q, want %q", action, ActionStopSearch)
	}
	if !v.Cancel() {
		t.Fatal("Cancel() found nothing running")
	}
	select {
	case <-canceled:
	case <-time.After(2 * time.Second):
		t.Fatal("the scan's context was not canceled")
	}
	drainView(t, v, cmd)
	// The cancellation surfacing as an error must not replace "stopped".
	v.Update(SearchErrMsg{ScanID: v.scanID, Err: context.Canceled}) //nolint:errcheck
	if v.state != searchStateCanceled || v.Count() != 1 {
		t.Errorf("after cancel: state %v, %d matches; want canceled with the 1 match kept", v.state, v.Count())
	}
	if got := v.progressFragments(); !strings.Contains(got, "results are partial") {
		t.Errorf("header = %q, want it to say the results are partial", got)
	}
	if action, _ := v.HandleKey(KeyStopSearch); action != "" {
		t.Errorf("x with nothing running = %q, want nothing", action)
	}
	if v.Cancel() {
		t.Error("Cancel() reported stopping a search that had already stopped")
	}
}

// TestSearchView_StaleScanIgnoredAcrossViews: scan IDs are unique across
// views, so a replaced view's late message is not taken for the new view's.
func TestSearchView_StaleScanIgnoredAcrossViews(t *testing.T) {
	t.Parallel()
	old := NewSearchView(blockingSearch(make(chan struct{})), "orders", "q")
	old.Init()
	defer old.Stop()
	fresh := NewSearchView(blockingSearch(make(chan struct{})), "orders", "q")
	fresh.Init()
	defer fresh.Stop()

	if old.scanID == fresh.scanID {
		t.Fatalf("two views share scan ID %d", old.scanID)
	}
	if cmd := fresh.Update(SearchMatchMsg{ScanID: old.scanID, Msg: kafka.ConsumedMessage{Value: "q"}}); cmd != nil {
		t.Error("a stale message started another reader")
	}
	if fresh.Count() != 0 {
		t.Error("the new view took the old view's match")
	}
}
