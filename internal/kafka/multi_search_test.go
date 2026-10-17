// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestIsTopicGlob(t *testing.T) {
	t.Parallel()
	for in, want := range map[string]bool{
		"orders":         false,
		"orders.v1-dlq_": false,
		"orders.*":       true,
		"orders.v?":      true,
		"orders.[ab]":    true,
	} {
		if got := IsTopicGlob(in); got != want {
			t.Errorf("IsTopicGlob(%q) = %v, want %v", in, got, want)
		}
	}
}

func TestMatchTopics(t *testing.T) {
	t.Parallel()
	infos := []TopicInfo{
		{Name: "orders.v2"},
		{Name: "orders.v1"},
		{Name: "orders-dlq"},
		{Name: "payments"},
		{Name: "__consumer_offsets", Internal: true},
		{Name: "_schemas", Internal: true},
	}
	tests := []struct {
		glob string
		want []string
	}{
		{"orders.*", []string{"orders.v1", "orders.v2"}},
		{"orders*", []string{"orders-dlq", "orders.v1", "orders.v2"}},
		{"orders.v[2-9]", []string{"orders.v2"}},
		// "*" means the user's topics; asking for "_" opts into internal ones.
		{"*", []string{"orders-dlq", "orders.v1", "orders.v2", "payments"}},
		{"_*", []string{"__consumer_offsets", "_schemas"}},
		{"nothing.*", nil},
	}
	for _, tt := range tests {
		got, err := MatchTopics(tt.glob, infos)
		if err != nil {
			t.Errorf("MatchTopics(%q): %v", tt.glob, err)
			continue
		}
		if !slices.Equal(got, tt.want) {
			t.Errorf("MatchTopics(%q) = %q, want %q", tt.glob, got, tt.want)
		}
	}
	if _, err := MatchTopics("orders.[", infos); err == nil {
		t.Error("a malformed glob was accepted")
	}
}

// fakeTopicRun answers each topic from a canned script and records how many
// topics were searched at once.
type fakeTopicRun struct {
	script  map[string]fakeTopicScript
	started sync.Map
	hold    time.Duration
	active  atomic.Int32
	peak    atomic.Int32
}

type fakeTopicScript struct {
	err     error
	prog    DeepSearchProgress
	matches int
}

func (f *fakeTopicRun) run(ctx context.Context, topic string, _ SearchParams) (
	<-chan ConsumedMessage, <-chan DeepSearchProgress, <-chan error,
) {
	f.started.Store(topic, true)
	s := f.script[topic]
	m := make(chan ConsumedMessage)
	p := make(chan DeepSearchProgress, 1)
	e := make(chan error, 1)
	go func() {
		defer close(m)
		defer close(p)
		defer close(e)
		n := f.active.Add(1)
		defer f.active.Add(-1)
		for {
			old := f.peak.Load()
			if n <= old || f.peak.CompareAndSwap(old, n) {
				break
			}
		}
		select {
		case <-time.After(f.hold):
		case <-ctx.Done():
			return
		}
		for i := range s.matches {
			// Sources set Topic themselves except where they cannot; leave it
			// empty here so SearchTopics has to fill it in.
			select {
			case m <- ConsumedMessage{Offset: int64(i)}:
			case <-ctx.Done():
				return
			}
		}
		if s.err != nil {
			e <- s.err
			return
		}
		p <- s.prog
	}()
	return m, p, e
}

type collected struct {
	matches map[string]int
	last    map[string]DeepSearchProgress
	errs    []error
}

func collectTopics(m <-chan ConsumedMessage, p <-chan DeepSearchProgress, e <-chan error) collected {
	out := collected{matches: map[string]int{}, last: map[string]DeepSearchProgress{}}
	for m != nil || p != nil || e != nil {
		select {
		case msg, ok := <-m:
			if !ok {
				m = nil
				continue
			}
			out.matches[msg.Topic]++
		case prog, ok := <-p:
			if !ok {
				p = nil
				continue
			}
			out.last[prog.Topic] = prog
		case err, ok := <-e:
			if !ok {
				e = nil
				continue
			}
			out.errs = append(out.errs, err)
		}
	}
	return out
}

// TestSearchTopicsKeepsCoveragePerTopic: every topic's final frame arrives
// tagged with its topic — including the one that says it is incomplete — and
// one topic's error neither stops nor taints the others.
func TestSearchTopicsKeepsCoveragePerTopic(t *testing.T) {
	t.Parallel()
	boom := errors.New("boom")
	f := &fakeTopicRun{script: map[string]fakeTopicScript{
		"a": {matches: 3, prog: DeepSearchProgress{Done: true, Matches: 3, Source: SourceScan}},
		"b": {matches: 1, prog: DeepSearchProgress{Done: true, Matches: 1, Truncated: true}},
		"c": {err: boom},
		"d": {prog: DeepSearchProgress{Done: true, Source: SourceSharedIndex}},
	}}
	got := collectTopics(SearchTopics(context.Background(), f.run, []string{"a", "b", "c", "d"}, SearchParams{}))

	if got.matches["a"] != 3 || got.matches["b"] != 1 || got.matches[""] != 0 {
		t.Errorf("matches by topic = %v, want a:3 b:1 and none untagged", got.matches)
	}
	if !got.last["b"].Truncated {
		t.Error("topic b's truncated frame was lost: an incomplete topic would read as complete")
	}
	if got.last["a"].Truncated || got.last["d"].Truncated {
		t.Error("one topic's truncation leaked into another's coverage")
	}
	if got.last["d"].Source != SourceSharedIndex || got.last["a"].Source != SourceScan {
		t.Errorf("sources = a:%v d:%v; each topic keeps its own", got.last["a"].Source, got.last["d"].Source)
	}
	var te *TopicError
	if len(got.errs) != 1 || !errors.As(got.errs[0], &te) || te.Topic != "c" || !errors.Is(te, boom) {
		t.Fatalf("errors = %v, want one TopicError for c wrapping boom", got.errs)
	}
}

func TestSearchTopicsBoundsParallelism(t *testing.T) {
	t.Parallel()
	f := &fakeTopicRun{script: map[string]fakeTopicScript{}, hold: 20 * time.Millisecond}
	topics := make([]string, 3*MaxParallelTopics)
	for i := range topics {
		topics[i] = fmt.Sprintf("t%d", i)
		f.script[topics[i]] = fakeTopicScript{prog: DeepSearchProgress{Done: true}}
	}
	got := collectTopics(SearchTopics(context.Background(), f.run, topics, SearchParams{}))
	if len(got.last) != len(topics) {
		t.Errorf("%d topics finished, want %d", len(got.last), len(topics))
	}
	if peak := f.peak.Load(); peak > MaxParallelTopics || peak < 2 {
		t.Errorf("peak concurrent topics = %d, want 2..%d", peak, MaxParallelTopics)
	}
}

// TestSearchTopicsCancel: canceling ends the merged stream without anyone
// reading it, and topics still queued are never started.
func TestSearchTopicsCancel(t *testing.T) {
	t.Parallel()
	f := &fakeTopicRun{script: map[string]fakeTopicScript{}, hold: time.Hour}
	topics := make([]string, 2*MaxParallelTopics)
	for i := range topics {
		topics[i] = fmt.Sprintf("t%d", i)
	}
	ctx, cancel := context.WithCancel(context.Background())
	m, p, e := SearchTopics(ctx, f.run, topics, SearchParams{})
	deadline := time.Now().Add(2 * time.Second)
	for f.active.Load() < MaxParallelTopics && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()

	done := make(chan struct{})
	go func() {
		collectTopics(m, p, e)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("the merged search did not end after cancel")
	}
	if _, ok := f.started.Load(topics[len(topics)-1]); ok {
		t.Error("a queued topic was started after cancel")
	}
}
