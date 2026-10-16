// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package searchcli

import (
	"context"
	"testing"
)

func TestRunTopicsKeepsEachTopicsSummary(t *testing.T) {
	t.Parallel()
	answers := map[string]Summary{
		"a": {Topic: "a", Matches: []Match{{Topic: "a"}, {Topic: "a"}}, Stats: Stats{Source: "scanned"}},
		"b": {Topic: "b", Matches: []Match{{Topic: "b"}}, Stats: Stats{Source: "indexed (shared)", Truncated: true}},
		"c": {Topic: "c", Stats: Stats{Source: "cached"}},
	}
	got := RunTopics(context.Background(), "q", "*", []string{"a", "b", "c"},
		func(_ context.Context, topic string) Summary { return answers[topic] })

	if got.Matches != 3 || len(got.Topics) != 3 {
		t.Fatalf("matches %d over %d topics, want 3 over 3", got.Matches, len(got.Topics))
	}
	for i, want := range []string{"a", "b", "c"} {
		if got.Topics[i].Topic != want || got.Topics[i].Stats.Source != answers[want].Stats.Source {
			t.Errorf("topic %d = %q from %q, want %q in order with its own source",
				i, got.Topics[i].Topic, got.Topics[i].Stats.Source, want)
		}
	}
	if !got.Incomplete {
		t.Error("b was truncated, yet the cross-topic summary claims to be complete")
	}

	delete(answers, "b")
	got = RunTopics(context.Background(), "q", "*", []string{"a", "c"},
		func(_ context.Context, topic string) Summary { return answers[topic] })
	if got.Incomplete {
		t.Error("every topic was whole, yet the summary says incomplete")
	}
}
