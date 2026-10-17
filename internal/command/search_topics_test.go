// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/searchcli"
)

// cannedTopics answers each topic from a fixed script.
func cannedTopics(script map[string]func() (int, kafka.DeepSearchProgress, error)) kafka.TopicSearchFunc {
	return func(_ context.Context, topic string, _ kafka.SearchParams) (
		<-chan kafka.ConsumedMessage, <-chan kafka.DeepSearchProgress, <-chan error,
	) {
		n, prog, err := script[topic]()
		m := make(chan kafka.ConsumedMessage, n)
		p := make(chan kafka.DeepSearchProgress, 1)
		e := make(chan error, 1)
		for i := range n {
			m <- kafka.ConsumedMessage{Topic: topic, Offset: int64(i)}
		}
		if err != nil {
			e <- err
		} else {
			p <- prog
		}
		close(e)
		close(p)
		close(m)
		return m, p, e
	}
}

// TestDrainTopicsReportsEachTopic: stderr names every topic's outcome, and
// the total says how many were not whole instead of adding them up as one.
func TestDrainTopicsReportsEachTopic(t *testing.T) {
	t.Parallel()
	run := cannedTopics(map[string]func() (int, kafka.DeepSearchProgress, error){
		"a": func() (int, kafka.DeepSearchProgress, error) {
			return 2, kafka.DeepSearchProgress{Done: true, Source: kafka.SourceCache}, nil
		},
		"b": func() (int, kafka.DeepSearchProgress, error) {
			return 1, kafka.DeepSearchProgress{Done: true, Truncated: true}, nil
		},
		"c": func() (int, kafka.DeepSearchProgress, error) {
			return 0, kafka.DeepSearchProgress{}, errors.New("not authorized")
		},
		"d": func() (int, kafka.DeepSearchProgress, error) {
			return 0, kafka.DeepSearchProgress{Scanned: 10}, nil // never said Done
		},
	})
	topics := []string{"a", "b", "c", "d"}
	m, p, e := kafka.SearchTopics(context.Background(), run, topics, kafka.SearchParams{})
	out := &searchOutput{jsonEnc: jsonDiscard()}
	cov, err := drainTopics(context.Background(), topics, m, p, e, out)
	if err != nil {
		t.Fatal(err)
	}
	rep := cov.report()
	for _, want := range []string{
		"a: cached · 2 matches",
		"b: scanned 0 msgs",
		"(INCOMPLETE",
		"c: error: not authorized · 0 matches",
		"d: not finished · 0 matches",
		"4 topics · 3 matches (INCOMPLETE — 3 of 4 topics not fully searched)",
	} {
		if !strings.Contains(rep, want) {
			t.Errorf("report lacks %q:\n%s", want, rep)
		}
	}
	if !cov.failed() {
		t.Error("topic c failed, yet the search would exit 0")
	}
}

func TestEmitTopicsSummaryExitStatus(t *testing.T) {
	t.Parallel()
	ok := searchcli.TopicsSummary{Topics: []searchcli.Summary{{Topic: "a"}, {Topic: "b"}}}
	if got := emitTopicsSummary(io.Discard, ok); got != 0 {
		t.Errorf("clean summary exits %d, want 0", got)
	}
	bad := searchcli.TopicsSummary{Topics: []searchcli.Summary{{Topic: "a"}, {Topic: "b", Error: "boom"}}}
	if got := emitTopicsSummary(io.Discard, bad); got != 1 {
		t.Errorf("a failed topic exits %d, want 1", got)
	}
}

func jsonDiscard() *json.Encoder { return json.NewEncoder(io.Discard) }
