// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package searchcli

import (
	"context"
	"sync"

	"github.com/blairham/k4a/internal/kafka"
)

// TopicsSummary is the summary of a search across the topics a glob
// selected. Each topic keeps its own complete Summary — its source, stats,
// capped and truncated flags — because a cross-topic answer is only as
// complete as its least complete topic, and a single merged Stats would
// hide which one that is.
type TopicsSummary struct {
	Query     string    `json:"query"`
	TopicGlob string    `json:"topic_glob"`
	Error     string    `json:"error,omitempty"`
	Topics    []Summary `json:"topics"`
	// Matches totals every topic's matches, for a quick look; per-topic
	// numbers and the matches themselves are under Topics.
	Matches       int `json:"matches"`
	SchemaVersion int `json:"schema_version"`
	// Incomplete is true when any topic failed, was truncated or was capped.
	Incomplete bool `json:"incomplete"`
}

// RunTopics runs search for each topic, at most kafka.MaxParallelTopics at
// once, and gathers the per-topic summaries in topic order.
func RunTopics(
	ctx context.Context,
	query, glob string,
	topics []string,
	search func(ctx context.Context, topic string) Summary,
) TopicsSummary {
	out := TopicsSummary{
		SchemaVersion: SummarySchemaVersion,
		Query:         query,
		TopicGlob:     glob,
		Topics:        make([]Summary, len(topics)),
	}
	sem := make(chan struct{}, kafka.MaxParallelTopics)
	var wg sync.WaitGroup
	for i, topic := range topics {
		sem <- struct{}{}
		wg.Go(func() {
			defer func() { <-sem }()
			out.Topics[i] = search(ctx, topic)
		})
	}
	wg.Wait()
	for i := range out.Topics {
		s := &out.Topics[i]
		out.Matches += len(s.Matches)
		if s.Error != "" || s.Stats.Truncated || s.Stats.Capped {
			out.Incomplete = true
		}
	}
	return out
}
