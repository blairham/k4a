// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/charmbracelet/log"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/remoteindex"
	"github.com/blairham/k4a/internal/searchcache"
	"github.com/blairham/k4a/internal/searchcli"
)

// topicsSearch is what a cross-topic `k4a search -t GLOB` runs with.
type topicsSearch struct {
	client  *kafka.Client
	cache   *searchcache.Cache
	remote  *remoteindex.Client
	flags   *SearchFlags
	pattern string
	format  string
	params  kafka.SearchParams
}

// runTopicsSearch resolves the glob and searches every topic it selects.
// Exit status is 1 when the glob selects nothing or any topic failed: a
// partial answer must not read as success to a script.
func runTopicsSearch(ctx context.Context, s topicsSearch) int {
	topics, err := s.client.ResolveTopics(ctx, s.flags.Topic)
	if err != nil {
		log.Error("resolving topics", "err", err)
		return 1
	}

	if s.format == formatSummary {
		sum := searchcli.RunTopics(ctx, s.pattern, s.flags.Topic, topics,
			func(ctx context.Context, topic string) searchcli.Summary {
				return searchcli.Run(ctx, s.cache, s.client, s.remote, topic, s.pattern, s.params, s.flags.CaseSensitive)
			})
		return emitTopicsSummary(os.Stdout, sum)
	}

	search := func(ctx context.Context, topic string, p kafka.SearchParams) (
		<-chan kafka.ConsumedMessage, <-chan kafka.DeepSearchProgress, <-chan error,
	) {
		return s.cache.Search(ctx, s.client, topic, p)
	}
	if s.remote != nil {
		search = s.remote.Wrap(search)
	}
	matchCh, progCh, errCh := kafka.SearchTopics(ctx, search, topics, s.params)

	out := newSearchOutput(s.format)
	out.withTopic = true
	cov, err := drainTopics(ctx, topics, matchCh, progCh, errCh, out)
	if err != nil {
		log.Error("writing match", "err", err)
		return 1
	}
	if !s.flags.Quiet {
		fmt.Fprint(os.Stderr, cov.report())
	}
	if cov.failed() {
		return 1
	}
	return 0
}

// emitTopicsSummary writes the per-topic summary document, returning 1 if it
// could not be written or any topic's search failed.
func emitTopicsSummary(w io.Writer, sum searchcli.TopicsSummary) int {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")
	if err := enc.Encode(sum); err != nil {
		log.Error("writing summary", "err", err)
		return 1
	}
	if sum.Error != "" {
		return 1
	}
	for i := range sum.Topics {
		if sum.Topics[i].Error != "" {
			return 1
		}
	}
	return 0
}

// topicResult is one topic's share of a streamed cross-topic search.
type topicResult struct {
	err     error
	prog    kafka.DeepSearchProgress
	matches int
	seen    bool
}

// topicsCoverage is the per-topic outcome of a streamed cross-topic search.
type topicsCoverage struct {
	byTopic map[string]*topicResult
	topics  []string
}

func newTopicsCoverage(topics []string) *topicsCoverage {
	c := &topicsCoverage{byTopic: make(map[string]*topicResult, len(topics)), topics: topics}
	for _, t := range topics {
		c.byTopic[t] = &topicResult{}
	}
	return c
}

func (c *topicsCoverage) of(topic string) *topicResult {
	r, ok := c.byTopic[topic]
	if !ok {
		r = &topicResult{}
		c.byTopic[topic] = r
		c.topics = append(c.topics, topic)
		sort.Strings(c.topics)
	}
	return r
}

func (c *topicsCoverage) failed() bool {
	for _, r := range c.byTopic {
		if r.err != nil {
			return true
		}
	}
	return false
}

// drainTopics emits every match as it arrives and records, per topic, the
// last progress frame, the match count and any error.
func drainTopics(
	ctx context.Context,
	topics []string,
	matchCh <-chan kafka.ConsumedMessage,
	progCh <-chan kafka.DeepSearchProgress,
	errCh <-chan error,
	out *searchOutput,
) (*topicsCoverage, error) {
	cov := newTopicsCoverage(topics)
	for matchCh != nil || progCh != nil || errCh != nil {
		select {
		case <-ctx.Done():
			matchCh, progCh, errCh = nil, nil, nil
		case m, ok := <-matchCh:
			if !ok {
				matchCh = nil
				continue
			}
			cov.of(m.Topic).matches++
			if err := out.emit(m); err != nil {
				return cov, err
			}
		case p, ok := <-progCh:
			if !ok {
				progCh = nil
				continue
			}
			r := cov.of(p.Topic)
			r.prog, r.seen = p, true
		case e, ok := <-errCh:
			if !ok {
				errCh = nil
				continue
			}
			cov.recordErr(e)
		}
	}
	return cov, nil
}

func (c *topicsCoverage) recordErr(e error) {
	if e == nil {
		return
	}
	var te *kafka.TopicError
	if errors.As(e, &te) {
		c.of(te.Topic).err = te.Err
		return
	}
	for _, t := range c.topics {
		if r := c.byTopic[t]; r.err == nil {
			r.err = e
		}
	}
}

// report is the stderr summary: one line per topic, so a truncated, capped,
// failed or unfinished topic is named rather than folded into a total.
func (c *topicsCoverage) report() string {
	var b strings.Builder
	total, incomplete := 0, 0
	b.WriteString("\n")
	for _, t := range c.topics {
		r := c.byTopic[t]
		total += r.matches
		line, whole := r.line()
		if !whole {
			incomplete++
		}
		fmt.Fprintf(&b, "%s: %s\n", t, line)
	}
	fmt.Fprintf(&b, "%d topics · %d matches", len(c.topics), total)
	if incomplete > 0 {
		fmt.Fprintf(&b, " (INCOMPLETE — %d of %d topics not fully searched)", incomplete, len(c.topics))
	}
	b.WriteString("\n")
	return b.String()
}

// line describes one topic's outcome and whether it is a whole answer.
func (r *topicResult) line() (string, bool) {
	switch {
	case r.err != nil:
		return fmt.Sprintf("error: %v · %d matches", r.err, r.matches), false
	case !r.seen || !r.prog.Done:
		return fmt.Sprintf("not finished · %d matches", r.matches), false
	}
	s := searchSummaryLine(r.prog, r.matches)
	s = strings.TrimPrefix(s, "\n")
	return s, !r.prog.Incomplete()
}
