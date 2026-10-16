// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package views

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"charm.land/bubbles/v2/table"
	tea "charm.land/bubbletea/v2"

	"github.com/blairham/k4a/internal/kafka"
)

// searchTopicsFixedCols lead a cross-topic results table with the topic.
var searchTopicsFixedCols = append([]table.Column{{Title: "TOPIC", Width: 24}}, msgFixedCols...)

// topicCoverage is what one topic of a cross-topic search has reported: its
// last progress frame and its error, if any. Coverage stays per topic so a
// topic that was truncated, capped or failed is named, rather than folded
// into a total that reads as one clean answer.
type topicCoverage struct {
	err  error
	prog kafka.DeepSearchProgress
	seen bool // a progress frame arrived, so the topic has started
}

// SearchTopicsMsg reports what a topic glob resolved to and, unless that
// failed, the merged stream searching them.
type SearchTopicsMsg struct {
	Err     error
	matchCh <-chan kafka.ConsumedMessage
	progCh  <-chan kafka.DeepSearchProgress
	errCh   <-chan error
	Topics  []string
	ScanID  int
}

// WithTopicResolver sets how a glob topic is resolved to topics. Without
// one, a glob search fails with an explanation instead of scanning nothing.
func (v *SearchView) WithTopicResolver(r TopicResolver) *SearchView {
	v.resolve = r
	return v
}

// Multi reports whether this view searches a topic glob.
func (v *SearchView) Multi() bool { return kafka.IsTopicGlob(v.topic) }

// errNoResolver is the error for a glob search with no way to list topics.
var errNoResolver = errors.New("searching several topics needs a cluster connection")

// startTopics resolves the glob off the UI goroutine, then starts one search
// per topic (kafka.SearchTopics) and hands the merged stream back.
func (v *SearchView) startTopics(ctx context.Context, params kafka.SearchParams) tea.Cmd {
	v.topics, v.perTopic = nil, nil
	id, glob, resolve, run := v.scanID, v.topic, v.resolve, v.runSearch
	return func() tea.Msg {
		if resolve == nil {
			return SearchTopicsMsg{ScanID: id, Err: errNoResolver}
		}
		topics, err := resolve(ctx, glob)
		if err != nil {
			return SearchTopicsMsg{ScanID: id, Err: err}
		}
		m, p, e := kafka.SearchTopics(ctx, kafka.TopicSearchFunc(run), topics, params)
		return SearchTopicsMsg{ScanID: id, Topics: topics, matchCh: m, progCh: p, errCh: e}
	}
}

// topicsResolved starts listening to the merged stream once the glob has
// resolved, or shows why it could not be.
func (v *SearchView) topicsResolved(m SearchTopicsMsg) tea.Cmd {
	if v.state == searchStateCanceled {
		return nil // stopped while resolving; the stream's ctx is already done
	}
	if m.Err != nil {
		v.state = searchStateErrored
		v.scanErr = m.Err
		return nil
	}
	v.topics = m.Topics
	v.perTopic = make(map[string]*topicCoverage, len(m.Topics))
	for _, t := range m.Topics {
		v.perTopic[t] = &topicCoverage{}
	}
	v.matchCh, v.progCh, v.errCh = m.matchCh, m.progCh, m.errCh
	return v.waitForNextEvent()
}

// coverage returns topic's entry, creating one for a topic the merged
// stream names but the resolution did not (it cannot, but a missing entry
// must never drop a frame that says a topic is incomplete).
func (v *SearchView) coverage(topic string) *topicCoverage {
	if v.perTopic == nil {
		v.perTopic = map[string]*topicCoverage{}
	}
	c, ok := v.perTopic[topic]
	if !ok {
		c = &topicCoverage{}
		v.perTopic[topic] = c
		v.topics = append(v.topics, topic)
	}
	return c
}

// topicProgress records one topic's frame and re-totals the header figures.
func (v *SearchView) topicProgress(p kafka.DeepSearchProgress) {
	c := v.coverage(p.Topic)
	c.prog, c.seen = p, true
	var total kafka.DeepSearchProgress
	for _, c := range v.perTopic {
		total.Scanned += c.prog.Scanned
		total.Total += c.prog.Total
		total.Bytes += c.prog.Bytes
		total.Matches += c.prog.Matches
		total.Partitions += c.prog.Partitions
		total.DonePartitions += c.prog.DonePartitions
		total.Elapsed = max(total.Elapsed, c.prog.Elapsed)
		total.Capped = total.Capped || c.prog.Capped
		total.Truncated = total.Truncated || c.prog.Truncated
	}
	v.progress = total
}

// topicErr records a failure against the topic it belongs to; the other
// topics keep going.
func (v *SearchView) topicErr(te *kafka.TopicError) {
	v.coverage(te.Topic).err = te.Err
}

// topicsTally counts the topics in each end state.
type topicsTally struct {
	firstErr                                error
	done, failed, incomplete, capped, total int
}

func (v *SearchView) tally() topicsTally {
	t := topicsTally{total: len(v.topics)}
	for _, name := range v.topics {
		c := v.perTopic[name]
		switch {
		case c == nil:
		case c.err != nil:
			t.failed++
			if t.firstErr == nil {
				t.firstErr = fmt.Errorf("%s: %w", name, c.err)
			}
		case c.prog.Truncated:
			t.incomplete++
		case c.prog.Capped:
			t.capped++
		case c.prog.Done:
			t.done++
		}
	}
	return t
}

// finishTopics sets the end state of a cross-topic search from its topics:
// it is complete only if every topic is. A topic that failed, could not be
// read in full or never reported finishing makes the whole answer
// INCOMPLETE; one that hit its cap makes it capped.
func (v *SearchView) finishTopics() {
	t := v.tally()
	switch {
	case t.total > 0 && t.failed == t.total:
		v.state = searchStateErrored
		v.scanErr = t.firstErr
	case t.done+t.capped < t.total: // failed, truncated, or never finished
		v.state = searchStateTruncated
	case t.capped > 0:
		v.state = searchStateCapped
	default:
		v.state = searchStateDone
	}
}

// topicsProgressFragments is the second header line of a cross-topic search.
func (v *SearchView) topicsProgressFragments() string {
	matches := len(v.matches)
	switch v.state {
	case searchStateRunning:
		if v.topics == nil {
			return "resolving topics…"
		}
		t := v.tally()
		return fmt.Sprintf(
			"running · %d/%d topics · %s msgs · %s · %s · %d matches",
			t.done+t.capped+t.failed+t.incomplete, t.total,
			formatCount(v.progress.Scanned), formatBytes(v.progress.Bytes),
			formatElapsed(v.progress.Elapsed), matches,
		)
	case searchStateDone:
		return fmt.Sprintf("complete · %d topics · %d matches · %s",
			len(v.topics), matches, formatElapsed(v.progress.Elapsed))
	case searchStateCapped:
		return fmt.Sprintf("%d of %d topics stopped at their match cap · %d matches · narrow your search",
			v.tally().capped, len(v.topics), matches)
	case searchStateTruncated:
		t := v.tally()
		return fmt.Sprintf(
			"INCOMPLETE · %d of %d topics not fully searched · %d matches — results are missing records",
			t.total-t.done-t.capped, t.total, matches,
		)
	case searchStateErrored:
		if v.scanErr != nil {
			return "error: " + v.scanErr.Error()
		}
		return "error"
	case searchStateCanceled:
		return fmt.Sprintf("stopped · %d matches · results are partial", matches)
	}
	return ""
}

// coverageLine is the per-topic coverage report: where each topic's answer
// came from and whether it is whole.
func (v *SearchView) coverageLine() string {
	if len(v.topics) == 0 {
		return ""
	}
	parts := make([]string, 0, len(v.topics))
	for _, name := range v.topics {
		parts = append(parts, name+": "+v.topicCoverageText(v.perTopic[name]))
	}
	return strings.Join(parts, " · ")
}

func (v *SearchView) topicCoverageText(c *topicCoverage) string {
	switch {
	case c == nil || !c.seen && c.err == nil:
		if v.state == searchStateRunning {
			return "queued"
		}
		return "not searched"
	case c.err != nil:
		return "error: " + c.err.Error()
	case c.prog.Truncated:
		return fmt.Sprintf("INCOMPLETE %d", c.prog.Matches)
	case c.prog.Capped:
		return fmt.Sprintf("capped %d", c.prog.Matches)
	case c.prog.Done:
		return fmt.Sprintf("%s %d", c.prog.Source.String(), c.prog.Matches)
	case v.state != searchStateRunning:
		return fmt.Sprintf("stopped %d", c.prog.Matches)
	}
	pct := int64(0)
	if c.prog.Total > 0 {
		pct = 100 * c.prog.Scanned / c.prog.Total
	}
	return fmt.Sprintf("%d%% %d", pct, c.prog.Matches)
}
