// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"errors"
	"fmt"
	"path"
	"sort"
	"strings"
	"sync"
)

// MaxParallelTopics bounds how many topics a multi-topic search scans at
// once. Each topic's scan already fans out to a worker per partition (and up
// to eight fetchers per partition), so running every topic of a wide glob at
// once would multiply that by the topic count.
const MaxParallelTopics = 4

// IsTopicGlob reports whether s selects topics by glob rather than naming
// one. Kafka topic names are limited to [a-zA-Z0-9._-], so a *, ? or [
// cannot be part of a real name and the two readings never collide.
func IsTopicGlob(s string) bool { return strings.ContainsAny(s, "*?[") }

// MatchTopics returns the names in topics that glob selects (path.Match
// syntax), sorted. Internal topics (__consumer_offsets and other
// underscore-prefixed names) are left out unless the glob itself starts with
// an underscore: "*" means the user's topics, not the cluster's bookkeeping.
func MatchTopics(glob string, topics []TopicInfo) ([]string, error) {
	if _, err := path.Match(glob, ""); err != nil {
		return nil, fmt.Errorf("invalid topic glob %q: %w", glob, err)
	}
	wantInternal := strings.HasPrefix(glob, "_")
	var out []string
	for _, t := range topics {
		if t.Internal && !wantInternal {
			continue
		}
		if ok, _ := path.Match(glob, t.Name); ok { //nolint:errcheck // the pattern was validated above
			out = append(out, t.Name)
		}
	}
	sort.Strings(out)
	return out, nil
}

// ErrNoTopicMatch is returned when a topic glob selects no topic.
var ErrNoTopicMatch = errors.New("no topic matches")

// ResolveTopics lists the cluster's topics and returns those glob selects.
// A glob that selects nothing is an error: searching zero topics would
// otherwise report a clean, empty result for a typo.
func (c *Client) ResolveTopics(ctx context.Context, glob string) ([]string, error) {
	infos, err := c.FetchTopics(ctx)
	if err != nil {
		return nil, err
	}
	names, err := MatchTopics(glob, infos)
	if err != nil {
		return nil, err
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("%w %q", ErrNoTopicMatch, glob)
	}
	return names, nil
}

// TopicSearchFunc runs a search against one topic in the standard
// three-channel shape (DeepSearch, or a cache- or index-wrapped variant).
type TopicSearchFunc func(
	ctx context.Context,
	topic string,
	params SearchParams,
) (<-chan ConsumedMessage, <-chan DeepSearchProgress, <-chan error)

// TopicError is an error from one topic of a multi-topic search. The other
// topics keep going, so callers record it against the topic rather than
// treating it as the end of the whole search.
type TopicError struct {
	Err   error
	Topic string
}

func (e *TopicError) Error() string { return e.Topic + ": " + e.Err.Error() }

func (e *TopicError) Unwrap() error { return e.Err }

// SearchTopics runs run once per topic, at most MaxParallelTopics at a time,
// and merges the results into one three-channel stream. Each topic is its
// own search with its own source decision (cache, index, shared index or
// scan) and its own match cap, so its coverage is reported per topic: every
// progress frame carries Topic, and every error is a *TopicError.
//
// Unlike a single search, progress frames are never dropped: a topic's last
// frame is the only place it says it was capped or truncated, and losing it
// would make an incomplete topic look complete. All three channels close
// once every topic has finished or ctx ends.
func SearchTopics(
	ctx context.Context,
	run TopicSearchFunc,
	topics []string,
	params SearchParams,
) (<-chan ConsumedMessage, <-chan DeepSearchProgress, <-chan error) {
	matchCh := make(chan ConsumedMessage, consumerChannelBuffer)
	progCh := make(chan DeepSearchProgress, len(topics)+1)
	errCh := make(chan error, len(topics))

	go func() {
		defer close(matchCh)
		defer close(progCh)
		defer close(errCh)

		sem := make(chan struct{}, MaxParallelTopics)
		var wg sync.WaitGroup
		for _, topic := range topics {
			select {
			case sem <- struct{}{}:
			case <-ctx.Done():
			}
			if ctx.Err() != nil {
				break
			}
			wg.Go(func() {
				defer func() { <-sem }()
				m, p, e := run(ctx, topic, params)
				topicRelay{matchCh: matchCh, progCh: progCh, errCh: errCh, topic: topic}.forward(ctx, m, p, e)
			})
		}
		wg.Wait()
	}()
	return matchCh, progCh, errCh
}

// topicRelay copies one topic's search into the merged channels, tagging
// what it forwards with the topic.
type topicRelay struct {
	matchCh chan<- ConsumedMessage
	progCh  chan<- DeepSearchProgress
	errCh   chan<- error
	topic   string
}

// forward relays until every source channel closes or ctx ends. Every send
// also watches ctx, so a canceled search never leaves a goroutine blocked on
// a reader that left.
func (r topicRelay) forward(
	ctx context.Context,
	srcMatch <-chan ConsumedMessage,
	srcProg <-chan DeepSearchProgress,
	srcErr <-chan error,
) {
	sent := true
	for sent && (srcMatch != nil || srcProg != nil || srcErr != nil) {
		select {
		case <-ctx.Done():
			return
		case m, ok := <-srcMatch:
			if !ok {
				srcMatch = nil
				continue
			}
			sent = r.match(ctx, m)
		case p, ok := <-srcProg:
			if !ok {
				srcProg = nil
				continue
			}
			p.Topic = r.topic
			sent = sendCtx(ctx, r.progCh, p)
		case e, ok := <-srcErr:
			if !ok {
				srcErr = nil
				continue
			}
			sent = e == nil || sendCtx[error](ctx, r.errCh, &TopicError{Topic: r.topic, Err: e})
		}
	}
}

func (r topicRelay) match(ctx context.Context, m ConsumedMessage) bool {
	if m.Topic == "" {
		m.Topic = r.topic
	}
	return sendCtx(ctx, r.matchCh, m)
}

// sendCtx sends v on ch unless ctx ends first, reporting whether it sent.
func sendCtx[T any](ctx context.Context, ch chan<- T, v T) bool {
	select {
	case ch <- v:
		return true
	case <-ctx.Done():
		return false
	}
}
