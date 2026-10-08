// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

// newConsumerClient builds a fresh kgo.Client configured to consume the given
// partitions starting at the given offsets. The returned client is independent
// of c.kgoClient (which is reserved for admin operations). Caller is
// responsible for Close.
func (c *Client) newConsumerClient(partitions map[string]map[int32]kgo.Offset) (*kgo.Client, error) {
	opts, err := NewClientOpts(c.authCfg, c.brokers)
	if err != nil {
		return nil, err
	}
	opts = append(opts, kgo.ConsumePartitions(partitions))
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("creating consumer client: %w", err)
	}
	return cl, nil
}

// recordToConsumed adapts a kgo.Record to the ConsumedMessage type used
// throughout the rest of k4a.
func recordToConsumed(r *kgo.Record) ConsumedMessage {
	headers := make([]MessageHeader, 0, len(r.Headers))
	for _, h := range r.Headers {
		headers = append(headers, MessageHeader{
			Key:   h.Key,
			Value: bytesToDisplay(h.Value),
		})
	}
	return ConsumedMessage{
		Key:       bytesToDisplay(r.Key),
		Value:     bytesToDisplay(r.Value),
		Topic:     r.Topic,
		Headers:   headers,
		Partition: int(r.Partition),
		Offset:    r.Offset,
		Time:      r.Timestamp,
	}
}

// ConsumeRange reads records from `topic` starting at `starts[partition]`
// for each listed partition, stopping each partition when its record's
// offset reaches `ends[partition]` (exclusive). onRecord is invoked for
// every record in range; it must not block long since it runs on the
// fetch goroutine. Returns when every selected partition has reached
// its end offset, ctx is canceled, or the broker reports a fatal error.
//
// Used by the index catch-up path (internal/index/catchup.go) to fill
// the gap between an indexer's newest-per-partition offset and the
// broker's current high watermark.
func (c *Client) ConsumeRange(
	ctx context.Context,
	topic string,
	starts, ends map[int32]int64,
	onRecord func(ConsumedMessage),
) error {
	if onRecord == nil {
		return fmt.Errorf("onRecord must not be nil")
	}
	if len(starts) == 0 {
		return nil
	}

	partitions := make(map[int32]kgo.Offset, len(starts))
	remaining := make(map[int32]int64, len(starts))
	for p, off := range starts {
		end, ok := ends[p]
		if !ok || end <= off {
			continue
		}
		partitions[p] = kgo.NewOffset().At(off)
		remaining[p] = end
	}
	if len(partitions) == 0 {
		return nil
	}

	cl, err := c.newConsumerClient(map[string]map[int32]kgo.Offset{topic: partitions})
	if err != nil {
		return err
	}
	defer cl.Close()

	for len(remaining) > 0 {
		if ctx.Err() != nil {
			return nil
		}
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fe := range errs {
				if ctx.Err() != nil {
					return nil
				}
				if IsAuthError(fe.Err) || isConnectionError(fe.Err) {
					return fe.Err
				}
			}
		}
		fetches.EachRecord(func(r *kgo.Record) {
			end, ok := remaining[r.Partition]
			if !ok {
				return
			}
			if r.Offset >= end {
				// This partition is done; remove from the tracking map
				// so we don't keep counting overshoots.
				delete(remaining, r.Partition)
				return
			}
			onRecord(recordToConsumed(r))
			if r.Offset+1 >= end {
				delete(remaining, r.Partition)
			}
		})
	}
	return nil
}

// ConsumeBlocking reads messages from a topic and calls onMessage for each one.
// It blocks until the context is canceled. When groupID is empty, it reads
// from partition 0 starting at the earliest offset. When groupID is set, it
// joins the group and consumes its assigned partitions.
func (c *Client) ConsumeBlocking(
	ctx context.Context,
	topic string,
	groupID string,
	onMessage func(ConsumedMessage),
) error {
	opts, err := NewClientOpts(c.authCfg, c.brokers)
	if err != nil {
		return err
	}
	if groupID != "" {
		opts = append(
			opts,
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(groupID),
		)
	} else {
		opts = append(opts, kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			topic: {0: kgo.NewOffset().AtStart()},
		}))
	}

	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return fmt.Errorf("creating consumer client: %w", err)
	}
	defer cl.Close()

	for {
		if ctx.Err() != nil {
			return nil
		}
		fetches := cl.PollFetches(ctx)
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fe := range errs {
				if ctx.Err() != nil {
					return nil
				}
				if IsAuthError(fe.Err) || isConnectionError(fe.Err) {
					return fe.Err
				}
			}
		}
		fetches.EachRecord(func(r *kgo.Record) {
			onMessage(recordToConsumed(r))
		})
	}
}

// Consume reads the latest messages from a topic and then tails for new ones.
// It fetches up to `initialMessageFetch` recent messages, then continues
// tailing for new ones. The ready channel closes once the initial fetch has
// started flowing (the first PollFetches has returned).
//
// The returned error channel carries fatal errors such as credential failures.
func (c *Client) Consume(ctx context.Context, topic string) (<-chan ConsumedMessage, <-chan error, <-chan struct{}) {
	ch := make(chan ConsumedMessage, consumerChannelBuffer)
	errCh := make(chan error, 1)
	ready := make(chan struct{})

	go c.runConsume(ctx, topic, ch, errCh, ready)
	return ch, errCh, ready
}

func (c *Client) runConsume(
	ctx context.Context,
	topic string,
	ch chan<- ConsumedMessage,
	errCh chan<- error,
	ready chan<- struct{},
) {
	defer close(ch)

	tds, err := c.admin.ListTopicsWithInternal(ctx, topic)
	if err != nil {
		sendErr(errCh, err)
		close(ready)
		return
	}
	td, ok := tds[topic]
	if !ok || len(td.Partitions) == 0 {
		close(ready)
		return
	}

	numPartitions := len(td.Partitions)
	perPartition := initialMessageFetch / numPartitions
	if perPartition < 1 {
		perPartition = 1
	}

	// Start each partition at end-perPartition so the first poll surfaces the
	// most recent messages, then the consumer keeps reading as new records
	// arrive — equivalent to the old fetchRecentMessages + tailPartition
	// handoff but without an explicit offset switchover.
	partitions := make(map[int32]kgo.Offset, numPartitions)
	for p := range td.Partitions {
		partitions[p] = kgo.NewOffset().AtEnd().Relative(-int64(perPartition))
	}

	cl, err := c.newConsumerClient(map[string]map[int32]kgo.Offset{topic: partitions})
	if err != nil {
		sendErr(errCh, err)
		close(ready)
		return
	}
	defer cl.Close()

	first := true
	for {
		if ctx.Err() != nil {
			return
		}
		fetches := cl.PollFetches(ctx)
		if first {
			close(ready)
			first = false
		}
		if errs := fetches.Errors(); len(errs) > 0 {
			for _, fe := range errs {
				if ctx.Err() != nil {
					return
				}
				if IsAuthError(fe.Err) || isConnectionError(fe.Err) {
					sendErr(errCh, fe.Err)
					return
				}
			}
		}
		fetches.EachRecord(func(r *kgo.Record) {
			msg := recordToConsumed(r)
			select {
			case ch <- msg:
			case <-ctx.Done():
				return
			}
		})
	}
}

// sendErr sends an error on the channel without blocking. Only the first error is kept.
func sendErr(ch chan<- error, err error) {
	select {
	case ch <- err:
	default:
	}
}
