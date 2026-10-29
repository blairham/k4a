// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// KeyedLog is a compacted topic read and written as a small key-value table:
// the latest record for a key is its value and a tombstone (nil value) deletes
// it. Snapshot reads the whole table up to the current end of each partition;
// Tail follows the writes made after it. k4a-index keeps its follow-set in one
// (docs/design/shared-index-service.md, #123), so a new pod finds what its
// predecessors followed and every replica of a set sees what the others did.
//
// Writes go through a producer of their own with idempotence off: a table
// write is a whole-value overwrite, so a duplicate is harmless, and leaving
// idempotence off spares the deployment the cluster-level idempotent-write
// grant (on MSK IAM, kafka-cluster:WriteDataIdempotently).
type KeyedLog struct {
	c     *Client
	prod  *kgo.Client
	topic string
}

// OpenKeyedLog returns the table kept in topic, creating the topic — one
// partition, the broker's default replication, cleanup.policy=compact — when
// the cluster does not have it. Creating needs the topic Create grant; a
// deployment without it should create the topic itself. Close releases it.
func (c *Client) OpenKeyedLog(ctx context.Context, topic string) (*KeyedLog, error) {
	if err := c.ensureCompactedTopic(ctx, topic); err != nil {
		return nil, err
	}
	opts, err := NewClientOpts(c.authCfg, c.brokers)
	if err != nil {
		return nil, err
	}
	opts = append(opts,
		kgo.DisableIdempotentWrite(),
		kgo.RequiredAcks(kgo.AllISRAcks()),
		kgo.DefaultProduceTopic(topic),
	)
	prod, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("creating producer client: %w", err)
	}
	return &KeyedLog{c: c, prod: prod, topic: topic}, nil
}

// ensureCompactedTopic creates topic compacted unless it exists. It asks to
// create first and looks second: a metadata lookup of a missing topic would be
// cached as missing, and the reads that follow creation would believe it.
// Without the Create grant the broker refuses even an existing topic, so a
// refusal is settled by looking.
func (c *Client) ensureCompactedTopic(ctx context.Context, topic string) error {
	compact := "compact"
	_, err := c.admin.CreateTopic(ctx, 1, -1, map[string]*string{cleanupPolicyConfig: &compact}, topic)
	switch {
	case err == nil:
		return c.waitTopicVisible(ctx, topic)
	case errors.Is(err, kerr.TopicAlreadyExists):
		return nil
	}
	createErr := err
	tds, err := c.admin.ListTopics(ctx, topic)
	if err != nil {
		return fmt.Errorf("creating topic %q: %w; listing it: %w", topic, createErr, err)
	}
	if td, ok := tds[topic]; !ok || td.Err != nil {
		return fmt.Errorf("topic %q does not exist and could not be created: %w", topic, createErr)
	}
	return nil
}

// waitTopicVisible polls uncached metadata until a just-created topic has its
// partitions, so the first Snapshot does not find it missing.
func (c *Client) waitTopicVisible(ctx context.Context, topic string) error {
	for {
		req := kmsg.NewPtrMetadataRequest()
		rt := kmsg.NewMetadataRequestTopic()
		rt.Topic = kmsg.StringPtr(topic)
		req.Topics = append(req.Topics, rt)
		resp, err := req.RequestWith(ctx, c.kgoClient)
		if err == nil && len(resp.Topics) == 1 && resp.Topics[0].ErrorCode == 0 && len(resp.Topics[0].Partitions) > 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("waiting for created topic %q: %w", topic, ctx.Err())
		case <-time.After(100 * time.Millisecond):
		}
	}
}

// cleanupPolicyConfig is the topic config that decides compaction.
const cleanupPolicyConfig = "cleanup.policy"

// Topic is the table's topic name.
func (l *KeyedLog) Topic() string { return l.topic }

// Close releases the producer. Idempotent.
func (l *KeyedLog) Close() {
	if l.prod != nil {
		l.prod.Close()
	}
}

// CleanupPolicy reports the topic's cleanup.policy, so a caller can warn about
// a table on a topic that deletes by time instead of compacting: such a table
// forgets every key whose last write is older than the retention.
func (l *KeyedLog) CleanupPolicy(ctx context.Context) (string, error) {
	entries, err := l.c.FetchTopicConfigs(ctx, l.topic)
	if err != nil {
		return "", err
	}
	for _, e := range entries {
		if e.Name == cleanupPolicyConfig {
			return e.Value, nil
		}
	}
	return "delete", nil
}

// Put writes key's value; a nil value is a tombstone that deletes it.
func (l *KeyedLog) Put(ctx context.Context, key string, value []byte) error {
	return l.prod.ProduceSync(ctx, &kgo.Record{Key: []byte(key), Value: value}).FirstErr()
}

// Snapshot reads the table as of now: every key's latest value, with deleted
// keys absent. next is the offset to Tail from in each partition, so no write
// made after the snapshot is missed.
func (l *KeyedLog) Snapshot(ctx context.Context) (table map[string][]byte, next map[int32]int64, err error) {
	next, from, err := l.snapshotRange(ctx)
	if err != nil {
		return nil, nil, err
	}
	table = map[string][]byte{}
	if len(from) == 0 {
		return table, next, nil
	}
	remaining := make(map[int32]int64, len(from))
	offsets := make(map[int32]kgo.Offset, len(from))
	for p, start := range from {
		offsets[p] = kgo.NewOffset().At(start)
		remaining[p] = next[p]
	}
	cl, ctx, err := l.c.newConsumerClient(ctx, map[string]map[int32]kgo.Offset{l.topic: offsets})
	if err != nil {
		return nil, nil, err
	}
	defer cl.Close()
	for len(remaining) > 0 {
		fetches := cl.PollFetches(ctx)
		if ctx.Err() != nil {
			return nil, nil, context.Cause(ctx)
		}
		if stop, err := fatalFetchError(ctx, fetches); stop {
			return nil, nil, err
		}
		fetches.EachRecord(func(r *kgo.Record) {
			if end, ok := remaining[r.Partition]; ok && r.Offset < end {
				applyKeyed(table, r)
				if r.Offset+1 >= end {
					delete(remaining, r.Partition)
				}
			}
		})
	}
	return table, next, nil
}

// snapshotRange lists each partition's end offset (next) and, for the
// partitions that hold anything, the offset to start reading from.
func (l *KeyedLog) snapshotRange(ctx context.Context) (next, from map[int32]int64, err error) {
	ends, err := l.c.admin.ListEndOffsets(ctx, l.topic)
	if err != nil {
		return nil, nil, fmt.Errorf("listing end offsets of %q: %w", l.topic, err)
	}
	starts, err := l.c.admin.ListStartOffsets(ctx, l.topic)
	if err != nil {
		return nil, nil, fmt.Errorf("listing start offsets of %q: %w", l.topic, err)
	}
	next, from = map[int32]int64{}, map[int32]int64{}
	ends.Each(func(o kadm.ListedOffset) {
		if o.Err != nil {
			err = errors.Join(err, o.Err)
			return
		}
		next[o.Partition] = o.Offset
		if start, ok := starts.Lookup(l.topic, o.Partition); ok && start.Err == nil && start.Offset < o.Offset {
			from[o.Partition] = start.Offset
		}
	})
	if err != nil {
		return nil, nil, fmt.Errorf("listing offsets of %q: %w", l.topic, err)
	}
	return next, from, nil
}

// Tail calls apply for every write from next on, in log order per partition,
// until ctx ends or a fetch fails on auth or connectivity. A nil value is a
// tombstone; partition and offset let a caller that restarts Tail resume
// where it stopped. With next empty it reads every partition from its start.
// apply runs on the fetch goroutine, so it must not block long.
func (l *KeyedLog) Tail(
	ctx context.Context, next map[int32]int64, apply func(key string, value []byte, partition int32, offset int64),
) error {
	from := make(map[int32]kgo.Offset, len(next))
	for p, off := range next {
		from[p] = kgo.NewOffset().At(off)
	}
	if len(from) == 0 {
		starts, err := l.c.admin.ListStartOffsets(ctx, l.topic)
		if err != nil {
			return fmt.Errorf("listing start offsets of %q: %w", l.topic, err)
		}
		starts.Each(func(o kadm.ListedOffset) { from[o.Partition] = kgo.NewOffset().AtStart() })
		if len(from) == 0 {
			return fmt.Errorf("topic %q has no partitions", l.topic)
		}
	}
	cl, ctx, err := l.c.newConsumerClient(ctx, map[string]map[int32]kgo.Offset{l.topic: from})
	if err != nil {
		return err
	}
	defer cl.Close()
	for ctx.Err() == nil {
		fetches := cl.PollFetches(ctx)
		if stop, err := fatalFetchError(ctx, fetches); stop {
			return err
		}
		fetches.EachRecord(func(r *kgo.Record) {
			apply(string(r.Key), r.Value, r.Partition, r.Offset)
		})
	}
	return authFailure(ctx)
}

func applyKeyed(table map[string][]byte, r *kgo.Record) {
	if r.Value == nil {
		delete(table, string(r.Key))
		return
	}
	table[string(r.Key)] = r.Value
}
