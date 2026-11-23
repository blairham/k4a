// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"hash/crc32"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Key partitioners a producer can be matched against, named as
// librdkafka's `partitioner` property names them.
const (
	// PartitionerMurmur2 is the Java client's (and franz-go's) default:
	// (murmur2(key) & 0x7fffffff) % partitions.
	PartitionerMurmur2 = "murmur2"
	// PartitionerCRC32 is crc32(key) % partitions, librdkafka's "consistent".
	PartitionerCRC32 = "crc32"
	// PartitionerConsistentRandom is librdkafka's default: crc32 for a keyed
	// record (a keyless one goes to a random partition, which no tool can
	// reproduce, so it is never asked for one).
	PartitionerConsistentRandom = "consistent_random"
)

// KeyPartition returns the partition the named partitioner sends key to
// among n partitions, so a record (a tombstone above all) lands where the
// key's real producer put it.
func KeyPartition(partitioner string, key []byte, n int) (int32, error) {
	if n <= 0 {
		return 0, fmt.Errorf("topic has %d partitions", n)
	}
	if len(key) == 0 {
		return 0, errors.New("a key partitioner needs a non-empty key")
	}
	switch partitioner {
	case PartitionerMurmur2:
		// A keyed record goes straight to the hasher; the sticky half of
		// this partitioner applies only to keyless records.
		p := kgo.StickyKeyPartitioner(nil).ForTopic("").Partition(&kgo.Record{Key: key}, n)
		return int32(p), nil //nolint:gosec // p < n, a partition count
	case PartitionerCRC32, PartitionerConsistentRandom:
		// librdkafka: rd_crc32(key) % partition_cnt, unsigned, no mask.
		return int32(crc32.ChecksumIEEE(key) % uint32(n)), nil //nolint:gosec // n > 0 is a partition count
	default:
		return 0, fmt.Errorf("unknown partitioner %q (want %s, %s or %s)",
			partitioner, PartitionerMurmur2, PartitionerCRC32, PartitionerConsistentRandom)
	}
}

// KeyLookup is what NewestKeyRecord found.
type KeyLookup struct {
	// Record is the newest record with the key, nil when none was found.
	Record *kgo.Record
	// Complete is false when part of the partition could not be read, so
	// a nil Record is not proof the key is absent, nor a found one proof
	// that no newer one exists.
	Complete bool
}

// NewestKeyRecord finds the newest record on one partition whose key is
// exactly key. It reads newest-chunk-first with one client and stops at the
// first chunk that holds the key, so a key written recently costs one small
// read; a key that is absent costs a read of the whole partition.
func (c *Client) NewestKeyRecord(ctx context.Context, topic string, partition int32, key []byte) (KeyLookup, error) {
	first, last, err := c.partitionBounds(ctx, topic, partition)
	if err != nil {
		return KeyLookup{}, err
	}
	out := KeyLookup{Complete: true}
	var cl *kgo.Client
	defer func() {
		if cl != nil {
			cl.Close()
		}
	}()
	for _, span := range planReverseChunks(first, last) {
		if cl == nil {
			// Positioned by the constructor: SetOffsets is a no-op before
			// the first poll (see scanPartitionReverse).
			if cl, err = c.newScanClient(topic, partition, span.start); err != nil {
				return KeyLookup{}, err
			}
		} else {
			cl.SetOffsets(map[string]map[int32]kgo.EpochOffset{
				topic: {partition: {Offset: span.start, Epoch: -1}},
			})
		}
		found, complete, err := readKeySpan(ctx, cl, span, key)
		if err != nil {
			return KeyLookup{}, err
		}
		out.Complete = out.Complete && complete
		if found != nil {
			out.Record = found
			return out, nil
		}
	}
	return out, nil
}

// partitionBounds is the partition's [log start, high watermark).
func (c *Client) partitionBounds(ctx context.Context, topic string, partition int32) (first, last int64, err error) {
	starts, err := c.admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return 0, 0, fmt.Errorf("listing start offsets: %w", err)
	}
	ends, err := c.admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return 0, 0, fmt.Errorf("listing end offsets: %w", err)
	}
	s, okS := starts.Lookup(topic, partition)
	e, okE := ends.Lookup(topic, partition)
	if !okS || !okE {
		return 0, 0, fmt.Errorf("topic %s has no partition %d", topic, partition)
	}
	if err := errors.Join(s.Err, e.Err); err != nil {
		return 0, 0, fmt.Errorf("listing offsets of %s/%d: %w", topic, partition, err)
	}
	return s.Offset, e.Offset, nil
}

// readKeySpan reads [span.start, span.end) and returns the newest record in
// it with the key. The span is complete once a record at or past its last
// offset arrives; a poll that brings nothing first (a slow broker, or a
// transaction marker holding the last offset) ends it incomplete.
func readKeySpan(ctx context.Context, cl *kgo.Client, span chunkSpan, key []byte) (*kgo.Record, bool, error) {
	var found *kgo.Record
	for {
		pollCtx, cancel := context.WithTimeout(ctx, deepSearchPollTimeout)
		fetches := cl.PollFetches(pollCtx)
		cancel()
		if err := ctx.Err(); err != nil {
			return nil, false, err
		}
		if fetches.NumRecords() == 0 {
			return found, false, nil
		}
		done := false
		fetches.EachRecord(func(r *kgo.Record) {
			if done || r.Offset < span.start {
				return
			}
			if r.Offset >= span.end {
				done = true
				return
			}
			if bytes.Equal(r.Key, key) {
				found = r
			}
			done = r.Offset >= span.end-1
		})
		if done {
			return found, true, nil
		}
	}
}
