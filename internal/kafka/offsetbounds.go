// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
)

// OffsetBounds is, per partition of one topic, the log-start offset, the
// high watermark, and the first offset at or after a point in time (the high
// watermark when nothing is that recent).
type OffsetBounds struct {
	Start map[int32]int64
	End   map[int32]int64
	After map[int32]int64
}

// TopicOffsetBounds lists a topic's offset bounds in three batched requests.
// k4a-index plans a bounded backfill from them: where history starts, where
// the tail is now, and where the backfill window opens.
func (c *Client) TopicOffsetBounds(ctx context.Context, topic string, since time.Time) (OffsetBounds, error) {
	starts, err := c.admin.ListStartOffsets(ctx, topic)
	if err != nil {
		return OffsetBounds{}, fmt.Errorf("listing start offsets: %w", err)
	}
	ends, err := c.admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return OffsetBounds{}, fmt.Errorf("listing end offsets: %w", err)
	}
	after, err := c.admin.ListOffsetsAfterMilli(ctx, since.UnixMilli(), topic)
	if err != nil {
		return OffsetBounds{}, fmt.Errorf("listing offsets after %s: %w", since.Format(time.RFC3339), err)
	}
	var b OffsetBounds
	if b.Start, err = listedOffsets(starts, topic); err != nil {
		return OffsetBounds{}, err
	}
	if b.End, err = listedOffsets(ends, topic); err != nil {
		return OffsetBounds{}, err
	}
	if b.After, err = listedOffsets(after, topic); err != nil {
		return OffsetBounds{}, err
	}
	return b, nil
}

// listedOffsets refuses a partial listing: a partition missing from the
// bounds would look like a partition the topic no longer has.
func listedOffsets(l kadm.ListedOffsets, topic string) (map[int32]int64, error) {
	out := map[int32]int64{}
	for p, o := range l[topic] {
		if o.Err != nil {
			return nil, fmt.Errorf("partition %d: %w", p, o.Err)
		}
		out[p] = o.Offset
	}
	return out, nil
}
