// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"fmt"
	"strings"
)

// TopicPartitions is one topic's partitions, as FetchPartitions returns them.
type TopicPartitions struct {
	Name       string
	Partitions []PartitionInfo
	Internal   bool
}

// FetchPartitions returns each partition's leader, replicas, ISR and offsets
// for the named topics, or for every topic when none is named. It issues only
// Metadata and ListOffsets requests, never joins a consumer group, and a
// whole-cluster leader map is one call. A named
// topic the cluster does not have is an error rather than an empty result.
// Reassignment progress (Adding/Removing) is not fetched; FetchTopicDetail has it.
func (c *Client) FetchPartitions(ctx context.Context, topics ...string) ([]TopicPartitions, error) {
	tds, err := c.admin.ListTopicsWithInternal(ctx, topics...)
	if err != nil {
		return nil, fmt.Errorf("fetching metadata: %w", err)
	}
	for _, name := range topics {
		td, ok := tds[name]
		if !ok {
			return nil, fmt.Errorf("topic %q not found", name)
		}
		if td.Err != nil {
			return nil, fmt.Errorf("topic %q: %w", name, td.Err)
		}
	}
	names := make([]string, 0, len(tds))
	for _, td := range tds.Sorted() {
		if td.Err != nil {
			return nil, fmt.Errorf("topic %q: %w", td.Topic, td.Err)
		}
		names = append(names, td.Topic)
	}
	if len(names) == 0 {
		return nil, nil
	}

	// Asked for by name: an empty list means "every topic" to kadm, which
	// would include topics created since the Metadata response above.
	starts, err := c.admin.ListStartOffsets(ctx, names...)
	if err != nil {
		return nil, fmt.Errorf("listing start offsets: %w", err)
	}
	ends, err := c.admin.ListEndOffsets(ctx, names...)
	if err != nil {
		return nil, fmt.Errorf("listing end offsets: %w", err)
	}

	out := make([]TopicPartitions, 0, len(names))
	for _, td := range tds.Sorted() {
		tp := TopicPartitions{
			Name:       td.Topic,
			Partitions: make([]PartitionInfo, 0, len(td.Partitions)),
			Internal:   td.IsInternal || strings.HasPrefix(td.Topic, "_"),
		}
		for _, p := range td.Partitions.Sorted() {
			tp.Partitions = append(tp.Partitions, partitionInfo(td.Topic, p, starts, ends))
		}
		out = append(out, tp)
	}
	return out, nil
}
