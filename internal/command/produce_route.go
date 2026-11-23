// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"fmt"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/blairham/k4a/internal/kafka"
)

// tombstonePreviewBytes bounds how much of the record a tombstone deletes
// is printed before asking.
const tombstonePreviewBytes = 80

// routeRecords sets each record's partition from --partition or the
// --partitioner hash. A --tombstone with neither is routed by murmur2,
// franz-go's default, worked out here so the preview can name it.
func routeRecords(ctx context.Context, client *kafka.Client, flags *ProduceFlags, records []*kgo.Record) error {
	if len(records) == 0 {
		return nil
	}
	topic := records[0].Topic
	ids, err := client.FetchPartitionIDs(ctx, topic)
	if err != nil {
		return err
	}
	n := len(ids)
	if n == 0 {
		return fmt.Errorf("topic %s does not exist", topic)
	}
	if flags.Partition != nil {
		p := *flags.Partition
		if int(p) >= n {
			return fmt.Errorf("--partition %d: topic %s has partitions 0-%d", p, topic, n-1)
		}
		for _, r := range records {
			r.Partition = p
		}
		return nil
	}
	name := flags.Partitioner
	if name == "" {
		name = kafka.PartitionerMurmur2
	}
	for i, r := range records {
		p, err := kafka.KeyPartition(name, r.Key, n)
		if err != nil {
			return fmt.Errorf("record %d: %w", i+1, err)
		}
		r.Partition = p
	}
	return nil
}

// routeSource says how the tombstone's partition was chosen.
func routeSource(flags *ProduceFlags) string {
	switch {
	case flags.Partition != nil:
		return "--partition"
	case flags.Partitioner != "":
		return "--partitioner " + flags.Partitioner
	default:
		return "murmur2, the Java client's default; a librdkafka producer needs --partitioner consistent_random"
	}
}

// confirmTombstone prints where the tombstone goes and the record it
// deletes, then asks unless --yes.
func (c *ProduceCommand) confirmTombstone(
	ctx context.Context, client *kafka.Client, flags *ProduceFlags, r *kgo.Record,
) error {
	fmt.Printf("Tombstone: topic %s, partition %d (%s), key %q\n", r.Topic, r.Partition, routeSource(flags), r.Key)
	found, err := client.NewestKeyRecord(ctx, r.Topic, r.Partition, r.Key)
	if err != nil {
		return fmt.Errorf("looking up the key's newest record: %w", err)
	}
	fmt.Println("  " + describeLookup(found, r.Partition))
	question := fmt.Sprintf("Send a tombstone for key %q to %s partition %d?", r.Key, r.Topic, r.Partition)
	return guardMutation(&flags.ConnectionFlags, c.Confirm, "produce --tombstone", flags.Yes, question)
}

// describeLookup is the one line naming the record a tombstone deletes.
func describeLookup(l kafka.KeyLookup, partition int32) string {
	const partial = "part of the partition could not be read"
	if l.Record == nil {
		if !l.Complete {
			return fmt.Sprintf("Newest record for this key: none found, but %s", partial)
		}
		return fmt.Sprintf("Newest record for this key: none on partition %d; the tombstone deletes nothing here", partition)
	}
	rec := l.Record
	s := fmt.Sprintf("Newest record for this key: offset %d, %s, value %s",
		rec.Offset, rec.Timestamp.UTC().Format(time.RFC3339), previewValue(rec.Value))
	if !l.Complete {
		s += "; " + partial + ", so a newer one may exist"
	}
	return s
}

// previewValue renders a value for the tombstone preview.
func previewValue(v []byte) string {
	switch {
	case v == nil:
		return "null (already a tombstone)"
	case len(v) <= tombstonePreviewBytes:
		return fmt.Sprintf("%q", v)
	default:
		return fmt.Sprintf("%q… (%d bytes)", v[:tombstonePreviewBytes], len(v))
	}
}
