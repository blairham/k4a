// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

const blockingPartitions = 3

// newBlockingFixture seeds a fake cluster whose topic has records in every
// partition, so a consumer that reads only partition 0 is visibly short.
func newBlockingFixture(t *testing.T, topic string, perPartition int) *Client {
	t.Helper()
	fake, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(blockingPartitions, topic))
	if err != nil {
		t.Fatalf("kfake cluster: %v", err)
	}
	t.Cleanup(fake.Close)
	seeds := fake.ListenAddrs()

	prod, err := kgo.NewClient(kgo.SeedBrokers(seeds...), kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer prod.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	for p := range int32(blockingPartitions) {
		for i := range perPartition {
			r := &kgo.Record{Topic: topic, Partition: p, Value: fmt.Appendf(nil, "p%d-%d", p, i)}
			if perr := prod.ProduceSync(ctx, r).FirstErr(); perr != nil {
				t.Fatalf("produce p%d: %v", p, perr)
			}
		}
	}

	client, err := NewClient(AuthConfig{Method: AuthPlaintext}, strings.Join(seeds, ","))
	if err != nil {
		t.Fatalf("kafka client: %v", err)
	}
	t.Cleanup(client.Close)
	return client
}

// consumeUntil runs ConsumeBlocking until want records arrive or the deadline
// passes, and returns the per-partition counts it saw.
func consumeUntil(
	t *testing.T, client *Client, topic string, partition int32, want int, wait time.Duration,
) map[int]int {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()

	var mu sync.Mutex
	seen := map[int]int{}
	total := 0
	err := client.ConsumeBlocking(ctx, topic, "", partition, func(m ConsumedMessage) {
		mu.Lock()
		defer mu.Unlock()
		seen[m.Partition]++
		total++
		if total >= want {
			cancel()
		}
	})
	if err != nil {
		t.Fatalf("ConsumeBlocking: %v", err)
	}
	return seen
}

// Without a group, consume used to read partition 0 only: two thirds of this
// topic never arrived and nothing said so.
func TestConsumeBlockingNoGroupReadsEveryPartition(t *testing.T) {
	t.Parallel()

	const topic, per = "consume.all.v1", 4
	client := newBlockingFixture(t, topic, per)

	seen := consumeUntil(t, client, topic, AllPartitions, blockingPartitions*per, 10*time.Second)
	for p := range blockingPartitions {
		if seen[p] != per {
			t.Errorf("partition %d: got %d records, want %d (seen %v)", p, seen[p], per, seen)
		}
	}
}

func TestConsumeBlockingSinglePartition(t *testing.T) {
	t.Parallel()

	const topic, per = "consume.one.v1", 4
	client := newBlockingFixture(t, topic, per)

	// Ask for more than partition 2 holds so a leak from another partition
	// would show up before the deadline ends the read.
	seen := consumeUntil(t, client, topic, 2, per+1, 2*time.Second)
	if seen[2] != per || len(seen) != 1 {
		t.Errorf("got %v, want only partition 2 with %d records", seen, per)
	}
}

func TestConsumeBlockingRejectsPartitionWithGroup(t *testing.T) {
	t.Parallel()

	c := &Client{}
	err := c.ConsumeBlocking(context.Background(), "t", "g", 1, func(ConsumedMessage) {})
	if err == nil {
		t.Fatal("ConsumeBlocking(group, partition 1) = nil, want an error")
	}
}
