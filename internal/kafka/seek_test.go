// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"
)

// seekUntil reads with ConsumeBlockingFrom until want records arrive or wait
// passes, returning the offsets seen per partition.
func seekUntil(
	t *testing.T,
	client *Client,
	topic string,
	partition int32,
	seek Seek,
	want int,
	wait time.Duration,
) map[int][]int64 {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), wait)
	defer cancel()
	var mu sync.Mutex
	seen := map[int][]int64{}
	total := 0
	err := client.ConsumeBlockingFrom(ctx, topic, partition, seek, func(m ConsumedMessage) {
		mu.Lock()
		defer mu.Unlock()
		seen[m.Partition] = append(seen[m.Partition], m.Offset)
		if total++; total >= want {
			cancel()
		}
	})
	if err != nil {
		t.Fatalf("ConsumeBlockingFrom: %v", err)
	}
	return seen
}

func TestConsumeBlockingFromOffset(t *testing.T) {
	t.Parallel()
	const topic, per = "seek.offset.v1", 4
	client := newBlockingFixture(t, topic, per)

	// Every partition from offset 2: offsets 2 and 3 of each. Asking for one
	// more than that lets a wrong start show up before the deadline.
	seen := seekUntil(
		t,
		client,
		topic,
		AllPartitions,
		Seek{Mode: SeekOffset, Offset: 2},
		blockingPartitions*2+1,
		2*time.Second,
	)
	for p := range blockingPartitions {
		if got := seen[p]; len(got) != 2 || got[0] != 2 {
			t.Errorf("partition %d offsets = %v, want [2 3]", p, got)
		}
	}

	// One partition, and an offset past its end starts at the end: nothing.
	seen = seekUntil(t, client, topic, 1, Seek{Mode: SeekOffset, Offset: 3}, 2, 2*time.Second)
	if got := seen[1]; len(seen) != 1 || len(got) != 1 || got[0] != 3 {
		t.Errorf("partition 1 from 3 = %v, want only [3]", seen)
	}
}

func TestConsumeBlockingFromTime(t *testing.T) {
	t.Parallel()
	const topic = "seek.time.v1"
	fake, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(1, topic))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fake.Close)
	seeds := fake.ListenAddrs()
	prod, err := kgo.NewClient(kgo.SeedBrokers(seeds...))
	if err != nil {
		t.Fatal(err)
	}
	defer prod.Close()

	base := time.Now().Add(-time.Hour).Truncate(time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for i := range 4 {
		r := &kgo.Record{Topic: topic, Value: []byte("v"), Timestamp: base.Add(time.Duration(i) * 10 * time.Minute)}
		if perr := prod.ProduceSync(ctx, r).FirstErr(); perr != nil {
			t.Fatal(perr)
		}
	}
	client, err := NewClient(AuthConfig{Method: AuthPlaintext}, strings.Join(seeds, ","))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(client.Close)

	// Since base+20m: records 2 and 3.
	seek := Seek{Mode: SeekTime, Time: base.Add(20 * time.Minute)}
	seen := seekUntil(t, client, topic, AllPartitions, seek, 3, 2*time.Second)
	if got := seen[0]; len(got) != 2 || got[0] != 2 {
		t.Errorf("offsets since base+20m = %v, want [2 3]", got)
	}
}

func TestSeekZeroValueIsStart(t *testing.T) {
	t.Parallel()
	if got := (Seek{}).kgoOffset().String(); got != kgo.NewOffset().AtStart().String() {
		t.Errorf("zero Seek = %s, want the log start", got)
	}
}
