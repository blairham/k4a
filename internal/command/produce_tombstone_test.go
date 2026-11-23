// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"context"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/blairham/k4a/internal/kafka"
)

const (
	compactedTopic  = "compacted.v1"
	compactedNParts = 6
)

func newPartitionedCluster(t *testing.T) string {
	t.Helper()
	fake, err := kfake.NewCluster(kfake.NumBrokers(1), kfake.SeedTopics(compactedNParts, compactedTopic))
	if err != nil {
		t.Fatalf("kfake: %v", err)
	}
	t.Cleanup(fake.Close)
	return strings.Join(fake.ListenAddrs(), ",")
}

// readPartition consumes one partition from the start until want records
// arrive or ten seconds pass.
func readPartition(t *testing.T, brokers string, partition int32, want int) []*kgo.Record {
	t.Helper()
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.ConsumePartitions(map[string]map[int32]kgo.Offset{
			compactedTopic: {partition: kgo.NewOffset().AtStart()},
		}),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var out []*kgo.Record
	for len(out) < want && ctx.Err() == nil {
		cl.PollFetches(ctx).EachRecord(func(r *kgo.Record) { out = append(out, r) })
	}
	return out
}

func runProduce(t *testing.T, brokers string, args ...string) int {
	t.Helper()
	cfg := writeConfig(t, t.TempDir(), "")
	return (&ProduceCommand{}).Run(append([]string{"--config", cfg, "-b", brokers, "-t", compactedTopic}, args...))
}

// The acceptance case of #177: a record, then its tombstone on the same
// explicit partition; the tombstone's value is nil, not an empty slice. The
// empty --value record is the positive control that the read can tell the
// two apart.
func TestProduceTombstoneToPartition(t *testing.T) {
	t.Parallel()
	brokers := newPartitionedCluster(t)
	steps := [][]string{
		{"--partition", "4", "--record-key", "stale", "--value", `{"v":1}`},
		{"--partition", "4", "--record-key", "stale", "--value", ""},
		{"--partition", "4", "--record-key", "stale", "--tombstone", "--yes"},
	}
	for _, args := range steps {
		if code := runProduce(t, brokers, args...); code != 0 {
			t.Fatalf("produce %v = %d", args, code)
		}
	}
	rs := readPartition(t, brokers, 4, 3)
	if len(rs) != 3 {
		t.Fatalf("partition 4 holds %d records, want 3", len(rs))
	}
	for _, r := range rs {
		if string(r.Key) != "stale" {
			t.Errorf("offset %d key = %q", r.Offset, r.Key)
		}
	}
	if string(rs[0].Value) != `{"v":1}` {
		t.Errorf("first value = %q", rs[0].Value)
	}
	if rs[1].Value == nil || len(rs[1].Value) != 0 {
		t.Errorf(`--value "" sent %#v, want an empty non-nil value`, rs[1].Value)
	}
	if rs[2].Value != nil {
		t.Errorf("--tombstone sent %#v, want a nil value", rs[2].Value)
	}
}

// A tombstone deletes; without --yes it must ask, and a non-terminal stdin
// cannot answer, so nothing is sent.
func TestProduceTombstoneNeedsConfirmation(t *testing.T) {
	t.Parallel()
	brokers := newPartitionedCluster(t)
	cfg := writeConfig(t, t.TempDir(), "")
	cmd := &ProduceCommand{Confirm: Prompt{In: strings.NewReader(""), Interactive: func() bool { return false }}}
	code := cmd.Run([]string{
		"--config", cfg, "-b", brokers, "-t", compactedTopic,
		"--partition", "1", "--record-key", "stale", "--tombstone",
	})
	if code == 0 {
		t.Fatal("a tombstone without --yes on a non-terminal stdin was sent")
	}
	client, err := kafka.NewClient(kafka.AuthConfig{}, brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ends, err := client.EndOffsets(context.Background(), compactedTopic)
	if err != nil {
		t.Fatal(err)
	}
	if ends[1] != 0 {
		t.Errorf("partition 1 ends at %d after a refused tombstone, want 0", ends[1])
	}
}

// --partitioner crc32 routes the way librdkafka does; "stale-key" is a key
// murmur2 sends elsewhere (partition 3, see TestKeyPartition), so ignoring
// the flag cannot pass. A tombstone with no routing flag follows murmur2.
func TestProducePartitionerRoutes(t *testing.T) {
	t.Parallel()
	brokers := newPartitionedCluster(t)
	if code := runProduce(t, brokers, "--partitioner", "crc32", "--record-key", "stale-key", "--value", "v"); code != 0 {
		t.Fatalf("produce = %d", code)
	}
	if code := runProduce(t, brokers, "--record-key", "stale-key", "--tombstone", "--yes"); code != 0 {
		t.Fatalf("tombstone = %d", code)
	}
	if rs := readPartition(t, brokers, 5, 1); len(rs) != 1 || string(rs[0].Value) != "v" {
		t.Errorf("crc32 partition 5 = %d records, want the value", len(rs))
	}
	if rs := readPartition(t, brokers, 3, 1); len(rs) != 1 || rs[0].Value != nil {
		t.Errorf("murmur2 partition 3 = %d records, want the tombstone", len(rs))
	}
}

func TestProducePartitionOutOfRange(t *testing.T) {
	t.Parallel()
	brokers := newPartitionedCluster(t)
	if code := runProduce(t, brokers, "--partition", "6", "--value", "v"); code == 0 {
		t.Error("--partition 6 on a 6-partition topic was accepted")
	}
}

// NewestKeyRecord must return the newest of several records for the key,
// including when it sits in an older chunk than the first one read, and
// say "none" only after reading everything.
func TestNewestKeyRecord(t *testing.T) {
	t.Parallel()
	brokers := newPartitionedCluster(t)
	cl, err := kgo.NewClient(kgo.SeedBrokers(strings.Split(brokers, ",")...),
		kgo.RecordPartitioner(kgo.ManualPartitioner()), kgo.ProducerLinger(0))
	if err != nil {
		t.Fatal(err)
	}
	defer cl.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	rec := func(k, v string) *kgo.Record {
		return &kgo.Record{Topic: compactedTopic, Partition: 2, Key: []byte(k), Value: []byte(v)}
	}
	// "old" at offsets 0 and 1, then 6000 others: the first chunk read
	// (the newest 5000) holds none of it.
	batch := make([]*kgo.Record, 0, 6003)
	batch = append(batch, rec("old", "first"), rec("old", "second"))
	for i := range 6000 {
		batch = append(batch, rec("other-"+strconv.Itoa(i), "x"))
	}
	batch = append(batch, rec("new", "only"))
	if perr := cl.ProduceSync(ctx, batch...).FirstErr(); perr != nil {
		t.Fatal(perr)
	}

	client, err := kafka.NewClient(kafka.AuthConfig{}, brokers)
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	for _, tc := range []struct {
		key    string
		value  string
		offset int64
	}{
		{key: "old", offset: 1, value: "second"},
		{key: "new", offset: 6002, value: "only"},
	} {
		got, lerr := client.NewestKeyRecord(ctx, compactedTopic, 2, []byte(tc.key))
		if lerr != nil || got.Record == nil || !got.Complete {
			t.Fatalf("%s: %+v, %v", tc.key, got, lerr)
		}
		if got.Record.Offset != tc.offset || string(got.Record.Value) != tc.value {
			t.Errorf("%s: offset %d value %q, want %d %q", tc.key, got.Record.Offset, got.Record.Value, tc.offset, tc.value)
		}
	}
	got, err := client.NewestKeyRecord(ctx, compactedTopic, 2, []byte("absent"))
	if err != nil || got.Record != nil || !got.Complete {
		t.Errorf("absent: %+v, %v; want none, complete", got, err)
	}
	got, err = client.NewestKeyRecord(ctx, compactedTopic, 0, []byte("old"))
	if err != nil || got.Record != nil || !got.Complete {
		t.Errorf("empty partition: %+v, %v; want none, complete", got, err)
	}
}

func TestDescribeLookup(t *testing.T) {
	t.Parallel()
	ts := time.Date(2026, 11, 23, 10, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		l    kafka.KeyLookup
		want string
	}{
		{kafka.KeyLookup{Complete: true}, "none on partition 3; the tombstone deletes nothing here"},
		{kafka.KeyLookup{}, "none found, but part of the partition could not be read"},
		{
			kafka.KeyLookup{Complete: true, Record: &kgo.Record{Offset: 7, Timestamp: ts, Value: []byte(`{"a":1}`)}},
			`offset 7, 2026-11-23T10:00:00Z, value "{\"a\":1}"`,
		},
		{kafka.KeyLookup{Complete: true, Record: &kgo.Record{Offset: 8, Timestamp: ts}}, "null (already a tombstone)"},
		{kafka.KeyLookup{Record: &kgo.Record{Offset: 9, Timestamp: ts, Value: []byte("v")}}, "so a newer one may exist"},
	} {
		if got := describeLookup(tc.l, 3); !strings.Contains(got, tc.want) {
			t.Errorf("describeLookup = %q, want it to contain %q", got, tc.want)
		}
	}
}

func TestBuildRecordsTombstone(t *testing.T) {
	t.Parallel()
	p := int32(1)
	for name, f := range map[string]ProduceFlags{
		"tombstone and value":        {Topic: "t", Key: "k", Tombstone: true, Value: "v"},
		"tombstone and empty value":  {Topic: "t", Key: "k", Tombstone: true, valueGiven: true},
		"tombstone and stdin":        {Topic: "t", Key: "k", Tombstone: true, Stdin: true},
		"tombstone and schema":       {Topic: "t", Key: "k", Tombstone: true, Schema: "s.yaml"},
		"tombstone without key":      {Topic: "t", Tombstone: true},
		"tombstone with count":       {Topic: "t", Key: "k", Tombstone: true, Count: 3},
		"tombstone without topic":    {Key: "k", Tombstone: true},
		"partition and partitioner":  {Topic: "t", Value: "v", Partition: &p, Partitioner: "crc32"},
		"partitioner, stdin, no sep": {Topic: "t", Stdin: true, Partitioner: "crc32"},
		"yes without tombstone":      {Topic: "t", Value: "v", Yes: true},
		"negative partition":         {Topic: "t", Value: "v", Partition: new(int32(-1))},
	} {
		if _, _, err := buildRecords(&f, strings.NewReader("x\n")); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}

	f := ProduceFlags{Topic: "t", Key: "k", Tombstone: true, Headers: []string{"h=1"}}
	_, rs, err := buildRecords(&f, nil)
	if err != nil || len(rs) != 1 || rs[0].Value != nil || string(rs[0].Key) != "k" || header(rs[0], "h") != "1" {
		t.Errorf("tombstone = %+v, %v", rs, err)
	}
	f = ProduceFlags{Topic: "t", valueGiven: true}
	_, rs, err = buildRecords(&f, nil)
	if err != nil || len(rs) != 1 || rs[0].Value == nil || len(rs[0].Value) != 0 {
		t.Errorf(`--value "" = %d records, %v; want one empty value`, len(rs), err)
	}
}
