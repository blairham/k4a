// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"bytes"
	"context"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/blairham/k4a/internal/kafka"
)

// partitionKey names one partition across topics.
func partitionKey(topic string, p int) string { return fmt.Sprintf("%s/%d", topic, p) }

// clusterMetadata is the reference the command is checked against: kadm's own
// Metadata, read on a separate client.
func clusterMetadata(t *testing.T, adm *kadm.Client) kadm.TopicDetails {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	md, err := adm.Metadata(ctx)
	if err != nil {
		t.Fatalf("metadata: %v", err)
	}
	return md.Topics
}

func ints(ids []int32) []int {
	out := make([]int, 0, len(ids))
	for _, id := range ids {
		out = append(out, int(id))
	}
	return out
}

func TestDescribePartitionsMatchesMetadata(t *testing.T) {
	t.Parallel()
	_, brokers, adm := newCluster(t, kfake.NumBrokers(3), kfake.SeedTopics(6, "orders", "payments"))

	var out bytes.Buffer
	var rows []partitionRow
	runJSON(t, &DescribePartitionsCmd{Out: &out}, &out, brokers, &rows, "orders")

	want := clusterMetadata(t, adm)["orders"]
	if len(rows) != len(want.Partitions) || len(rows) != 6 {
		t.Fatalf("got %d rows, metadata has %d partitions (want 6)", len(rows), len(want.Partitions))
	}
	for _, r := range rows {
		if r.Topic != "orders" {
			t.Errorf("row for %q, asked only for orders", r.Topic)
			continue
		}
		p := want.Partitions[int32(r.Partition)]
		if r.Leader != int(p.Leader) || !slices.Equal(r.Replicas, ints(p.Replicas)) || !slices.Equal(r.ISR, ints(p.ISR)) {
			t.Errorf("partition %d: leader %d replicas %v isr %v, metadata %d %v %v",
				r.Partition, r.Leader, r.Replicas, r.ISR, p.Leader, p.Replicas, p.ISR)
		}
	}
}

func TestDescribePartitionsLeaderAcrossTopics(t *testing.T) {
	t.Parallel()
	_, brokers, adm := newCluster(t, kfake.NumBrokers(3), kfake.SeedTopics(6, "orders", "payments", "refunds"))
	md := clusterMetadata(t, adm)

	// Every broker must lead something, or a filter that returned nothing
	// would pass this test for the broker that leads nothing.
	for _, leader := range []int{0, 1, 2} {
		want := map[string]bool{}
		for _, td := range md {
			if td.IsInternal {
				continue
			}
			for _, p := range td.Partitions {
				if int(p.Leader) == leader {
					want[partitionKey(td.Topic, int(p.Partition))] = true
				}
			}
		}
		if len(want) == 0 {
			t.Fatalf("broker %d leads nothing; the fixture cannot test --leader", leader)
		}

		var out bytes.Buffer
		var rows []partitionRow
		runJSON(t, &DescribePartitionsCmd{Out: &out}, &out, brokers, &rows, "--leader", fmt.Sprint(leader))
		got := map[string]bool{}
		for _, r := range rows {
			got[partitionKey(r.Topic, r.Partition)] = true
		}
		if len(got) != len(want) {
			t.Errorf("--leader %d: %d partitions, metadata says %d", leader, len(got), len(want))
		}
		for k := range want {
			if !got[k] {
				t.Errorf("--leader %d: missing %s", leader, k)
			}
		}
	}
}

func TestDescribePartitionsUnknownTopic(t *testing.T) {
	t.Parallel()
	_, brokers, _ := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(1, "orders"))
	cfg := writeConfig(t, t.TempDir(), "")
	var out bytes.Buffer
	cmd := &DescribePartitionsCmd{Out: &out}
	if code := cmd.Run([]string{"--config", cfg, "-b", brokers, "no-such-topic"}); code == 0 {
		t.Errorf("Run on a missing topic = 0, output:\n%s", out.String())
	}
}

func TestDescribePartitionsText(t *testing.T) {
	t.Parallel()
	_, brokers, _ := newCluster(t, kfake.NumBrokers(1), kfake.SeedTopics(2, "orders"))
	cfg := writeConfig(t, t.TempDir(), "")
	var out bytes.Buffer
	cmd := &DescribePartitionsCmd{Out: &out}
	if code := cmd.Run([]string{"--config", cfg, "-b", brokers, "orders"}); code != 0 {
		t.Fatalf("Run = %d, output:\n%s", code, out.String())
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 3 || !strings.HasPrefix(lines[0], "TOPIC") || !strings.HasPrefix(lines[1], "orders") {
		t.Errorf("want a header and two orders rows, got:\n%s", out.String())
	}
}

func TestPartitionRowsFilters(t *testing.T) {
	t.Parallel()
	tps := []kafka.TopicPartitions{
		{Name: "orders", Partitions: []kafka.PartitionInfo{
			{ID: 0, Leader: 1, Replicas: []int{1, 2, 3}, ISR: []int{1, 2, 3}},
			{ID: 1, Leader: 2, Replicas: []int{2, 3, 1}, ISR: []int{2, 3}},
			{ID: 2, Leader: -1, Replicas: []int{3, 4, 5}, ISR: []int{}},
		}},
		{Name: "__consumer_offsets", Internal: true, Partitions: []kafka.PartitionInfo{
			{ID: 0, Leader: 1, Replicas: []int{1}, ISR: []int{1}},
		}},
	}
	keys := func(rows []partitionRow) []string {
		out := make([]string, 0, len(rows))
		for _, r := range rows {
			out = append(out, partitionKey(r.Topic, r.Partition))
		}
		return out
	}
	unfiltered := DescribePartitionsCmdFlags{Leader: noBroker, Broker: noBroker}
	tests := []struct {
		name  string
		want  []string
		flags DescribePartitionsCmdFlags
		named bool
	}{
		{name: "all hides internal", flags: unfiltered, want: []string{"orders/0", "orders/1", "orders/2"}},
		{
			name:  "internal flag",
			flags: DescribePartitionsCmdFlags{Leader: noBroker, Broker: noBroker, Internal: true},
			want:  []string{"orders/0", "orders/1", "orders/2", "__consumer_offsets/0"},
		},
		{
			name:  "named internal shown",
			flags: unfiltered,
			named: true,
			want:  []string{"orders/0", "orders/1", "orders/2", "__consumer_offsets/0"},
		},
		{
			name:  "leader 1",
			flags: DescribePartitionsCmdFlags{Leader: 1, Broker: noBroker},
			want:  []string{"orders/0"},
		},
		{
			name:  "broker 1",
			flags: DescribePartitionsCmdFlags{Leader: noBroker, Broker: 1},
			want:  []string{"orders/0", "orders/1"},
		},
		{
			name:  "under-replicated",
			flags: DescribePartitionsCmdFlags{Leader: noBroker, Broker: noBroker, UnderReplicated: true},
			want:  []string{"orders/1", "orders/2"},
		},
		{
			name:  "leader and under-replicated",
			flags: DescribePartitionsCmdFlags{Leader: 2, Broker: noBroker, UnderReplicated: true},
			want:  []string{"orders/1"},
		},
		{
			name:  "broker 0 is a real broker id",
			flags: DescribePartitionsCmdFlags{Leader: noBroker, Broker: 0},
			want:  []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			if got := keys(partitionRows(tps, &tt.flags, tt.named)); !slices.Equal(got, tt.want) {
				t.Errorf("got %v, want %v", got, tt.want)
			}
		})
	}
}
