// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"
	"github.com/twmb/franz-go/pkg/kmsg"
)

func TestParseReplicaList(t *testing.T) {
	t.Parallel()
	for in, want := range map[string][]int{
		"1,2,3":   {1, 2, 3},
		" 3 1 2 ": {3, 1, 2},
		"0, 4":    {0, 4},
		"":        {},
	} {
		if got, err := ParseReplicaList(in); err != nil || !slices.Equal(got, want) {
			t.Errorf("ParseReplicaList(%q) = %v, %v; want %v", in, got, err, want)
		}
	}
	for _, in := range []string{"1,1", "a", "-1", "1;2", "4294967296"} {
		if got, err := ParseReplicaList(in); err == nil {
			t.Errorf("ParseReplicaList(%q) = %v, want an error", in, got)
		}
	}
}

// TestReassignPartition sends a reassignment to an in-process cluster and
// refuses one naming a broker the cluster does not have. kfake accepts a
// reassignment without performing it, so the move itself is not observable
// here; TestTopicDetailShowsReassignment covers the progress side.
func TestReassignPartition(t *testing.T) {
	const topic = "orders"
	fake, err := kfake.NewCluster(kfake.NumBrokers(3), kfake.SeedTopics(1, topic))
	if err != nil {
		t.Fatalf("kfake cluster: %v", err)
	}
	t.Cleanup(fake.Close)
	c := newScanTestClient(t, fake.ListenAddrs())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var sent [][]int32
	fake.ControlKey(int16(kmsg.AlterPartitionAssignments), func(req kmsg.Request) (kmsg.Response, error, bool) {
		fake.KeepControl()
		r, ok := req.(*kmsg.AlterPartitionAssignmentsRequest)
		if !ok {
			return nil, nil, false //nolint:nilnil // kfake reads (nil, nil, false) as "not handled here"
		}
		for _, rt := range r.Topics {
			for _, rp := range rt.Partitions {
				sent = append(sent, rp.Replicas)
			}
		}
		return nil, nil, false //nolint:nilnil // kfake reads (nil, nil, false) as "let kfake answer"
	})

	if err := c.ReassignPartition(ctx, topic, 0, []int{2, 0}); err != nil {
		t.Fatalf("ReassignPartition: %v", err)
	}
	if len(sent) != 1 || !slices.Equal(sent[0], []int32{2, 0}) {
		t.Errorf("sent replicas %v, want [[2 0]] in the order given (preferred leader first)", sent)
	}

	if err := c.ReassignPartition(
		ctx,
		topic,
		0,
		[]int{7},
	); err == nil ||
		!strings.Contains(err.Error(), "not in the cluster") {
		t.Errorf("an unknown broker: err = %v", err)
	}
	if len(sent) != 1 {
		t.Errorf("a refused reassignment reached the cluster: %v", sent)
	}
	// Canceling with nothing in progress is the broker's refusal to report.
	if err := c.ReassignPartition(
		ctx,
		topic,
		0,
		nil,
	); err == nil ||
		!strings.Contains(err.Error(), "NO_REASSIGNMENT_IN_PROGRESS") {
		t.Errorf("cancel with nothing in progress: err = %v", err)
	}
	if err := c.ReassignPartition(ctx, topic, -1, []int{0}); err == nil {
		t.Error("a negative partition was accepted")
	}
}

// TestTopicDetailShowsReassignment: a partition the controller reports as
// moving carries its adding and removing replicas, and the topic counts it.
func TestTopicDetailShowsReassignment(t *testing.T) {
	const topic = "orders"
	fake, err := kfake.NewCluster(kfake.NumBrokers(3), kfake.SeedTopics(2, topic))
	if err != nil {
		t.Fatalf("kfake cluster: %v", err)
	}
	t.Cleanup(fake.Close)
	c := newScanTestClient(t, fake.ListenAddrs())

	fake.ControlKey(int16(kmsg.ListPartitionReassignments), func(kmsg.Request) (kmsg.Response, error, bool) {
		fake.KeepControl()
		resp := kmsg.NewPtrListPartitionReassignmentsResponse()
		rt := kmsg.NewListPartitionReassignmentsResponseTopic()
		rt.Topic = topic
		rp := kmsg.NewListPartitionReassignmentsResponseTopicPartition()
		rp.Partition = 1
		rp.Replicas = []int32{0, 1, 2}
		rp.AddingReplicas = []int32{2}
		rp.RemovingReplicas = []int32{0}
		rt.Partitions = append(rt.Partitions, rp)
		resp.Topics = append(resp.Topics, rt)
		return resp, nil, true
	})

	d, err := c.FetchTopicDetail(context.Background(), topic)
	if err != nil {
		t.Fatal(err)
	}
	if d.Reassigning != 1 {
		t.Errorf("Reassigning = %d, want 1", d.Reassigning)
	}
	p0, p1 := d.Partitions[0], d.Partitions[1]
	if len(p0.Adding)+len(p0.Removing) != 0 {
		t.Errorf("partition 0 is not moving but shows %+v", p0)
	}
	if !slices.Equal(p1.Adding, []int{2}) || !slices.Equal(p1.Removing, []int{0}) {
		t.Errorf("partition 1 adding %v removing %v, want [2] and [0]", p1.Adding, p1.Removing)
	}
}
