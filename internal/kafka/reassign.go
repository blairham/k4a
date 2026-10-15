// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"slices"
	"strconv"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
)

// ParseReplicaList reads a comma- or space-separated list of broker IDs, the
// first being the preferred leader. An empty list is allowed: it means
// "cancel the reassignment in progress".
func ParseReplicaList(s string) ([]int, error) {
	fields := strings.FieldsFunc(s, func(r rune) bool { return r == ',' || r == ' ' })
	out := make([]int, 0, len(fields))
	for _, f := range fields {
		n, err := strconv.ParseInt(f, 10, 32)
		if err != nil || n < 0 {
			return nil, fmt.Errorf("replica %q is not a broker ID", f)
		}
		id := int(n)
		if slices.Contains(out, id) {
			return nil, fmt.Errorf("broker %d is listed twice", id)
		}
		out = append(out, id)
	}
	return out, nil
}

// ReassignPartition moves one partition's replicas to replicas (preferred
// leader first) with AlterPartitionAssignments. An empty list cancels a
// reassignment in progress instead. Every broker must exist: the request
// would otherwise sit in the controller forever, adding a replica that can
// never catch up.
func (c *Client) ReassignPartition(ctx context.Context, topic string, partition int, replicas []int) error {
	p, err := narrowPartition(partition)
	if err != nil {
		return err
	}
	var req kadm.AlterPartitionAssignmentsReq
	if len(replicas) == 0 {
		req.CancelAssign(topic, p)
	} else {
		ids, berr := c.knownBrokers(ctx, replicas)
		if berr != nil {
			return berr
		}
		req.Assign(topic, p, ids)
	}
	resps, err := c.admin.AlterPartitionAssignments(ctx, req)
	if err != nil {
		return fmt.Errorf("reassigning %s-%d: %w", topic, partition, err)
	}
	for _, r := range resps.Sorted() {
		if r.Err != nil {
			if r.ErrMessage != "" {
				return fmt.Errorf("reassigning %s-%d: %w: %s", topic, partition, r.Err, r.ErrMessage)
			}
			return fmt.Errorf("reassigning %s-%d: %w", topic, partition, r.Err)
		}
	}
	return nil
}

// knownBrokers narrows replicas to broker IDs, refusing any ID the cluster
// does not have.
func (c *Client) knownBrokers(ctx context.Context, replicas []int) ([]int32, error) {
	brokers, err := c.admin.ListBrokers(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing brokers: %w", err)
	}
	ids := make([]int32, 0, len(replicas))
	for _, r := range replicas {
		i := slices.IndexFunc(brokers, func(b kadm.BrokerDetail) bool { return int(b.NodeID) == r })
		if i < 0 {
			return nil, fmt.Errorf("broker %d is not in the cluster (brokers: %s)", r, brokerList(brokers))
		}
		ids = append(ids, brokers[i].NodeID) // the broker's own ID: no narrowing of r
	}
	return ids, nil
}

func brokerList(bs kadm.BrokerDetails) string {
	ids := make([]string, 0, len(bs))
	for _, b := range bs {
		ids = append(ids, strconv.Itoa(int(b.NodeID)))
	}
	slices.Sort(ids)
	return strings.Join(ids, ", ")
}

func narrowPartition(p int) (int32, error) {
	if p < 0 || p > math.MaxInt32 {
		return 0, errors.New("partition out of range")
	}
	return int32(p), nil
}

// applyReassignments marks the partitions of detail that are being
// reassigned. Best effort: a broker too old for ListPartitionReassignments,
// or a principal not allowed to describe the cluster, still gets the rest of
// the topic detail.
func (c *Client) applyReassignments(ctx context.Context, detail *TopicDetail) {
	set := kadm.TopicsSet{}
	for _, p := range detail.Partitions {
		if n, err := narrowPartition(p.ID); err == nil {
			set.Add(detail.Name, n)
		}
	}
	listed, err := c.admin.ListPartitionReassignments(ctx, set)
	if err != nil {
		slog.Debug("FetchTopicDetail: listing reassignments", "topic", detail.Name, "err", err)
		return
	}
	for i := range detail.Partitions {
		p := &detail.Partitions[i]
		n, err := narrowPartition(p.ID)
		if err != nil {
			continue
		}
		r, ok := listed[detail.Name][n]
		if !ok || len(r.AddingReplicas)+len(r.RemovingReplicas) == 0 {
			continue
		}
		p.Adding, p.Removing = brokerInts(r.AddingReplicas), brokerInts(r.RemovingReplicas)
		detail.Reassigning++
	}
}
