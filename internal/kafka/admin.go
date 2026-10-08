// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kmsg"
)

// CreateTopicConfig holds the parameters for creating a topic.
type CreateTopicConfig struct {
	Topic             string
	NumPartitions     int
	ReplicationFactor int
}

// CreateTopic creates a Kafka topic using the admin API. Returns true if the
// topic was created, false if it already existed.
func (c *Client) CreateTopic(ctx context.Context, cfg CreateTopicConfig) (bool, error) {
	_, err := c.admin.CreateTopic(
		ctx,
		int32(cfg.NumPartitions),     //nolint:gosec // partition count fits in int32
		int16(cfg.ReplicationFactor), //nolint:gosec // replication factor fits in int16
		nil,
		cfg.Topic,
	)
	if err != nil {
		if errors.Is(err, kerr.TopicAlreadyExists) {
			return false, nil
		}
		return false, fmt.Errorf("creating topic %q: %w", cfg.Topic, err)
	}
	return true, nil
}

// DeleteTopic deletes a Kafka topic using the admin API. Returns true if the
// topic was deleted, false if it did not exist.
func (c *Client) DeleteTopic(ctx context.Context, topic string) (bool, error) {
	_, err := c.admin.DeleteTopic(ctx, topic)
	if err != nil {
		if errors.Is(err, kerr.UnknownTopicOrPartition) {
			return false, nil
		}
		return false, fmt.Errorf("deleting topic %q: %w", topic, err)
	}
	return true, nil
}

// CreateACLConfig holds the parameters for creating an ACL entry.
type CreateACLConfig struct {
	ResourceType string
	ResourceName string
	PatternType  string
	Principal    string
	Host         string
	Operation    string
	Permission   string
}

// parseResourceType converts a string to a kmsg.ACLResourceType.
func parseResourceType(s string) (kmsg.ACLResourceType, error) {
	var rt kmsg.ACLResourceType
	if err := rt.UnmarshalText([]byte(s)); err != nil {
		return 0, fmt.Errorf("invalid resource type %q (expected: topic, group, cluster, transactionalid)", s)
	}
	return rt, nil
}

// parsePatternType converts a string to a kadm.ACLPattern.
func parsePatternType(s string) (kadm.ACLPattern, error) {
	switch strings.ToLower(s) {
	case "literal":
		return kadm.ACLPatternLiteral, nil
	case "prefixed":
		return kadm.ACLPatternPrefixed, nil
	default:
		return 0, fmt.Errorf("invalid pattern type %q (expected: literal, prefixed)", s)
	}
}

// parseOperation converts a string to a kadm.ACLOperation.
func parseOperation(s string) (kadm.ACLOperation, error) {
	var op kadm.ACLOperation
	if err := op.UnmarshalText([]byte(s)); err != nil {
		return 0, fmt.Errorf(
			"invalid operation %q (expected: all, read, write, create, delete, alter, describe, describe-configs, alter-configs)",
			s,
		)
	}
	return op, nil
}

// parsePermission converts a string to a kmsg.ACLPermissionType.
func parsePermission(s string) (kmsg.ACLPermissionType, error) {
	var pt kmsg.ACLPermissionType
	if err := pt.UnmarshalText([]byte(s)); err != nil {
		return 0, fmt.Errorf("invalid permission %q (expected: allow, deny)", s)
	}
	return pt, nil
}

// aclBuilderForEntry constructs a kadm.ACLBuilder populated for a single
// ACL entry, suitable for either creation (when used with c.admin.CreateACLs)
// or matching (delete/describe). The caller chooses which builder method to
// call.
func aclBuilderForEntry(
	resourceType kmsg.ACLResourceType,
	resourceName string,
	patternType kadm.ACLPattern,
	principal, host string,
	operation kadm.ACLOperation,
	permission kmsg.ACLPermissionType,
) *kadm.ACLBuilder {
	b := kadm.NewACLs().ResourcePatternType(patternType).Operations(operation)

	switch resourceType {
	case kmsg.ACLResourceTypeTopic:
		b = b.Topics(resourceName)
	case kmsg.ACLResourceTypeGroup:
		b = b.Groups(resourceName)
	case kmsg.ACLResourceTypeCluster:
		b = b.Clusters()
	case kmsg.ACLResourceTypeTransactionalId:
		b = b.TransactionalIDs(resourceName)
	}

	switch permission {
	case kmsg.ACLPermissionTypeAllow:
		b = b.Allow(principal).AllowHosts(host)
	case kmsg.ACLPermissionTypeDeny:
		b = b.Deny(principal).DenyHosts(host)
	}
	return b
}

// CreateACL creates a Kafka ACL entry using the admin API.
func (c *Client) CreateACL(ctx context.Context, cfg CreateACLConfig) error {
	resourceType, err := parseResourceType(cfg.ResourceType)
	if err != nil {
		return err
	}
	patternType, err := parsePatternType(cfg.PatternType)
	if err != nil {
		return err
	}
	operation, err := parseOperation(cfg.Operation)
	if err != nil {
		return err
	}
	permission, err := parsePermission(cfg.Permission)
	if err != nil {
		return err
	}

	b := aclBuilderForEntry(resourceType, cfg.ResourceName, patternType, cfg.Principal, cfg.Host, operation, permission)
	results, err := c.admin.CreateACLs(ctx, b)
	if err != nil {
		return fmt.Errorf("creating ACL: %w", err)
	}
	for _, r := range results {
		if r.Err != nil {
			return fmt.Errorf("creating ACL: %w", r.Err)
		}
	}
	return nil
}

// Ping measures the round-trip time for a metadata request to the cluster.
func (c *Client) Ping(ctx context.Context) (time.Duration, error) {
	start := time.Now()
	_, err := c.admin.BrokerMetadata(ctx)
	return time.Since(start), err
}

// ConfigEntry holds a single configuration entry from DescribeConfigs.
type ConfigEntry struct {
	Name      string
	Value     string
	Source    string
	ReadOnly  bool
	IsDefault bool
	Sensitive bool
}

// configSourceName maps Kafka ConfigSource int8 to human-readable names.
func configSourceName(src kmsg.ConfigSource) string {
	switch src {
	case kmsg.ConfigSourceDynamicTopicConfig:
		return "DYNAMIC_TOPIC"
	case kmsg.ConfigSourceDynamicBrokerConfig:
		return "DYNAMIC_BROKER"
	case kmsg.ConfigSourceDynamicDefaultBrokerConfig:
		return "DYNAMIC_DEFAULT_BROKER"
	case kmsg.ConfigSourceStaticBrokerConfig:
		return "STATIC_BROKER"
	case kmsg.ConfigSourceDefaultConfig:
		return "DEFAULT"
	case kmsg.ConfigSourceDynamicBrokerLoggerConfig:
		return "DYNAMIC_BROKER_LOGGER"
	default:
		return "UNKNOWN"
	}
}

// FetchTopicConfigs returns all configuration entries for a topic.
func (c *Client) FetchTopicConfigs(ctx context.Context, topic string) ([]ConfigEntry, error) {
	rcs, err := c.admin.DescribeTopicConfigs(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("describing topic configs: %w", err)
	}
	return resourceConfigsToEntries(rcs, topic)
}

// FetchBrokerConfigs returns all configuration entries for a broker.
func (c *Client) FetchBrokerConfigs(ctx context.Context, brokerID string) ([]ConfigEntry, error) {
	id, err := parseBrokerID(brokerID)
	if err != nil {
		return nil, err
	}
	rcs, err := c.admin.DescribeBrokerConfigs(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("describing broker configs: %w", err)
	}
	return resourceConfigsToEntries(rcs, brokerID)
}

func parseBrokerID(s string) (int32, error) {
	var id int32
	if _, err := fmt.Sscanf(s, "%d", &id); err != nil {
		return 0, fmt.Errorf("invalid broker ID %q: %w", s, err)
	}
	return id, nil
}

func resourceConfigsToEntries(rcs kadm.ResourceConfigs, name string) ([]ConfigEntry, error) {
	for _, rc := range rcs {
		if rc.Name != name {
			continue
		}
		if rc.Err != nil {
			return nil, fmt.Errorf("describing configs for %q: %w", name, rc.Err)
		}
		entries := make([]ConfigEntry, 0, len(rc.Configs))
		for _, cfg := range rc.Configs {
			entries = append(entries, ConfigEntry{
				Name:      cfg.Key,
				Value:     cfg.MaybeValue(),
				Source:    configSourceName(cfg.Source),
				IsDefault: cfg.Source == kmsg.ConfigSourceDefaultConfig,
				Sensitive: cfg.Sensitive,
			})
		}
		return entries, nil
	}
	return nil, nil
}

// AlterTopicConfig sets a single topic configuration key using the
// IncrementalAlterConfigs API (Kafka 2.3+).
func (c *Client) AlterTopicConfig(ctx context.Context, topic, key, value string) error {
	resps, err := c.admin.AlterTopicConfigs(ctx, []kadm.AlterConfig{
		{Op: kadm.SetConfig, Name: key, Value: &value},
	}, topic)
	if err != nil {
		return fmt.Errorf("altering topic config: %w", err)
	}
	for _, r := range resps {
		if r.Err != nil {
			return fmt.Errorf("altering config %q for %q: %w", key, topic, r.Err)
		}
	}
	return nil
}

// ChangeReplicationFactor alters the replica assignments for every partition of
// a topic so that each partition has exactly newRF replicas. The new replicas
// are distributed evenly across the cluster's brokers.
func (c *Client) ChangeReplicationFactor(ctx context.Context, topic string, newRF int) error {
	tds, err := c.admin.ListTopicsWithInternal(ctx, topic)
	if err != nil {
		return fmt.Errorf("fetching metadata: %w", err)
	}
	td, ok := tds[topic]
	if !ok {
		return fmt.Errorf("topic %q not found", topic)
	}
	if td.Err != nil {
		return fmt.Errorf("topic %q: %w", topic, td.Err)
	}
	if len(td.Partitions) == 0 {
		return fmt.Errorf("topic %q has no partitions", topic)
	}

	brokers, err := c.admin.ListBrokers(ctx)
	if err != nil {
		return fmt.Errorf("listing brokers: %w", err)
	}
	brokerIDs := make([]int32, 0, len(brokers))
	for _, b := range brokers {
		brokerIDs = append(brokerIDs, b.NodeID)
	}
	if newRF > len(brokerIDs) {
		return fmt.Errorf("replication factor %d exceeds number of brokers (%d)", newRF, len(brokerIDs))
	}

	var req kadm.AlterPartitionAssignmentsReq
	for _, p := range td.Partitions.Sorted() {
		current := p.Replicas
		currentSet := make(map[int32]bool, len(current))
		for _, r := range current {
			currentSet[r] = true
		}

		var replicas []int32
		if newRF <= len(current) {
			replicas = current[:newRF]
		} else {
			replicas = make([]int32, len(current), newRF)
			copy(replicas, current)
			for _, bid := range brokerIDs {
				if len(replicas) >= newRF {
					break
				}
				if !currentSet[bid] {
					replicas = append(replicas, bid)
				}
			}
		}
		req.Assign(topic, p.Partition, replicas)
	}

	resps, err := c.admin.AlterPartitionAssignments(ctx, req)
	if err != nil {
		return fmt.Errorf("alter partition reassignments: %w", err)
	}
	if err := resps.Error(); err != nil {
		return fmt.Errorf("alter partition reassignments: %w", err)
	}
	return nil
}

// OffsetResetMode describes how offsets should be reset.
type OffsetResetMode int

const (
	// ResetToEarliest resets to the earliest available offset.
	ResetToEarliest OffsetResetMode = iota
	// ResetToLatest resets to the latest offset (skip all messages).
	ResetToLatest
	// ResetToOffset resets to a specific absolute offset.
	ResetToOffset
	// ResetShiftBy shifts the current committed offset by N (positive or negative).
	ResetShiftBy
)

// OffsetResetStrategy describes how to reset offsets for a consumer group.
type OffsetResetStrategy struct {
	Mode  OffsetResetMode
	Value int64 // target offset for ResetToOffset, shift amount for ResetShiftBy
}

// ResetGroupOffsets resets committed offsets for a consumer group on a topic.
// The group must be in Empty or Dead state (no active consumers).
func (c *Client) ResetGroupOffsets(ctx context.Context, groupID, topic string, strategy OffsetResetStrategy) error {
	if err := c.requireGroupInactive(ctx, groupID, "reset offsets"); err != nil {
		return err
	}

	current, err := c.admin.FetchOffsetsForTopics(ctx, groupID, topic)
	if err != nil {
		return fmt.Errorf("fetching offsets: %w", err)
	}

	// Determine partitions: from current offsets, falling back to metadata.
	partitions := make(map[int32]int64)
	for partition, or := range current[topic] {
		if or.Err != nil && !errors.Is(or.Err, kerr.UnknownTopicOrPartition) {
			continue
		}
		partitions[partition] = or.At
	}
	if len(partitions) == 0 {
		tds, err := c.admin.ListTopicsWithInternal(ctx, topic)
		if err != nil {
			return fmt.Errorf("fetching topic metadata: %w", err)
		}
		td, ok := tds[topic]
		if !ok {
			return fmt.Errorf("topic %q not found", topic)
		}
		for p := range td.Partitions {
			partitions[p] = -1
		}
	}

	var commit kadm.Offsets
	for partition, currentOffset := range partitions {
		target, terr := c.resolveTargetOffset(ctx, topic, partition, currentOffset, strategy)
		if terr != nil {
			return terr
		}
		commit.AddOffset(topic, partition, target, -1)
	}

	if err := c.admin.CommitAllOffsets(ctx, groupID, commit); err != nil {
		return fmt.Errorf("committing offsets: %w", err)
	}
	return nil
}

func (c *Client) resolveTargetOffset(
	ctx context.Context, topic string, partition int32, current int64, strategy OffsetResetStrategy,
) (int64, error) {
	switch strategy.Mode {
	case ResetToEarliest:
		return c.listPartitionOffset(ctx, topic, partition, true)
	case ResetToLatest:
		return c.listPartitionOffset(ctx, topic, partition, false)
	case ResetToOffset:
		return strategy.Value, nil
	case ResetShiftBy:
		if current < 0 {
			return 0, fmt.Errorf("no committed offset for partition %d, cannot shift", partition)
		}
		target := current + strategy.Value
		if target < 0 {
			target = 0
		}
		return target, nil
	default:
		return 0, fmt.Errorf("unknown reset mode: %d", strategy.Mode)
	}
}

func (c *Client) listPartitionOffset(
	ctx context.Context, topic string, partition int32, first bool,
) (int64, error) {
	var listed kadm.ListedOffsets
	var err error
	if first {
		listed, err = c.admin.ListStartOffsets(ctx, topic)
	} else {
		listed, err = c.admin.ListEndOffsets(ctx, topic)
	}
	if err != nil {
		return 0, fmt.Errorf("listing offsets: %w", err)
	}
	lo, ok := listed.Lookup(topic, partition)
	if !ok {
		return 0, fmt.Errorf("no offset for %s:%d", topic, partition)
	}
	if lo.Err != nil {
		return 0, fmt.Errorf("listing offset for partition %d: %w", partition, lo.Err)
	}
	return lo.Offset, nil
}

// IncreasePartitions increases the partition count for a topic.
func (c *Client) IncreasePartitions(ctx context.Context, topic string, newCount int) error {
	resps, err := c.admin.UpdatePartitions(ctx, newCount, topic)
	if err != nil {
		return fmt.Errorf("increasing partitions: %w", err)
	}
	for _, r := range resps {
		if r.Err != nil {
			return fmt.Errorf("increasing partitions for %q: %w", topic, r.Err)
		}
	}
	return nil
}

// DeleteTopicConfigOverride removes a dynamic topic-level config override,
// reverting the key to the broker or cluster default.
func (c *Client) DeleteTopicConfigOverride(ctx context.Context, topic, key string) error {
	resps, err := c.admin.AlterTopicConfigs(ctx, []kadm.AlterConfig{
		{Op: kadm.DeleteConfig, Name: key},
	}, topic)
	if err != nil {
		return fmt.Errorf("deleting topic config override: %w", err)
	}
	for _, r := range resps {
		if r.Err != nil {
			return fmt.Errorf("deleting config %q for %q: %w", key, topic, r.Err)
		}
	}
	return nil
}

// requireGroupInactive errors if a group is currently active (not Empty/Dead).
func (c *Client) requireGroupInactive(ctx context.Context, groupID, action string) error {
	described, err := c.admin.DescribeGroups(ctx, groupID)
	if err != nil {
		return fmt.Errorf("describing group: %w", err)
	}
	dg, ok := described[groupID]
	if !ok {
		return fmt.Errorf("group %q not found", groupID)
	}
	if dg.Err != nil {
		return fmt.Errorf("describing group %q: %w", groupID, dg.Err)
	}
	if s := dg.State; s != "" && s != "Empty" && s != "Dead" {
		return fmt.Errorf("group %q is %s — must be Empty or Dead to %s", groupID, s, action)
	}
	return nil
}

// DeleteGroupOffsets deletes the committed offset for a specific partition of a
// topic in a consumer group. The group must be in Empty or Dead state.
func (c *Client) DeleteGroupOffsets(ctx context.Context, groupID, topic string, partition int) error {
	if err := c.requireGroupInactive(ctx, groupID, "delete offsets"); err != nil {
		return err
	}

	var s kadm.TopicsSet
	s.Add(topic, int32(partition)) //nolint:gosec // partition ID fits in int32

	resps, err := c.admin.DeleteOffsets(ctx, groupID, s)
	if err != nil {
		return fmt.Errorf("deleting offset: %w", err)
	}
	for _, ps := range resps {
		for partition, perr := range ps {
			if perr != nil {
				return fmt.Errorf("deleting offset for partition %d: %w", partition, perr)
			}
		}
	}
	return nil
}

// FetchGroupTopics returns the topics a consumer group has committed offsets for.
func (c *Client) FetchGroupTopics(ctx context.Context, groupID string) ([]string, error) {
	resps, err := c.admin.FetchOffsets(ctx, groupID)
	if err != nil {
		return nil, fmt.Errorf("fetching group topics: %w", err)
	}
	topics := make([]string, 0, len(resps))
	for t := range resps {
		topics = append(topics, t)
	}
	return topics, nil
}

// DeleteGroup deletes a consumer group. The group must be in Empty state.
func (c *Client) DeleteGroup(ctx context.Context, groupID string) error {
	_, err := c.admin.DeleteGroup(ctx, groupID)
	if err != nil {
		return fmt.Errorf("deleting group %q: %w", groupID, err)
	}
	return nil
}

// ACLEntry holds a single ACL binding flattened from the describe response.
type ACLEntry struct {
	ResourceType string
	ResourceName string
	PatternType  string
	Principal    string
	Host         string
	Operation    string
	Permission   string
}

// FetchACLs returns all ACL bindings in the cluster. Returns an empty slice
// (not an error) when the cluster has no ACLs configured.
func (c *Client) FetchACLs(ctx context.Context) ([]ACLEntry, error) {
	b := kadm.NewACLs().
		AnyResource().
		ResourcePatternType(kadm.ACLPatternAny).
		Operations(kadm.OpAny).
		Allow().Deny().
		AllowHosts().DenyHosts()

	results, err := c.admin.DescribeACLs(ctx, b)
	if err != nil {
		return nil, fmt.Errorf("describing ACLs: %w", err)
	}
	entries := make([]ACLEntry, 0)
	for _, r := range results {
		if r.Err != nil {
			return nil, fmt.Errorf("describing ACLs: %w", r.Err)
		}
		for _, acl := range r.Described {
			entries = append(entries, ACLEntry{
				ResourceType: aclResourceTypeString(acl.Type),
				ResourceName: acl.Name,
				PatternType:  aclPatternString(acl.Pattern),
				Principal:    acl.Principal,
				Host:         acl.Host,
				Operation:    strings.ToLower(acl.Operation.String()),
				Permission:   strings.ToLower(acl.Permission.String()),
			})
		}
	}
	return entries, nil
}

func aclResourceTypeString(rt kmsg.ACLResourceType) string {
	switch rt {
	case kmsg.ACLResourceTypeTopic:
		return "topic"
	case kmsg.ACLResourceTypeGroup:
		return "group"
	case kmsg.ACLResourceTypeCluster:
		return "cluster"
	case kmsg.ACLResourceTypeTransactionalId:
		return "transactionalid"
	default:
		return strings.ToLower(rt.String())
	}
}

func aclPatternString(p kadm.ACLPattern) string {
	switch p {
	case kadm.ACLPatternLiteral:
		return "literal"
	case kadm.ACLPatternPrefixed:
		return "prefixed"
	default:
		return strings.ToLower(p.String())
	}
}

// DeleteACL deletes a single ACL binding identified by exact match on all fields.
func (c *Client) DeleteACL(ctx context.Context, entry ACLEntry) error {
	resourceType, err := parseResourceType(entry.ResourceType)
	if err != nil {
		return err
	}
	patternType, err := parsePatternType(entry.PatternType)
	if err != nil {
		return err
	}
	operation, err := parseOperation(entry.Operation)
	if err != nil {
		return err
	}
	permission, err := parsePermission(entry.Permission)
	if err != nil {
		return err
	}

	b := aclBuilderForEntry(
		resourceType,
		entry.ResourceName,
		patternType,
		entry.Principal,
		entry.Host,
		operation,
		permission,
	)
	results, err := c.admin.DeleteACLs(ctx, b)
	if err != nil {
		return fmt.Errorf("deleting ACL: %w", err)
	}
	for _, r := range results {
		if r.Err != nil {
			return fmt.Errorf("deleting ACL: %w", r.Err)
		}
		for _, m := range r.Deleted {
			if m.Err != nil {
				return fmt.Errorf("deleting ACL match: %w", m.Err)
			}
		}
	}
	return nil
}

// PurgeTopicMessages uses the Kafka DeleteRecords API to immediately advance
// the log start offset for every partition. Messages before the new start
// offset become inaccessible and will be cleaned up by the broker.
//
// If retentionMs <= 1000, all messages are deleted (offset set to high watermark).
// Otherwise, messages older than retentionMs are deleted by looking up the
// offset at (now - retentionMs) via ListOffsetsAfterMilli.
func (c *Client) PurgeTopicMessages(ctx context.Context, topic string, retentionMs int64) error {
	if c.admin == nil {
		return fmt.Errorf("admin client not initialized")
	}

	offsets, err := c.resolveDeleteOffsets(ctx, topic, retentionMs)
	if err != nil {
		return err
	}
	if len(offsets) == 0 {
		return fmt.Errorf("no messages older than the specified duration")
	}

	results, err := c.admin.DeleteRecords(ctx, offsets)
	if err != nil {
		return fmt.Errorf("delete records: %w", err)
	}
	for _, partResults := range results {
		for _, r := range partResults {
			if r.Err != nil {
				return fmt.Errorf("delete records partition %d: %w", r.Partition, r.Err)
			}
		}
	}
	return nil
}

// resolveDeleteOffsets returns a per-partition target offset map for
// DeleteRecords. For retentionMs <= 1000 (purge all), uses the high
// watermark. Otherwise, the offset at the cutoff timestamp.
func (c *Client) resolveDeleteOffsets(
	ctx context.Context, topic string, retentionMs int64,
) (kadm.Offsets, error) {
	if retentionMs <= 1000 {
		listed, err := c.admin.ListEndOffsets(ctx, topic)
		if err != nil {
			return nil, fmt.Errorf("listing end offsets: %w", err)
		}
		return endOffsetsToTargets(listed), nil
	}

	cutoffMs := time.Now().Add(-time.Duration(retentionMs) * time.Millisecond).UnixMilli()
	listed, err := c.admin.ListOffsetsAfterMilli(ctx, cutoffMs, topic)
	if err != nil {
		return nil, fmt.Errorf("listing offsets at timestamp: %w", err)
	}
	return endOffsetsToTargets(listed), nil
}

func endOffsetsToTargets(listed kadm.ListedOffsets) kadm.Offsets {
	out := make(kadm.Offsets)
	for topic, partitions := range listed {
		for partition, lo := range partitions {
			if lo.Err != nil || lo.Offset <= 0 {
				continue
			}
			out.AddOffset(topic, partition, lo.Offset, -1)
		}
	}
	return out
}
