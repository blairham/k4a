// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package kafka

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

const (
	// consumerChannelBuffer is the buffer size for the consumed-message channel.
	consumerChannelBuffer = 256

	// initialMessageFetch is how many recent messages to fetch when first
	// opening a topic consumer, before switching to live tailing.
	initialMessageFetch = 100

	// fetchRecentTimeout is the deadline for the initial "fetch recent messages"
	// call when opening a consumer.
	fetchRecentTimeout = 3 * time.Second
)

// Client wraps the franz-go consumer/admin clients with convenience methods
// for the TUI.
type Client struct {
	kgoClient   *kgo.Client
	admin       *kadm.Client
	brokers     string
	clusterID   string
	authCfg     AuthConfig
	clusterIDMu sync.Mutex
}

// NewClient creates a new Kafka client backed by franz-go.
func NewClient(authCfg AuthConfig, brokers string) (*Client, error) {
	opts, err := NewClientOpts(authCfg, brokers)
	if err != nil {
		return nil, err
	}
	kc, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("creating kgo client: %w", err)
	}
	return &Client{
		kgoClient: kc,
		admin:     kadm.NewClient(kc),
		authCfg:   authCfg,
		brokers:   brokers,
	}, nil
}

// Close releases underlying connections. Idempotent.
func (c *Client) Close() {
	if c.kgoClient != nil {
		c.kgoClient.Close()
	}
}

// CloseIdleConnections drops cached connections and rebuilds the underlying
// client so the next request dials fresh. Useful after a credential refresh
// or transient network failure.
func (c *Client) CloseIdleConnections() {
	if c.kgoClient != nil {
		c.kgoClient.Close()
	}
	opts, err := NewClientOpts(c.authCfg, c.brokers)
	if err != nil {
		slog.Warn("CloseIdleConnections: rebuilding client opts failed", "err", err)
		return
	}
	kc, err := kgo.NewClient(opts...)
	if err != nil {
		slog.Warn("CloseIdleConnections: rebuilding kgo client failed", "err", err)
		return
	}
	c.kgoClient = kc
	c.admin = kadm.NewClient(kc)
}

// BrokerInfo holds broker metadata.
type BrokerInfo struct {
	Host string
	ID   int
	Port int
}

// ClusterInfo holds cluster-level metadata.
type ClusterInfo struct {
	ClusterID    string
	Brokers      []BrokerInfo
	ControllerID int
}

// TopicInfo holds topic metadata.
type TopicInfo struct {
	Name              string
	Partitions        int
	ReplicationFactor int
	OutOfSync         int
	Messages          int64
	Internal          bool
}

// PartitionInfo holds partition-level detail.
type PartitionInfo struct {
	Replicas    []int
	ISR         []int
	ID          int
	Leader      int
	FirstOffset int64
	LastOffset  int64
	Messages    int64
}

// TopicDetail holds detailed topic information including partitions.
type TopicDetail struct {
	Name              string
	CleanupPolicy     string
	Partitions        []PartitionInfo
	ReplicationFactor int
	TotalISR          int
	TotalReplicas     int
	URP               int
	TotalMessages     int64
	RetentionMs       int64
	RetentionBytes    int64
	Internal          bool
}

// GroupInfo holds consumer group metadata.
type GroupInfo struct {
	GroupID     string
	State       string
	TotalLag    int64 // -1 = not yet fetched
	Members     int
	Topics      int // -1 = not yet fetched
	Coordinator int // -1 = not yet fetched
}

// GroupEnrichment holds async-fetched enrichment data for a consumer group.
type GroupEnrichment struct {
	TotalLag    int64
	Topics      int
	Coordinator int
}

// GroupMemberInfo holds consumer group member details.
type GroupMemberInfo struct {
	MemberID string
	ClientID string
	Host     string
	Topics   []string
}

// GroupOffsetInfo holds per-partition offset/lag for a consumer group.
type GroupOffsetInfo struct {
	Topic           string
	Partition       int
	CommittedOffset int64
	HighWatermark   int64
	Lag             int64
}

// GroupDetail holds full consumer group details.
type GroupDetail struct {
	GroupID  string
	State    string
	Members  []GroupMemberInfo
	Offsets  []GroupOffsetInfo
	TotalLag int64
}

// EndOffsets returns the per-partition end (high-watermark) offsets for
// a single topic. Convenience wrapper over kadm.ListEndOffsets used by
// callers that need raw HWMs without the full topic-detail surface —
// e.g., the searchcache for invalidation.
func (c *Client) EndOffsets(ctx context.Context, topic string) (map[int32]int64, error) {
	listed, err := c.admin.ListEndOffsets(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("listing end offsets: %w", err)
	}
	out := make(map[int32]int64, len(listed[topic]))
	for partition, eo := range listed[topic] {
		if eo.Err != nil {
			continue
		}
		out[partition] = eo.Offset
	}
	return out, nil
}

// ClusterID returns the broker-reported cluster ID, caching it across
// calls. Returns the empty string if the broker doesn't report one
// (some non-Kafka implementations); callers should treat that as
// "unknown cluster" for keying purposes.
func (c *Client) ClusterID(ctx context.Context) (string, error) {
	c.clusterIDMu.Lock()
	cached := c.clusterID
	c.clusterIDMu.Unlock()
	if cached != "" {
		return cached, nil
	}
	m, err := c.admin.BrokerMetadata(ctx)
	if err != nil {
		return "", fmt.Errorf("fetching cluster id: %w", err)
	}
	c.clusterIDMu.Lock()
	c.clusterID = m.Cluster
	c.clusterIDMu.Unlock()
	return m.Cluster, nil
}

// FetchCluster returns cluster metadata.
func (c *Client) FetchCluster(ctx context.Context) (*ClusterInfo, error) {
	m, err := c.admin.BrokerMetadata(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetching metadata: %w", err)
	}

	info := &ClusterInfo{
		ClusterID:    m.Cluster,
		ControllerID: int(m.Controller),
		Brokers:      make([]BrokerInfo, 0, len(m.Brokers)),
	}
	for _, b := range m.Brokers {
		info.Brokers = append(info.Brokers, BrokerInfo{
			ID:   int(b.NodeID),
			Host: b.Host,
			Port: int(b.Port),
		})
	}
	return info, nil
}

// FetchTopics returns a list of topics with metadata. Fast — only fetches metadata.
func (c *Client) FetchTopics(ctx context.Context) ([]TopicInfo, error) {
	tds, err := c.admin.ListTopicsWithInternal(ctx)
	if err != nil {
		return nil, fmt.Errorf("fetching metadata: %w", err)
	}

	topics := make([]TopicInfo, 0, len(tds))
	for _, t := range tds.Sorted() {
		rf := 0
		oos := 0
		for _, p := range t.Partitions {
			if len(p.Replicas) > rf {
				rf = len(p.Replicas)
			}
			if len(p.ISR) < len(p.Replicas) {
				oos++
			}
		}
		topics = append(topics, TopicInfo{
			Name:              t.Topic,
			Partitions:        len(t.Partitions),
			ReplicationFactor: rf,
			OutOfSync:         oos,
			Messages:          -1, // Not yet fetched.
			Internal:          t.IsInternal || strings.HasPrefix(t.Topic, "_"),
		})
	}
	return topics, nil
}

// FetchTopicMessageCounts returns message counts for the given topics.
// This is a separate call because ListOffsets can be slow.
func (c *Client) FetchTopicMessageCounts(ctx context.Context, topicNames []string) (map[string]int64, error) {
	starts, err := c.admin.ListStartOffsets(ctx, topicNames...)
	if err != nil {
		return nil, fmt.Errorf("listing start offsets: %w", err)
	}
	ends, err := c.admin.ListEndOffsets(ctx, topicNames...)
	if err != nil {
		return nil, fmt.Errorf("listing end offsets: %w", err)
	}

	counts := make(map[string]int64, len(ends))
	for topic, partitions := range ends {
		var total int64
		for partition, eo := range partitions {
			if eo.Err != nil {
				continue
			}
			so, ok := starts.Lookup(topic, partition)
			if !ok || so.Err != nil {
				continue
			}
			if diff := eo.Offset - so.Offset; diff > 0 {
				total += diff
			}
		}
		counts[topic] = total
	}
	return counts, nil
}

// TopicSize holds on-disk size information for a topic, derived from broker log
// directories (DescribeLogDirs). This is real bytes on disk — distinct from the
// logical message count returned by FetchTopicMessageCounts.
type TopicSize struct {
	Name        string
	Partitions  int
	LeaderBytes int64 // logical size: one copy of the data (sum of leader-replica partition sizes)
	TotalBytes  int64 // on-disk footprint summed across every replica on every broker
	Messages    int64 // -1 when message counts were not requested
	Internal    bool
}

// FetchTopicSizes returns per-topic on-disk sizes by describing broker log
// directories. If topics is empty, every topic is described. LeaderBytes is the
// logical size (one replica's worth); TotalBytes is the full on-disk footprint
// across all replicas — for an RF=3 topic TotalBytes is roughly 3×LeaderBytes.
//
// Future replicas (created by an in-flight reassignment) are excluded so a
// partition mid-move is not double-counted.
func (c *Client) FetchTopicSizes(ctx context.Context, topics []string) ([]TopicSize, error) {
	// Metadata gives us the partition→leader map and the authoritative topic set.
	tds, err := c.admin.ListTopicsWithInternal(ctx, topics...)
	if err != nil {
		return nil, fmt.Errorf("fetching metadata: %w", err)
	}

	leaders, acc := seedTopicSizes(tds)

	// Describe log dirs on every broker. On partial (shard) failures franz-go
	// returns the successful shards alongside a *ShardErrors — keep what we got
	// and warn, rather than failing the whole command.
	dirs, err := c.admin.DescribeAllLogDirs(ctx, nil)
	if err != nil {
		var se *kadm.ShardErrors
		if !errors.As(err, &se) || se.AllFailed {
			return nil, fmt.Errorf("describing log dirs: %w", err)
		}
		slog.Warn("FetchTopicSizes partial log-dir describe", "err", err)
	}

	dirs.Each(func(d kadm.DescribedLogDir) {
		if d.Err != nil {
			slog.Warn("FetchTopicSizes log-dir error", "broker", d.Broker, "dir", d.Dir, "err", d.Err)
			return
		}
		d.Topics.Each(func(p kadm.DescribedLogDirPartition) {
			addLogDirPartition(p, leaders, acc)
		})
	})

	out := make([]TopicSize, 0, len(acc))
	for _, ts := range acc {
		out = append(out, *ts)
	}
	return out, nil
}

// seedTopicSizes builds leaders[topic][partition] = leader broker id, and seeds
// the result set so a topic with zero on-disk bytes (freshly created) still
// appears.
func seedTopicSizes(tds kadm.TopicDetails) (leaders map[string]map[int32]int32, acc map[string]*TopicSize) {
	leaders = make(map[string]map[int32]int32, len(tds))
	acc = make(map[string]*TopicSize, len(tds))
	for _, td := range tds.Sorted() {
		if td.Err != nil {
			continue
		}
		lm := make(map[int32]int32, len(td.Partitions))
		for _, p := range td.Partitions {
			lm[p.Partition] = p.Leader
		}
		leaders[td.Topic] = lm
		acc[td.Topic] = &TopicSize{
			Name:       td.Topic,
			Partitions: len(td.Partitions),
			Messages:   -1,
			Internal:   td.IsInternal || strings.HasPrefix(td.Topic, "_"),
		}
	}
	return leaders, acc
}

// addLogDirPartition adds one replica's on-disk size to its topic's totals,
// and to the leader figure when the replica is on the partition's leader.
func addLogDirPartition(
	p kadm.DescribedLogDirPartition,
	leaders map[string]map[int32]int32,
	acc map[string]*TopicSize,
) {
	if p.IsFuture {
		return // in-flight reassignment replica; not part of the steady-state footprint
	}
	ts, ok := acc[p.Topic]
	if !ok {
		return // topic not in the requested set
	}
	ts.TotalBytes += p.Size
	if lm, ok := leaders[p.Topic]; ok && lm[p.Partition] == p.Broker {
		ts.LeaderBytes += p.Size
	}
}

// FetchTopicDetail returns detailed partition info for a topic including offsets.
func (c *Client) FetchTopicDetail(ctx context.Context, topic string) (*TopicDetail, error) {
	tds, err := c.admin.ListTopicsWithInternal(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("fetching topic detail: %w", err)
	}
	td, ok := tds[topic]
	if !ok {
		return nil, fmt.Errorf("topic %q not found", topic)
	}
	if td.Err != nil {
		return nil, fmt.Errorf("topic %q: %w", topic, td.Err)
	}

	starts, soErr := c.admin.ListStartOffsets(ctx, topic)
	if soErr != nil {
		slog.Warn("FetchTopicDetail start offsets partial error", "topic", topic, "err", soErr)
	}
	ends, eoErr := c.admin.ListEndOffsets(ctx, topic)
	if eoErr != nil {
		slog.Warn("FetchTopicDetail end offsets partial error", "topic", topic, "err", eoErr)
	}

	detail := &TopicDetail{
		Name:       td.Topic,
		Partitions: make([]PartitionInfo, 0, len(td.Partitions)),
		Internal:   td.IsInternal || strings.HasPrefix(topic, "_"),
	}

	rf := 0
	for _, p := range td.Partitions.Sorted() {
		pi := partitionInfo(topic, p, starts, ends)
		rf = max(rf, len(pi.Replicas))
		detail.Partitions = append(detail.Partitions, pi)
		detail.TotalISR += len(pi.ISR)
		detail.TotalReplicas += len(pi.Replicas)
		detail.TotalMessages += pi.Messages
		if len(pi.ISR) < len(pi.Replicas) {
			detail.URP++
		}
	}
	detail.ReplicationFactor = rf

	c.applyTopicConfig(ctx, topic, detail)
	return detail, nil
}

// partitionInfo describes one partition. An offset whose lookup failed reads
// as 0, and the message count is floored at 0.
func partitionInfo(topic string, p kadm.PartitionDetail, starts, ends kadm.ListedOffsets) PartitionInfo {
	var first, last int64
	if so, ok := starts.Lookup(topic, p.Partition); ok && so.Err == nil {
		first = so.Offset
	}
	if eo, ok := ends.Lookup(topic, p.Partition); ok && eo.Err == nil {
		last = eo.Offset
	}
	return PartitionInfo{
		ID:          int(p.Partition),
		Leader:      int(p.Leader),
		Replicas:    brokerInts(p.Replicas),
		ISR:         brokerInts(p.ISR),
		FirstOffset: first,
		LastOffset:  last,
		Messages:    max(last-first, 0),
	}
}

// brokerInts widens a broker ID list to ints, never returning nil.
func brokerInts(ids []int32) []int {
	out := make([]int, 0, len(ids))
	for _, id := range ids {
		out = append(out, int(id))
	}
	return out
}

// applyTopicConfig fetches cleanup.policy, retention.ms, and retention.bytes
// and applies them to the detail struct.
func (c *Client) applyTopicConfig(ctx context.Context, topic string, detail *TopicDetail) {
	detail.RetentionMs = -1
	detail.RetentionBytes = -1

	rcs, err := c.admin.DescribeTopicConfigs(ctx, topic)
	if err == nil {
		for _, rc := range rcs {
			if rc.Name != topic || rc.Err != nil {
				continue
			}
			for _, cfg := range rc.Configs {
				detail.applyConfigEntry(cfg.Key, cfg.MaybeValue())
			}
		}
	}
	if detail.CleanupPolicy == "" {
		detail.CleanupPolicy = "delete"
	}
}

// applyConfigEntry records one topic config entry if it is one TopicDetail
// shows; an unparseable retention value leaves the existing figure.
func (d *TopicDetail) applyConfigEntry(key, v string) {
	switch key {
	case "cleanup.policy":
		d.CleanupPolicy = v
	case "retention.ms":
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			d.RetentionMs = n
		}
	case "retention.bytes":
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			d.RetentionBytes = n
		}
	}
}

// FetchPartitionIDs returns the sorted partition IDs for a topic.
func (c *Client) FetchPartitionIDs(ctx context.Context, topic string) ([]int, error) {
	tds, err := c.admin.ListTopicsWithInternal(ctx, topic)
	if err != nil {
		return nil, fmt.Errorf("fetching metadata: %w", err)
	}
	td, ok := tds[topic]
	if !ok {
		return nil, nil
	}
	ids := make([]int, 0, len(td.Partitions))
	for p := range td.Partitions {
		ids = append(ids, int(p))
	}
	slices.Sort(ids)
	return ids, nil
}

// FetchGroups returns a list of consumer groups.
func (c *Client) FetchGroups(ctx context.Context) ([]GroupInfo, error) {
	listed, err := c.admin.ListGroups(ctx)
	if err != nil {
		return nil, fmt.Errorf("listing groups: %w", err)
	}

	ids := make([]string, 0, len(listed))
	for id := range listed {
		ids = append(ids, id)
	}
	described, err := c.admin.DescribeGroups(ctx, ids...)
	if err != nil {
		slog.Warn("describe groups partial error", "err", err)
	}

	groups := make([]GroupInfo, 0, len(listed))
	for _, id := range ids {
		gi := GroupInfo{
			GroupID:     id,
			Topics:      -1,
			TotalLag:    -1,
			Coordinator: -1,
		}
		if dg, ok := described[id]; ok && dg.Err == nil {
			gi.State = dg.State
			gi.Members = len(dg.Members)
		}
		groups = append(groups, gi)
	}
	return groups, nil
}

// FetchGroupDetail returns full group detail with offsets and lag.
func (c *Client) FetchGroupDetail(ctx context.Context, groupID string) (*GroupDetail, error) {
	described, err := c.admin.DescribeGroups(ctx, groupID)
	if err != nil {
		return nil, fmt.Errorf("describing group: %w", err)
	}
	dg, ok := described[groupID]
	if !ok {
		return nil, fmt.Errorf("group %q not found", groupID)
	}
	if dg.Err != nil {
		return nil, fmt.Errorf("group %q: %w", groupID, dg.Err)
	}

	detail := &GroupDetail{
		GroupID: dg.Group,
		State:   dg.State,
		Members: make([]GroupMemberInfo, 0, len(dg.Members)),
	}
	for _, m := range dg.Members {
		topics := memberAssignmentTopics(m)
		detail.Members = append(detail.Members, GroupMemberInfo{
			MemberID: m.MemberID,
			ClientID: m.ClientID,
			Host:     m.ClientHost,
			Topics:   topics,
		})
	}

	offsets, err := c.admin.FetchOffsets(ctx, groupID)
	if err != nil {
		slog.Warn("FetchGroupDetail offset fetch failed, returning partial result", "group", groupID, "err", err)
		return detail, nil //nolint:nilerr // partial result is useful
	}

	// High watermarks for every topic the group has committed offsets on.
	topicList := make([]string, 0, len(offsets))
	for topic := range offsets {
		topicList = append(topicList, topic)
	}
	ends, endsErr := c.admin.ListEndOffsets(ctx, topicList...)
	if endsErr != nil {
		slog.Warn("FetchGroupDetail end offsets partial error", "group", groupID, "err", endsErr)
	}

	detail.Offsets = make([]GroupOffsetInfo, 0)
	eachCommitted(offsets, ends, func(topic string, partition int32, committed, high int64) {
		lag := max(high-committed, 0)
		detail.Offsets = append(detail.Offsets, GroupOffsetInfo{
			Topic:           topic,
			Partition:       int(partition),
			CommittedOffset: committed,
			HighWatermark:   high,
			Lag:             lag,
		})
		detail.TotalLag += lag
	})
	return detail, nil
}

// eachCommitted calls fn for every committed offset that fetched without
// error, with its partition's high watermark (0 when ends has none for it).
func eachCommitted(
	offsets kadm.OffsetResponses,
	ends kadm.ListedOffsets,
	fn func(topic string, partition int32, committed, high int64),
) {
	for topic, parts := range offsets {
		for partition, or := range parts {
			if or.Err != nil {
				continue
			}
			var high int64
			if eo, ok := ends.Lookup(topic, partition); ok && eo.Err == nil {
				high = eo.Offset
			}
			fn(topic, partition, or.At, high)
		}
	}
}

// memberAssignmentTopics extracts assigned topics from a described group
// member. Returns the parsed consumer-protocol topic list, or nil for
// non-consumer protocols.
func memberAssignmentTopics(m kadm.DescribedGroupMember) []string {
	asn, ok := m.Assigned.AsConsumer()
	if !ok || asn == nil {
		return nil
	}
	out := make([]string, 0, len(asn.Topics))
	for _, t := range asn.Topics {
		out = append(out, t.Topic)
	}
	return out
}

// FetchGroupsEnrichment returns enrichment data (topic count, lag, coordinator)
// for the given group IDs. This is expensive and meant to be called asynchronously.
func (c *Client) FetchGroupsEnrichment(ctx context.Context, groupIDs []string) map[string]GroupEnrichment {
	result := make(map[string]GroupEnrichment, len(groupIDs))
	if len(groupIDs) == 0 {
		return result
	}

	described, err := c.admin.DescribeGroups(ctx, groupIDs...)
	if err != nil {
		slog.Warn("FetchGroupsEnrichment describe groups partial error", "err", err)
	}
	fetched := c.admin.FetchManyOffsets(ctx, groupIDs...)

	// Collect unique topics across all groups for a single ListEndOffsets call.
	topicSet := make(map[string]struct{})
	for _, fr := range fetched {
		for topic := range fr.Fetched {
			topicSet[topic] = struct{}{}
		}
	}
	topicList := make([]string, 0, len(topicSet))
	for t := range topicSet {
		topicList = append(topicList, t)
	}
	var ends kadm.ListedOffsets
	if len(topicList) > 0 {
		var endsErr error
		ends, endsErr = c.admin.ListEndOffsets(ctx, topicList...)
		if endsErr != nil {
			slog.Warn("FetchGroupsEnrichment end offsets partial error", "err", endsErr)
		}
	}

	for _, gid := range groupIDs {
		e := GroupEnrichment{Coordinator: -1}
		if dg, ok := described[gid]; ok && dg.Err == nil {
			e.Coordinator = int(dg.Coordinator.NodeID)
		}
		if fr, ok := fetched[gid]; ok {
			e.Topics = len(fr.Fetched)
			eachCommitted(fr.Fetched, ends, func(_ string, _ int32, committed, high int64) {
				if lag := high - committed; lag > 0 {
					e.TotalLag += lag
				}
			})
		}
		result[gid] = e
	}
	return result
}

// MessageHeader holds a single Kafka message header.
type MessageHeader struct {
	Key   string
	Value string
}

// ConsumedMessage holds a message read from Kafka.
type ConsumedMessage struct {
	Time      time.Time
	Key       string
	Value     string
	Topic     string
	Headers   []MessageHeader
	Partition int
	Offset    int64
}
