// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"maps"
	"strings"
	"time"

	"github.com/charmbracelet/log"

	"github.com/blairham/k4a/internal/kafka"
)

// followStore is the shared follow-set store (#123): one entry per followed
// topic holding its LRU clock, kept somewhere every pod of the deployment can
// reach. The local file (followset.go) survives a container restart on the
// same volume; this survives a brand-new pod, and lets the replicas of a set
// converge on one follow-set. See docs/design/shared-index-service.md
// §Follow-set persistence across pods.
type followStore interface {
	// snapshot reads the whole store and where to tail it from.
	snapshot(ctx context.Context) (map[string]int64, map[int32]int64, error)
	// put records topic as followed, last used at lastUse (unix millis).
	put(ctx context.Context, topic string, lastUse int64) error
	// remove records that this replica stopped following topic.
	remove(ctx context.Context, topic string) error
	// tail applies every later write until ctx ends, retrying on failure.
	tail(ctx context.Context, next map[int32]int64, apply func(topic string, lastUse int64, removed bool))
	close()
}

// followStoreTimeout bounds each store call, so a broker outage delays the
// publisher, never a registration or shutdown, by more than this.
const followStoreTimeout = 10 * time.Second

// followStoreRetry is how long the tail waits before reconnecting.
var followStoreRetry = 30 * time.Second

// storeEntry is one value in the compacted topic. V lets a later format
// coexist with this one: an entry of an unknown version is skipped.
type storeEntry struct {
	V       int   `json:"v"`
	LastUse int64 `json:"lastUse"`
}

const storeEntryVersion = 1

// kafkaFollowStore keeps the follow-set in a compacted Kafka topic: key = the
// followed topic, value = its storeEntry, tombstone = no longer followed.
type kafkaFollowStore struct {
	log *kafka.KeyedLog
}

// openKafkaFollowStore opens (creating if absent) the compacted store topic.
func openKafkaFollowStore(ctx context.Context, client *kafka.Client, topic string) (*kafkaFollowStore, error) {
	ctx, cancel := context.WithTimeout(ctx, followStoreTimeout)
	defer cancel()
	kl, err := client.OpenKeyedLog(ctx, topic)
	if err != nil {
		return nil, err
	}
	// Best effort: describing configs is a grant of its own, and a store the
	// daemon cannot inspect still works.
	if policy, err := kl.CleanupPolicy(ctx); err != nil {
		log.Debug("follow-set store: could not read cleanup.policy", "topic", topic, "err", err)
	} else if !strings.Contains(policy, "compact") {
		log.Warn("follow-set store topic is not compacted — it grows without bound and forgets "+
			"entries past its retention; set cleanup.policy=compact", "topic", topic, "cleanup_policy", policy)
	}
	return &kafkaFollowStore{log: kl}, nil
}

func (s *kafkaFollowStore) snapshot(ctx context.Context) (map[string]int64, map[int32]int64, error) {
	table, next, err := s.log.Snapshot(ctx)
	if err != nil {
		return nil, nil, err
	}
	out := make(map[string]int64, len(table))
	for topic, raw := range table {
		if last, ok := decodeStoreEntry(raw); ok {
			out[topic] = last
		}
	}
	return out, next, nil
}

func (s *kafkaFollowStore) put(ctx context.Context, topic string, lastUse int64) error {
	raw, err := json.Marshal(storeEntry{V: storeEntryVersion, LastUse: lastUse})
	if err != nil {
		return err
	}
	return s.log.Put(ctx, topic, raw)
}

func (s *kafkaFollowStore) remove(ctx context.Context, topic string) error {
	return s.log.Put(ctx, topic, nil)
}

func (s *kafkaFollowStore) tail(
	ctx context.Context, next map[int32]int64, apply func(topic string, lastUse int64, removed bool),
) {
	pos := maps.Clone(next)
	if pos == nil {
		pos = map[int32]int64{}
	}
	for ctx.Err() == nil {
		err := s.log.Tail(ctx, pos, func(key string, value []byte, partition int32, offset int64) {
			pos[partition] = offset + 1
			if value == nil {
				apply(key, 0, true)
				return
			}
			if last, ok := decodeStoreEntry(value); ok {
				apply(key, last, false)
			}
		})
		if ctx.Err() != nil {
			return
		}
		log.Warn("follow-set store: tail stopped — retrying", "topic", s.log.Topic(), "err", err, "in", followStoreRetry)
		select {
		case <-ctx.Done():
			return
		case <-time.After(followStoreRetry):
		}
	}
}

func (s *kafkaFollowStore) close() { s.log.Close() }

func decodeStoreEntry(raw []byte) (int64, bool) {
	var e storeEntry
	if err := json.Unmarshal(raw, &e); err != nil || e.V != storeEntryVersion || e.LastUse <= 0 {
		return 0, false
	}
	return e.LastUse, true
}
