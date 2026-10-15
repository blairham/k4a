// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"errors"
	"maps"
	"time"

	"github.com/charmbracelet/log"

	"github.com/blairham/k4a/internal/state"
)

// followSetFile is the persisted follow-set: which topics were followed and
// when each was last used. It lives next to the index under --data-dir, so it
// survives exactly what the indexes survive — a container restart (an OOM
// kill, the drain-stall watchdog's exit), not a new pod's empty volume. That
// pairing is the point: a restarted container re-follows what it had, and
// each topic resumes from its own index instead of re-warming from nothing.
type followSetFile struct {
	Topics  map[string]int64 `json:"topics"` // topic -> last Search/Follow, unix millis
	Cluster string           `json:"cluster"`
}

const (
	followSetSubsystem = state.Subsystem("k4a-index-followset.json")
	followSetVersion   = 1
)

// lastUse returns each followed topic's LRU clock.
func (m *manager) lastUse() map[string]int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make(map[string]int64, len(m.set))
	for t, e := range m.set {
		out[t] = e.lastQuery.Load()
	}
	return out
}

// saveFollowSet writes the follow-set when its membership changed since the
// last write. It runs on every registration and teardown, since the restart
// this is for (the drain-stall watchdog's exit) skips shutdown; the minute
// sweep is a backstop. LRU clocks alone moving do not trigger a write, so a
// restored clock can be up to a minute stale.
func (m *manager) saveFollowSet() {
	topics := m.lastUse()
	m.saveMu.Lock()
	defer m.saveMu.Unlock()
	if m.saved != nil && sameKeys(m.saved, topics) {
		return
	}
	m.writeFollowSet(topics)
}

// saveFollowSetNow writes unconditionally (shutdown).
func (m *manager) saveFollowSetNow(topics map[string]int64) {
	m.saveMu.Lock()
	defer m.saveMu.Unlock()
	m.writeFollowSet(topics)
}

func (m *manager) writeFollowSet(topics map[string]int64) {
	if err := state.Save(m.root, followSetSubsystem, followSetVersion,
		followSetFile{Cluster: m.cluster, Topics: topics}); err != nil {
		log.Warn("follow-set: could not persist", "err", err)
		return
	}
	m.saved = topics
}

func sameKeys(a, b map[string]int64) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if _, ok := b[k]; !ok {
			return false
		}
	}
	return true
}

// restoreFollowSet re-follows the topics persisted by an earlier run of this
// container, synchronously and before serving, as preFollow does. Each still
// has to pass today's allowlist, belong to this cluster, and have been used
// within evictAfter — a narrowed --allow or a long outage must not bring a
// topic back that the current configuration would not have followed. The
// saved LRU clock is restored, so an idle topic still ages out on schedule.
func (m *manager) restoreFollowSet() int {
	var f followSetFile
	if err := state.Load(m.root, followSetSubsystem, followSetVersion, &f); err != nil {
		if !errors.Is(err, state.ErrNotFound) {
			log.Warn("follow-set: could not read the persisted set", "err", err)
		}
		return 0
	}
	if f.Cluster != m.cluster {
		log.Info("follow-set: persisted set is for another cluster — ignored", "persisted", f.Cluster)
		return 0
	}
	cutoff := time.Now().Add(-m.evictAfter).UnixMilli()
	restored := 0
	for topic, last := range f.Topics {
		if last < cutoff || !m.policy.allowed(topic) {
			continue
		}
		m.mu.Lock()
		_, followed := m.set[topic]
		if m.closing || followed {
			m.mu.Unlock()
			continue
		}
		_, pinned := m.pins[topic]
		m.registering[topic] = struct{}{}
		m.mu.Unlock()
		m.register(topic, pinned)

		m.mu.Lock()
		if e, ok := m.set[topic]; ok {
			e.lastQuery.Store(last)
			restored++
		}
		m.mu.Unlock()
	}
	m.saveMu.Lock()
	m.saved = maps.Clone(f.Topics)
	m.saveMu.Unlock()
	return restored
}
