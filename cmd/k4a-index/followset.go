// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"errors"
	"maps"
	"sync/atomic"
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
// restored clock can be up to a minute stale. It also wakes the shared
// store's publisher, which does send clock moves, so replicas age a topic out
// on the clock of its most recent use anywhere.
func (m *manager) saveFollowSet() {
	defer m.kickStore()
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

// restoreFollowSet re-follows the topics persisted by an earlier run,
// synchronously and before serving, as preFollow does: the local file a
// restarted container left on its volume, and the shared store a brand-new
// pod or another replica wrote, merged by the newer LRU clock. Each still has
// to pass today's allowlist, belong to this cluster, and have been used within
// evictAfter — a narrowed --allow or a long outage must not bring a topic back
// that the current configuration would not have followed. The saved LRU clock
// is restored, so an idle topic still ages out on schedule.
func (m *manager) restoreFollowSet(ctx context.Context) int {
	local := m.loadLocalFollowSet()
	shared := m.loadStoreFollowSet(ctx)
	merged := maps.Clone(local)
	if merged == nil {
		merged = make(map[string]int64, len(shared))
	}
	for t, last := range shared {
		if last > merged[t] {
			merged[t] = last
		}
	}

	cutoff := time.Now().Add(-m.evictAfter).UnixMilli()
	restored := 0
	for topic, last := range merged {
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
		m.register(topic, pinned, last) //nolint:contextcheck // a follow outlives restore: it runs on baseCtx
		if m.followed(topic) {
			restored++
		}
	}
	m.saveMu.Lock()
	m.saved = local
	m.saveMu.Unlock()

	// What the store already says needs no write back: the entries for topics
	// now followed, and the stale ones, which the first sync then deletes
	// since nobody follows them. A fresh entry this replica did not follow
	// (another replica's wider --allow, or an index that failed to open) is
	// left alone — deleting it would undo another replica's follow.
	m.storeMu.Lock()
	for t, last := range shared {
		if last < cutoff || m.followed(t) {
			m.published[t] = last
		}
	}
	m.storeMu.Unlock()
	return restored
}

// loadLocalFollowSet reads the file a previous container on this volume left,
// or nil.
func (m *manager) loadLocalFollowSet() map[string]int64 {
	var f followSetFile
	if err := state.Load(m.root, followSetSubsystem, followSetVersion, &f); err != nil {
		if !errors.Is(err, state.ErrNotFound) {
			log.Warn("follow-set: could not read the persisted set", "err", err)
		}
		return nil
	}
	if f.Cluster != m.cluster {
		log.Info("follow-set: persisted set is for another cluster — ignored", "persisted", f.Cluster)
		return nil
	}
	return f.Topics
}

// loadStoreFollowSet reads the shared store and remembers where to tail it
// from, or returns nil when there is no store or it cannot be read; the tail
// then reads it from the start and the set converges a little later.
func (m *manager) loadStoreFollowSet(ctx context.Context) map[string]int64 {
	if m.store == nil {
		return nil
	}
	ctx, cancel := context.WithTimeout(ctx, 3*followStoreTimeout)
	defer cancel()
	set, next, err := m.store.snapshot(ctx)
	if err != nil {
		log.Error("follow-set store: could not read it — restoring from the local file only", "err", err)
		return nil
	}
	m.storeNext = next
	log.Info("follow-set store: read", "topics", len(set))
	return set
}

// startStore runs the shared store's publisher and tail until stopStore or
// the end of ctx. restoreFollowSet runs first, so the tail starts after the
// snapshot.
func (m *manager) startStore(ctx context.Context) {
	if m.store == nil {
		return
	}
	ctx, cancel := context.WithCancel(ctx)
	m.storeStop = cancel
	m.storeDone.Add(2)
	go func() {
		defer m.storeDone.Done()
		m.store.tail(ctx, m.storeNext, m.applyStoreEntry)
	}()
	go func() {
		defer m.storeDone.Done()
		for {
			select {
			case <-ctx.Done():
				return
			case <-m.storeKick:
				m.syncStore(ctx)
			}
		}
	}()
	// Publish what the local file restored that the store did not have.
	m.kickStore()
}

// kickStore asks the publisher for a sync without waiting for it.
func (m *manager) kickStore() {
	if m.store == nil {
		return
	}
	select {
	case m.storeKick <- struct{}{}:
	default:
	}
}

// stopStore stops the tail and publisher, then makes one last sync while the
// follow-set is still whole, so a pod replacing this one restores what it
// followed. Never a deletion for the shutdown itself: stopAll calls this
// before it empties the set.
func (m *manager) stopStore() {
	if m.store == nil {
		return
	}
	m.storeOnce.Do(func() {
		if m.storeStop != nil {
			m.storeStop()
			m.storeDone.Wait()
		}
		ctx, cancel := context.WithTimeout(context.WithoutCancel(m.baseCtx), followStoreTimeout)
		defer cancel()
		m.syncStore(ctx)
		m.store.close()
	})
}

// syncStore brings the store in line with this replica's follow-set: an
// entry for each topic it follows whose clock moved since it last wrote one,
// and a deletion for each topic it followed and no longer does. It stops at
// the first failure; the next kick retries from what was actually written.
func (m *manager) syncStore(ctx context.Context) {
	topics := m.lastUse()
	m.storeMu.Lock()
	defer m.storeMu.Unlock()
	for t, last := range topics {
		if p, ok := m.published[t]; ok && last <= p {
			continue
		}
		if err := m.storeCall(ctx, func(c context.Context) error { return m.store.put(c, t, last) }); err != nil {
			log.Warn("follow-set store: could not record a follow — retrying on the next change", "topic", t, "err", err)
			return
		}
		m.published[t] = last
	}
	for t := range m.published {
		if _, ok := topics[t]; ok {
			continue
		}
		if err := m.storeCall(ctx, func(c context.Context) error { return m.store.remove(c, t) }); err != nil {
			log.Warn("follow-set store: could not record an unfollow — retrying on the next change", "topic", t, "err", err)
			return
		}
		delete(m.published, t)
	}
}

func (m *manager) storeCall(ctx context.Context, call func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, followStoreTimeout)
	defer cancel()
	return call(ctx)
}

// applyStoreEntry takes one write from the store, made by any replica
// (including this one). A follow of an allowed, recently used topic is
// followed here too, and a newer clock for a topic followed here moves its
// clock forward, so every replica ages a topic out on its most recent use
// anywhere. A deletion changes nothing: each replica evicts on its own LRU,
// and acting on another's eviction would let two replicas at the cap evict
// each other's topics back and forth.
func (m *manager) applyStoreEntry(topic string, lastUse int64, removed bool) {
	if removed {
		return
	}
	now := time.Now().UnixMilli()
	lastUse = min(lastUse, now) // a clock from the future would never age out
	if lastUse < now-m.evictAfter.Milliseconds() || !m.policy.allowed(topic) {
		return
	}
	m.mu.Lock()
	if m.closing {
		m.mu.Unlock()
		return
	}
	if e, ok := m.set[topic]; ok {
		advanceClock(&e.lastQuery, lastUse)
		m.mu.Unlock()
		m.storeMu.Lock()
		if lastUse > m.published[topic] {
			m.published[topic] = lastUse // the store already has it; do not echo it
		}
		m.storeMu.Unlock()
		return
	}
	if _, inflight := m.registering[topic]; inflight {
		m.mu.Unlock()
		return
	}
	if d, ok := m.dropped[topic]; ok && lastUse <= d {
		// This replica dropped the topic after this use: a stale entry, or
		// this replica's own write read back after an eviction.
		m.mu.Unlock()
		return
	}
	m.registering[topic] = struct{}{}
	_, pinned := m.pins[topic]
	m.wg.Add(1)
	m.mu.Unlock()

	log.Info("follow: registering topic another replica follows", "topic", topic)
	go func() {
		defer m.wg.Done()
		m.register(topic, pinned, lastUse)
	}()
}

// advanceClock moves an LRU clock forward to at least t, never back.
func advanceClock(c *atomic.Int64, t int64) {
	for {
		cur := c.Load()
		if cur >= t || c.CompareAndSwap(cur, t) {
			return
		}
	}
}

// markDropped records the clock a topic had when this replica stopped
// following it (see manager.dropped).
func (m *manager) markDropped(topic string, e *followEntry) {
	m.mu.Lock()
	m.dropped[topic] = e.lastQuery.Load()
	m.mu.Unlock()
}

// forgetDropped prunes drops older than cutoff: the store entries they guard
// against are past evictAfter, which applyStoreEntry already ignores.
func (m *manager) forgetDropped(cutoff int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for t, last := range m.dropped {
		if last < cutoff {
			delete(m.dropped, t)
		}
	}
}
