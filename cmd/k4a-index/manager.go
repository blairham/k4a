// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"maps"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/charmbracelet/log"

	"github.com/blairham/k4a/internal/index"
	"github.com/blairham/k4a/internal/state"
)

// Defaults for the follow-set bounds (docs/design/shared-index-service.md
// §Architecture 1). They bound consumer connections and disk; the design
// justifies each number there.
const (
	defaultMaxFollowed = 256                // hard ceiling on concurrently-followed topics
	defaultEvictAfter  = 7 * 24 * time.Hour // LRU idle timeout before a topic is dropped
	defaultSweep       = time.Hour          // how often the LRU sweeper runs
)

// followEntry is one followed topic: its worker plus the lifecycle handles the
// manager needs to evict it (cancel the tail, wait for it to stop, then delete
// the index dir). lastQuery drives LRU eviction; pinned topics (the startup
// pre-follow set) are exempt from both LRU and cap eviction so an operator's
// "always warm" list can never be dropped out from under it.
type followEntry struct {
	worker    *topicWorker
	cancel    context.CancelFunc
	done      chan struct{} // closed when the worker's run() returns
	lastQuery atomic.Int64  // unix millis of the last Search/Coverage that hit it
	pinned    bool
}

// touch records that the topic was just queried, resetting its LRU clock.
func (e *followEntry) touch() { e.lastQuery.Store(time.Now().UnixMilli()) }

// manager owns the demand-driven follow-set: it spawns a topicWorker the first
// time an allowed topic is searched, caps the set, and evicts idle topics. It
// replaces the Phase-1 static one-topic map; the wire behavior a client sees is
// unchanged for a followed topic, and an unfollowed-but-allowed topic now warms
// as a side effect of the first search (warm-on-first-search).
//
// Concurrency: `mu` guards `set` and `registering`. The slow work (opening a
// Bleve index, stopping/deleting an evicted one) happens OUTSIDE the lock so a
// registration never stalls concurrent lookups on the query hot path.
type manager struct {
	baseCtx       context.Context
	src           tailSource
	root          *state.Root
	set           map[string]*followEntry
	registering   map[string]struct{}
	cluster       string
	policy        allowPolicy
	maxFollowed   int
	evictAfter    time.Duration
	sweepEvery    time.Duration
	maxTotalBytes int64
	worker        workerOptions
	mu            sync.Mutex
}

// managerConfig carries the tunables; zero values fall back to the defaults
// (except Worker, whose zero values mean disabled — the flag defaults arm it).
type managerConfig struct {
	MaxFollowed int
	EvictAfter  time.Duration
	SweepEvery  time.Duration
	// MaxTotalBytes caps the whole index root's on-disk size; 0 disables.
	// Unlike Worker.TrimBytes (per-topic, stored bytes) this is measured on
	// disk and across all topics — see enforceDiskBudget.
	MaxTotalBytes int64
	Worker        workerOptions // per-topic knobs, passed to each spawned worker
}

func newManager(
	baseCtx context.Context,
	root *state.Root,
	cluster string,
	src tailSource,
	policy allowPolicy,
	cfg managerConfig,
) *manager {
	m := &manager{
		baseCtx:       baseCtx,
		root:          root,
		src:           src,
		cluster:       cluster,
		policy:        policy,
		maxFollowed:   cfg.MaxFollowed,
		evictAfter:    cfg.EvictAfter,
		sweepEvery:    cfg.SweepEvery,
		maxTotalBytes: cfg.MaxTotalBytes,
		worker:        cfg.Worker,
		set:           make(map[string]*followEntry),
		registering:   make(map[string]struct{}),
	}
	if m.maxFollowed <= 0 {
		m.maxFollowed = defaultMaxFollowed
	}
	if m.evictAfter <= 0 {
		m.evictAfter = defaultEvictAfter
	}
	if m.sweepEvery <= 0 {
		m.sweepEvery = defaultSweep
	}
	return m
}

// lookup returns the worker for a followed topic (touching its LRU clock) or nil
// if the topic is not followed. It never registers — the read path stays a pure
// map lookup; registration is the caller's explicit ensureFollowed step.
func (m *manager) lookup(topic string) *topicWorker {
	m.mu.Lock()
	defer m.mu.Unlock()
	e := m.set[topic]
	if e == nil {
		return nil
	}
	e.touch()
	return e.worker
}

// ensureFollowed is the demand-driven registration entry point. It gates on the
// allowlist (default-deny) and, if allowed, kicks off registration as a
// non-blocking side effect so the Search hot path returns immediately —
// warm-on-first-search: this call still reports no coverage (the client scans),
// and the topic is warm for the next one. Returns whether the topic is (or is
// becoming) followed; false means denied by policy and the client stays on scan
// permanently for it.
func (m *manager) ensureFollowed(topic string) bool {
	if !m.policy.allowed(topic) {
		return false
	}
	m.mu.Lock()
	_, followed := m.set[topic]
	_, inflight := m.registering[topic]
	if followed || inflight {
		m.mu.Unlock()
		return true
	}
	m.registering[topic] = struct{}{}
	m.mu.Unlock()

	log.Info("follow: registering topic on demand", "topic", topic)
	go m.register(topic, false)
	return true
}

// preFollow registers the startup pre-warm topics synchronously and pinned
// (exempt from eviction). A failure to open one is logged and skipped rather
// than fatal — the topic will be retried on its next on-demand search — so a
// transient broker hiccup at boot never takes the daemon down. Callers MUST
// pass only allowlisted topics (validated at startup).
func (m *manager) preFollow(topics []string) {
	for _, t := range topics {
		m.mu.Lock()
		if _, ok := m.set[t]; ok {
			m.mu.Unlock()
			continue
		}
		m.registering[t] = struct{}{}
		m.mu.Unlock()
		m.register(t, true)
	}
}

// register opens the topic's index, starts its live tail, and inserts it into
// the follow-set, evicting the coldest topic first if the set is at its cap. It
// runs off the hot path (a goroutine for on-demand follows, inline for the
// startup pre-warm). On open failure it clears the in-flight marker so a later
// search retries cleanly.
func (m *manager) register(topic string, pinned bool) {
	w, err := newTopicWorker(m.root, m.cluster, topic, m.src, m.worker)
	if err != nil {
		log.Error("follow: opening index failed — topic stays unfollowed", "topic", topic, "err", err)
		m.mu.Lock()
		delete(m.registering, topic)
		m.mu.Unlock()
		return
	}

	ctx, cancel := context.WithCancel(m.baseCtx)
	entry := &followEntry{worker: w, cancel: cancel, done: make(chan struct{}), pinned: pinned}
	entry.touch()

	// Start the tail BEFORE the entry can become visible in the map, so entry.done
	// is always backed by a live goroutine — an eviction that races registration
	// can safely cancel()+<-done without deadlocking on an unstarted worker.
	go func() {
		w.run(ctx)
		close(entry.done)
	}()

	// Make room under the cap before inserting. evictColdestLocked removes the
	// victim from the map while holding the lock; we stop it (slow) after.
	m.mu.Lock()
	if _, ok := m.set[topic]; ok {
		// Lost a race — another register() beat us. Discard THIS worker without
		// deleting the on-disk index: the winner owns the same dir, so only
		// release our handle (cancel the tail, wait for it, drop the writer lock).
		delete(m.registering, topic)
		m.mu.Unlock()
		entry.cancel()
		<-entry.done
		if stopErr := entry.worker.stop(); stopErr != nil {
			log.Error("follow: discarding duplicate worker", "topic", topic, "err", stopErr)
		}
		return
	}
	var victim *followEntry
	var victimTopic string
	if len(m.set) >= m.maxFollowed {
		victimTopic, victim = m.evictColdestLocked()
	}
	m.set[topic] = entry
	delete(m.registering, topic)
	m.mu.Unlock()

	if victim != nil {
		log.Warn("follow: at cap — evicting coldest topic to make room",
			"evicted", victimTopic, "for", topic, "cap", m.maxFollowed)
		m.teardown(victimTopic, victim)
	}

	log.Info("follow: now following", "topic", topic, "pinned", pinned, "followed_total", m.size())
}

// evictColdestLocked removes and returns the least-recently-queried non-pinned
// entry. Caller holds m.mu. Returns "", nil if every entry is pinned (the set
// can then legitimately exceed maxFollowed — an operator who pins more than the
// cap gets what they asked for, and pinned topics are bounded by config, not
// demand). The caller stops the victim outside the lock.
func (m *manager) evictColdestLocked() (string, *followEntry) {
	var coldTopic string
	var cold *followEntry
	for t, e := range m.set {
		if e.pinned {
			continue
		}
		if cold == nil || e.lastQuery.Load() < cold.lastQuery.Load() {
			coldTopic, cold = t, e
		}
	}
	if cold != nil {
		delete(m.set, coldTopic)
	}
	return coldTopic, cold
}

// sweep runs the two reclaim loops until baseCtx is done.
//
// They are deliberately on different clocks. Idle (LRU) eviction is a
// housekeeping concern and runs on sweepEvery (hours). The disk budget is a
// safety concern and runs on diskCheckEvery (a minute): a single high-volume
// topic can add hundreds of MB in the time one idle sweep waits — in one
// measurement a single high-volume topic put ~600MB on disk in nine minutes — so
// an hourly check would let the volume fill long before it looked.
func (m *manager) sweep() {
	idle := time.NewTicker(m.sweepEvery)
	defer idle.Stop()
	disk := time.NewTicker(diskCheckEvery)
	defer disk.Stop()
	for {
		select {
		case <-m.baseCtx.Done():
			return
		case <-idle.C:
			m.evictIdle()
		case <-disk.C:
			m.enforceDiskBudget()
		}
	}
}

// diskCheckEvery is how often the disk budget is enforced. See sweep.
const diskCheckEvery = time.Minute

// orphanMinAge is how long an unowned index directory must have been untouched
// before the budget sweep may delete it. It exists to close a race, not to be
// patient: a registration creates the directory before the worker lands in the
// follow-set, so a young unowned directory may be one that is about to be
// adopted. A live index touches files continuously, so anything genuinely
// abandoned goes quiet immediately and ages past this within one sweep.
const orphanMinAge = 10 * time.Minute

// enforceDiskBudget keeps the whole index root under maxTotalBytes.
//
// Nothing else does: topicMaxBytes bounds one topic's STORED bytes (on disk it
// runs ~3x that, since scorch defers segment reclamation), maxFollowed bounds
// the topic COUNT, and evictAfter bounds IDLE TIME. With none of them watching
// the total, the only backstop was the kubelet evicting the pod at the volume's
// sizeLimit — an abrupt, whole-service failure in place of what should be a
// graceful loss of the coldest coverage.
//
// Reclaim order, cheapest first:
//  1. Orphaned directories — no worker owns them, so deleting one costs
//     nothing. These accumulate because a watchdog restart re-follows only the
//     PINNED set, abandoning every on-demand topic's directory while the LRU
//     sweeper (which walks the follow-set, not the disk) can never see it
//     again.
//  2. Coldest non-pinned followed topics, by the same LRU order eviction
//     already uses, until back under budget.
//
// Pinned topics are never evicted here: an operator pinned them precisely so
// they stay warm, and they are bounded by topicMaxBytes. If they alone exceed
// the budget, that is a configuration problem no sweep can fix, and it says so.
func (m *manager) enforceDiskBudget() {
	if m.maxTotalBytes <= 0 {
		return
	}
	dirs, err := index.TopicDirs(m.root, m.cluster)
	if err != nil {
		log.Error("disk budget: listing index dirs", "err", err)
		return
	}
	var total int64
	for _, d := range dirs {
		total += d.Bytes
	}
	if total <= m.maxTotalBytes {
		return
	}
	log.Warn("disk budget: index root over budget — reclaiming",
		"used_bytes", total, "budget_bytes", m.maxTotalBytes, "dirs", len(dirs))

	total -= m.reapOrphans(dirs, orphanMinAge)
	if total <= m.maxTotalBytes {
		return
	}

	// Still over: give up the coldest coverage until we fit.
	for total > m.maxTotalBytes {
		m.mu.Lock()
		topic, entry := m.evictColdestLocked()
		m.mu.Unlock()
		if entry == nil {
			log.Error("disk budget: over budget with only pinned topics left — "+
				"lower topic-max-bytes, unpin a topic, or raise the volume sizeLimit",
				"used_bytes", total, "budget_bytes", m.maxTotalBytes)
			return
		}
		freed := entry.worker.async.Inner().DiskBytes()
		log.Warn("disk budget: evicting coldest topic to reclaim disk",
			"topic", topic, "frees_bytes", freed, "used_bytes", total, "budget_bytes", m.maxTotalBytes)
		m.teardown(topic, entry)
		total -= freed
	}
}

// reapOrphans deletes index directories that no worker owns and that have been
// untouched for at least minAge, returning the bytes reclaimed. Membership is
// checked under the lock against both the follow-set and in-flight
// registrations, so a topic being adopted right now is never reaped.
//
// minAge is the caller's policy, and the two callers want different things.
// An orphan is not garbage — it is a WARM index, and if its topic is searched
// again Open() reuses the directory, so the covered window survives and the
// re-follow costs nothing. That is worth keeping while disk is plentiful and
// not worth keeping when it is scarce:
//
//   - evictIdle passes evictAfter: with headroom, an orphan ages out on the
//     same clock a followed topic does. Anything older than evictAfter is
//     unwanted by definition — that is what the setting means.
//   - enforceDiskBudget passes orphanMinAge: under pressure, reclaim anything
//     safe to delete immediately rather than protect a cache.
func (m *manager) reapOrphans(dirs []index.TopicDir, minAge time.Duration) int64 {
	cutoff := time.Now().Add(-minAge)
	var orphans []index.TopicDir
	m.mu.Lock()
	for _, d := range dirs {
		if _, followed := m.set[d.Topic]; followed {
			continue
		}
		if _, registering := m.registering[d.Topic]; registering {
			continue
		}
		if d.ModTime.After(cutoff) {
			continue
		}
		orphans = append(orphans, d)
	}
	m.mu.Unlock()

	var freed int64
	for _, d := range orphans {
		if err := os.RemoveAll(d.Path); err != nil {
			log.Error("orphan reap: deleting orphaned index dir", "topic", d.Topic, "dir", d.Path, "err", err)
			continue
		}
		log.Warn("orphan reap: deleted orphaned index dir (no worker owns it)",
			"topic", d.Topic, "freed_bytes", d.Bytes,
			"idle_since", d.ModTime.UTC().Format(time.RFC3339), "min_age", minAge)
		freed += d.Bytes
	}
	return freed
}

// evictIdle removes every non-pinned entry idle longer than evictAfter, and
// deletes orphaned index dirs that have been untouched at least as long. It
// collects victims under the lock, then tears them down outside it.
//
// The orphan half runs regardless of the disk budget. The budget's
// own sweep only reclaims when the volume is under pressure, which bounds
// growth but leaves an abandoned index holding disk indefinitely whenever
// there is headroom — the exact "held for the life of the pod regardless of
// evictAfter" behavior that issue reported. A followed topic aging out while
// an unfollowed one outlives it is an inconsistency, not a policy.
func (m *manager) evictIdle() {
	cutoff := time.Now().Add(-m.evictAfter).UnixMilli()
	type victim struct {
		entry *followEntry
		topic string
	}
	var victims []victim
	m.mu.Lock()
	for t, e := range m.set {
		if e.pinned {
			continue
		}
		if e.lastQuery.Load() < cutoff {
			delete(m.set, t)
			victims = append(victims, victim{topic: t, entry: e})
		}
	}
	m.mu.Unlock()

	for _, v := range victims {
		log.Info("follow: evicting idle topic (LRU)", "topic", v.topic, "idle_after", m.evictAfter)
		m.teardown(v.topic, v.entry)
	}

	// After the evictions, so a topic torn down just now (teardown already
	// deleted its dir) is not also considered here.
	m.reapIdleOrphans()
}

// reapIdleOrphans deletes index dirs no worker owns that have gone untouched
// for evictAfter — the same clock a followed topic is evicted on. Runs on the
// idle sweep and does NOT depend on the disk budget, which only reclaims under
// pressure.
func (m *manager) reapIdleOrphans() {
	dirs, err := index.TopicDirs(m.root, m.cluster)
	if err != nil {
		log.Error("orphan sweep: listing index dirs", "err", err)
		return
	}
	if freed := m.reapOrphans(dirs, m.evictAfter); freed > 0 {
		log.Info("orphan sweep: reclaimed abandoned index dirs", "freed_bytes", freed, "idle_after", m.evictAfter)
	}
}

// teardown stops an evicted worker and deletes its on-disk index. Order matters:
// cancel the tail and wait for run() to return (no more Submit), stop the async
// indexer (drain + flush + release the writer lock), THEN remove the dir — so
// the lock is released before we delete the directory that held it.
func (m *manager) teardown(topic string, e *followEntry) {
	dir := e.worker.async.Inner().Dir()
	e.cancel()
	<-e.done
	if err := e.worker.stop(); err != nil {
		log.Error("follow: stopping evicted worker", "topic", topic, "err", err)
	}
	if err := os.RemoveAll(dir); err != nil {
		log.Error("follow: deleting evicted index dir", "topic", topic, "dir", dir, "err", err)
	}
}

// stopAll tears down every followed worker — used on daemon shutdown so each
// index is flushed and its writer lock released cleanly.
func (m *manager) stopAll() {
	m.mu.Lock()
	entries := make(map[string]*followEntry, len(m.set))
	maps.Copy(entries, m.set)
	m.set = make(map[string]*followEntry)
	m.mu.Unlock()

	for t, e := range entries {
		e.cancel()
		<-e.done
		if err := e.worker.stop(); err != nil {
			log.Error("follow: stopping worker on shutdown", "topic", t, "err", err)
		}
	}
}

// size returns the current followed-topic count.
func (m *manager) size() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.set)
}

// followed reports whether the topic is currently in the follow-set (used by
// tests; the query path uses lookup).
func (m *manager) followed(topic string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.set[topic]
	return ok
}
