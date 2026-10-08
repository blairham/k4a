// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Package searchcache implements the Tier 2 query-result cache.
//
// Wraps kafka.Client.DeepSearch with a content-addressed cache keyed by
// (cluster, topic, query, scope, time-range, partitions). Cache hits
// stream the previously-recorded matches and are validated by
// re-checking each partition's high watermark — if any partition has
// advanced past the cached snapshot, the entry is considered stale and
// the underlying scan runs. Per docs/design/state-management.md, the
// cache is purely an optimization: deleting it never breaks anything.
package searchcache

import (
	"container/list"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/state"
)

const (
	// schemaVersion is the on-disk format version for cached entries.
	// Bump when the Entry shape changes incompatibly; old files at a
	// different version are renamed aside on load and the cache rebuilds.
	//
	// v2: invalidates every entry written before the chunked-scan fix. Those
	// entries are not merely stale, they are known-wrong — a multi-chunk scan
	// silently dropped its newest records and duplicated the rest, and the
	// result was cached as if complete. Nothing about a poisoned entry
	// distinguishes it from a good one at read time, and it is keyed on the
	// query rather than on data that ages out, so re-running the same search
	// would keep serving the bad answer indefinitely. The version bump is the
	// only thing that retires them.
	schemaVersion = 2

	// MaxInMemoryEntries is how many entries we keep hot in RAM.
	// Older entries fall back to a disk read on next hit.
	MaxInMemoryEntries = 32

	// matchProgressChunk is how many cached matches we emit between
	// synthetic progress updates on a cache replay.
	matchProgressChunk = 200
)

// Key uniquely identifies a cacheable search. All fields are part of
// the cache key; changing any of them produces a different entry.
type Key struct {
	Cluster       string
	Topic         string
	Pattern       string
	PartitionsCSV string // sorted, comma-separated; "" means all partitions
	SinceMS       int64  // Unix milliseconds; 0 means "no lower bound"
	UntilMS       int64  // Unix milliseconds; 0 means "no upper bound"
	Scope         uint8
}

// Hash returns a stable hex hash of the key suitable for use as a
// filename. The hash includes every field so two keys that differ only
// in (e.g.) partition order produce different hashes — partition order
// is normalized at Key construction time so logically-equivalent keys
// collide on the hash.
func (k Key) Hash() string {
	canon, _ := json.Marshal(k) //nolint:errcheck // marshal of a fixed struct cannot fail
	sum := sha256.Sum256(canon)
	return hex.EncodeToString(sum[:16])
}

// BuildKey constructs a Key from a SearchParams plus the (cluster, topic)
// context. Partition slices are sorted so cache hits don't depend on the
// order the caller supplied them in.
func BuildKey(cluster, topic string, params kafka.SearchParams) Key {
	parts := slices.Clone(params.Partitions)
	slices.Sort(parts)
	csv := make([]string, 0, len(parts))
	for _, p := range parts {
		csv = append(csv, strconv.Itoa(int(p)))
	}
	k := Key{
		Cluster:       cluster,
		Topic:         topic,
		Pattern:       "",
		Scope:         uint8(params.Scope),
		PartitionsCSV: strings.Join(csv, ","),
	}
	if params.Pattern != nil {
		k.Pattern = params.Pattern.String()
	}
	if !params.Since.IsZero() {
		k.SinceMS = params.Since.UnixMilli()
	}
	if !params.Until.IsZero() {
		k.UntilMS = params.Until.UnixMilli()
	}
	return k
}

// Summary mirrors the fields of kafka.DeepSearchProgress we care about
// for replaying cache hits. We don't reuse DeepSearchProgress directly
// to keep the on-disk format decoupled from kafka package internals.
type Summary struct {
	Elapsed        time.Duration `json:"elapsed"`
	Scanned        int64         `json:"scanned"`
	Bytes          int64         `json:"bytes"`
	Matches        int           `json:"matches"`
	Partitions     int           `json:"partitions"`
	DonePartitions int           `json:"done_partitions"`
}

// Entry is one cached search result.
type Entry struct {
	HWM           map[int32]int64         `json:"hwm"`
	CreatedAt     time.Time               `json:"created_at"`
	Matches       []kafka.ConsumedMessage `json:"matches"`
	Key           Key                     `json:"key"`
	Summary       Summary                 `json:"summary"`
	SchemaVersion int                     `json:"schema_version"`
}

// closedWindowMargin is the safety buffer applied when accepting a
// cached entry against a closed past window. Guards against producer
// clock skew and within-partition out-of-order timestamps: a record
// appended right after the scan with a slightly-late producer
// timestamp could still fall inside [since, until] if we cut it too
// close. Five seconds is a generous margin compared to typical Kafka
// timing.
const closedWindowMargin = 5 * time.Second

// IsValidAgainst reports whether the entry's HWM snapshot is still
// compatible with the current per-partition end offsets, given the
// query's upper time bound.
//
// Two regimes:
//
//  1. Open-ended query (until == zero): any partition advance might
//     contain a record matching the query, so the cache is stale the
//     moment any HWM moves. We require strict equality.
//
//  2. Closed past query (until set, entry.CreatedAt > until + margin):
//     records appended after the scan completed have broker-side
//     timestamps >= the scan completion time, which is > until + margin
//     — so they can't fall inside [since, until]. We accept the cache
//     as long as no partition has lost data (current HWM >= cached
//     HWM); a partition that shrank below the cached HWM means records
//     covered by the scan no longer exist on the broker, and the
//     cache's claim "this is everything matching [since, until]"
//     becomes unsafe.
//
// A partition count change (topic grew or shrank since the cache was
// written) always invalidates: we don't have data for newly-added
// partitions and can't tell whether they would have produced matches.
func (e *Entry) IsValidAgainst(current map[int32]int64, until time.Time) bool {
	if len(e.HWM) == 0 {
		return false
	}
	if len(current) != len(e.HWM) {
		return false
	}

	if !until.IsZero() && e.CreatedAt.After(until.Add(closedWindowMargin)) {
		for partition, cachedHWM := range e.HWM {
			cur, ok := current[partition]
			if !ok || cur < cachedHWM {
				return false
			}
		}
		return true
	}

	for partition, cachedHWM := range e.HWM {
		cur, ok := current[partition]
		if !ok || cur != cachedHWM {
			return false
		}
	}
	return true
}

// Cache is the in-memory + on-disk query-result cache.
type Cache struct {
	root   *state.Root
	lru    *list.List
	byHash map[string]*list.Element
	mu     sync.Mutex
	maxMem int
}

// New constructs a Cache rooted at the given state.Root. Disk reads
// happen lazily on Get-miss; nothing is loaded eagerly at startup.
func New(root *state.Root) *Cache {
	return &Cache{
		root:   root,
		lru:    list.New(),
		byHash: make(map[string]*list.Element),
		maxMem: MaxInMemoryEntries,
	}
}

// Get returns the entry for k from memory, falling back to disk. Returns
// (nil, false) if no entry exists. Entries with mismatched schema
// version are removed and treated as misses.
func (c *Cache) Get(k Key) (*Entry, bool) {
	hash := k.Hash()
	c.mu.Lock()
	if elem, ok := c.byHash[hash]; ok {
		c.lru.MoveToFront(elem)
		entry, _ := elem.Value.(*Entry) //nolint:errcheck // values are always *Entry; assertion can't fail
		c.mu.Unlock()
		return entry, true
	}
	c.mu.Unlock()

	entry, ok := c.loadFromDisk(hash)
	if !ok {
		return nil, false
	}
	c.put(entry)
	return entry, true
}

// Put stores entry in the cache (memory + disk), evicting the oldest
// in-memory entry if we're at capacity. Disk writes happen
// best-effort — failures are logged and the in-memory cache continues
// to serve.
func (c *Cache) Put(entry *Entry) {
	c.put(entry)
	c.writeToDisk(entry)
}

func (c *Cache) put(entry *Entry) {
	hash := entry.Key.Hash()
	c.mu.Lock()
	defer c.mu.Unlock()

	if existing, ok := c.byHash[hash]; ok {
		existing.Value = entry
		c.lru.MoveToFront(existing)
		return
	}

	elem := c.lru.PushFront(entry)
	c.byHash[hash] = elem

	for c.lru.Len() > c.maxMem {
		oldest := c.lru.Back()
		if oldest == nil {
			break
		}
		oldEntry, _ := oldest.Value.(*Entry) //nolint:errcheck // values are always *Entry; assertion can't fail
		delete(c.byHash, oldEntry.Key.Hash())
		c.lru.Remove(oldest)
	}
}

// Forget removes the cache entry for k from both memory and disk.
// Useful when callers detect a problem with the cached data and want to
// force a re-scan.
func (c *Cache) Forget(k Key) {
	hash := k.Hash()
	c.mu.Lock()
	if elem, ok := c.byHash[hash]; ok {
		c.lru.Remove(elem)
		delete(c.byHash, hash)
	}
	c.mu.Unlock()
	_ = os.Remove(c.diskPath(hash)) //nolint:errcheck // best-effort
}

// Len returns the in-memory entry count.
func (c *Cache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lru.Len()
}

// dirPath returns the directory holding cached search entries.
func (c *Cache) dirPath() string {
	return filepath.Join(c.root.File(state.SubsystemCache), "searches")
}

func (c *Cache) diskPath(hash string) string {
	return filepath.Join(c.dirPath(), hash+".json")
}

func (c *Cache) writeToDisk(entry *Entry) {
	if c.root == nil {
		return
	}
	if err := os.MkdirAll(c.dirPath(), 0o700); err != nil {
		return
	}

	entry.SchemaVersion = schemaVersion

	body, err := json.Marshal(entry)
	if err != nil {
		return
	}

	hash := entry.Key.Hash()
	path := c.diskPath(hash)

	tmp, err := os.CreateTemp(c.dirPath(), hash+".tmp-*")
	if err != nil {
		return
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) //nolint:errcheck // cleanup after Rename

	if _, err := tmp.Write(body); err != nil {
		tmp.Close() //nolint:errcheck // already failing
		return
	}
	_ = tmp.Chmod(0o600) //nolint:errcheck // best-effort
	if err := tmp.Close(); err != nil {
		return
	}
	_ = os.Rename(tmpName, path) //nolint:errcheck // best-effort
}

func (c *Cache) loadFromDisk(hash string) (*Entry, bool) {
	if c.root == nil {
		return nil, false
	}
	raw, err := os.ReadFile(c.diskPath(hash)) //nolint:gosec // path is constructed from hash within state dir
	if err != nil {
		return nil, false
	}
	var entry Entry
	if err := json.Unmarshal(raw, &entry); err != nil {
		_ = os.Remove(c.diskPath(hash)) //nolint:errcheck // bad file; drop
		return nil, false
	}
	if entry.SchemaVersion != schemaVersion {
		_ = os.Remove(c.diskPath(hash)) //nolint:errcheck // old format; drop
		return nil, false
	}
	return &entry, true
}

// Clear empties both memory and disk. Used by the `k4a state clear cache`
// CLI; the kafka package itself never invokes this.
func (c *Cache) Clear() error {
	c.mu.Lock()
	c.lru = list.New()
	c.byHash = make(map[string]*list.Element)
	c.mu.Unlock()
	if c.root == nil {
		return nil
	}
	return os.RemoveAll(c.dirPath())
}

// HWMOracle is the subset of *kafka.Client behavior the cache needs for
// invalidation: a way to ask the broker for the current end offsets of
// a topic. Lets us mock the broker in tests.
type HWMOracle interface {
	EndOffsets(ctx context.Context, topic string) (map[int32]int64, error)
}
