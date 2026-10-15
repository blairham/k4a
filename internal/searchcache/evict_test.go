// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package searchcache

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/blairham/k4a/internal/kafka"
	"github.com/blairham/k4a/internal/state"
)

func evictEntry(pattern string) *Entry {
	return &Entry{
		Key:       Key{Cluster: "c", Topic: "t", Pattern: pattern},
		Matches:   []kafka.ConsumedMessage{{Topic: "t", Offset: 1, Value: strings.Repeat("x", 512)}},
		HWM:       map[int32]int64{0: 10},
		CreatedAt: time.Now(),
	}
}

func backdate(t *testing.T, path string, age time.Duration) {
	t.Helper()
	when := time.Now().Add(-age)
	if err := os.Chtimes(path, when, when); err != nil {
		t.Fatal(err)
	}
}

func dirBytes(t *testing.T, dir string) int64 {
	t.Helper()
	des, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	var total int64
	for _, de := range des {
		info, err := de.Info()
		if err != nil {
			t.Fatal(err)
		}
		total += info.Size()
	}
	return total
}

func exists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func TestCache_PutEvictsLeastRecentlyUsedOverSizeCap(t *testing.T) {
	t.Parallel()

	cache := New(state.NewRoot(t.TempDir()))
	keys := []string{"a", "b", "c", "d"}
	for i, p := range keys {
		cache.Put(evictEntry(p))
		// Spread the mtimes so "a" is oldest and "d" newest.
		backdate(t, cache.diskPath(evictEntry(p).Key.Hash()), time.Duration(len(keys)-i)*time.Minute)
	}
	one, err := os.Stat(cache.diskPath(evictEntry("a").Key.Hash()))
	if err != nil {
		t.Fatal(err)
	}

	// Room for two entries and a bit: the next Put must drop the three oldest.
	cache.maxDiskBytes = 2*one.Size() + one.Size()/2
	cache.Put(evictEntry("e"))

	for _, p := range []string{"a", "b", "c"} {
		if exists(cache.diskPath(evictEntry(p).Key.Hash())) {
			t.Errorf("entry %q should have been evicted from disk", p)
		}
	}
	for _, p := range []string{"d", "e"} {
		if !exists(cache.diskPath(evictEntry(p).Key.Hash())) {
			t.Errorf("entry %q should still be on disk", p)
		}
	}
	if got := dirBytes(t, cache.dirPath()); got > cache.maxDiskBytes {
		t.Errorf("disk usage %d exceeds cap %d", got, cache.maxDiskBytes)
	}
}

func TestCache_DiskHitCountsAsUseForEviction(t *testing.T) {
	t.Parallel()

	root := state.NewRoot(t.TempDir())
	first := New(root)
	first.Put(evictEntry("old"))
	first.Put(evictEntry("new"))
	backdate(t, first.diskPath(evictEntry("old").Key.Hash()), 2*time.Minute)
	backdate(t, first.diskPath(evictEntry("new").Key.Hash()), time.Minute)

	// A fresh process reads "old" from disk, making it the most recent.
	second := New(root)
	if _, ok := second.Get(evictEntry("old").Key); !ok {
		t.Fatal("expected a disk hit")
	}
	info, err := os.Stat(second.diskPath(evictEntry("old").Key.Hash()))
	if err != nil {
		t.Fatal(err)
	}
	second.maxDiskBytes = 2*info.Size() + info.Size()/2
	second.Put(evictEntry("third"))

	if !exists(second.diskPath(evictEntry("old").Key.Hash())) {
		t.Error("recently read entry was evicted")
	}
	if exists(second.diskPath(evictEntry("new").Key.Hash())) {
		t.Error("least recently used entry should have been evicted")
	}
}

func TestCache_OpenPrunesExpiredAndStaleTemps(t *testing.T) {
	t.Parallel()

	root := state.NewRoot(t.TempDir())
	first := New(root)
	first.Put(evictEntry("expired"))
	first.Put(evictEntry("fresh"))
	expired := first.diskPath(evictEntry("expired").Key.Hash())
	fresh := first.diskPath(evictEntry("fresh").Key.Hash())
	backdate(t, expired, DefaultMaxAge+time.Hour)

	staleTmp := filepath.Join(first.dirPath(), "dead.tmp-1")
	liveTmp := filepath.Join(first.dirPath(), "live.tmp-2")
	for _, p := range []string{staleTmp, liveTmp} {
		if err := os.WriteFile(p, []byte("{}"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	backdate(t, staleTmp, 2*staleTempAge)

	New(root)

	if exists(expired) {
		t.Error("expired entry survived open")
	}
	if !exists(fresh) {
		t.Error("fresh entry was pruned")
	}
	if exists(staleTmp) {
		t.Error("stale temp file survived open")
	}
	if !exists(liveTmp) {
		t.Error("in-flight temp file was pruned")
	}
}

func TestCache_GetRefusesExpiredDiskEntry(t *testing.T) {
	t.Parallel()

	root := state.NewRoot(t.TempDir())
	New(root).Put(evictEntry("foo"))

	// Opened before the entry aged out, so the open-time prune kept it.
	reader := New(root)
	path := reader.diskPath(evictEntry("foo").Key.Hash())
	backdate(t, path, DefaultMaxAge+time.Hour)

	if _, ok := reader.Get(evictEntry("foo").Key); ok {
		t.Error("expired disk entry was served")
	}
	if exists(path) {
		t.Error("expired disk entry was not removed")
	}
}

func TestCache_OversizedEntryStaysInMemoryOnly(t *testing.T) {
	t.Parallel()

	cache := New(state.NewRoot(t.TempDir()))
	cache.Put(evictEntry("small"))
	cache.maxDiskBytes = 64
	cache.Put(evictEntry("huge"))

	if exists(cache.diskPath(evictEntry("huge").Key.Hash())) {
		t.Error("entry larger than the cap was written to disk")
	}
	if _, ok := cache.Get(evictEntry("huge").Key); !ok {
		t.Error("oversized entry should still be served from memory")
	}
}
