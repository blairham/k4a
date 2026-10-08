// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package index

import (
	"os"
	"path/filepath"
	"time"

	"github.com/blairham/k4a/internal/state"
)

// TopicInventory is the readable summary of one topic's on-disk index.
// Used by `k4a state status` and the TUI Topics view to surface which
// topics are indexed and how much they cover, without taking the
// per-topic writer lock.
type TopicInventory struct {
	OldestAt   time.Time
	NewestAt   time.Time
	Cluster    string
	Topic      string
	Bytes      int64
	Partitions int
}

// List walks the index subsystem root and returns one TopicInventory
// per topic that has a bookkeeper file. Directories without a parseable
// bookkeeper (corrupt, mid-rename) are silently skipped — the live
// indexer will repair them on next open.
func List(root *state.Root) ([]TopicInventory, error) {
	base := root.File(state.SubsystemIndex)
	clusters, err := os.ReadDir(base)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}

	var out []TopicInventory
	for _, c := range clusters {
		if !c.IsDir() {
			continue
		}
		cluster := c.Name()
		topics, err := os.ReadDir(filepath.Join(base, cluster))
		if err != nil {
			continue
		}
		for _, t := range topics {
			if !t.IsDir() {
				continue
			}
			dir := filepath.Join(base, cluster, t.Name())
			inv, ok := summarizeTopic(dir, cluster, t.Name())
			if !ok {
				continue
			}
			out = append(out, inv)
		}
	}
	return out, nil
}

// HasIndex reports whether (cluster, topic) currently has a readable
// bookkeeper. Used by the TUI to render an "indexed?" indicator.
func HasIndex(root *state.Root, cluster, topic string) bool {
	dir := topicDir(root, cluster, topic)
	_, err := readBookkeeper(dir)
	return err == nil
}

func summarizeTopic(dir, cluster, topic string) (TopicInventory, bool) {
	book, err := readBookkeeper(dir)
	if err != nil {
		return TopicInventory{}, false
	}
	inv := TopicInventory{
		Cluster:    cluster,
		Topic:      topic,
		Bytes:      dirBytes(dir),
		Partitions: len(book.NewestOffsetPerPartition),
	}
	for _, ts := range book.NewestTimePerPartition {
		t := time.UnixMilli(ts).UTC()
		if inv.NewestAt.IsZero() || t.After(inv.NewestAt) {
			inv.NewestAt = t
		}
	}
	for _, ts := range book.OldestTimePerPartition {
		t := time.UnixMilli(ts).UTC()
		if inv.OldestAt.IsZero() || t.Before(inv.OldestAt) {
			inv.OldestAt = t
		}
	}
	return inv, true
}

// TopicDir is one topic's index directory as seen from the filesystem, without
// opening it or taking its writer lock. The shared daemon's disk-budget sweep
// uses these to total real usage and to spot directories no worker owns.
type TopicDir struct {
	// ModTime is the newest mtime anywhere under the directory. A live index
	// touches files constantly (segments arrive and are reclaimed), so a stale
	// ModTime is what distinguishes an abandoned directory from one that is
	// merely between writes — and it is the guard that keeps the reaper from
	// racing a registration that just created the directory.
	ModTime time.Time
	Topic   string
	Path    string
	Bytes   int64
}

// TopicDirs lists the on-disk index directories for one cluster, with their
// real size and last activity. Unlike List it does not require a parseable
// bookkeeper: a directory that is corrupt or half-written still occupies disk,
// so the disk budget must see it.
func TopicDirs(root *state.Root, cluster string) ([]TopicDir, error) {
	base := filepath.Join(root.File(state.SubsystemIndex), cluster)
	entries, err := os.ReadDir(base)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	out := make([]TopicDir, 0, len(entries))
	for _, e := range entries {
		if !e.IsDir() {
			continue
		}
		dir := filepath.Join(base, e.Name())
		bytes, mod := dirStats(dir)
		out = append(out, TopicDir{Topic: e.Name(), Path: dir, Bytes: bytes, ModTime: mod})
	}
	return out, nil
}

// DiskBytes is this topic index's on-disk footprint — the figure that matters
// for the volume, as opposed to ByteSize's stored-bytes estimate which drives
// trimming. The two differ by roughly 3x in practice because scorch defers
// segment reclamation (measured on a staging deployment: 2.79GB on disk under a 1GiB
// stored cap).
func (ix *Indexer) DiskBytes() int64 { return dirBytes(ix.dir) }

// dirStats returns the total regular-file bytes under dir and the newest mtime
// seen (including the directory's own).
func dirStats(dir string) (int64, time.Time) {
	var total int64
	var newest time.Time
	_ = filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error { //nolint:errcheck // best-effort
		if err != nil || info == nil {
			return nil //nolint:nilerr // swallow per-entry errors so one bad file doesn't truncate the total
		}
		if info.Mode().IsRegular() {
			total += info.Size()
		}
		if mt := info.ModTime(); mt.After(newest) {
			newest = mt
		}
		return nil
	})
	return total, newest
}

// dirBytes sums the size of every regular file under dir. The
// Bookkeeper has a ByteSize field but the indexer never updates it, so
// we measure the on-disk footprint directly — matches the per-tier
// total in `k4a state status` and accounts for Bleve segments, the
// bookkeeper file, and any backup-aside copies left from schema bumps.
func dirBytes(dir string) int64 {
	var total int64
	_ = filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error { //nolint:errcheck // best-effort
		if err != nil || info == nil {
			return nil //nolint:nilerr // swallow per-entry errors so one bad file doesn't truncate the total
		}
		if info.Mode().IsRegular() {
			total += info.Size()
		}
		return nil
	})
	return total
}
