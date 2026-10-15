// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package searchcache

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"time"
)

const (
	// DefaultMaxDiskBytes caps the total size of ~/.k4a/cache/searches/.
	// A cached search can hold every match of a broad pattern, so without a
	// cap the directory grows for as long as people keep searching.
	DefaultMaxDiskBytes int64 = 256 << 20

	// DefaultMaxAge is how long an entry survives on disk after it was last
	// written or served. Old entries are rarely still valid (any open-ended
	// search goes stale as soon as a partition advances), so keeping them
	// costs disk for answers that would be rejected anyway.
	DefaultMaxAge = 7 * 24 * time.Hour

	// staleTempAge is how old a leftover temp file must be before pruning
	// removes it. A crash between CreateTemp and Rename leaves one behind;
	// the margin keeps a prune from deleting another process's write in
	// flight.
	staleTempAge = time.Hour
)

type diskFile struct {
	mod  time.Time
	path string
	size int64
}

// prune enforces the disk bounds: entries untouched for longer than maxAge
// are removed, then the least recently used entries go until the directory
// fits in maxDiskBytes. Every failure is ignored — the cache is an
// optimization, and a file another process removed first is the same
// outcome as removing it here.
func (c *Cache) prune() {
	if c.root == nil {
		return
	}
	files, total := c.sweep()
	if c.maxDiskBytes <= 0 || total <= c.maxDiskBytes {
		return
	}
	slices.SortFunc(files, func(a, b diskFile) int { return a.mod.Compare(b.mod) })
	for _, f := range files {
		if total <= c.maxDiskBytes {
			break
		}
		if err := os.Remove(f.path); err == nil || os.IsNotExist(err) {
			total -= f.size
		}
	}
}

// sweep removes expired entries and stale temp files, and returns the
// entries that remain with their total size.
func (c *Cache) sweep() (files []diskFile, total int64) {
	dirents, err := os.ReadDir(c.dirPath())
	if err != nil {
		return nil, 0
	}
	now := time.Now()
	files = make([]diskFile, 0, len(dirents))
	for _, de := range dirents {
		info, err := de.Info()
		if err != nil || de.IsDir() {
			continue
		}
		path := filepath.Join(c.dirPath(), de.Name())
		age := now.Sub(info.ModTime())
		switch name := de.Name(); {
		case strings.Contains(name, ".tmp-"):
			if age > staleTempAge {
				_ = os.Remove(path) //nolint:errcheck // best-effort
			}
		case !strings.HasSuffix(name, ".json"):
		case c.maxAge > 0 && age > c.maxAge:
			_ = os.Remove(path) //nolint:errcheck // best-effort
		default:
			files = append(files, diskFile{path: path, size: info.Size(), mod: info.ModTime()})
			total += info.Size()
		}
	}
	return files, total
}

// expired reports whether a disk entry last touched at mod has outlived
// maxAge and must not be served.
func (c *Cache) expired(mod time.Time) bool {
	return c.maxAge > 0 && time.Since(mod) > c.maxAge
}
