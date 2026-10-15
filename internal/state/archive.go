// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package state

import (
	"archive/tar"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"
)

// Archive layout (docs/design/state-management.md §6): a gzip-compressed
// tar whose first entry is manifest.json and whose other entries mirror
// their paths under the state root.
const (
	ArchiveFormat        = "k4a-state"
	archiveSchemaVersion = 1
	manifestName         = "manifest.json"
	configName           = "config.yaml"
	sessionName          = "state.json"
	lockName             = "lock"
	subdirCache          = "cache"
	subdirIndex          = "index"
	maxManifestBytes     = 1 << 20
)

// Archive parts, as the manifest lists them.
const (
	PartConfig  = "config"
	PartSession = "session"
	PartCache   = subdirCache
	PartIndex   = subdirIndex
)

// Manifest describes an export. It is the archive's first entry so an
// importer knows what it is reading before it reads anything else.
type Manifest struct {
	CreatedAt       time.Time `json:"created_at"`
	Format          string    `json:"format"`
	K4AVersion      string    `json:"k4a_version,omitempty"`
	Parts           []string  `json:"parts"`
	StrippedSecrets []string  `json:"stripped_secrets,omitempty"`
	SchemaVersion   int       `json:"schema_version"`
	SecretsIncluded bool      `json:"secrets_included"`
}

// ExportOptions selects what Export writes. Config is the config file's
// content as it should appear in the archive (the caller strips secrets);
// nil leaves it out.
type ExportOptions struct {
	Config   []byte
	Manifest Manifest
	Session  bool
	Cache    bool
	Index    bool
}

// ExportResult reports what Export wrote. Locked lists the topic indexes
// ("cluster/topic") left out because another process is writing them.
type ExportResult struct {
	Locked   []string
	Manifest Manifest
	Files    int
	Bytes    int64
}

type archiveWriter struct {
	tw  *tar.Writer
	res *ExportResult
	now time.Time
}

// Export writes the selected parts of r to w as a tar.gz archive. A topic
// index is read under its writer lock so the snapshot is never taken while
// an indexer is changing it; one that is locked is skipped and reported.
func Export(r *Root, w io.Writer, opts ExportOptions) (*ExportResult, error) {
	m := opts.Manifest
	m.Format, m.SchemaVersion = ArchiveFormat, archiveSchemaVersion
	if m.CreatedAt.IsZero() {
		m.CreatedAt = time.Now().UTC()
	}
	m.Parts = exportParts(r, opts)

	gz := gzip.NewWriter(w)
	aw := &archiveWriter{tw: tar.NewWriter(gz), res: &ExportResult{Manifest: m}, now: m.CreatedAt}
	if err := aw.writeAll(r, opts); err != nil {
		return nil, err
	}
	if err := aw.tw.Close(); err != nil {
		return nil, fmt.Errorf("closing archive: %w", err)
	}
	if err := gz.Close(); err != nil {
		return nil, fmt.Errorf("closing archive: %w", err)
	}
	return aw.res, nil
}

// exportParts names the parts the archive will hold, so the manifest does
// not claim a part the source root does not have.
func exportParts(r *Root, opts ExportOptions) []string {
	var parts []string
	if opts.Config != nil {
		parts = append(parts, PartConfig)
	}
	if _, err := os.Stat(r.File(SubsystemSession)); opts.Session && err == nil {
		parts = append(parts, PartSession)
	}
	if opts.Cache {
		parts = append(parts, PartCache)
	}
	if opts.Index {
		parts = append(parts, PartIndex)
	}
	return parts
}

func (aw *archiveWriter) writeAll(r *Root, opts ExportOptions) error {
	body, err := json.MarshalIndent(aw.res.Manifest, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling manifest: %w", err)
	}
	if err := aw.writeFile(manifestName, body, aw.now); err != nil {
		return err
	}
	if opts.Config != nil {
		if err := aw.writeFile(configName, opts.Config, aw.now); err != nil {
			return err
		}
	}
	if opts.Session {
		if err := aw.copyFile(r.Path(), sessionName); err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	if opts.Cache {
		if err := aw.copyTree(r.Path(), subdirCache); err != nil {
			return err
		}
	}
	if opts.Index {
		return aw.copyIndexes(r)
	}
	return nil
}

func (aw *archiveWriter) writeFile(name string, body []byte, mod time.Time) error {
	hdr := &tar.Header{Name: name, Mode: 0o600, Size: int64(len(body)), ModTime: mod, Typeflag: tar.TypeReg}
	if err := aw.tw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("archiving %s: %w", name, err)
	}
	if _, err := aw.tw.Write(body); err != nil {
		return fmt.Errorf("archiving %s: %w", name, err)
	}
	aw.res.Files++
	aw.res.Bytes += int64(len(body))
	return nil
}

// copyFile archives base/rel. Only regular files are taken: a symlink in
// the state root is not followed out of it.
func (aw *archiveWriter) copyFile(base, rel string) error {
	full := filepath.Join(base, filepath.FromSlash(rel))
	info, err := os.Lstat(full)
	if err != nil {
		return err
	}
	if !info.Mode().IsRegular() {
		return nil
	}
	f, err := os.Open(full) //nolint:gosec // a file under the user's own state root
	if err != nil {
		return err
	}
	defer f.Close() //nolint:errcheck // read-only
	hdr := &tar.Header{Name: rel, Mode: 0o600, Size: info.Size(), ModTime: info.ModTime(), Typeflag: tar.TypeReg}
	if werr := aw.tw.WriteHeader(hdr); werr != nil {
		return fmt.Errorf("archiving %s: %w", rel, werr)
	}
	n, err := io.CopyN(aw.tw, f, info.Size())
	if err != nil {
		return fmt.Errorf("archiving %s: %w", rel, err)
	}
	aw.res.Files++
	aw.res.Bytes += n
	return nil
}

// copyTree archives every regular file under base/rel, skipping lock files,
// temp files a crashed write left behind, and renamed-aside backups.
func (aw *archiveWriter) copyTree(base, rel string) error {
	root := filepath.Join(base, filepath.FromSlash(rel))
	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		name := d.Name()
		if p != root && strings.HasPrefix(name, ".") {
			if d.IsDir() {
				return filepath.SkipDir
			}
			return nil
		}
		if !d.Type().IsRegular() || name == lockName || strings.Contains(name, ".tmp-") ||
			strings.Contains(name, ".bak.") {
			return nil
		}
		relPath, err := filepath.Rel(base, p)
		if err != nil {
			return err
		}
		return aw.copyFile(base, filepath.ToSlash(relPath))
	})
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return err
}

// copyIndexes archives each topic index under its own writer lock.
func (aw *archiveWriter) copyIndexes(r *Root) error {
	for _, rel := range topicIndexDirs(r) {
		lock, err := AcquireWriter(filepath.Join(r.Path(), filepath.FromSlash(rel)))
		if errors.Is(err, ErrLocked) {
			aw.res.Locked = append(aw.res.Locked, strings.TrimPrefix(rel, subdirIndex+"/"))
			continue
		}
		if err != nil {
			return err
		}
		err = aw.copyTree(r.Path(), rel)
		_ = lock.Release() //nolint:errcheck // closing releases anyway
		if err != nil {
			return err
		}
	}
	return nil
}

// topicIndexDirs lists "index/<cluster>/<topic>" for every topic index,
// sorted, skipping renamed-aside backups and symlinks.
func topicIndexDirs(r *Root) []string {
	var out []string
	base := r.File(SubsystemIndex)
	for _, cluster := range realDirs(base) {
		for _, topic := range realDirs(filepath.Join(base, cluster)) {
			out = append(out, path.Join(subdirIndex, cluster, topic))
		}
	}
	sort.Strings(out)
	return out
}

func realDirs(dir string) []string {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}
	var out []string
	for _, e := range entries {
		if e.Type().IsDir() && !strings.HasPrefix(e.Name(), ".") {
			out = append(out, e.Name())
		}
	}
	return out
}
