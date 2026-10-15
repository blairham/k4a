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

// ErrBadArchive wraps every reason InspectArchive refuses an archive.
var ErrBadArchive = errors.New("not a valid k4a state archive")

// ImportPlan is an archive that has been read end to end and found to hold
// only what an export writes. Nothing is written until Apply.
type ImportPlan struct {
	path string
	// Files are the regular files in archive order, manifest excluded.
	Files []string
	// Topics are the "index/<cluster>/<topic>" the archive replaces.
	Topics   []string
	Manifest Manifest
	Bytes    int64
}

// ImportResult reports what Apply wrote.
type ImportResult struct {
	Files int
	Bytes int64
}

// entryCheck validates one archive at a time, entry by entry.
type entryCheck struct {
	seen  map[string]bool
	count int
}

func badArchive(format string, args ...any) error {
	return fmt.Errorf("%w: %s", ErrBadArchive, fmt.Sprintf(format, args...))
}

// check refuses any entry an export would not have written: a link of
// either kind, a device or FIFO, a name that is absolute, unclean or climbs
// out with "..", a name outside the known parts, or a duplicate. It returns
// the cleaned name and whether the entry is a directory.
func (c *entryCheck) check(hdr *tar.Header) (string, bool, error) {
	c.count++
	if err := checkType(hdr); err != nil {
		return "", false, err
	}
	isDir := hdr.Typeflag == tar.TypeDir
	name, err := localName(hdr.Name, isDir)
	if err != nil {
		return "", false, err
	}
	if c.count == 1 && (name != manifestName || isDir) {
		return "", false, badArchive("first entry is %q, not %s", hdr.Name, manifestName)
	}
	if err := checkPlacement(name, isDir); err != nil {
		return "", false, err
	}
	if c.seen[name] {
		return "", false, badArchive("%q appears twice", name)
	}
	c.seen[name] = true
	return name, isDir, nil
}

func checkType(hdr *tar.Header) error {
	switch hdr.Typeflag {
	case tar.TypeReg, tar.TypeDir:
		return nil
	case tar.TypeSymlink, tar.TypeLink:
		return badArchive("%q is a link; an export holds only files and directories", hdr.Name)
	default:
		return badArchive("%q has unsupported entry type %q", hdr.Name, hdr.Typeflag)
	}
}

// localName returns raw as a clean, relative, slash-separated name that
// stays inside the directory it is extracted into.
func localName(raw string, isDir bool) (string, error) {
	name := raw
	if isDir {
		name = strings.TrimSuffix(name, "/")
	}
	bad := badArchive("%q is not a path inside the state directory", raw)
	if name == "" || strings.HasPrefix(name, "/") || strings.Contains(name, `\`) ||
		path.Clean(name) != name || !filepath.IsLocal(filepath.FromSlash(name)) {
		return "", bad
	}
	for elem := range strings.SplitSeq(name, "/") {
		if elem == ".." || elem == "." {
			return "", bad
		}
	}
	return name, nil
}

// checkPlacement allows only the layout Export writes.
func checkPlacement(name string, isDir bool) error {
	parts := strings.Split(name, "/")
	switch parts[0] {
	case manifestName, configName, sessionName:
		if isDir || len(parts) > 1 {
			return badArchive("%q must be a file", name)
		}
		return nil
	case subdirCache:
		return nil
	case subdirIndex:
		return checkIndexPlacement(name, parts, isDir)
	}
	return badArchive("%q is not part of a k4a state export", name)
}

// checkIndexPlacement allows index/<cluster>/<topic>/... and the
// directories above it, but no writer lock and no stray file.
func checkIndexPlacement(name string, parts []string, isDir bool) error {
	switch {
	case len(parts) <= 3 && isDir:
		return nil
	case len(parts) == 4 && parts[3] == lockName:
		return badArchive("%q is a writer lock, which is never exported", name)
	case len(parts) >= 4:
		return nil
	}
	return badArchive("%q is not inside a topic index", name)
}

// InspectArchive reads the archive at p end to end and validates every
// entry before anything is written, so a bad entry anywhere in it refuses
// the whole import rather than leaving it half applied.
func InspectArchive(p string) (*ImportPlan, error) {
	plan := &ImportPlan{path: p}
	topics := map[string]bool{}
	err := walkArchive(p, func(name string, isDir bool, hdr *tar.Header, r io.Reader) error {
		switch {
		case name == manifestName:
			return plan.readManifest(r, hdr.Size)
		case isDir:
		default:
			plan.Files = append(plan.Files, name)
			plan.Bytes += hdr.Size
		}
		if t := topicOf(name); t != "" {
			topics[t] = true
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	for t := range topics {
		plan.Topics = append(plan.Topics, t)
	}
	sort.Strings(plan.Topics)
	return plan, nil
}

func (p *ImportPlan) readManifest(r io.Reader, size int64) error {
	if size > maxManifestBytes {
		return badArchive("manifest is %d bytes", size)
	}
	if err := json.NewDecoder(io.LimitReader(r, maxManifestBytes)).Decode(&p.Manifest); err != nil {
		return badArchive("reading manifest: %v", err)
	}
	if p.Manifest.Format != ArchiveFormat {
		return badArchive("manifest format is %q, want %q", p.Manifest.Format, ArchiveFormat)
	}
	if p.Manifest.SchemaVersion != archiveSchemaVersion {
		return badArchive("archive schema version %d; this k4a reads %d",
			p.Manifest.SchemaVersion, archiveSchemaVersion)
	}
	return nil
}

// topicOf returns "index/<cluster>/<topic>" for a name inside a topic
// index, or "".
func topicOf(name string) string {
	parts := strings.Split(name, "/")
	if len(parts) < 3 || parts[0] != subdirIndex {
		return ""
	}
	return path.Join(parts[:3]...)
}

// walkArchive opens the tar.gz at p and calls fn with every entry, each
// already validated.
func walkArchive(p string, fn func(name string, isDir bool, hdr *tar.Header, r io.Reader) error) error {
	f, err := os.Open(p) //nolint:gosec // the archive the user named
	if err != nil {
		return err
	}
	defer f.Close() //nolint:errcheck // read-only
	gz, err := gzip.NewReader(f)
	if err != nil {
		return badArchive("%v", err)
	}
	defer gz.Close() //nolint:errcheck // read-only
	tr := tar.NewReader(gz)
	check := &entryCheck{seen: map[string]bool{}}
	for {
		hdr, err := tr.Next()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			return badArchive("%v", err)
		}
		name, isDir, err := check.check(hdr)
		if err != nil {
			return err
		}
		if err := fn(name, isDir, hdr, tr); err != nil {
			return err
		}
	}
	if check.count == 0 {
		return badArchive("archive is empty")
	}
	return nil
}

// Conflicts lists what Apply would replace in r: each file that already
// exists, and each topic index that already holds data (a topic index is
// replaced whole, never merged).
func (p *ImportPlan) Conflicts(r *Root) []string {
	var out []string
	for _, name := range p.Files {
		if topicOf(name) != "" {
			continue
		}
		if _, err := os.Lstat(filepath.Join(r.Path(), filepath.FromSlash(name))); err == nil {
			out = append(out, name)
		}
	}
	for _, t := range p.Topics {
		entries, err := os.ReadDir(filepath.Join(r.Path(), filepath.FromSlash(t)))
		if err == nil && (len(entries) > 1 || len(entries) == 1 && entries[0].Name() != lockName) {
			out = append(out, t)
		}
	}
	return out
}

// Apply writes the plan into r. Every topic index it replaces is taken
// under its writer lock first; if any is held by another process nothing
// is written. Writes go through an os.Root, so neither the archive nor a
// symlink already in the state directory can place a file outside it, and
// each file is written to a temp name and renamed into place, mode 0600.
func (p *ImportPlan) Apply(r *Root) (*ImportResult, error) {
	if err := os.MkdirAll(r.Path(), 0o700); err != nil {
		return nil, fmt.Errorf("creating state dir: %w", err)
	}
	rt, err := os.OpenRoot(r.Path())
	if err != nil {
		return nil, err
	}
	defer rt.Close() //nolint:errcheck // nothing buffered

	locks, err := lockTopics(rt, r, p.Topics)
	defer func() {
		for _, l := range locks {
			_ = l.Release() //nolint:errcheck // closing releases anyway
		}
	}()
	if err != nil {
		return nil, err
	}
	for _, t := range p.Topics {
		if eerr := emptyTopic(rt, t); eerr != nil {
			return nil, eerr
		}
	}

	res := &ImportResult{}
	err = walkArchive(p.path, func(name string, isDir bool, hdr *tar.Header, src io.Reader) error {
		switch {
		case name == manifestName:
			return nil
		case isDir:
			return rt.MkdirAll(name, 0o700)
		}
		if werr := writeEntry(rt, name, src, hdr.Size); werr != nil {
			return fmt.Errorf("writing %s: %w", name, werr)
		}
		res.Files++
		res.Bytes += hdr.Size
		return nil
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func lockTopics(rt *os.Root, r *Root, topics []string) ([]*Lock, error) {
	locks := make([]*Lock, 0, len(topics))
	for _, t := range topics {
		if err := rt.MkdirAll(t, 0o700); err != nil {
			return locks, fmt.Errorf("creating %s: %w", t, err)
		}
		l, err := AcquireWriter(filepath.Join(r.Path(), filepath.FromSlash(t)))
		if errors.Is(err, ErrLocked) {
			return locks, fmt.Errorf("%s is being written by another k4a process; close it and import again: %w",
				t, ErrLocked)
		}
		if err != nil {
			return locks, err
		}
		locks = append(locks, l)
	}
	return locks, nil
}

// emptyTopic removes everything in a topic index but its (held) lock, so
// the imported index replaces it rather than mixing with it.
func emptyTopic(rt *os.Root, t string) error {
	entries, err := fs.ReadDir(rt.FS(), t)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if e.Name() == lockName {
			continue
		}
		if err := rt.RemoveAll(path.Join(t, e.Name())); err != nil {
			return fmt.Errorf("replacing %s: %w", t, err)
		}
	}
	return nil
}

func writeEntry(rt *os.Root, name string, src io.Reader, size int64) error {
	if dir := path.Dir(name); dir != "." {
		if err := rt.MkdirAll(dir, 0o700); err != nil {
			return err
		}
	}
	tmp := fmt.Sprintf("%s.tmp-import-%d", name, time.Now().UnixNano())
	f, err := rt.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	defer rt.Remove(tmp) //nolint:errcheck // gone after a successful Rename
	if _, err := io.CopyN(f, src, size); err != nil {
		f.Close() //nolint:errcheck // already failing
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return rt.Rename(tmp, name)
}
