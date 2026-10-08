// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Package state implements k4a's local state management.
//
// See docs/design/state-management.md for the architectural rules. The
// short version: every state subsystem above the user-owned config tier
// is independently deletable and rebuildable. The cluster is always the
// source of truth; this package's job is to manage local caches and
// indexes safely (locking, schema versioning, recover-by-wipe).
package state

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"time"
)

// Subsystem identifies a state subsystem. The string form is the
// directory name under the state root (or, for single-file subsystems,
// the file basename without extension).
type Subsystem string

// Known subsystems.
const (
	SubsystemSession Subsystem = "session" // ~/.k4a/state.json
	SubsystemCache   Subsystem = "cache"   // ~/.k4a/cache/
	SubsystemIndex   Subsystem = "index"   // ~/.k4a/index/
)

// envelope wraps every persisted state document with a schema version
// and a timestamp. Loaders check the schema version against the caller's
// expected value; mismatches are handled by rename-and-rebuild.
type envelope struct {
	UpdatedAt     time.Time       `json:"updated_at"`
	Data          json.RawMessage `json:"data"`
	SchemaVersion int             `json:"schema_version"`
}

// Root represents a k4a state directory (~/.k4a by default). All state
// subsystems are anchored to a Root.
type Root struct {
	path string
}

// NewRoot returns a Root rooted at path. The directory is created lazily
// on first write; reads against a non-existent root return a not-found
// sentinel rather than failing.
func NewRoot(path string) *Root {
	return &Root{path: path}
}

// Path returns the state directory path.
func (r *Root) Path() string { return r.path }

// File returns the on-disk path for a single-file subsystem (e.g.,
// SubsystemSession → ~/.k4a/state.json). For directory subsystems the
// returned path is the directory itself.
func (r *Root) File(s Subsystem) string {
	switch s {
	case SubsystemSession:
		return filepath.Join(r.path, "state.json")
	case SubsystemCache:
		return filepath.Join(r.path, "cache")
	case SubsystemIndex:
		return filepath.Join(r.path, "index")
	default:
		return filepath.Join(r.path, string(s))
	}
}

// ErrNotFound is returned by Load when no persisted state exists for a
// subsystem. Callers should treat this as "fresh start, use zero value."
var ErrNotFound = errors.New("state: not found")

// Load reads the JSON envelope for a single-file subsystem, validates
// its schema_version matches the caller's expectation, and unmarshals
// the inner data into v.
//
// On schema mismatch or corruption, the existing file is renamed to
// `<name>.bak.<RFC3339Nano>` and ErrNotFound is returned, signaling
// "rebuild from zero." This is the cardinal rebuild path: state above
// the config tier is always reconstructible.
//
// v must be a pointer.
func Load(r *Root, s Subsystem, expectedVersion int, v any) error {
	path := r.File(s)
	raw, err := os.ReadFile(path) //nolint:gosec // user-owned state dir
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return ErrNotFound
		}
		return fmt.Errorf("reading %s: %w", path, err)
	}

	var env envelope
	if jerr := json.Unmarshal(raw, &env); jerr != nil {
		backupAndRemove(path, "parse")
		return ErrNotFound
	}
	if env.SchemaVersion != expectedVersion {
		backupAndRemove(path, "version")
		return ErrNotFound
	}
	if jerr := json.Unmarshal(env.Data, v); jerr != nil {
		backupAndRemove(path, "data")
		return ErrNotFound
	}
	return nil
}

// Save writes v as a versioned JSON envelope to the subsystem's file.
// The write is atomic: data is written to a temp file in the same
// directory and renamed into place. Intermediate directories are created
// as needed with mode 0700 (user-only access — state may contain
// sensitive cluster data).
func Save(r *Root, s Subsystem, version int, v any) error {
	path := r.File(s)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return fmt.Errorf("creating state dir: %w", err)
	}

	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshaling state data: %w", err)
	}
	env := envelope{
		SchemaVersion: version,
		UpdatedAt:     time.Now().UTC(),
		Data:          data,
	}
	body, err := json.MarshalIndent(env, "", "  ")
	if err != nil {
		return fmt.Errorf("marshaling state envelope: %w", err)
	}

	tmp, err := os.CreateTemp(filepath.Dir(path), filepath.Base(path)+".tmp-*")
	if err != nil {
		return fmt.Errorf("creating temp file: %w", err)
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName) //nolint:errcheck // best-effort cleanup after Rename

	if _, err := tmp.Write(body); err != nil {
		tmp.Close() //nolint:errcheck // already failing
		return fmt.Errorf("writing temp file: %w", err)
	}
	if err := tmp.Chmod(0o600); err != nil {
		tmp.Close() //nolint:errcheck // already failing
		return fmt.Errorf("chmod temp file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("closing temp file: %w", err)
	}
	if err := os.Rename(tmpName, path); err != nil {
		return fmt.Errorf("rename into place: %w", err)
	}
	return nil
}

// Clear removes the on-disk artifacts for a subsystem. Single-file
// subsystems just delete the file; directory subsystems recursively
// remove the directory. Missing artifacts are a no-op.
func Clear(r *Root, s Subsystem) error {
	path := r.File(s)
	info, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("stat %s: %w", path, err)
	}
	if info.IsDir() {
		return os.RemoveAll(path)
	}
	return os.Remove(path)
}

// ClearAll removes every state subsystem under the Root *except* the
// config tier (config.yaml), which is user-owned and must not be
// touched by k4a's state machinery.
func ClearAll(r *Root) error {
	entries, err := os.ReadDir(r.path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("reading state dir: %w", err)
	}
	for _, e := range entries {
		if e.Name() == "config.yaml" {
			continue
		}
		path := filepath.Join(r.path, e.Name())
		if err := os.RemoveAll(path); err != nil {
			return fmt.Errorf("removing %s: %w", path, err)
		}
	}
	return nil
}

// Stat returns size, modification time, and existence for a subsystem.
// Directories are summed recursively. Missing artifacts return ok=false.
//
//nolint:errcheck // best-effort sizing; WalkDir errors are non-fatal
func Stat(r *Root, s Subsystem) (size int64, modTime time.Time, ok bool) {
	path := r.File(s)
	info, err := os.Stat(path)
	if err != nil {
		return 0, time.Time{}, false
	}
	if !info.IsDir() {
		return info.Size(), info.ModTime(), true
	}
	var total int64
	var newest time.Time
	_ = filepath.WalkDir(
		path,
		func(_ string, d os.DirEntry, walkErr error) error { //nolint:errcheck // best-effort sizing; missing entries are fine
			if walkErr != nil {
				return nil //nolint:nilerr // best-effort sizing
			}
			if d.IsDir() {
				return nil
			}
			fi, ierr := d.Info()
			if ierr != nil {
				return nil //nolint:nilerr // best-effort sizing
			}
			total += fi.Size()
			if fi.ModTime().After(newest) {
				newest = fi.ModTime()
			}
			return nil
		},
	)
	if newest.IsZero() {
		newest = info.ModTime()
	}
	return total, newest, true
}

// backupAndRemove renames a corrupt or wrong-version state file to a
// timestamped backup so it survives for post-mortem and removes the
// original. Failures are swallowed — the rebuild path doesn't depend
// on the backup succeeding.
func backupAndRemove(path, reason string) {
	suffix := fmt.Sprintf(".bak.%s.%s", reason, time.Now().UTC().Format("20060102T150405"))
	_ = os.Rename(path, path+suffix) //nolint:errcheck // best-effort
}
