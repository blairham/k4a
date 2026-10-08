// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package state

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

type sample struct {
	LastContext    string   `json:"last_context"`
	RecentSearches []string `json:"recent_searches"`
}

func TestSaveLoad_RoundTrip(t *testing.T) {
	t.Parallel()

	r := NewRoot(t.TempDir())
	in := sample{LastContext: "staging", RecentSearches: []string{"foo", "bar"}}

	if err := Save(r, SubsystemSession, 1, in); err != nil {
		t.Fatalf("Save: %v", err)
	}

	var out sample
	if err := Load(r, SubsystemSession, 1, &out); err != nil {
		t.Fatalf("Load: %v", err)
	}
	if out.LastContext != in.LastContext {
		t.Errorf("LastContext = %q, want %q", out.LastContext, in.LastContext)
	}
	if len(out.RecentSearches) != 2 || out.RecentSearches[0] != "foo" {
		t.Errorf("RecentSearches = %v, want [foo bar]", out.RecentSearches)
	}
}

func TestLoad_Missing(t *testing.T) {
	t.Parallel()

	r := NewRoot(t.TempDir())
	var out sample
	err := Load(r, SubsystemSession, 1, &out)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Load missing = %v, want ErrNotFound", err)
	}
}

func TestLoad_CorruptJSON_RebuildsAndBacksUp(t *testing.T) {
	t.Parallel()

	r := NewRoot(t.TempDir())
	path := r.File(SubsystemSession)
	if err := os.WriteFile(path, []byte("not json"), 0o600); err != nil {
		t.Fatal(err)
	}

	var out sample
	err := Load(r, SubsystemSession, 1, &out)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Load corrupt = %v, want ErrNotFound", err)
	}

	// Original file should be moved aside, not retained at its old path.
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("expected original path to be moved aside, stat err = %v", statErr)
	}

	// A backup with the .bak. infix should exist alongside.
	matches, _ := filepath.Glob(filepath.Join(filepath.Dir(path), "state.json.bak.*"))
	if len(matches) == 0 {
		t.Errorf("expected a backup file, none found")
	}
}

func TestLoad_VersionMismatch_RebuildsAndBacksUp(t *testing.T) {
	t.Parallel()

	r := NewRoot(t.TempDir())
	if err := Save(r, SubsystemSession, 1, sample{LastContext: "old"}); err != nil {
		t.Fatalf("Save v1: %v", err)
	}

	// Reading with a different expected version triggers rebuild.
	var out sample
	err := Load(r, SubsystemSession, 2, &out)
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("Load v2-expecting-v1 = %v, want ErrNotFound", err)
	}

	path := r.File(SubsystemSession)
	if _, statErr := os.Stat(path); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("expected original path to be moved aside")
	}
	matches, _ := filepath.Glob(filepath.Join(filepath.Dir(path), "state.json.bak.*"))
	if len(matches) == 0 {
		t.Errorf("expected a backup file, none found")
	}
}

func TestSave_AtomicTempFileCleanup(t *testing.T) {
	t.Parallel()

	r := NewRoot(t.TempDir())
	if err := Save(r, SubsystemSession, 1, sample{LastContext: "a"}); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// No leftover .tmp- files alongside state.json.
	matches, _ := filepath.Glob(filepath.Join(r.Path(), "state.json.tmp-*"))
	if len(matches) != 0 {
		t.Errorf("expected no temp leftovers, got %v", matches)
	}
}

func TestSave_FileModeUserOnly(t *testing.T) {
	t.Parallel()

	r := NewRoot(t.TempDir())
	if err := Save(r, SubsystemSession, 1, sample{}); err != nil {
		t.Fatalf("Save: %v", err)
	}
	info, err := os.Stat(r.File(SubsystemSession))
	if err != nil {
		t.Fatal(err)
	}
	if mode := info.Mode().Perm(); mode != 0o600 {
		t.Errorf("file mode = %o, want 0600", mode)
	}
}

func TestClear_FileSubsystem(t *testing.T) {
	t.Parallel()

	r := NewRoot(t.TempDir())
	if err := Save(r, SubsystemSession, 1, sample{}); err != nil {
		t.Fatal(err)
	}
	if err := Clear(r, SubsystemSession); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if _, err := os.Stat(r.File(SubsystemSession)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("expected file removed, stat err = %v", err)
	}
}

func TestClear_DirSubsystem(t *testing.T) {
	t.Parallel()

	r := NewRoot(t.TempDir())
	dir := r.File(SubsystemCache)
	if err := os.MkdirAll(filepath.Join(dir, "inner"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "inner", "x"), []byte("hi"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := Clear(r, SubsystemCache); err != nil {
		t.Fatalf("Clear: %v", err)
	}
	if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("expected dir removed, stat err = %v", err)
	}
}

func TestClear_Missing(t *testing.T) {
	t.Parallel()

	r := NewRoot(t.TempDir())
	if err := Clear(r, SubsystemSession); err != nil {
		t.Errorf("Clear missing returned %v, want nil", err)
	}
}

func TestClearAll_PreservesConfig(t *testing.T) {
	t.Parallel()

	root := t.TempDir()
	r := NewRoot(root)
	if err := Save(r, SubsystemSession, 1, sample{}); err != nil {
		t.Fatal(err)
	}
	cfgPath := filepath.Join(root, "config.yaml")
	if err := os.WriteFile(cfgPath, []byte("preserved: true\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := ClearAll(r); err != nil {
		t.Fatalf("ClearAll: %v", err)
	}

	if _, err := os.Stat(r.File(SubsystemSession)); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("expected session removed, stat err = %v", err)
	}
	if _, err := os.Stat(cfgPath); err != nil {
		t.Errorf("config.yaml should survive ClearAll, stat err = %v", err)
	}
}

func TestStat_FileSubsystem(t *testing.T) {
	t.Parallel()

	r := NewRoot(t.TempDir())
	if err := Save(r, SubsystemSession, 1, sample{LastContext: "x"}); err != nil {
		t.Fatal(err)
	}
	size, modTime, ok := Stat(r, SubsystemSession)
	if !ok {
		t.Fatal("Stat ok=false, want true")
	}
	if size <= 0 {
		t.Errorf("size = %d, want > 0", size)
	}
	if modTime.IsZero() {
		t.Errorf("modTime is zero")
	}
}

func TestStat_DirSubsystem_RecursiveSize(t *testing.T) {
	t.Parallel()

	r := NewRoot(t.TempDir())
	dir := r.File(SubsystemCache)
	if err := os.MkdirAll(filepath.Join(dir, "a"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "a", "f1"), []byte("hello"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "f2"), []byte("world"), 0o600); err != nil {
		t.Fatal(err)
	}

	size, _, ok := Stat(r, SubsystemCache)
	if !ok {
		t.Fatal("Stat ok=false, want true")
	}
	if size != int64(len("hello")+len("world")) {
		t.Errorf("recursive size = %d, want %d", size, len("hello")+len("world"))
	}
}

func TestStat_Missing(t *testing.T) {
	t.Parallel()

	r := NewRoot(t.TempDir())
	_, _, ok := Stat(r, SubsystemSession)
	if ok {
		t.Error("Stat ok=true, want false for missing file")
	}
}

func TestEnvelope_RoundTripsThroughJSON(t *testing.T) {
	t.Parallel()
	// Belt-and-suspenders: the envelope struct must marshal cleanly so
	// that Load can read what Save writes regardless of Go map ordering.
	body, err := json.Marshal(envelope{SchemaVersion: 7})
	if err != nil {
		t.Fatal(err)
	}
	var env envelope
	if err := json.Unmarshal(body, &env); err != nil {
		t.Fatal(err)
	}
	if env.SchemaVersion != 7 {
		t.Errorf("SchemaVersion = %d, want 7", env.SchemaVersion)
	}
}
