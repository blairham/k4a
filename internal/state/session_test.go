// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package state

import (
	"os"
	"slices"
	"testing"
)

func TestLoadSession_Empty(t *testing.T) {
	t.Parallel()

	s := LoadSession(NewRoot(t.TempDir()))
	if s.LastContext() != "" {
		t.Errorf("LastContext = %q, want empty", s.LastContext())
	}
	if r := s.RecentSearches(); len(r) != 0 {
		t.Errorf("RecentSearches = %v, want empty", r)
	}
}

func TestSession_SetLastContext_PersistsAcrossReload(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s := LoadSession(NewRoot(dir))
	if err := s.SetLastContext("staging"); err != nil {
		t.Fatalf("SetLastContext: %v", err)
	}

	s2 := LoadSession(NewRoot(dir))
	if got := s2.LastContext(); got != "staging" {
		t.Errorf("reloaded LastContext = %q, want staging", got)
	}
}

func TestSession_SetLastContext_EmptyIsNoOp(t *testing.T) {
	t.Parallel()

	s := LoadSession(NewRoot(t.TempDir()))
	_ = s.SetLastContext("staging")
	_ = s.SetLastContext("") // shouldn't clobber
	if got := s.LastContext(); got != "staging" {
		t.Errorf("LastContext = %q, want staging", got)
	}
}

func TestSession_AddRecentSearch_NewestFirstAndDedup(t *testing.T) {
	t.Parallel()

	s := LoadSession(NewRoot(t.TempDir()))
	_ = s.AddRecentSearch("foo")
	_ = s.AddRecentSearch("bar")
	_ = s.AddRecentSearch("foo") // duplicate, should move foo to front

	got := s.RecentSearches()
	want := []string{"foo", "bar"}
	if !slices.Equal(got, want) {
		t.Errorf("RecentSearches = %v, want %v", got, want)
	}
}

func TestSession_AddRecentSearch_RespectsLimit(t *testing.T) {
	t.Parallel()

	s := LoadSession(NewRoot(t.TempDir()))
	for i := range SessionRecentLimit + 5 {
		_ = s.AddRecentSearch(string(rune('a' + i)))
	}
	if got := len(s.RecentSearches()); got != SessionRecentLimit {
		t.Errorf("len(RecentSearches) = %d, want %d", got, SessionRecentLimit)
	}
}

func TestSession_AddRecentSearch_IgnoresWhitespace(t *testing.T) {
	t.Parallel()

	s := LoadSession(NewRoot(t.TempDir()))
	_ = s.AddRecentSearch("")
	_ = s.AddRecentSearch("   ")
	_ = s.AddRecentSearch("\t\n")
	if got := s.RecentSearches(); len(got) != 0 {
		t.Errorf("whitespace queries were retained: %v", got)
	}
}

func TestSession_AddRecentSearch_TrimsWhitespace(t *testing.T) {
	t.Parallel()

	s := LoadSession(NewRoot(t.TempDir()))
	_ = s.AddRecentSearch("  foo  ")
	if got := s.RecentSearches(); len(got) != 1 || got[0] != "foo" {
		t.Errorf("RecentSearches = %v, want [foo]", got)
	}
}

func TestSession_PersistsRecentSearches(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	s := LoadSession(NewRoot(dir))
	_ = s.AddRecentSearch("first")
	_ = s.AddRecentSearch("second")

	s2 := LoadSession(NewRoot(dir))
	want := []string{"second", "first"}
	if got := s2.RecentSearches(); !slices.Equal(got, want) {
		t.Errorf("reloaded RecentSearches = %v, want %v", got, want)
	}
}

func TestSession_SnapshotIsDeepCopy(t *testing.T) {
	t.Parallel()

	s := LoadSession(NewRoot(t.TempDir()))
	_ = s.AddRecentSearch("foo")
	snap := s.Snapshot()
	snap.RecentSearches[0] = "modified"

	if got := s.RecentSearches(); got[0] != "foo" {
		t.Errorf("internal state mutated by snapshot caller: %v", got)
	}
}

func TestLoadSession_CorruptFileResetsToFresh(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	r := NewRoot(dir)
	if err := os.WriteFile(r.File(SubsystemSession), []byte("garbage"), 0o600); err != nil {
		t.Fatal(err)
	}

	s := LoadSession(r)
	if s.LastContext() != "" || len(s.RecentSearches()) != 0 {
		t.Errorf("expected fresh session after corrupt file, got %+v", s.Snapshot())
	}
}
