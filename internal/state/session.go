// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package state

import (
	"errors"
	"strings"
	"sync"
)

// sessionSchemaVersion is the on-disk schema version for SessionState.
// Bump when the struct shape changes in a way the old code can't read.
// Old files at a different version are renamed aside and re-created
// fresh — there's no migrator because session state is cheap to rebuild.
const sessionSchemaVersion = 1

// SessionRecentLimit caps how many recent search queries we retain.
// Older entries are dropped FIFO.
const SessionRecentLimit = 20

// SessionState is the Tier 1 state document: things that should survive
// a k4a quit but are trivially rebuildable. Nothing here is required for
// k4a to function — losing it just resets the user to the context
// picker and an empty recent-search list.
type SessionState struct {
	LastContext    string   `json:"last_context,omitempty"`
	RecentSearches []string `json:"recent_searches,omitempty"`
}

// Session is the live, mutable view over the persisted SessionState. It
// owns its own lock so concurrent updates from different goroutines
// (UI thread, search submission, context switch) stay consistent.
//
// All mutations are write-through: after a setter returns, the on-disk
// file reflects the change. We accept the modest disk cost in exchange
// for never losing recent state on a panic.
type Session struct {
	root  *Root
	state SessionState
	mu    sync.Mutex
}

// LoadSession reads ~/.k4a/state.json into a Session. Missing or
// corrupt files yield a fresh zero-value Session — the caller doesn't
// need to distinguish "first run" from "previous run wrote garbage."
func LoadSession(r *Root) *Session {
	s := &Session{root: r}
	var loaded SessionState
	err := Load(r, SubsystemSession, sessionSchemaVersion, &loaded)
	if err == nil {
		s.state = loaded
	} else if !errors.Is(err, ErrNotFound) {
		// We only get here on filesystem errors (permissions, I/O).
		// Treat as "fresh" — the caller can't usefully do anything
		// better and we don't want to fail k4a startup over state.
		s.state = SessionState{}
	}
	return s
}

// Snapshot returns a copy of the current state. Safe to read without
// holding the caller's lock.
func (s *Session) Snapshot() SessionState {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.state
	if len(s.state.RecentSearches) > 0 {
		out.RecentSearches = make([]string, len(s.state.RecentSearches))
		copy(out.RecentSearches, s.state.RecentSearches)
	}
	return out
}

// LastContext returns the most recently active context name, or "" if
// none has been recorded.
func (s *Session) LastContext() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.state.LastContext
}

// SetLastContext records name as the active context and persists. A
// no-op for empty names so a transient ""-context doesn't clobber a
// real one.
func (s *Session) SetLastContext(name string) error {
	if name == "" {
		return nil
	}
	s.mu.Lock()
	if s.state.LastContext == name {
		s.mu.Unlock()
		return nil
	}
	s.state.LastContext = name
	s.mu.Unlock()
	return s.save()
}

// RecentSearches returns the recent-search list, newest first.
func (s *Session) RecentSearches() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.state.RecentSearches) == 0 {
		return nil
	}
	out := make([]string, len(s.state.RecentSearches))
	copy(out, s.state.RecentSearches)
	return out
}

// AddRecentSearch prepends query to the recent-search list, deduping
// any existing entry. Trailing entries past SessionRecentLimit are
// dropped. Whitespace-only queries are ignored.
func (s *Session) AddRecentSearch(query string) error {
	query = strings.TrimSpace(query)
	if query == "" {
		return nil
	}
	s.mu.Lock()
	// Filter out an existing entry that matches; we'll prepend below.
	filtered := s.state.RecentSearches[:0:0]
	for _, q := range s.state.RecentSearches {
		if q != query {
			filtered = append(filtered, q)
		}
	}
	s.state.RecentSearches = append([]string{query}, filtered...)
	if len(s.state.RecentSearches) > SessionRecentLimit {
		s.state.RecentSearches = s.state.RecentSearches[:SessionRecentLimit]
	}
	s.mu.Unlock()
	return s.save()
}

// save persists the current state to disk. Called by every mutator
// after the in-memory state has been updated.
func (s *Session) save() error {
	s.mu.Lock()
	snap := s.state
	if len(s.state.RecentSearches) > 0 {
		snap.RecentSearches = make([]string, len(s.state.RecentSearches))
		copy(snap.RecentSearches, s.state.RecentSearches)
	}
	s.mu.Unlock()
	return Save(s.root, SubsystemSession, sessionSchemaVersion, snap)
}
