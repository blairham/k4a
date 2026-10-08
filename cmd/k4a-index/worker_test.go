// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDrainStalled(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name       string
		buffered   int
		sinceFlush time.Duration
		stallAfter time.Duration
		want       bool
	}{
		{"wedged: buffer backing up, no progress", 4096, 6 * time.Minute, 5 * time.Minute, true},
		{"healthy: drain flushed recently", 100, 3 * time.Second, 5 * time.Minute, false},
		{"idle: nothing buffered, stale flush is fine", 0, time.Hour, 5 * time.Minute, false},
		{"disabled: zero timeout never trips", 4096, time.Hour, 0, false},
	}
	for _, tc := range cases {
		if got := drainStalled(tc.buffered, tc.sinceFlush, tc.stallAfter); got != tc.want {
			t.Errorf("%s: drainStalled(%d, %s, %s) = %v, want %v",
				tc.name, tc.buffered, tc.sinceFlush, tc.stallAfter, got, tc.want)
		}
	}
}

func TestWatchdogVerdict(t *testing.T) {
	t.Parallel()

	const grace = 30 * time.Minute
	cases := []struct {
		name          string
		stalled       bool
		mergeInFlight bool
		sinceFlush    time.Duration
		mergeGrace    time.Duration
		want          watchdogAction
	}{
		{"healthy drain", false, false, time.Second, grace, watchdogHealthy},
		{"healthy even mid-merge", false, true, time.Second, grace, watchdogHealthy},
		{"genuine stall, no merge", true, false, 6 * time.Minute, grace, watchdogExit},
		{"stall behind a working merge defers", true, true, 6 * time.Minute, grace, watchdogDefer},
		{"merge overran its grace", true, true, 31 * time.Minute, grace, watchdogExit},
		{"grace disabled: merge does not save the stall", true, true, 6 * time.Minute, 0, watchdogExit},
	}
	for _, tc := range cases {
		got := watchdogVerdict(tc.stalled, tc.mergeInFlight, tc.sinceFlush, tc.mergeGrace)
		if got != tc.want {
			t.Errorf("%s: watchdogVerdict(%v, %v, %s, %s) = %v, want %v",
				tc.name, tc.stalled, tc.mergeInFlight, tc.sinceFlush, tc.mergeGrace, got, tc.want)
		}
	}
}

func TestStallStrikes(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	if got := stallStrikes(dir); got != 0 {
		t.Fatalf("fresh dir: stallStrikes = %d, want 0", got)
	}
	if got := bumpStallStrikes(dir); got != 1 {
		t.Fatalf("first bump = %d, want 1", got)
	}
	if got := bumpStallStrikes(dir); got != 2 {
		t.Fatalf("second bump = %d, want 2", got)
	}
	if got := stallStrikes(dir); got != 2 {
		t.Fatalf("read-back = %d, want 2", got)
	}
	resetStallStrikes(dir)
	if got := stallStrikes(dir); got != 0 {
		t.Fatalf("after reset: stallStrikes = %d, want 0", got)
	}
	// A corrupt counter degrades to 0, never to a panic or an instant wipe.
	if err := os.WriteFile(filepath.Join(dir, stallStrikesFile), []byte("bogus"), 0o600); err != nil {
		t.Fatal(err)
	}
	if got := stallStrikes(dir); got != 0 {
		t.Fatalf("corrupt file: stallStrikes = %d, want 0", got)
	}
}

func TestQuarantineIndexDir(t *testing.T) {
	t.Parallel()

	parent := t.TempDir()
	dir := filepath.Join(parent, "some-topic")
	if err := os.MkdirAll(filepath.Join(dir, "bleve"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, stallStrikesFile), []byte("3"), 0o600); err != nil {
		t.Fatal(err)
	}

	quarantineIndexDir(dir, "some-topic")

	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("wedged dir still present after quarantine: %v", err)
	}
	// The aside copy is deleted too — repeated 1GiB quarantines must not fill
	// the emptyDir.
	entries, err := os.ReadDir(parent)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		t.Errorf("unexpected leftover after quarantine: %s", e.Name())
	}
	// The next boot's Open() sees a fresh path — strikes are implicitly 0.
	if got := stallStrikes(dir); got != 0 {
		t.Fatalf("after quarantine: stallStrikes = %d, want 0", got)
	}
}

func TestDumpGoroutines(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	dumpGoroutines(&buf)
	if !bytes.Contains(buf.Bytes(), []byte("goroutine ")) {
		t.Errorf("dump does not look like goroutine stacks:\n%.200s", buf.String())
	}
}
