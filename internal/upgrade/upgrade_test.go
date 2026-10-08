// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package upgrade

import (
	"bytes"
	"testing"
)

func TestCleanVersion(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in        string
		wantClean string
		wantDev   bool
	}{
		{"", "", true},
		{"dev", "", true},
		{"v1.2.3", "1.2.3", false},
		{"1.2.3", "1.2.3", false},
		{"v1.2.3-dirty", "1.2.3", false},
		{"v1.2.3-rc1", "1.2.3", false},
		{"v0.0.0-23-ged635d6-dirty", "0.0.0", false},
		// A pre-release without a v prefix is still cleaned at the first dash.
		{"1.2.3-beta.1", "1.2.3", false},
	}
	for _, tc := range cases {
		gotClean, gotDev := cleanVersion(tc.in)
		if gotClean != tc.wantClean || gotDev != tc.wantDev {
			t.Errorf("cleanVersion(%q) = (%q, %v), want (%q, %v)",
				tc.in, gotClean, gotDev, tc.wantClean, tc.wantDev)
		}
	}
}

func TestEnvGitHubTokenPrecedence(t *testing.T) {
	// Cannot run in parallel — mutates process env.
	t.Setenv("GH_TOKEN", "from-gh")
	t.Setenv("GITHUB_TOKEN", "from-github")

	if got := envGitHubToken(); got != "from-gh" {
		t.Errorf("GH_TOKEN should win, got %q", got)
	}
}

func TestEnvGitHubTokenFallback(t *testing.T) {
	t.Setenv("GH_TOKEN", "")
	t.Setenv("GITHUB_TOKEN", "from-github")

	if got := envGitHubToken(); got != "from-github" {
		t.Errorf("GITHUB_TOKEN fallback, got %q", got)
	}
}

func TestEnvGitHubTokenEmpty(t *testing.T) {
	t.Setenv("GH_TOKEN", "")
	t.Setenv("GITHUB_TOKEN", "")

	if got := envGitHubToken(); got != "" {
		t.Errorf("both unset should yield empty string, got %q", got)
	}
}

func TestProgressWriters(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	fprintln(&buf, "hello", "world")
	fprintf(&buf, "%d items\n", 7)

	got := buf.String()
	want := "hello world\n7 items\n"
	if got != want {
		t.Errorf("progress output = %q, want %q", got, want)
	}
}

func TestNewUpdater(t *testing.T) {
	t.Parallel()

	// Sanity: the constructor wires both source and validator with no
	// panics on the happy path. This is the closest we can get to
	// testing newUpdater without a network round-trip.
	u, err := newUpdater()
	if err != nil {
		t.Fatalf("newUpdater: %v", err)
	}
	if u == nil {
		t.Fatal("newUpdater returned nil updater")
	}
}
