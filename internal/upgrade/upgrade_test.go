// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package upgrade

import (
	"bytes"
	"os"
	"path/filepath"
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

func TestInHomebrewKeg(t *testing.T) {
	t.Parallel()

	cases := []struct {
		exe  string
		want bool
	}{
		{"/opt/homebrew/Cellar/k4a/0.0.0/bin/k4a", true},
		{"/usr/local/Cellar/k4a/0.1.2/bin/k4a", true},
		{"/home/linuxbrew/.linuxbrew/Cellar/k4a/0.0.0/bin/k4a", true},
		{"/Users/me/go/bin/k4a", false},
		{"/Users/me/.local/bin/k4a", false},
		// Another formula's keg is not ours.
		{"/opt/homebrew/Cellar/k4a-dev/0.0.0/bin/k4a", false},
		// A directory named Cellar/k4a with nothing below it is not a keg.
		{"/tmp/Cellar/k4a", false},
	}
	for _, tc := range cases {
		if got := inHomebrewKeg(tc.exe); got != tc.want {
			t.Errorf("inHomebrewKeg(%q) = %v, want %v", tc.exe, got, tc.want)
		}
	}
}

// Brew puts a symlink into the keg on PATH; the keg is what decides.
func TestInHomebrewKegFollowsSymlinks(t *testing.T) {
	t.Parallel()

	prefix := t.TempDir()
	keg := filepath.Join(prefix, "Cellar", "k4a", "0.0.0", "bin")
	if err := os.MkdirAll(keg, 0o755); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(keg, "k4a")
	if err := os.WriteFile(target, nil, 0o755); err != nil { //nolint:gosec // test executable
		t.Fatal(err)
	}
	bin := filepath.Join(prefix, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(bin, "k4a")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}

	if !inHomebrewKeg(link) {
		t.Errorf("inHomebrewKeg(%q) = false for a symlink into the keg", link)
	}

	plain := filepath.Join(t.TempDir(), "k4a")
	if err := os.WriteFile(plain, nil, 0o755); err != nil { //nolint:gosec // test executable
		t.Fatal(err)
	}
	if inHomebrewKeg(plain) {
		t.Errorf("inHomebrewKeg(%q) = true for a binary outside any keg", plain)
	}
}
