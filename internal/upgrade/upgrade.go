// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package upgrade

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	selfupdate "github.com/creativeprojects/go-selfupdate"
)

const repo = "blairham/k4a"

// BrewUpgradeCommand is how a Homebrew-installed k4a is upgraded.
const BrewUpgradeCommand = "brew upgrade blairham/tap/k4a"

// ErrHomebrewManaged is returned by Run when the running binary belongs to a
// Homebrew keg. Replacing it in place would leave brew recording the old
// version, and the next `brew upgrade` would swap it back.
var ErrHomebrewManaged = errors.New("k4a is installed by Homebrew; run `" + BrewUpgradeCommand + "` instead")

// HomebrewManaged reports whether the running binary was installed by
// Homebrew.
func HomebrewManaged() bool {
	exe, err := os.Executable()
	if err != nil {
		return false
	}
	return inHomebrewKeg(exe)
}

// Command is the command that upgrades this k4a: brew's for a Homebrew
// install, k4a's own otherwise.
func Command() string {
	if HomebrewManaged() {
		return BrewUpgradeCommand
	}
	return "k4a upgrade"
}

// inHomebrewKeg reports whether exe, once symlinks are resolved, lives in a
// k4a keg: <prefix>/Cellar/k4a/<version>/... Brew's bin/ and opt/ entries are
// symlinks into the keg, so they resolve there too.
func inHomebrewKeg(exe string) bool {
	if resolved, err := filepath.EvalSymlinks(exe); err == nil {
		exe = resolved
	}
	parts := strings.Split(filepath.ToSlash(exe), "/")
	for i := 0; i+2 < len(parts); i++ {
		if parts[i] == "Cellar" && parts[i+1] == "k4a" {
			return true
		}
	}
	return false
}

// Run checks for the latest release, downloads it, and replaces the current
// binary. Progress is written to w. Returns the new version tag on success.
func Run(ctx context.Context, currentVersion string, w io.Writer) (string, error) {
	exe, err := os.Executable()
	if err != nil {
		return "", fmt.Errorf("cannot determine executable path: %w", err)
	}
	if inHomebrewKeg(exe) {
		return "", ErrHomebrewManaged
	}

	fprintln(w, "Checking for updates...")

	updater, err := newUpdater()
	if err != nil {
		return "", fmt.Errorf("failed to initialize updater: %w", err)
	}

	latest, found, err := updater.DetectLatest(ctx, selfupdate.ParseSlug(repo))
	if err != nil {
		return "", fmt.Errorf("failed to check for updates: %w", err)
	}
	if !found {
		return "", fmt.Errorf("no release found for this platform")
	}

	if !needsUpdate(currentVersion, latest) {
		return "", fmt.Errorf("already up to date (%s)", currentVersion)
	}

	fprintf(w, "Upgrading %s -> %s\n", currentVersion, latest.Version())

	fprintf(w, "Downloading and installing to %s...\n", exe)

	if err := updater.UpdateTo(ctx, latest, exe); err != nil {
		return "", fmt.Errorf("upgrade failed: %w", err)
	}

	ver := latest.Version()
	fprintf(w, "Successfully upgraded to %s\n", ver)
	return ver, nil
}

// Check reports whether an update is available without installing it.
// Returns the latest version and whether it is newer than currentVersion.
func Check(ctx context.Context, currentVersion string) (string, bool, error) {
	updater, err := newUpdater()
	if err != nil {
		return "", false, err
	}

	latest, found, err := updater.DetectLatest(ctx, selfupdate.ParseSlug(repo))
	if err != nil {
		return "", false, err
	}
	if !found {
		return "", false, nil
	}

	if !needsUpdate(currentVersion, latest) {
		return latest.Version(), false, nil
	}
	return latest.Version(), true, nil
}

// needsUpdate returns true when the detected release is newer than current.
func needsUpdate(current string, latest *selfupdate.Release) bool {
	clean, dev := cleanVersion(current)
	if dev {
		return true
	}
	return latest.GreaterThan(clean)
}

// cleanVersion strips the "v" prefix and any "-suffix" (e.g. "-dirty",
// "-rc1") from a build version string. Returns dev=true when the input
// is the empty string or "dev" — both of which should always be treated
// as "older than any tagged release."
func cleanVersion(current string) (clean string, dev bool) {
	if current == "dev" || current == "" {
		return "", true
	}
	clean = strings.TrimPrefix(current, "v")
	if idx := strings.IndexByte(clean, '-'); idx >= 0 {
		clean = clean[:idx]
	}
	return clean, false
}

func newUpdater() (*selfupdate.Updater, error) {
	token := resolveGitHubToken()

	source, err := selfupdate.NewGitHubSource(selfupdate.GitHubConfig{
		APIToken: token,
	})
	if err != nil {
		return nil, err
	}

	return selfupdate.NewUpdater(selfupdate.Config{
		Source:    source,
		Validator: &selfupdate.ChecksumValidator{UniqueFilename: "checksums.txt"},
	})
}

func resolveGitHubToken() string {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if out, err := exec.CommandContext(ctx, "gh", "auth", "token").Output(); err == nil {
		if token := strings.TrimSpace(string(out)); token != "" {
			return token
		}
	}

	return envGitHubToken()
}

// envGitHubToken returns the first non-empty GitHub token found in the
// environment, preferring GH_TOKEN (the `gh` CLI convention) over the
// older GITHUB_TOKEN. Returns "" when neither is set.
func envGitHubToken() string {
	if token := os.Getenv("GH_TOKEN"); token != "" {
		return token
	}
	return os.Getenv("GITHUB_TOKEN")
}

// fprintf is a best-effort write to the progress writer.
func fprintf(w io.Writer, format string, a ...any) {
	fmt.Fprintf(w, format, a...) //nolint:errcheck // best-effort progress output
}

// fprintln is a best-effort write to the progress writer.
func fprintln(w io.Writer, a ...any) {
	fmt.Fprintln(w, a...) //nolint:errcheck // best-effort progress output
}
