// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Package upgrade replaces the running k4a with its latest GitHub release,
// verified against the release's checksums.txt.
package upgrade

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"time"

	"golang.org/x/mod/semver"
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
	if resolved, err := filepath.EvalSymlinks(exe); err == nil {
		exe = resolved
	}
	return run(ctx, newClient(), currentVersion, exe, runtime.GOOS, runtime.GOARCH, w)
}

func run(ctx context.Context, c *client, currentVersion, exe, goos, goarch string, w io.Writer) (string, error) {
	fprintln(w, "Checking for updates...")

	latest, err := c.latest(ctx)
	if err != nil {
		return "", fmt.Errorf("failed to check for updates: %w", err)
	}
	if !needsUpdate(currentVersion, latest.Version()) {
		return "", fmt.Errorf("already up to date (%s)", currentVersion)
	}

	name, ok := archiveName(goos, goarch)
	if !ok {
		return "", fmt.Errorf("no release for %s/%s", goos, goarch)
	}
	archiveURL, ok := latest.Assets[name]
	if !ok {
		return "", fmt.Errorf("release %s has no %s", latest.Tag, name)
	}
	sumsURL, ok := latest.Assets[checksumsAsset]
	if !ok {
		return "", fmt.Errorf("release %s has no %s", latest.Tag, checksumsAsset)
	}

	fprintf(w, "Upgrading %s -> %s\n", currentVersion, latest.Version())
	fprintf(w, "Downloading and installing to %s...\n", exe)

	sums, err := c.download(ctx, sumsURL, maxChecksumsBytes)
	if err != nil {
		return "", fmt.Errorf("upgrade failed: %w", err)
	}
	want, err := checksumFor(sums, name)
	if err != nil {
		return "", fmt.Errorf("upgrade failed: %w", err)
	}
	archive, err := c.download(ctx, archiveURL, maxArchiveBytes)
	if err != nil {
		return "", fmt.Errorf("upgrade failed: %w", err)
	}
	if err = verifyChecksum(archive, want); err != nil {
		return "", fmt.Errorf("upgrade failed: %s: %w", name, err)
	}
	bin, err := extractBinary(archive, name, binaryName(goos))
	if err != nil {
		return "", fmt.Errorf("upgrade failed: %w", err)
	}
	if err = replaceExecutable(exe, bin, goos); err != nil {
		return "", fmt.Errorf("upgrade failed: %w", err)
	}

	ver := latest.Version()
	fprintf(w, "Successfully upgraded to %s\n", ver)
	return ver, nil
}

// Check reports whether an update is available without installing it.
// Returns the latest version and whether it is newer than currentVersion.
func Check(ctx context.Context, currentVersion string) (string, bool, error) {
	return check(ctx, newClient(), currentVersion)
}

func check(ctx context.Context, c *client, currentVersion string) (string, bool, error) {
	latest, err := c.latest(ctx)
	if errors.Is(err, errNoRelease) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return latest.Version(), needsUpdate(currentVersion, latest.Version()), nil
}

// needsUpdate returns true when the latest release version is newer than
// current.
func needsUpdate(current, latest string) bool {
	clean, dev := cleanVersion(current)
	if dev {
		return true
	}
	return semver.Compare("v"+latest, "v"+clean) > 0
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
