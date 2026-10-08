// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package tui

import (
	"context"
	"encoding/json"
	"net/http"
	"os"
	"os/exec"
	"strings"
	"time"

	tea "charm.land/bubbletea/v2"
)

func (a *App) checkVersion() tea.Cmd {
	ver := a.version
	return func() tea.Msg {
		latest := fetchLatestRelease()
		if latest == "" {
			// API call failed — for dev builds, still show notice.
			if ver == "dev" || ver == "" {
				return versionCheckMsg{latest: "dev build"}
			}
			return versionCheckMsg{}
		}

		current := strings.TrimPrefix(ver, "v")
		latestClean := strings.TrimPrefix(latest, "v")

		// Strip pre-release/build metadata (e.g. "-dirty", "-rc1") for comparison.
		if idx := strings.IndexByte(current, '-'); idx >= 0 {
			current = current[:idx]
		}

		if ver == "dev" || ver == "" || current > latestClean {
			// Dev build or ahead of release — show the latest release version.
			return versionCheckMsg{latest: latest + " (latest release)"}
		}
		if latestClean != current {
			// Behind — update available.
			return versionCheckMsg{latest: latest}
		}
		return versionCheckMsg{}
	}
}

func fetchLatestRelease() string {
	// Try gh CLI first (handles private repo auth).
	if tag := fetchViaGH(); tag != "" {
		return tag
	}
	return fetchViaAPI()
}

func fetchViaGH() string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	out, err := exec.CommandContext(
		ctx, "gh", "api",
		"repos/blairham/k4a/releases/latest", "--jq", ".tag_name",
	).Output()
	if err != nil {
		return ""
	}
	return strings.TrimSpace(string(out))
}

func fetchViaAPI() string {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(
		ctx,
		http.MethodGet,
		"https://api.github.com/repos/blairham/k4a/releases/latest",
		http.NoBody,
	)
	if err != nil {
		return ""
	}
	req.Header.Set("Accept", "application/vnd.github+json")

	// Use GH_TOKEN or GITHUB_TOKEN if available.
	if token := os.Getenv("GH_TOKEN"); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	} else if token := os.Getenv("GITHUB_TOKEN"); token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil || resp.StatusCode != http.StatusOK {
		return ""
	}
	defer resp.Body.Close() //nolint:errcheck // best-effort cleanup

	var release struct {
		TagName string `json:"tag_name"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&release); err != nil {
		return ""
	}
	return release.TagName
}
