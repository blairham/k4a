// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Package version resolves build metadata (version, commit, date) for every
// k4a binary from one place.
//
// Values may be injected at build time via ldflags, e.g.
//
//	-ldflags "-X github.com/blairham/k4a/internal/version.Version=v1.2.3"
//
// (GoReleaser, the Makefile, and the Dockerfile all do this). When they are
// NOT injected — `go run`, `go install`, a plain `go build` — they are derived
// dynamically from the Go build info: the module version for an installed
// binary, or the VCS revision/time/dirty stamp for a source build. So every
// binary reports a meaningful version without anyone passing -ldflags.
package version

import "runtime/debug"

// Injected at build time via -ldflags -X. Empty ⇒ use the build-info fallback.
var (
	Version string
	Commit  string
	Date    string
)

// Info is the resolved build metadata.
type Info struct {
	Version string
	Commit  string
	Date    string
}

// Get resolves version/commit/date, preferring ldflags-injected values and
// falling back to Go build info, then to sensible placeholders.
func Get() Info {
	v, c, d := Version, Commit, Date
	modVer, rev, revTime, dirty := buildInfo()

	revStamp := rev
	if rev != "" && dirty {
		revStamp = rev + "-dirty"
	}

	if v == "" || v == "dev" {
		v = firstNonEmpty(modVer, revStamp)
	}
	if c == "" {
		c = revStamp
	}
	if d == "" {
		d = revTime
	}
	return Info{
		Version: orDefault(v, "dev"),
		Commit:  orDefault(c, "none"),
		Date:    orDefault(d, "unknown"),
	}
}

// String returns just the resolved version — the convenience the daemons use.
func String() string { return Get().Version }

// buildInfo pulls the module version and VCS stamp out of the embedded build
// info. Any field is empty when unavailable (e.g. `go run`, or a build with
// -buildvcs=false).
func buildInfo() (modVer, rev, revTime string, dirty bool) {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return "", "", "", false
	}
	if info.Main.Version != "" && info.Main.Version != "(devel)" {
		modVer = info.Main.Version
	}
	for _, s := range info.Settings {
		switch s.Key {
		case "vcs.revision":
			if len(s.Value) >= 7 {
				rev = s.Value[:7]
			}
		case "vcs.time":
			revTime = s.Value
		case "vcs.modified":
			dirty = s.Value == "true"
		}
	}
	return modVer, rev, revTime, dirty
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func orDefault(v, def string) string {
	if v == "" {
		return def
	}
	return v
}
