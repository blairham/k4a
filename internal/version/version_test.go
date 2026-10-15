// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package version

import (
	"runtime/debug"
	"testing"
)

// These tests swap package state (the ldflags vars and readBuildInfo), so
// none of them run in parallel.

func setLdflags(t *testing.T, v, c, d string) {
	t.Helper()
	ov, oc, od := Version, Commit, Date
	Version, Commit, Date = v, c, d
	t.Cleanup(func() { Version, Commit, Date = ov, oc, od })
}

func setBuildInfo(t *testing.T, info *debug.BuildInfo) {
	t.Helper()
	orig := readBuildInfo
	readBuildInfo = func() (*debug.BuildInfo, bool) { return info, info != nil }
	t.Cleanup(func() { readBuildInfo = orig })
}

func vcsBuild(modVer, rev, when, modified string) *debug.BuildInfo {
	return &debug.BuildInfo{
		Main: debug.Module{Path: "github.com/blairham/k4a", Version: modVer},
		Settings: []debug.BuildSetting{
			{Key: "vcs.revision", Value: rev},
			{Key: "vcs.time", Value: when},
			{Key: "vcs.modified", Value: modified},
		},
	}
}

const rev = "0123456789abcdef0123456789abcdef01234567"

func TestGet(t *testing.T) {
	cases := []struct {
		info    *debug.BuildInfo
		name    string
		ldflags [3]string
		want    Info
	}{
		{
			name:    "ldflags win over build info",
			ldflags: [3]string{"v1.2.3", "feedbee", "2026-01-02T03:04:05Z"},
			info:    vcsBuild("v9.9.9", rev, "2020-01-01T00:00:00Z", "true"),
			want:    Info{"v1.2.3", "feedbee", "2026-01-02T03:04:05Z"},
		},
		{
			name: "go install: module version, VCS stamp for commit and date",
			info: vcsBuild("v0.4.0", rev, "2026-10-01T12:00:00Z", "false"),
			want: Info{"v0.4.0", "0123456", "2026-10-01T12:00:00Z"},
		},
		{
			name: "source build: (devel) falls back to the short revision",
			info: vcsBuild("(devel)", rev, "2026-10-01T12:00:00Z", "false"),
			want: Info{"0123456", "0123456", "2026-10-01T12:00:00Z"},
		},
		{
			name: "dirty source build is marked",
			info: vcsBuild("(devel)", rev, "2026-10-01T12:00:00Z", "true"),
			want: Info{"0123456-dirty", "0123456-dirty", "2026-10-01T12:00:00Z"},
		},
		{
			// The Makefile's "dev" placeholder must not hide a real version.
			name:    "ldflags dev is a placeholder, not a version",
			ldflags: [3]string{"dev", "", ""},
			info:    vcsBuild("v0.4.0", rev, "2026-10-01T12:00:00Z", "false"),
			want:    Info{"v0.4.0", "0123456", "2026-10-01T12:00:00Z"},
		},
		{
			name:    "partial ldflags: only the missing fields come from build info",
			ldflags: [3]string{"", "abcdef0", ""},
			info:    vcsBuild("v0.4.0", rev, "2026-10-01T12:00:00Z", "false"),
			want:    Info{"v0.4.0", "abcdef0", "2026-10-01T12:00:00Z"},
		},
		{
			name: "no build info at all: placeholders",
			want: Info{"dev", "none", "unknown"},
		},
		{
			name: "-buildvcs=false: no stamp, placeholders",
			info: &debug.BuildInfo{Main: debug.Module{Version: "(devel)"}},
			want: Info{"dev", "none", "unknown"},
		},
		{
			name: "a short revision is ignored rather than sliced",
			info: vcsBuild("(devel)", "abc", "", "false"),
			want: Info{"dev", "none", "unknown"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			setLdflags(t, tc.ldflags[0], tc.ldflags[1], tc.ldflags[2])
			setBuildInfo(t, tc.info)
			if got := Get(); got != tc.want {
				t.Errorf("Get() = %+v, want %+v", got, tc.want)
			}
			if got := String(); got != tc.want.Version {
				t.Errorf("String() = %q, want %q", got, tc.want.Version)
			}
		})
	}
}
