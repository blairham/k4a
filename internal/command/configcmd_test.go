// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"testing"

	"github.com/blairham/k4a/internal/config"
)

func TestParseBool(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   string
		want bool
		err  bool
	}{
		{"true", true, false},
		{"FALSE", false, false},
		{"yes", true, false},
		{"on", true, false},
		{"no", false, false},
		{"off", false, false},
		{"1", true, false},
		{"0", false, false},
		{"  TRUE  ", true, false},
		{"maybe", false, true},
		{"", false, true},
	}
	for _, tc := range cases {
		got, err := parseBool(tc.in)
		if (err != nil) != tc.err {
			t.Errorf("parseBool(%q) err=%v wantErr=%v", tc.in, err, tc.err)
			continue
		}
		if err == nil && got != tc.want {
			t.Errorf("parseBool(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestConfigGet(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{}
	cfg.UI.Logoless = true
	cfg.UI.Readonly = false
	cfg.Index.Enabled = true

	cases := map[string]string{
		"ui.logoless":   "true",
		"UI.Readonly":   "false",
		"index.enabled": "true",
	}
	for key, want := range cases {
		got, err := configGet(cfg, key)
		if err != nil {
			t.Errorf("configGet(%q) err=%v", key, err)
			continue
		}
		if got != want {
			t.Errorf("configGet(%q) = %q, want %q", key, got, want)
		}
	}

	if _, err := configGet(cfg, "unknown.key"); err == nil {
		t.Error("configGet(unknown.key) should error")
	}
}

func TestConfigSet(t *testing.T) {
	t.Parallel()

	cfg := &config.Config{}
	if err := configSet(cfg, "ui.logoless", "yes"); err != nil {
		t.Fatalf("set ui.logoless: %v", err)
	}
	if !cfg.UI.Logoless {
		t.Error("ui.logoless not set to true")
	}

	if err := configSet(cfg, "index.enabled", "false"); err != nil {
		t.Fatalf("set index.enabled: %v", err)
	}
	if cfg.Index.Enabled {
		t.Error("index.enabled not set to false")
	}

	if err := configSet(cfg, "ui.readonly", "garbage"); err == nil {
		t.Error("invalid bool should error")
	}
	if err := configSet(cfg, "unknown.key", "true"); err == nil {
		t.Error("unknown key should error")
	}
}
