// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package main

import "testing"

func TestParseByteSize(t *testing.T) {
	t.Parallel()
	cases := []struct {
		in      string
		want    int64
		wantErr bool
	}{
		{"", 0, false},
		{"0", 0, false},
		{"1024", 1024, false},
		{"1B", 1, false},
		{"2KiB", 2 * 1024, false},
		{"1MiB", 1 << 20, false},
		{"1GiB", 1 << 30, false},
		{"1Gi", 1 << 30, false},           // k8s-style binary suffix
		{"512MB", 512 * 1_000_000, false}, // decimal
		{"3gb", 3 * 1_000_000_000, false}, // case-insensitive
		{"  1GiB  ", 1 << 30, false},      // trimmed
		{"1.5GiB", 0, true},               // no fractional support
		{"bogus", 0, true},
		{"-5", 0, true},
	}
	for _, c := range cases {
		got, err := parseByteSize(c.in)
		if c.wantErr {
			if err == nil {
				t.Errorf("parseByteSize(%q): expected error, got %d", c.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("parseByteSize(%q): unexpected error: %v", c.in, err)
			continue
		}
		if got != c.want {
			t.Errorf("parseByteSize(%q) = %d, want %d", c.in, got, c.want)
		}
	}
}
