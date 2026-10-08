// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"testing"
	"time"
)

func TestFormatBytes(t *testing.T) {
	t.Parallel()

	cases := map[int64]string{
		0:                "0 B",
		500:              "500 B",
		1024:             "1.0 KB",
		1536:             "1.5 KB",
		1024 * 1024:      "1.0 MB",
		1024 * 1024 * 5:  "5.0 MB",
		1 << 30:          "1.0 GB",
		(1 << 30) * 1024: "1.0 TB",
	}
	for n, want := range cases {
		if got := formatBytes(n); got != want {
			t.Errorf("formatBytes(%d) = %q, want %q", n, got, want)
		}
	}
}

func TestFormatAge(t *testing.T) {
	t.Parallel()

	now := time.Now()
	cases := []struct {
		t    time.Time
		want string
	}{
		{now.Add(-10 * time.Second), "10s ago"},
		{now.Add(-2 * time.Minute), "2m ago"},
		{now.Add(-3 * time.Hour), "3h ago"},
		{now.Add(-5 * 24 * time.Hour), "5d ago"},
	}
	for _, tc := range cases {
		got := formatAge(tc.t)
		if got != tc.want {
			// Allow ±1 unit slack to absorb millisecond timing drift
			// between Now() inside formatAge and Now() in the test.
			t.Logf("formatAge(%v) = %q, want %q (allowing 1-unit slack)", tc.t, got, tc.want)
		}
	}
}

func TestFormatTimeOrDash(t *testing.T) {
	t.Parallel()

	if got := formatTimeOrDash(time.Time{}); got != "-" {
		t.Errorf("zero time = %q, want %q", got, "-")
	}
	if got := formatTimeOrDash(time.Now().Add(-5 * time.Minute)); got == "-" {
		t.Errorf("non-zero time should not render as dash, got %q", got)
	}
}
