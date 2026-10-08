// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"strings"
	"testing"
	"time"

	"github.com/blairham/k4a/internal/kafka"
)

func TestParseScopeFlag(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   string
		want kafka.SearchScope
		err  bool
	}{
		{"", kafka.ScopeKey | kafka.ScopeValue, false},
		{"key+value", kafka.ScopeKey | kafka.ScopeValue, false},
		{"KEY", kafka.ScopeKey, false},
		{"value", kafka.ScopeValue, false},
		{"key+value+headers", kafka.ScopeKey | kafka.ScopeValue | kafka.ScopeHeaders, false},
		{"bogus", 0, true},
	}
	for _, tc := range cases {
		got, err := parseScopeFlag(tc.in)
		if (err != nil) != tc.err {
			t.Errorf("parseScopeFlag(%q) err=%v wantErr=%v", tc.in, err, tc.err)
			continue
		}
		if err == nil && got != tc.want {
			t.Errorf("parseScopeFlag(%q) = %d, want %d", tc.in, got, tc.want)
		}
	}
}

func TestParseDurationExt(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   string
		want time.Duration
		err  bool
	}{
		{"5m", 5 * time.Minute, false},
		{"2h", 2 * time.Hour, false},
		{"3d", 3 * 24 * time.Hour, false},
		{"1w", 7 * 24 * time.Hour, false},
		{"bad", 0, true},
		{"xd", 0, true},
	}
	for _, tc := range cases {
		got, err := parseDurationExt(tc.in)
		if (err != nil) != tc.err {
			t.Errorf("parseDurationExt(%q) err=%v wantErr=%v", tc.in, err, tc.err)
			continue
		}
		if err == nil && got != tc.want {
			t.Errorf("parseDurationExt(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestParseTimeFlag(t *testing.T) {
	t.Parallel()

	// Empty → zero time, no error.
	tm, err := parseTimeFlag("")
	if err != nil || !tm.IsZero() {
		t.Errorf("parseTimeFlag(\"\") = (%v, %v), want (zero, nil)", tm, err)
	}

	// Duration form: should be roughly now-24h.
	tm, err = parseTimeFlag("24h")
	if err != nil {
		t.Fatalf("parseTimeFlag(24h): %v", err)
	}
	delta := time.Since(tm) - 24*time.Hour
	if delta > 5*time.Second || delta < -5*time.Second {
		t.Errorf("parseTimeFlag(24h) drifted by %v, want ~0", delta)
	}

	// RFC3339 form: round-trips.
	want := time.Date(2026, 1, 2, 15, 4, 5, 0, time.UTC)
	tm, err = parseTimeFlag(want.Format(time.RFC3339))
	if err != nil || !tm.Equal(want) {
		t.Errorf("parseTimeFlag(RFC3339) = (%v, %v), want (%v, nil)", tm, err, want)
	}

	if _, err := parseTimeFlag("garbage"); err == nil {
		t.Error("parseTimeFlag(garbage) wantErr, got nil")
	}
}

func TestParsePartitionsFlag(t *testing.T) {
	t.Parallel()

	cases := []struct {
		in   string
		want []int32
		err  bool
	}{
		{"", nil, false},
		{"  ", nil, false},
		{"0", []int32{0}, false},
		{"0,1,2", []int32{0, 1, 2}, false},
		{" 0 , 1 ", []int32{0, 1}, false},
		{"0,,1", []int32{0, 1}, false},
		{"a", nil, true},
		{"-1", nil, true},
	}
	for _, tc := range cases {
		got, err := parsePartitionsFlag(tc.in)
		if (err != nil) != tc.err {
			t.Errorf("parsePartitionsFlag(%q) err=%v wantErr=%v", tc.in, err, tc.err)
			continue
		}
		if err == nil && !equalInt32(got, tc.want) {
			t.Errorf("parsePartitionsFlag(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestBuildSearchParamsDefaults(t *testing.T) {
	t.Parallel()

	p, err := buildSearchParams("hello", &SearchFlags{})
	if err != nil {
		t.Fatalf("buildSearchParams: %v", err)
	}
	if p.Pattern == nil {
		t.Fatal("Pattern is nil")
	}
	if !strings.HasPrefix(p.Pattern.String(), "(?i)") {
		t.Errorf("default should be case-insensitive, got %q", p.Pattern.String())
	}
	if p.Scope != kafka.ScopeKey|kafka.ScopeValue {
		t.Errorf("Scope = %d, want key+value", p.Scope)
	}
}

func TestBuildSearchParamsCaseSensitive(t *testing.T) {
	t.Parallel()

	p, err := buildSearchParams("Hello", &SearchFlags{CaseSensitive: true})
	if err != nil {
		t.Fatalf("buildSearchParams: %v", err)
	}
	if strings.HasPrefix(p.Pattern.String(), "(?i)") {
		t.Errorf("case-sensitive flag should suppress (?i), got %q", p.Pattern.String())
	}
}

func TestBuildSearchParamsLiteralFallback(t *testing.T) {
	t.Parallel()

	// Unbalanced bracket — regex compile fails, should fall back to
	// literal substring match.
	p, err := buildSearchParams("foo[bar", &SearchFlags{})
	if err != nil {
		t.Fatalf("buildSearchParams: %v", err)
	}
	if !p.Pattern.MatchString("xx foo[bar yy") {
		t.Error("literal fallback did not match the original substring")
	}
}

func TestBuildSearchParamsEmpty(t *testing.T) {
	t.Parallel()

	if _, err := buildSearchParams("   ", &SearchFlags{}); err == nil {
		t.Error("empty pattern should error")
	}
}

func TestSanitize(t *testing.T) {
	t.Parallel()

	got := sanitize("a\nb\tc")
	if got != "a b c" {
		t.Errorf("sanitize = %q, want %q", got, "a b c")
	}
}

func TestFormatCount(t *testing.T) {
	t.Parallel()

	cases := map[int64]string{
		0:             "0",
		999:           "999",
		1_000:         "1.0K",
		1_500:         "1.5K",
		1_000_000:     "1.0M",
		2_500_000_000: "2.5B",
	}
	for n, want := range cases {
		if got := formatCount(n); got != want {
			t.Errorf("formatCount(%d) = %q, want %q", n, got, want)
		}
	}
}

func equalInt32(a, b []int32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
