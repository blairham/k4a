// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package timebound

import (
	"math/big"
	"strings"
	"testing"
	"time"
)

// FuzzParseDuration checks ParseDuration against an independent oracle. A
// bound arrives from the command line and, through k4a-mcp, from the network,
// so whatever it accepts must be exactly what it says: never negative (a
// lookback into the future silently matches nothing), and for the d and w
// suffixes exactly n days or weeks, computed here in arbitrary precision so an
// overflow cannot hide behind the same int64 arithmetic it would corrupt.
func FuzzParseDuration(f *testing.F) {
	for _, s := range []string{
		"24h", "90m", "1h30m", "0", "0d", "7d", "2w", "+3d", "-1h", "-2d", "-1w",
		"106751d", "106752d", "15250w", "15251w", "9223372036854775807d",
		"d", "w", "1.5d", "", "garbage",
	} {
		f.Add(s)
	}
	f.Fuzz(func(t *testing.T, s string) {
		d, err := ParseDuration(s)
		if err != nil {
			return
		}
		if d < 0 {
			t.Fatalf("ParseDuration(%q) = %v: a lookback cannot be negative", s, d)
		}
		var unit *big.Int
		switch {
		case strings.HasSuffix(s, "d"):
			unit = big.NewInt(int64(24 * time.Hour))
		case strings.HasSuffix(s, "w"):
			unit = big.NewInt(int64(7 * 24 * time.Hour))
		default:
			return // time.ParseDuration's own contract
		}
		n, ok := new(big.Int).SetString(s[:len(s)-1], 10)
		if !ok {
			t.Fatalf("ParseDuration(%q) = %v, but %q is not an integer", s, d, s[:len(s)-1])
		}
		want := new(big.Int).Mul(n, unit)
		if !want.IsInt64() {
			t.Fatalf("ParseDuration(%q) = %v, but %v ns does not fit in a Duration", s, d, want)
		}
		if want.Int64() != int64(d) {
			t.Fatalf("ParseDuration(%q) = %v, want %v", s, d, time.Duration(want.Int64()))
		}
	})
}

// FuzzParse checks that an accepted relative bound never lands after now.
func FuzzParse(f *testing.F) {
	for _, s := range []string{"24h", "7d", "2w", "-1h", "2026-10-07T12:00:00Z", " 3d ", ""} {
		f.Add(s)
	}
	now := time.Date(2026, 10, 7, 12, 0, 0, 0, time.UTC)
	f.Fuzz(func(t *testing.T, s string) {
		got, err := Parse(s, now)
		if err != nil || got.IsZero() {
			return
		}
		if _, derr := ParseDuration(strings.TrimSpace(s)); derr == nil && got.After(now) {
			t.Fatalf("Parse(%q) = %v, after now (%v)", s, got, now)
		}
	})
}
