// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

// Package timebound parses the time bounds a search accepts: a lookback such
// as "24h", "7d" or "2w", or an RFC3339 timestamp. The CLI, `k4a mcp` and
// k4a-mcp all read bounds through it, so the three surfaces agree on what a
// bound means and on what they refuse.
package timebound

import (
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// ParseDuration extends time.ParseDuration with d (days) and w (weeks). A
// bound is a lookback, so a negative duration, which would put the bound in
// the future and silently match nothing, is refused, as is one too long for a
// time.Duration (about 292 years), which would otherwise wrap around.
func ParseDuration(s string) (time.Duration, error) {
	unit := time.Duration(0)
	switch {
	case strings.HasSuffix(s, "d"):
		unit = 24 * time.Hour
	case strings.HasSuffix(s, "w"):
		unit = 7 * 24 * time.Hour
	}
	if unit == 0 {
		d, err := time.ParseDuration(s)
		if err != nil {
			return 0, err
		}
		if d < 0 {
			return 0, fmt.Errorf("duration %q is negative", s)
		}
		return d, nil
	}
	n, err := strconv.ParseInt(s[:len(s)-1], 10, 64)
	if err != nil {
		return 0, err
	}
	if n < 0 {
		return 0, fmt.Errorf("duration %q is negative", s)
	}
	if n > math.MaxInt64/int64(unit) {
		return 0, fmt.Errorf("duration %q is too long", s)
	}
	return time.Duration(n) * unit, nil
}

// Parse reads a bound relative to now: a duration means that long before
// now, anything else must be an RFC3339 timestamp. Empty means no bound and
// returns the zero time.
func Parse(s string, now time.Time) (time.Time, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return time.Time{}, nil
	}
	if d, err := ParseDuration(s); err == nil {
		return now.Add(-d), nil
	}
	if t, err := time.Parse(time.RFC3339, s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("unrecognized time %q (use a duration like 24h or an RFC3339 timestamp)", s)
}
