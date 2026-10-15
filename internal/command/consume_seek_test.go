// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"testing"
	"time"

	"github.com/blairham/k4a/internal/kafka"
)

func TestConsumeSeek(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 10, 15, 12, 0, 0, 0, time.UTC)
	unset := ConsumeFlags{Offset: -1}

	if _, seeking, err := consumeSeek(unset, false, now); err != nil || seeking {
		t.Errorf("no seek flags: seeking=%t err=%v", seeking, err)
	}

	f := unset
	f.FromBeginning = true
	if s, seeking, err := consumeSeek(f, false, now); err != nil || !seeking || s.Mode != kafka.SeekStart {
		t.Errorf("--from-beginning = %+v %t %v", s, seeking, err)
	}

	f = unset
	f.Offset = 42
	if s, _, err := consumeSeek(f, false, now); err != nil || s.Mode != kafka.SeekOffset || s.Offset != 42 {
		t.Errorf("--offset 42 = %+v %v", s, err)
	}
	f.Offset = 0
	if s, seeking, err := consumeSeek(f, false, now); err != nil || !seeking || s.Offset != 0 {
		t.Errorf("--offset 0 must seek, got %+v %t %v", s, seeking, err)
	}

	f = unset
	f.Since = "2h"
	if s, _, err := consumeSeek(
		f,
		false,
		now,
	); err != nil || s.Mode != kafka.SeekTime ||
		!s.Time.Equal(now.Add(-2*time.Hour)) {
		t.Errorf("--since 2h = %+v %v", s, err)
	}
	f.Since = "2026-10-15T08:00:00Z"
	if s, _, err := consumeSeek(f, false, now); err != nil || s.Time.Hour() != 8 {
		t.Errorf("--since RFC3339 = %+v %v", s, err)
	}

	for name, bad := range map[string]ConsumeFlags{
		"two seeks":       {Offset: 3, Since: "1h"},
		"bad since":       {Offset: -1, Since: "yesterday"},
		"negative since":  {Offset: -1, Since: "-1h"},
		"negative offset": {Offset: -5},
	} {
		if _, _, err := consumeSeek(bad, false, now); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	f = unset
	f.Since = "1h"
	if _, _, err := consumeSeek(f, true, now); err == nil {
		t.Error("a seek with an explicit --group was accepted")
	}
}

// Only a -g typed on the command line counts as choosing a group; the
// "k4a" default must not turn every seek into an error.
func TestParseConsumeArgsGroupSet(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		args []string
		want bool
	}{
		{[]string{"-t", "x", "--since", "1h"}, false},
		{[]string{"-t", "x", "-g", "mine"}, true},
		{[]string{"-t", "x", "--group", "k4a"}, true},
	} {
		_, got, err := parseConsumeArgs(tc.args)
		if err != nil || got != tc.want {
			t.Errorf("parseConsumeArgs(%v) groupSet = %t, %v; want %t", tc.args, got, err, tc.want)
		}
	}
}

func TestValidateConsumePartitionWithSeek(t *testing.T) {
	t.Parallel()
	// A seek sets NoGroup, which is what lets --partition through.
	if err := validateConsumePartition(ConsumeFlags{Partition: 2, NoGroup: true}); err != nil {
		t.Errorf("partition with a seek: %v", err)
	}
}
