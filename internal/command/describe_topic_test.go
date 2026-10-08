// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"testing"

	"github.com/blairham/k4a/internal/kafka"
)

func TestDescribeTopicCmdHelp(t *testing.T) {
	t.Parallel()
	assertNonEmpty(t, (&DescribeTopicCmd{}).Help())
}

func TestDescribeTopicCmdSynopsis(t *testing.T) {
	t.Parallel()
	assertNonEmpty(t, (&DescribeTopicCmd{}).Synopsis())
}

func TestDescribeTopicCmdRunBadFlags(t *testing.T) {
	t.Parallel()
	if (&DescribeTopicCmd{}).Run([]string{"--invalid-xyz"}) == 0 {
		t.Error("expected non-zero exit for bad flags")
	}
}

// A missing TOPIC argument must fail before any connection is attempted —
// otherwise the user gets a confusing dial error for a usage mistake.
func TestDescribeTopicCmdRunRequiresTopic(t *testing.T) {
	t.Parallel()
	if (&DescribeTopicCmd{}).Run([]string{"--brokers", "localhost:9092"}) == 0 {
		t.Error("expected non-zero exit when no TOPIC argument is given")
	}
}

func describeTopicEntries() []kafka.ConfigEntry {
	return []kafka.ConfigEntry{
		{Name: "min.insync.replicas", Value: "3", Source: "DYNAMIC_TOPIC"},
		{Name: "retention.ms", Value: "604800000", Source: "DEFAULT", IsDefault: true},
		{Name: "cleanup.policy", Value: "delete", Source: "DEFAULT", IsDefault: true},
	}
}

// The default view is overrides only: that is the question people actually ask
// of a topic, and it keeps a ~30-entry dump from burying the one pinned key.
func TestFilterTopicConfigsOverridesOnly(t *testing.T) {
	t.Parallel()

	rows := filterTopicConfigs("t1", describeTopicEntries(), "", false)
	if len(rows) != 1 {
		t.Fatalf("expected 1 override row, got %d", len(rows))
	}
	if rows[0].Name != "min.insync.replicas" || rows[0].Value != "3" {
		t.Errorf("unexpected row: %+v", rows[0])
	}
	if rows[0].Topic != "t1" {
		t.Errorf("row not tagged with its topic: %+v", rows[0])
	}
}

func TestFilterTopicConfigsAll(t *testing.T) {
	t.Parallel()

	rows := filterTopicConfigs("t1", describeTopicEntries(), "", true)
	if len(rows) != 3 {
		t.Fatalf("expected all 3 rows with --all, got %d", len(rows))
	}
}

// --config-key must win over the overrides-only default, so an inherited value
// can still be compared across topics.
func TestFilterTopicConfigsConfigKeyBeatsOverrideFilter(t *testing.T) {
	t.Parallel()

	rows := filterTopicConfigs("t1", describeTopicEntries(), "retention.ms", false)
	if len(rows) != 1 {
		t.Fatalf("expected 1 row for the named key, got %d", len(rows))
	}
	if rows[0].Name != "retention.ms" || rows[0].Source != "DEFAULT" {
		t.Errorf("expected the inherited key to be reported: %+v", rows[0])
	}
}

func TestFilterTopicConfigsConfigKeyUnknown(t *testing.T) {
	t.Parallel()

	if rows := filterTopicConfigs("t1", describeTopicEntries(), "no.such.key", true); len(rows) != 0 {
		t.Errorf("expected no rows for an unknown key, got %d", len(rows))
	}
}

func TestSortTopicConfigRows(t *testing.T) {
	t.Parallel()

	rows := []topicConfigRow{
		{Topic: "b", Name: "z"},
		{Topic: "a", Name: "z"},
		{Topic: "b", Name: "a"},
		{Topic: "a", Name: "a"},
	}
	sortTopicConfigRows(rows)

	want := []topicConfigRow{
		{Topic: "a", Name: "a"},
		{Topic: "a", Name: "z"},
		{Topic: "b", Name: "a"},
		{Topic: "b", Name: "z"},
	}
	for i := range want {
		if rows[i].Topic != want[i].Topic || rows[i].Name != want[i].Name {
			t.Fatalf("row %d = %s/%s, want %s/%s", i, rows[i].Topic, rows[i].Name, want[i].Topic, want[i].Name)
		}
	}
}
