// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package command

import (
	"testing"

	"github.com/blairham/k4a/internal/kafka"
)

func TestTopicSizeCmdHelp(t *testing.T) {
	t.Parallel()
	assertNonEmpty(t, (&TopicSizeCmd{}).Help())
}

func TestTopicSizeCmdSynopsis(t *testing.T) {
	t.Parallel()
	assertNonEmpty(t, (&TopicSizeCmd{}).Synopsis())
}

func TestTopicSizeCmdRunBadFlags(t *testing.T) {
	t.Parallel()
	if (&TopicSizeCmd{}).Run([]string{"--invalid-xyz"}) == 0 {
		t.Error("expected non-zero exit for bad flags")
	}
}

func TestTopicSizeCmdRunNoConnection(t *testing.T) {
	t.Parallel()
	code := (&TopicSizeCmd{}).Run([]string{"--config", "/nonexistent/config.yaml"})
	if code == 0 {
		t.Error("expected non-zero exit for missing connection")
	}
}

func TestSortTopicSizesBySize(t *testing.T) {
	t.Parallel()

	sizes := []kafka.TopicSize{
		{Name: "small", TotalBytes: 100},
		{Name: "big", TotalBytes: 900},
		{Name: "mid", TotalBytes: 500},
	}
	sortTopicSizes(sizes, "size")

	want := []string{"big", "mid", "small"}
	for i, w := range want {
		if sizes[i].Name != w {
			t.Errorf("position %d = %q, want %q", i, sizes[i].Name, w)
		}
	}
}

func TestSortTopicSizesBySizeTieBreaksOnName(t *testing.T) {
	t.Parallel()

	sizes := []kafka.TopicSize{
		{Name: "zulu", TotalBytes: 500},
		{Name: "alpha", TotalBytes: 500},
	}
	sortTopicSizes(sizes, "size")

	if sizes[0].Name != "alpha" || sizes[1].Name != "zulu" {
		t.Errorf("equal sizes should tie-break by name asc, got %q then %q", sizes[0].Name, sizes[1].Name)
	}
}

func TestSortTopicSizesByName(t *testing.T) {
	t.Parallel()

	sizes := []kafka.TopicSize{
		{Name: "charlie", TotalBytes: 100},
		{Name: "alpha", TotalBytes: 900},
		{Name: "bravo", TotalBytes: 500},
	}
	sortTopicSizes(sizes, "name")

	want := []string{"alpha", "bravo", "charlie"}
	for i, w := range want {
		if sizes[i].Name != w {
			t.Errorf("position %d = %q, want %q", i, sizes[i].Name, w)
		}
	}
}

func TestTopicMessageCount(t *testing.T) {
	t.Parallel()

	if got := topicMessageCount(-1); got != "-" {
		t.Errorf("not-fetched sentinel = %q, want %q", got, "-")
	}
	if got := topicMessageCount(0); got != "0" {
		t.Errorf("zero count = %q, want %q", got, "0")
	}
	if got := topicMessageCount(1500); got != "1.5K" {
		t.Errorf("1500 = %q, want %q", got, "1.5K")
	}
}
