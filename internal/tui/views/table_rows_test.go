// SPDX-FileCopyrightText: 2026 Blair Hamilton
// SPDX-License-Identifier: Apache-2.0

package views

import (
	"testing"

	"charm.land/bubbles/v2/table"

	"github.com/blairham/k4a/internal/kafka"
)

func reproTopics(names ...string) []kafka.TopicInfo {
	out := make([]kafka.TopicInfo, 0, len(names))
	for _, n := range names {
		out = append(out, kafka.TopicInfo{Name: n, Partitions: 3, Messages: 10})
	}
	return out
}

// An empty row set clamps the bubbles cursor to -1. setTableRows must restore
// it when rows come back, because bubbles' own clamp is one-directional.
func TestSetTableRows_RestoresCursorAfterEmptyRowSet(t *testing.T) {
	tbl := table.New(table.WithColumns([]table.Column{{Title: "NAME", Width: 10}}))
	rows := []table.Row{{"a"}, {"b"}, {"c"}}

	setTableRows(&tbl, rows)
	if got := tbl.Cursor(); got != 0 {
		t.Fatalf("cursor after initial rows = %d, want 0", got)
	}

	setTableRows(&tbl, nil)
	if tbl.SelectedRow() != nil {
		t.Fatalf("SelectedRow on an empty table should be nil")
	}

	setTableRows(&tbl, rows)
	if got := tbl.Cursor(); got < 0 {
		t.Fatalf("cursor stayed poisoned at %d after rows returned", got)
	}
	if tbl.SelectedRow() == nil {
		t.Fatal("SelectedRow is nil after rows returned — row actions are dead")
	}
}

// setTableRows must not move a cursor the user has already placed.
func TestSetTableRows_PreservesExistingCursor(t *testing.T) {
	tbl := table.New(table.WithColumns([]table.Column{{Title: "NAME", Width: 10}}))
	rows := []table.Row{{"a"}, {"b"}, {"c"}}
	setTableRows(&tbl, rows)
	tbl.SetCursor(2)

	setTableRows(&tbl, rows)
	if got := tbl.Cursor(); got != 2 {
		t.Fatalf("cursor = %d, want 2 (unchanged)", got)
	}
}

// The end-to-end symptom: a filter that matches nothing used to leave the
// topics list permanently unselectable, so enter no longer opened Messages.
func TestTopicsView_EnterWorksAfterAnEmptyFilter(t *testing.T) {
	v := NewTopicsView(nil)
	v.Update(TopicsRefreshMsg{Topics: reproTopics("alpha", "beta", "gamma")})

	if action, _ := v.HandleKey(KeyEnter); action != "messages" {
		t.Fatalf("baseline enter = %q, want %q", action, "messages")
	}

	v.SetFilter("no-such-topic")
	if len(v.table.Rows()) != 0 {
		t.Fatalf("filter should have matched nothing, got %d rows", len(v.table.Rows()))
	}

	v.SetFilter("")
	action, param := v.HandleKey(KeyEnter)
	if action != "messages" {
		t.Fatalf("enter after clearing an empty filter = %q, want %q", action, "messages")
	}
	if param != "alpha" {
		t.Fatalf("enter selected %q, want %q", param, "alpha")
	}
}
